#![cfg(test)]
use network::rpc::methods::*;
use network::{BehaviourEvent, Libp2pEvent, ReportSource, Request, Response};
use ssz_types::VariableList;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::time::sleep;
use tracing::{debug, warn};
use tracing_test::traced_test;

mod common;

// Tests the STATUS RPC message
#[test]
#[traced_test]
#[allow(clippy::single_match)]
fn test_status_rpc() {
    let rt = Arc::new(Runtime::new().unwrap());

    rt.block_on(async {
        // get sender/receiver
        let (mut sender, mut receiver) = common::build_node_pair(Arc::downgrade(&rt)).await;

        // Dummy STATUS RPC message
        let rpc_request = Request::Status(StatusMessage { data: 2 });

        // Dummy STATUS RPC message
        let rpc_response = Response::Status(StatusMessage { data: 3 });

        // build the sender future
        let sender_future = async {
            loop {
                match sender.next_event().await {
                    Libp2pEvent::Behaviour(BehaviourEvent::PeerConnectedOutgoing(peer_id)) => {
                        // Send a STATUS message
                        debug!("Sending RPC");
                        sender
                            .swarm
                            .behaviour_mut()
                            .send_request(peer_id, 10, rpc_request.clone());
                    }
                    Libp2pEvent::Behaviour(BehaviourEvent::ResponseReceived {
                        peer_id: _,
                        id: 10,
                        response,
                    }) => {
                        // Should receive the RPC response
                        debug!("Sender Received");
                        assert_eq!(response, rpc_response.clone());
                        debug!("Sender Completed");
                        return;
                    }
                    _ => {}
                }
            }
        };

        // build the receiver future
        let receiver_future = async {
            loop {
                match receiver.next_event().await {
                    Libp2pEvent::Behaviour(BehaviourEvent::RequestReceived {
                        peer_id,
                        id,
                        request,
                    }) => {
                        if request == rpc_request {
                            // send the response
                            debug!("Receiver Received");
                            receiver.swarm.behaviour_mut().send_successful_response(
                                peer_id,
                                id,
                                rpc_response.clone(),
                            );
                        }
                    }
                    _ => {} // Ignore other events
                }
            }
        };

        tokio::select! {
            _ = sender_future => {}
            _ = receiver_future => {}
            _ = sleep(Duration::from_secs(30)) => {
                panic!("Future timed out");
            }
        }
    })
}

// Tests a streamed DataByHash RPC Message
#[test]
#[traced_test]
#[allow(clippy::single_match)]
fn test_data_by_hash_chunked_rpc() {
    let messages_to_send = 6;
    let rt = Arc::new(Runtime::new().unwrap());

    rt.block_on(async {
        // get sender/receiver
        let (mut sender, mut receiver) = common::build_node_pair(Arc::downgrade(&rt)).await;

        // DataByHash Request
        let rpc_request = Request::DataByHash(DataByHashRequest {
            hashes: VariableList::from(vec![
                Hash256::from_low_u64_be(0),
                Hash256::from_low_u64_be(0),
                Hash256::from_low_u64_be(0),
                Hash256::from_low_u64_be(0),
                Hash256::from_low_u64_be(0),
                Hash256::from_low_u64_be(0),
            ]),
        });

        // DataByHash Response
        let data = ZgsData {
            hash: Hash256::from_low_u64_be(0),
        };
        let rpc_response = Response::DataByHash(Some(Box::new(data)));

        // keep count of the number of messages received
        let mut messages_received = 0;
        let request_id = messages_to_send as usize;

        // build the sender future
        let sender_future = async {
            loop {
                match sender.next_event().await {
                    Libp2pEvent::Behaviour(BehaviourEvent::PeerConnectedOutgoing(peer_id)) => {
                        // Send a DATA_BY_HASH message
                        debug!("Sending RPC");
                        sender.swarm.behaviour_mut().send_request(
                            peer_id,
                            request_id,
                            rpc_request.clone(),
                        );
                    }
                    Libp2pEvent::Behaviour(BehaviourEvent::ResponseReceived {
                        peer_id: _,
                        id: _,
                        response,
                    }) => {
                        warn!("Sender received a response: {:?}", &response);
                        match response {
                            Response::DataByHash(Some(_)) => {
                                assert_eq!(response, rpc_response.clone());
                                messages_received += 1;
                                warn!("Chunk received");
                            }
                            Response::DataByHash(None) => {
                                // should be exactly `messages_to_send` messages before terminating
                                assert_eq!(messages_received, messages_to_send);
                                // end the test
                                return;
                            }
                            _ => panic!("Invalid RPC received"),
                        }
                    }
                    _ => {} // Ignore other behaviour events
                }
            }
        };

        // build the receiver future
        let receiver_future = async {
            loop {
                match receiver.next_event().await {
                    Libp2pEvent::Behaviour(BehaviourEvent::RequestReceived {
                        peer_id,
                        id,
                        request,
                    }) => {
                        if request == rpc_request {
                            // send the response
                            warn!("Receiver got request");
                            for _ in 0..messages_to_send {
                                let rpc_response = rpc_response.clone();

                                receiver.swarm.behaviour_mut().send_successful_response(
                                    peer_id,
                                    id,
                                    rpc_response.clone(),
                                );
                            }
                            // send the stream termination
                            receiver.swarm.behaviour_mut().send_successful_response(
                                peer_id,
                                id,
                                Response::DataByHash(None),
                            );
                        }
                    }
                    _ => {} // Ignore other events
                }
            }
        };

        tokio::select! {
            _ = sender_future => {}
            _ = receiver_future => {}
            _ = sleep(Duration::from_secs(30)) => {
                    panic!("Future timed out");
            }
        }
    })
}

// Tests that a streamed DataByHash RPC Message terminates when all expected chunks were received
#[test]
#[traced_test]
fn test_data_by_hash_chunked_rpc_terminates_correctly() {
    let messages_to_send = 10;
    let extra_messages_to_send = 10;
    let rt = Arc::new(Runtime::new().unwrap());

    rt.block_on(async {
        // get sender/receiver
        let (mut sender, mut receiver) = common::build_node_pair(Arc::downgrade(&rt)).await;

        // DataByHash Request
        let rpc_request = Request::DataByHash(DataByHashRequest {
            hashes: VariableList::from(
                (0..messages_to_send)
                    .map(|_| Hash256::from_low_u64_be(0))
                    .collect::<Vec<_>>(),
            ),
        });

        // DataByHash Response
        let data = ZgsData {
            hash: Hash256::from_low_u64_be(0),
        };
        let rpc_response = Response::DataByHash(Some(Box::new(data)));

        // keep count of the number of messages received
        let mut messages_received: u64 = 0;
        let request_id = messages_to_send as usize;
        // build the sender future
        let sender_future = async {
            loop {
                match sender.next_event().await {
                    Libp2pEvent::Behaviour(BehaviourEvent::PeerConnectedOutgoing(peer_id)) => {
                        // Send a STATUS message
                        debug!("Sending RPC");
                        sender.swarm.behaviour_mut().send_request(
                            peer_id,
                            request_id,
                            rpc_request.clone(),
                        );
                    }
                    Libp2pEvent::Behaviour(BehaviourEvent::ResponseReceived {
                        peer_id: _,
                        id: _,
                        response,
                    }) =>
                    // Should receive the RPC response
                    {
                        debug!("Sender received a response");
                        match response {
                            Response::DataByHash(Some(_)) => {
                                assert_eq!(response, rpc_response.clone());
                                messages_received += 1;
                            }
                            Response::DataByHash(None) => {
                                // should be exactly 10 messages, as requested
                                assert_eq!(messages_received, messages_to_send);
                            }
                            _ => panic!("Invalid RPC received"),
                        }
                    }

                    _ => {} // Ignore other behaviour events
                }
            }
        };

        // determine messages to send (PeerId, RequestId). If some, indicates we still need to send
        // messages
        let mut message_info = None;
        // the number of messages we've sent
        let mut messages_sent = 0;
        let receiver_future = async {
            loop {
                // this future either drives the sending/receiving or times out allowing messages to be
                // sent in the timeout
                match futures::future::select(
                    Box::pin(receiver.next_event()),
                    Box::pin(tokio::time::sleep(Duration::from_secs(1))),
                )
                .await
                {
                    futures::future::Either::Left((
                        Libp2pEvent::Behaviour(BehaviourEvent::RequestReceived {
                            peer_id,
                            id,
                            request,
                        }),
                        _,
                    )) => {
                        if request == rpc_request {
                            // send the response
                            warn!("Receiver got request");
                            message_info = Some((peer_id, id));
                        }
                    }
                    futures::future::Either::Right((_, _)) => {} // The timeout hit, send messages if required
                    _ => continue,
                }

                // if we need to send messages send them here. This will happen after a delay
                if message_info.is_some() {
                    messages_sent += 1;
                    let (peer_id, stream_id) = message_info.as_ref().unwrap();
                    receiver.swarm.behaviour_mut().send_successful_response(
                        *peer_id,
                        *stream_id,
                        rpc_response.clone(),
                    );
                    debug!("Sending message {}", messages_sent);
                    if messages_sent == messages_to_send + extra_messages_to_send {
                        // stop sending messages
                        return;
                    }
                }
            }
        };

        tokio::select! {
            _ = sender_future => {}
            _ = receiver_future => {}
            _ = sleep(Duration::from_secs(30)) => {
                panic!("Future timed out");
            }
        }
    })
}

// Tests a Goodbye RPC message
#[test]
#[traced_test]
#[allow(clippy::single_match)]
fn test_goodbye_rpc() {
    let rt = Arc::new(Runtime::new().unwrap());

    rt.block_on(async {
        let (mut sender, mut receiver) = common::build_node_pair(Arc::downgrade(&rt)).await;

        // build the sender future
        let sender_future = async {
            loop {
                match sender.next_event().await {
                    Libp2pEvent::Behaviour(BehaviourEvent::PeerConnectedOutgoing(peer_id)) => {
                        // Send a goodbye and disconnect
                        debug!("Sending RPC");
                        sender.swarm.behaviour_mut().goodbye_peer(
                            &peer_id,
                            GoodbyeReason::IrrelevantNetwork,
                            ReportSource::SyncService,
                        );
                    }
                    Libp2pEvent::Behaviour(BehaviourEvent::PeerDisconnected(_)) => {
                        return;
                    }
                    _ => {} // Ignore other RPC messages
                }
            }
        };

        // build the receiver future
        let receiver_future = async {
            loop {
                match receiver.next_event().await {
                    Libp2pEvent::Behaviour(BehaviourEvent::PeerDisconnected(_)) => {
                        // Should receive sent RPC request
                        return;
                    }
                    _ => {} // Ignore other events
                }
            }
        };

        let total_future = futures::future::join(sender_future, receiver_future);

        tokio::select! {
            _ = total_future => {}
            _ = sleep(Duration::from_secs(30)) => {
                panic!("Future timed out");
            }
        }
    })
}
