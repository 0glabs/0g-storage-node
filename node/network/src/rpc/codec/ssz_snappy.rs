use crate::rpc::methods::*;
use crate::rpc::{
    codec::base::OutboundCodec,
    protocol::{Encoding, Protocol, ProtocolId, RPCError, Version, ERROR_TYPE_MAX, ERROR_TYPE_MIN},
};
use crate::rpc::{InboundRequest, OutboundRequest, RPCCodedResponse, RPCResponse};
use libp2p::bytes::BytesMut;
use shared_types::ChunkArrayWithProof;
use snap::read::FrameDecoder;
use snap::write::FrameEncoder;
use ssz::{Decode, Encode};
use ssz_types::VariableList;
use std::io::Cursor;
use std::io::ErrorKind;
use std::io::{Read, Write};
use tokio_util::codec::{Decoder, Encoder};
use unsigned_varint::codec::Uvi;

/* Inbound Codec */

pub struct SSZSnappyInboundCodec {
    protocol: ProtocolId,
    inner: Uvi<usize>,
    len: Option<usize>,
    /// Maximum bytes that can be sent in one req/resp chunked responses.
    max_packet_size: usize,
}

impl SSZSnappyInboundCodec {
    pub fn new(protocol: ProtocolId, max_packet_size: usize) -> Self {
        let uvi_codec = Uvi::default();
        // this encoding only applies to ssz_snappy.
        debug_assert_eq!(protocol.encoding, Encoding::SSZSnappy);

        SSZSnappyInboundCodec {
            inner: uvi_codec,
            protocol,
            len: None,
            max_packet_size,
        }
    }
}

// Encoder for inbound streams: Encodes RPC Responses sent to peers.
impl Encoder<RPCCodedResponse> for SSZSnappyInboundCodec {
    type Error = RPCError;

    fn encode(&mut self, item: RPCCodedResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = match &item {
            RPCCodedResponse::Success(resp) => match &resp {
                RPCResponse::Status(res) => res.as_ssz_bytes(),
                RPCResponse::Pong(res) => res.data.as_ssz_bytes(),
                RPCResponse::DataByHash(res) => res.as_ssz_bytes(),
                RPCResponse::Chunks(res) => res.as_ssz_bytes(),
            },
            RPCCodedResponse::Error(_, err) => err.as_ssz_bytes(),
            RPCCodedResponse::StreamTermination(_) => {
                unreachable!("Code error - attempting to encode a stream termination")
            }
        };
        // SSZ encoded bytes should be within `max_packet_size`
        if bytes.len() > self.max_packet_size {
            return Err(RPCError::InternalError(
                "attempting to encode data > max_packet_size",
            ));
        }

        // Inserts the length prefix of the uncompressed bytes into dst
        // encoded as a unsigned varint
        self.inner
            .encode(bytes.len(), dst)
            .map_err(RPCError::from)?;

        let mut writer = FrameEncoder::new(Vec::new());
        writer.write_all(&bytes).map_err(RPCError::from)?;
        writer.flush().map_err(RPCError::from)?;

        // Write compressed bytes to `dst`
        dst.extend_from_slice(writer.get_ref());
        Ok(())
    }
}

// Decoder for inbound streams: Decodes RPC requests from peers
impl Decoder for SSZSnappyInboundCodec {
    type Item = InboundRequest;
    type Error = RPCError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let length = match handle_length(&mut self.inner, &mut self.len, src)? {
            Some(len) => len,
            None => return Ok(None),
        };

        // Should not attempt to decode rpc chunks with `length > max_packet_size` or not within bounds of
        // packet size for ssz container corresponding to `self.protocol`.
        let ssz_limits = self.protocol.rpc_request_limits();
        if ssz_limits.is_out_of_bounds(length, self.max_packet_size) {
            return Err(RPCError::InvalidData(format!(
                "RPC request length is out of bounds, length {} bounds = {:?}, protocol = {:?}",
                length, ssz_limits, self.protocol,
            )));
        }
        // Calculate worst case compression length for given uncompressed length
        let max_compressed_len = snap::raw::max_compress_len(length) as u64;

        // Create a limit reader as a wrapper that reads only upto `max_compressed_len` from `src`.
        let limit_reader = Cursor::new(src.as_ref()).take(max_compressed_len);
        let mut reader = FrameDecoder::new(limit_reader);
        let mut decoded_buffer = vec![0; length];

        match reader.read_exact(&mut decoded_buffer) {
            Ok(()) => {
                // `n` is how many bytes the reader read in the compressed stream
                let n = reader.get_ref().get_ref().position();
                self.len = None;
                let _read_bytes = src.split_to(n as usize);

                match self.protocol.version {
                    Version::V1 => handle_v1_request(self.protocol.message_name, &decoded_buffer),
                }
            }
            Err(e) => handle_error(e, reader.get_ref().get_ref().position(), max_compressed_len),
        }
    }
}

/* Outbound Codec: Codec for initiating RPC requests */
pub struct SSZSnappyOutboundCodec {
    inner: Uvi<usize>,
    len: Option<usize>,
    protocol: ProtocolId,
    /// Maximum bytes that can be sent in one req/resp chunked responses.
    max_packet_size: usize,
}

impl SSZSnappyOutboundCodec {
    pub fn new(protocol: ProtocolId, max_packet_size: usize) -> Self {
        let uvi_codec = Uvi::default();
        // this encoding only applies to ssz_snappy.
        debug_assert_eq!(protocol.encoding, Encoding::SSZSnappy);

        SSZSnappyOutboundCodec {
            inner: uvi_codec,
            protocol,
            max_packet_size,
            len: None,
        }
    }
}

// Encoder for outbound streams: Encodes RPC Requests to peers
impl Encoder<OutboundRequest> for SSZSnappyOutboundCodec {
    type Error = RPCError;

    fn encode(&mut self, item: OutboundRequest, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = match item {
            OutboundRequest::Status(req) => req.as_ssz_bytes(),
            OutboundRequest::Goodbye(req) => req.as_ssz_bytes(),
            OutboundRequest::Ping(req) => req.as_ssz_bytes(),
            OutboundRequest::DataByHash(req) => req.hashes.as_ssz_bytes(),
            OutboundRequest::GetChunks(req) => req.as_ssz_bytes(),
        };
        // SSZ encoded bytes should be within `max_packet_size`
        if bytes.len() > self.max_packet_size {
            return Err(RPCError::InternalError(
                "attempting to encode data > max_packet_size",
            ));
        }

        // Inserts the length prefix of the uncompressed bytes into dst
        // encoded as a unsigned varint
        self.inner
            .encode(bytes.len(), dst)
            .map_err(RPCError::from)?;

        let mut writer = FrameEncoder::new(Vec::new());
        writer.write_all(&bytes).map_err(RPCError::from)?;
        writer.flush().map_err(RPCError::from)?;

        // Write compressed bytes to `dst`
        dst.extend_from_slice(writer.get_ref());
        Ok(())
    }
}

// Decoder for outbound streams: Decodes RPC responses from peers.
//
// The majority of the decoding has now been pushed upstream due to the changing specification.
// We prefer to decode blocks and attestations with extra knowledge about the chain to perform
// faster verification checks before decoding entire blocks/attestations.
impl Decoder for SSZSnappyOutboundCodec {
    type Item = RPCResponse;
    type Error = RPCError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let length = match handle_length(&mut self.inner, &mut self.len, src)? {
            Some(len) => len,
            None => return Ok(None),
        };

        // Should not attempt to decode rpc chunks with `length > max_packet_size` or not within bounds of
        // packet size for ssz container corresponding to `self.protocol`.
        let ssz_limits = self.protocol.rpc_response_limits();

        if ssz_limits.is_out_of_bounds(length, self.max_packet_size) {
            return Err(RPCError::InvalidData(format!(
                "RPC response length is out of bounds, length {}",
                length,
            )));
        }
        // Calculate worst case compression length for given uncompressed length
        let max_compressed_len = snap::raw::max_compress_len(length) as u64;
        // Create a limit reader as a wrapper that reads only upto `max_compressed_len` from `src`.
        let limit_reader = Cursor::new(src.as_ref()).take(max_compressed_len);
        let mut reader = FrameDecoder::new(limit_reader);

        let mut decoded_buffer = vec![0; length];

        match reader.read_exact(&mut decoded_buffer) {
            Ok(()) => {
                // `n` is how many bytes the reader read in the compressed stream
                let n = reader.get_ref().get_ref().position();
                self.len = None;
                let _read_bytes = src.split_to(n as usize);

                match self.protocol.version {
                    Version::V1 => handle_v1_response(self.protocol.message_name, &decoded_buffer),
                }
            }
            Err(e) => handle_error(e, reader.get_ref().get_ref().position(), max_compressed_len),
        }
    }
}

impl OutboundCodec<OutboundRequest> for SSZSnappyOutboundCodec {
    type CodecErrorType = ErrorType;

    fn decode_error(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<Self::CodecErrorType>, RPCError> {
        let length = match handle_length(&mut self.inner, &mut self.len, src)? {
            Some(len) => len,
            None => return Ok(None),
        };

        // Should not attempt to decode rpc chunks with `length > max_packet_size` or not within bounds of
        // packet size for ssz container corresponding to `ErrorType`.
        if length > self.max_packet_size || length > *ERROR_TYPE_MAX || length < *ERROR_TYPE_MIN {
            return Err(RPCError::InvalidData(format!(
                "RPC Error length is out of bounds, length {}",
                length
            )));
        }

        // Calculate worst case compression length for given uncompressed length
        let max_compressed_len = snap::raw::max_compress_len(length) as u64;
        // Create a limit reader as a wrapper that reads only upto `max_compressed_len` from `src`.
        let limit_reader = Cursor::new(src.as_ref()).take(max_compressed_len);
        let mut reader = FrameDecoder::new(limit_reader);
        let mut decoded_buffer = vec![0; length];
        match reader.read_exact(&mut decoded_buffer) {
            Ok(()) => {
                // `n` is how many bytes the reader read in the compressed stream
                let n = reader.get_ref().get_ref().position();
                self.len = None;
                let _read_bytes = src.split_to(n as usize);
                Ok(Some(ErrorType(VariableList::from_ssz_bytes(
                    &decoded_buffer,
                )?)))
            }
            Err(e) => handle_error(e, reader.get_ref().get_ref().position(), max_compressed_len),
        }
    }
}

/// Handle errors that we get from decoding an RPC message from the stream.
/// `num_bytes_read` is the number of bytes the snappy decoder has read from the underlying stream.
/// `max_compressed_len` is the maximum compressed size for a given uncompressed size.
fn handle_error<T>(
    err: std::io::Error,
    num_bytes: u64,
    max_compressed_len: u64,
) -> Result<Option<T>, RPCError> {
    match err.kind() {
        ErrorKind::UnexpectedEof => {
            // If snappy has read `max_compressed_len` from underlying stream and still can't fill buffer, we have a malicious message.
            // Report as `InvalidData` so that malicious peer gets banned.
            if num_bytes >= max_compressed_len {
                Err(RPCError::InvalidData(format!(
                    "Received malicious snappy message, num_bytes {}, max_compressed_len {}",
                    num_bytes, max_compressed_len
                )))
            } else {
                // Haven't received enough bytes to decode yet, wait for more
                Ok(None)
            }
        }
        _ => Err(err).map_err(RPCError::from),
    }
}

/// Decodes the length-prefix from the bytes as an unsigned protobuf varint.
///
/// Returns `Ok(Some(length))` by decoding the bytes if required.
/// Returns `Ok(None)` if more bytes are needed to decode the length-prefix.
/// Returns an `RPCError` for a decoding error.
fn handle_length(
    uvi_codec: &mut Uvi<usize>,
    len: &mut Option<usize>,
    bytes: &mut BytesMut,
) -> Result<Option<usize>, RPCError> {
    if let Some(length) = len {
        Ok(Some(*length))
    } else {
        // Decode the length of the uncompressed bytes from an unsigned varint
        // Note: length-prefix of > 10 bytes(uint64) would be a decoding error
        match uvi_codec.decode(bytes).map_err(RPCError::from)? {
            Some(length) => {
                *len = Some(length);
                Ok(Some(length))
            }
            None => Ok(None), // need more bytes to decode length
        }
    }
}

/// Decodes a `Version::V1` `InboundRequest` from the byte stream.
/// `decoded_buffer` should be an ssz-encoded bytestream with
// length = length-prefix received in the beginning of the stream.
fn handle_v1_request(
    protocol: Protocol,
    decoded_buffer: &[u8],
) -> Result<Option<InboundRequest>, RPCError> {
    match protocol {
        Protocol::Status => Ok(Some(InboundRequest::Status(StatusMessage::from_ssz_bytes(
            decoded_buffer,
        )?))),
        Protocol::Goodbye => Ok(Some(InboundRequest::Goodbye(
            GoodbyeReason::from_ssz_bytes(decoded_buffer)?,
        ))),
        Protocol::Ping => Ok(Some(InboundRequest::Ping(Ping {
            data: u64::from_ssz_bytes(decoded_buffer)?,
        }))),
        Protocol::DataByHash => Ok(Some(InboundRequest::DataByHash(DataByHashRequest {
            hashes: VariableList::from_ssz_bytes(decoded_buffer)?,
        }))),
        Protocol::GetChunks => Ok(Some(InboundRequest::GetChunks(
            GetChunksRequest::from_ssz_bytes(decoded_buffer)?,
        ))),
    }
}

/// Decodes a `Version::V1` `RPCResponse` from the byte stream.
/// `decoded_buffer` should be an ssz-encoded bytestream with
// length = length-prefix received in the beginning of the stream.
fn handle_v1_response(
    protocol: Protocol,
    decoded_buffer: &[u8],
) -> Result<Option<RPCResponse>, RPCError> {
    match protocol {
        Protocol::Status => Ok(Some(RPCResponse::Status(StatusMessage::from_ssz_bytes(
            decoded_buffer,
        )?))),
        // This case should be unreachable as `Goodbye` has no response.
        Protocol::Goodbye => Err(RPCError::InvalidData(
            "Goodbye RPC message has no valid response".to_string(),
        )),
        Protocol::Ping => Ok(Some(RPCResponse::Pong(Ping {
            data: u64::from_ssz_bytes(decoded_buffer)?,
        }))),
        Protocol::DataByHash => Ok(Some(RPCResponse::DataByHash(Box::new(
            ZgsData::from_ssz_bytes(decoded_buffer)?,
        )))),
        Protocol::GetChunks => Ok(Some(RPCResponse::Chunks(
            ChunkArrayWithProof::from_ssz_bytes(decoded_buffer)?,
        ))),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::rpc::protocol::*;
    use crate::rpc::{methods::StatusMessage, Ping};

    use snap::write::FrameEncoder;
    use ssz::Encode;
    use std::io::Write;

    fn status_message() -> StatusMessage {
        StatusMessage { data: 1 }
    }

    fn ping_message() -> Ping {
        Ping { data: 1 }
    }

    /// Encodes the given protocol response as bytes.
    fn encode(
        protocol: Protocol,
        version: Version,
        message: RPCCodedResponse,
    ) -> Result<BytesMut, RPCError> {
        let snappy_protocol_id = ProtocolId::new(protocol, version, Encoding::SSZSnappy);
        let max_packet_size = max_rpc_size();

        let mut buf = BytesMut::new();
        let mut snappy_inbound_codec =
            SSZSnappyInboundCodec::new(snappy_protocol_id, max_packet_size);

        snappy_inbound_codec.encode(message, &mut buf)?;
        Ok(buf)
    }

    // fn encode_without_length_checks(
    //     bytes: Vec<u8>,
    // ) -> Result<BytesMut, RPCError> {
    //     let mut dst = BytesMut::new();

    //     let mut uvi_codec: Uvi<usize> = Uvi::default();

    //     // Inserts the length prefix of the uncompressed bytes into dst
    //     // encoded as a unsigned varint
    //     uvi_codec
    //         .encode(bytes.len(), &mut dst)
    //         .map_err(RPCError::from)?;

    //     let mut writer = FrameEncoder::new(Vec::new());
    //     writer.write_all(&bytes).map_err(RPCError::from)?;
    //     writer.flush().map_err(RPCError::from)?;

    //     // Write compressed bytes to `dst`
    //     dst.extend_from_slice(writer.get_ref());

    //     Ok(dst)
    // }

    /// Attempts to decode the given protocol bytes as an rpc response
    fn decode(
        protocol: Protocol,
        version: Version,
        message: &mut BytesMut,
    ) -> Result<Option<RPCResponse>, RPCError> {
        let snappy_protocol_id = ProtocolId::new(protocol, version, Encoding::SSZSnappy);
        let max_packet_size = max_rpc_size();
        let mut snappy_outbound_codec =
            SSZSnappyOutboundCodec::new(snappy_protocol_id, max_packet_size);
        // decode message just as snappy message
        snappy_outbound_codec.decode(message)
    }

    /// Encodes the provided protocol message as bytes and tries to decode the encoding bytes.
    fn encode_then_decode(
        protocol: Protocol,
        version: Version,
        message: RPCCodedResponse,
    ) -> Result<Option<RPCResponse>, RPCError> {
        let mut encoded = encode(protocol, version.clone(), message)?;
        decode(protocol, version, &mut encoded)
    }

    // Test RPCResponse encoding/decoding for V1 messages
    #[test]
    fn test_encode_then_decode_v1() {
        assert_eq!(
            encode_then_decode(
                Protocol::Status,
                Version::V1,
                RPCCodedResponse::Success(RPCResponse::Status(status_message())),
            ),
            Ok(Some(RPCResponse::Status(status_message())))
        );

        assert_eq!(
            encode_then_decode(
                Protocol::Ping,
                Version::V1,
                RPCCodedResponse::Success(RPCResponse::Pong(ping_message())),
            ),
            Ok(Some(RPCResponse::Pong(ping_message())))
        );

        assert_eq!(
            encode_then_decode(
                Protocol::DataByHash,
                Version::V1,
                RPCCodedResponse::Success(RPCResponse::DataByHash(Box::new(ZgsData {
                    hash: Default::default()
                }))),
            ),
            Ok(Some(RPCResponse::DataByHash(Box::new(ZgsData {
                hash: Default::default()
            }))))
        );

        // TODO: add tests for outbound requests
    }

    // /// Test a malicious snappy encoding for a V1 `Status` message where the attacker
    // /// sends a valid message filled with a stream of useless padding before the actual message.
    // #[test]
    // fn test_decode_malicious_v1_message() {
    //     // 10 byte snappy stream identifier
    //     let stream_identifier: &'static [u8] = b"\xFF\x06\x00\x00sNaPpY";

    //     assert_eq!(stream_identifier.len(), 10);

    //     // byte 0(0xFE) is padding chunk type identifier for snappy messages
    //     // byte 1,2,3 are chunk length (little endian)
    //     let malicious_padding: &'static [u8] = b"\xFE\x00\x00\x00";

    //     // Status message is 84 bytes uncompressed. `max_compressed_len` is 32 + 84 + 84/6 = 130.
    //     let status_message_bytes = StatusMessage {
    //         data: 1
    //     }
    //     .as_ssz_bytes();

    //     assert_eq!(status_message_bytes.len(), 8);
    //     assert_eq!(snap::raw::max_compress_len(status_message_bytes.len()), 41);

    //     let mut uvi_codec: Uvi<usize> = Uvi::default();
    //     let mut dst = BytesMut::with_capacity(1024);

    //     // Insert length-prefix
    //     uvi_codec
    //         .encode(status_message_bytes.len(), &mut dst)
    //         .unwrap();

    //     // Insert snappy stream identifier
    //     dst.extend_from_slice(stream_identifier);

    //     // Insert malicious padding of 80 bytes.
    //     for _ in 0..20 {
    //         dst.extend_from_slice(malicious_padding);
    //     }

    //     // Insert payload (42 bytes compressed)
    //     let mut writer = FrameEncoder::new(Vec::new());
    //     writer.write_all(&status_message_bytes).unwrap();
    //     writer.flush().unwrap();
    //     assert_eq!(writer.get_ref().len(), 42);
    //     dst.extend_from_slice(writer.get_ref());

    //     // 10 (for stream identifier) + 80 + 42 = 132 > `max_compressed_len`. Hence, decoding should fail with `InvalidData`.
    //     assert!(matches!(
    //         decode(Protocol::Status, Version::V1, &mut dst).unwrap_err(),
    //         RPCError::InvalidData(_)
    //     ));
    // }

    /// Test sending a message with encoded length prefix > max_rpc_size.
    #[test]
    fn test_decode_invalid_length() {
        // 10 byte snappy stream identifier
        let stream_identifier: &'static [u8] = b"\xFF\x06\x00\x00sNaPpY";

        assert_eq!(stream_identifier.len(), 10);

        // Status message is 84 bytes uncompressed. `max_compressed_len` is 32 + 84 + 84/6 = 130.
        let status_message_bytes = StatusMessage { data: 1 }.as_ssz_bytes();

        let mut uvi_codec: Uvi<usize> = Uvi::default();
        let mut dst = BytesMut::with_capacity(1024);

        // Insert length-prefix
        uvi_codec.encode(MAX_RPC_SIZE + 1, &mut dst).unwrap();

        // Insert snappy stream identifier
        dst.extend_from_slice(stream_identifier);

        // Insert payload
        let mut writer = FrameEncoder::new(Vec::new());
        writer.write_all(&status_message_bytes).unwrap();
        writer.flush().unwrap();
        dst.extend_from_slice(writer.get_ref());

        assert!(matches!(
            decode(Protocol::Status, Version::V1, &mut dst).unwrap_err(),
            RPCError::InvalidData(_)
        ));
    }
}
