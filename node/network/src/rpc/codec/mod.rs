pub(crate) mod base;
pub(crate) mod ssz_snappy;

use self::base::{BaseInboundCodec, BaseOutboundCodec};
use self::ssz_snappy::{SSZSnappyInboundCodec, SSZSnappyOutboundCodec};
use crate::rpc::protocol::RPCError;
use crate::rpc::{InboundRequest, OutboundRequest, RPCCodedResponse};
use libp2p::bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

// Known types of codecs
pub enum InboundCodec {
    SSZSnappy(BaseInboundCodec<SSZSnappyInboundCodec>),
}

pub enum OutboundCodec {
    SSZSnappy(BaseOutboundCodec<SSZSnappyOutboundCodec>),
}

impl Encoder<RPCCodedResponse> for InboundCodec {
    type Error = RPCError;

    fn encode(&mut self, item: RPCCodedResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match self {
            InboundCodec::SSZSnappy(codec) => codec.encode(item, dst),
        }
    }
}

impl Decoder for InboundCodec {
    type Item = InboundRequest;
    type Error = RPCError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self {
            InboundCodec::SSZSnappy(codec) => codec.decode(src),
        }
    }
}

impl Encoder<OutboundRequest> for OutboundCodec {
    type Error = RPCError;

    fn encode(&mut self, item: OutboundRequest, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match self {
            OutboundCodec::SSZSnappy(codec) => codec.encode(item, dst),
        }
    }
}

impl Decoder for OutboundCodec {
    type Item = RPCCodedResponse;
    type Error = RPCError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self {
            OutboundCodec::SSZSnappy(codec) => codec.decode(src),
        }
    }
}
