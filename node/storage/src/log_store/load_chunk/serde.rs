use super::{chunk_data::PartialBatch, EntryBatchData};

use crate::log_store::load_chunk::chunk_data::IncompleteData;
use ssz::{Decode, DecodeError, Encode};
use std::mem;

const COMPLETE_BATCH_TYPE: u8 = 0;
const INCOMPLETE_BATCH_TYPE: u8 = 1;

impl Encode for EntryBatchData {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        match &self {
            EntryBatchData::Complete(data) => {
                buf.extend_from_slice(&[COMPLETE_BATCH_TYPE]);
                buf.extend_from_slice(data.as_slice());
            }
            EntryBatchData::Incomplete(data_list) => {
                buf.extend_from_slice(&[INCOMPLETE_BATCH_TYPE]);
                buf.extend_from_slice(&data_list.as_ssz_bytes());
            }
        }
    }

    fn ssz_bytes_len(&self) -> usize {
        match &self {
            EntryBatchData::Complete(data) => 1 + data.len(),
            EntryBatchData::Incomplete(batch_list) => 1 + batch_list.ssz_bytes_len(),
        }
    }
}

impl Decode for EntryBatchData {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn from_ssz_bytes(bytes: &[u8]) -> std::result::Result<Self, DecodeError> {
        match *bytes.first().ok_or(DecodeError::ZeroLengthItem)? {
            COMPLETE_BATCH_TYPE => Ok(EntryBatchData::Complete(bytes[1..].to_vec())),
            INCOMPLETE_BATCH_TYPE => Ok(EntryBatchData::Incomplete(
                IncompleteData::from_ssz_bytes(&bytes[1..])?,
            )),
            unknown => Err(DecodeError::BytesInvalid(format!(
                "Unrecognized EntryBatchData indentifier {}",
                unknown
            ))),
        }
    }
}

impl Encode for PartialBatch {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.start_sector.to_be_bytes());
        buf.extend_from_slice(&self.data);
    }

    fn ssz_bytes_len(&self) -> usize {
        1 + self.data.len()
    }
}

impl Decode for PartialBatch {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn from_ssz_bytes(bytes: &[u8]) -> std::result::Result<Self, DecodeError> {
        Ok(Self {
            start_sector: usize::from_be_bytes(
                bytes[..mem::size_of::<usize>()].try_into().unwrap(),
            ),
            data: bytes[mem::size_of::<usize>()..].to_vec(),
        })
    }
}
