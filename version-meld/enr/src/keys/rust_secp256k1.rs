use super::{EnrKey, EnrKeyUnambiguous, EnrPublicKey, SigningError};
use crate::{digest, Key};
use bytes::Bytes;
use rlp::DecoderError;
use secp256k1::SECP256K1;
use std::collections::BTreeMap;

/// The ENR key that stores the public key in the ENR record.
pub const ENR_KEY: &str = "secp256k1";

impl EnrKey for secp256k1::SecretKey {
    type PublicKey = secp256k1::PublicKey;

    fn sign_v4(&self, msg: &[u8]) -> Result<Vec<u8>, SigningError> {
        // take a keccak256 hash then sign.
        let hash = digest(msg);
        let m = secp256k1::Message::from_slice(&hash)
            .map_err(|_| SigningError::new("failed to parse secp256k1 digest"))?;
        // serialize to an uncompressed 64 byte vector
        Ok(SECP256K1.sign_ecdsa(&m, self).serialize_compact().to_vec())
    }

    fn public(&self) -> Self::PublicKey {
        Self::PublicKey::from_secret_key(SECP256K1, self)
    }

    fn enr_to_public(content: &BTreeMap<Key, Bytes>) -> Result<Self::PublicKey, DecoderError> {
        let pubkey_bytes = content
            .get(ENR_KEY.as_bytes())
            .ok_or(DecoderError::Custom("Unknown signature"))?;
        // Decode the RLP
        let pubkey_bytes = rlp::Rlp::new(pubkey_bytes).data()?;

        Self::decode_public(pubkey_bytes)
    }
}

impl EnrKeyUnambiguous for secp256k1::SecretKey {
    fn decode_public(bytes: &[u8]) -> Result<Self::PublicKey, DecoderError> {
        // should be encoded in compressed form, i.e 33 byte raw secp256k1 public key
        secp256k1::PublicKey::from_slice(bytes)
            .map_err(|_| DecoderError::Custom("Invalid Secp256k1 Signature"))
    }
}

impl EnrPublicKey for secp256k1::PublicKey {
    type Raw = [u8; secp256k1::constants::PUBLIC_KEY_SIZE];
    type RawUncompressed = [u8; secp256k1::constants::UNCOMPRESSED_PUBLIC_KEY_SIZE - 1];

    fn verify_v4(&self, msg: &[u8], sig: &[u8]) -> bool {
        let msg = digest(msg);
        if let Ok(sig) = secp256k1::ecdsa::Signature::from_compact(sig) {
            if let Ok(msg) = secp256k1::Message::from_slice(&msg) {
                return SECP256K1.verify_ecdsa(&msg, &sig, self).is_ok();
            }
        }
        false
    }

    fn encode(&self) -> Self::Raw {
        self.serialize()
    }

    fn encode_uncompressed(&self) -> Self::RawUncompressed {
        let mut out = [0_u8; secp256k1::constants::UNCOMPRESSED_PUBLIC_KEY_SIZE - 1];
        out.copy_from_slice(&self.serialize_uncompressed()[1..]);
        out
    }

    fn enr_key(&self) -> Key {
        ENR_KEY.into()
    }
}
