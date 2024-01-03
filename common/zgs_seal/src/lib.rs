use ethereum_types::H256;
use tiny_keccak::{Hasher, Keccak};

pub fn compute_first_mask_seed(
    miner_id: &H256,
    context_digest: &H256,
    start_sector: u64,
) -> [u8; 96] {
    let mut output = [0u8; 96];
    output[0..32].copy_from_slice(&miner_id.0);
    output[32..64].copy_from_slice(&context_digest.0);
    output[88..96].clone_from_slice(&start_sector.to_be_bytes());
    output
}

fn keccak(input: impl AsRef<[u8]>) -> [u8; 32] {
    let mut hasher = Keccak::v256();
    let mut output = [0u8; 32];
    hasher.update(input.as_ref());
    hasher.finalize(&mut output);
    output
}

pub fn seal_with_mask(data: &mut [u8], first_mask: [u8; 32]) {
    assert!(data.len() % 32 == 0);
    let mut mask = first_mask;
    for word in data.chunks_mut(32) {
        word.iter_mut().zip(mask.iter()).for_each(|(x, y)| *x ^= *y);
        mask = keccak(&*word);
    }
}

pub fn unseal_with_mask(data: &mut [u8], first_mask: [u8; 32]) {
    assert!(data.len() % 32 == 0);

    let mut mask = first_mask;
    data.chunks_exact_mut(32).for_each(|x| {
        let next_mask = keccak(&*x);
        x.iter_mut()
            .zip(mask.iter())
            .for_each(|(x, mask)| *x ^= *mask);
        mask = next_mask;
    })
}

pub fn seal_with_mask_seed(data: &mut [u8], first_mask_seed: impl AsRef<[u8]>) {
    seal_with_mask(data, keccak(first_mask_seed))
}

pub fn unseal_with_mask_seed(data: &mut [u8], first_mask_seed: impl AsRef<[u8]>) {
    unseal_with_mask(data, keccak(first_mask_seed))
}

pub fn seal(data: &mut [u8], miner_id: &H256, context_digest: &H256, start_sector: u64) {
    let first_mask_seed = compute_first_mask_seed(miner_id, context_digest, start_sector);
    seal_with_mask_seed(data, first_mask_seed)
}

pub fn unseal(data: &mut [u8], miner_id: &H256, context_digest: &H256, start_sector: u64) {
    let first_mask_seed = compute_first_mask_seed(miner_id, context_digest, start_sector);
    unseal_with_mask_seed(data, first_mask_seed)
}
