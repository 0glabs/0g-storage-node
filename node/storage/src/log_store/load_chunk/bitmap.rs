use std::ops::{Deref, DerefMut};

use ssz::{Decode, DecodeError, Encode};

use bitmaps::{Bitmap, Bits, BitsImpl};

#[derive(Default, Debug)]
pub struct WrappedBitmap<const N: usize>(pub Bitmap<N>)
where
    BitsImpl<{ N }>: Bits;
type PrimitveInner<const N: usize> = <BitsImpl<N> as Bits>::Store;

impl<const N: usize> Encode for WrappedBitmap<N>
where
    BitsImpl<{ N }>: Bits,
    PrimitveInner<{ N }>: Encode,
{
    fn is_ssz_fixed_len() -> bool {
        true
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        buf.append(&mut self.0.into_value().as_ssz_bytes())
    }

    fn ssz_bytes_len(&self) -> usize {
        <Self as Encode>::ssz_fixed_len()
    }

    fn ssz_fixed_len() -> usize {
        <PrimitveInner<{ N }> as Encode>::ssz_fixed_len()
    }
}

impl<const N: usize> Decode for WrappedBitmap<N>
where
    BitsImpl<{ N }>: Bits,
    PrimitveInner<{ N }>: Decode,
{
    fn is_ssz_fixed_len() -> bool {
        true
    }

    fn ssz_fixed_len() -> usize {
        <PrimitveInner<{ N }> as Decode>::ssz_fixed_len()
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(Self(Bitmap::<{ N }>::from_value(
            PrimitveInner::from_ssz_bytes(bytes)?,
        )))
    }
}

impl<const N: usize> Deref for WrappedBitmap<N>
where
    BitsImpl<{ N }>: Bits,
{
    type Target = Bitmap<{ N }>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const N: usize> DerefMut for WrappedBitmap<N>
where
    BitsImpl<{ N }>: Bits,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub trait TruncateBitmap {
    fn truncate(&mut self, index: u16);
}

impl TruncateBitmap for u16 {
    fn truncate(&mut self, index: u16) {
        let mask: u16 = (1 << index) - 1;
        *self &= mask
    }
}

impl TruncateBitmap for u32 {
    fn truncate(&mut self, index: u16) {
        let mask: u32 = (1 << index) - 1;
        *self &= mask
    }
}

impl TruncateBitmap for u64 {
    fn truncate(&mut self, index: u16) {
        let mask: u64 = (1 << index) - 1;
        *self &= mask
    }
}

impl TruncateBitmap for u128 {
    fn truncate(&mut self, index: u16) {
        let mask: u128 = (1 << index) - 1;
        *self &= mask
    }
}

impl<const N: usize> TruncateBitmap for [u128; N] {
    fn truncate(&mut self, index: u16) {
        let blob_index = index as usize / u128::BITS as usize;
        let bit_index = index as usize % u128::BITS as usize;
        let mask: u128 = (1 << (bit_index as u128)) - 1;
        self[blob_index] &= mask;
        for blob in &mut self[(blob_index + 1)..N] {
            *blob = 0;
        }
    }
}

impl<const N: usize> WrappedBitmap<N>
where
    BitsImpl<{ N }>: Bits,
    PrimitveInner<{ N }>: TruncateBitmap,
{
    /// Set the position large or equal to `index` to false
    pub fn truncate(&mut self, index: u16) {
        let mut current = *self.as_value();
        TruncateBitmap::truncate(&mut current, index);
        self.0 = Bitmap::<{ N }>::from_value(current)
    }
}

#[test]
fn bitmap_serde() {
    let mut bitmap = WrappedBitmap::<64>::default();
    bitmap.set(10, true);
    bitmap.set(29, true);

    let serialized = bitmap.as_ssz_bytes();
    let deserialized = WrappedBitmap::<64>::from_ssz_bytes(&serialized).unwrap();
    assert_eq!(bitmap.into_value(), deserialized.into_value());
}

#[test]
fn bitmap_truncate() {
    let mut bitmap = WrappedBitmap::<64>::default();
    bitmap.set(10, true);
    bitmap.set(29, true);
    bitmap.set(30, true);
    bitmap.set(55, true);

    bitmap.truncate(30);

    assert!(bitmap.get(10));
    assert!(bitmap.get(29));
    assert!(!bitmap.get(30));
    assert!(!bitmap.get(55));
}

#[test]
fn bitmap_big_truncate() {
    let mut bitmap = WrappedBitmap::<300>::default();
    bitmap.set(110, true);
    bitmap.set(129, true);
    bitmap.set(130, true);
    bitmap.set(155, true);
    bitmap.set(299, true);

    bitmap.truncate(130);

    assert!(bitmap.get(110));
    assert!(bitmap.get(129));
    assert!(!bitmap.get(130));
    assert!(!bitmap.get(155));
    assert!(!bitmap.get(299));
}
