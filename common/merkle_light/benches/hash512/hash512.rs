use std::cmp::Ordering;

#[derive(Copy, Clone)]
pub struct Hash512(pub [u8; 64]);

impl Default for Hash512 {
    fn default() -> Self {
        Hash512([0u8; 64])
    }
}

impl AsRef<[u8]> for Hash512 {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl PartialOrd for Hash512 {
    #[inline]
    fn partial_cmp(&self, other: &Hash512) -> Option<Ordering> {
        PartialOrd::partial_cmp(&&self.0[..], &&other.0[..])
    }

    #[inline]
    fn lt(&self, other: &Hash512) -> bool {
        PartialOrd::lt(&&self.0[..], &&other.0[..])
    }

    #[inline]
    fn le(&self, other: &Hash512) -> bool {
        PartialOrd::le(&&self.0[..], &&other.0[..])
    }

    #[inline]
    fn ge(&self, other: &Hash512) -> bool {
        PartialOrd::ge(&&self.0[..], &&other.0[..])
    }

    #[inline]
    fn gt(&self, other: &Hash512) -> bool {
        PartialOrd::gt(&&self.0[..], &&other.0[..])
    }
}

impl Ord for Hash512 {
    #[inline]
    fn cmp(&self, other: &Hash512) -> Ordering {
        Ord::cmp(&&self.0[..], &&other.0[..])
    }
}

impl PartialEq for Hash512 {
    fn eq(&self, other: &Hash512) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl Eq for Hash512 {}
