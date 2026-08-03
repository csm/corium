//! Engine-internal values.

use std::{cmp::Ordering, sync::Arc};

use crate::{EntityId, KwId};

/// A finite `f64` wrapper ordered by IEEE-754 total-order bit transformation.
#[derive(Clone, Copy, Debug)]
pub struct TotalF64(pub f64);

impl TotalF64 {
    /// Returns the sortable transformed bits.
    #[must_use]
    pub const fn sortable_bits(self) -> u64 {
        let bits = self.0.to_bits();
        if (bits & (1_u64 << 63)) == 0 {
            bits ^ (1_u64 << 63)
        } else {
            !bits
        }
    }
}
impl PartialEq for TotalF64 {
    fn eq(&self, other: &Self) -> bool {
        self.sortable_bits() == other.sortable_bits()
    }
}
impl Eq for TotalF64 {}
impl PartialOrd for TotalF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TotalF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sortable_bits().cmp(&other.sortable_bits())
    }
}

/// An encrypted value whose cleartext header names its protection class,
/// key epoch, and declared value type (see `docs/design/encryption.md`).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Sealed {
    /// Protection class entity id.
    pub class: crate::EntityId,
    /// Key epoch within the class.
    pub epoch: u32,
    /// Declared plaintext value type.
    pub vtype: crate::ValueType,
    /// Deterministic AEAD ciphertext and tag.
    pub body: Arc<[u8]>,
}

/// Core v1 Corium value types.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Value {
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    Long(i64),
    /// Totally ordered double.
    Double(TotalF64),
    /// Milliseconds since Unix epoch, UTC.
    Instant(i64),
    /// 128-bit UUID bytes represented as an integer.
    Uuid(u128),
    /// Interned keyword.
    Keyword(KwId),
    /// UTF-8 string.
    Str(Arc<str>),
    /// Byte array.
    Bytes(Arc<[u8]>),
    /// Entity reference.
    Ref(EntityId),
    /// Encrypted value (sorts after every plaintext type).
    Sealed(Sealed),
}

impl Value {
    /// Returns whether this value has the requested schema type.
    #[must_use]
    pub fn has_type(&self, value_type: crate::ValueType) -> bool {
        match self {
            Self::Sealed(sealed) => sealed.vtype == value_type,
            _ => matches!(
                (self, value_type),
                (Self::Bool(_), crate::ValueType::Bool)
                    | (Self::Long(_), crate::ValueType::Long)
                    | (Self::Double(_), crate::ValueType::Double)
                    | (Self::Instant(_), crate::ValueType::Instant)
                    | (Self::Uuid(_), crate::ValueType::Uuid)
                    | (Self::Keyword(_), crate::ValueType::Keyword)
                    | (Self::Str(_), crate::ValueType::Str)
                    | (Self::Bytes(_), crate::ValueType::Bytes)
                    | (Self::Ref(_), crate::ValueType::Ref)
            ),
        }
    }
}
