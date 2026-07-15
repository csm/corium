//! Entity, transaction, attribute, keyword, and partition identifiers.

/// Number of low bits reserved for the sequence within a partition.
pub const SEQUENCE_BITS: u32 = 42;
const SEQUENCE_MASK: u64 = (1_u64 << SEQUENCE_BITS) - 1;

/// A raw partition id stored in the high bits of an entity id.
pub type PartitionId = u32;
/// Interned keyword id.
pub type KwId = u64;
/// Attribute ids are entity ids in the database partition.
pub type AttrId = EntityId;
/// Transaction ids are entity ids in the transaction partition.
pub type TxId = EntityId;

/// Built-in partitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u32)]
pub enum Partition {
    /// Schema and database metadata entities.
    Db = 0,
    /// Transaction entities.
    Tx = 1,
    /// Default user data partition.
    User = 2,
}

/// A Datomic-style entity id: 22-bit partition plus 42-bit sequence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EntityId(u64);

impl EntityId {
    /// Constructs an entity id from a partition and sequence.
    #[must_use]
    pub const fn new(partition: PartitionId, sequence: u64) -> Self {
        Self(((partition as u64) << SEQUENCE_BITS) | (sequence & SEQUENCE_MASK))
    }

    /// Constructs an entity id from raw bits.
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw integer representation.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Returns the partition component.
    #[must_use]
    pub const fn partition(self) -> PartitionId {
        (self.0 >> SEQUENCE_BITS) as PartitionId
    }

    /// Returns the sequence component.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.0 & SEQUENCE_MASK
    }
}
