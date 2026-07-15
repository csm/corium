//! Datoms and covering index key composition.

use crate::{AttrId, EntityId, TxId, Value, encoding::Encodable};

/// A single immutable fact assertion or retraction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Datom {
    /// Entity id.
    pub e: EntityId,
    /// Attribute id.
    pub a: AttrId,
    /// Value.
    pub v: Value,
    /// Transaction id.
    pub tx: TxId,
    /// Assertion (`true`) or retraction (`false`).
    pub added: bool,
}

/// Covering index orders.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexOrder {
    /// Entity, attribute, value, transaction.
    Eavt,
    /// Attribute, entity, value, transaction.
    Aevt,
    /// Attribute, value, entity, transaction.
    Avet,
    /// Value, attribute, entity, transaction.
    Vaet,
}

impl Datom {
    /// Returns this datom's byte key for the requested index order.
    #[must_use]
    pub fn key(&self, order: IndexOrder) -> Vec<u8> {
        let mut out = Vec::new();
        match order {
            IndexOrder::Eavt => {
                self.e.encode_into(&mut out);
                self.a.encode_into(&mut out);
                self.v.encode_into(&mut out);
            }
            IndexOrder::Aevt => {
                self.a.encode_into(&mut out);
                self.e.encode_into(&mut out);
                self.v.encode_into(&mut out);
            }
            IndexOrder::Avet => {
                self.a.encode_into(&mut out);
                self.v.encode_into(&mut out);
                self.e.encode_into(&mut out);
            }
            IndexOrder::Vaet => {
                self.v.encode_into(&mut out);
                self.a.encode_into(&mut out);
                self.e.encode_into(&mut out);
            }
        }
        encode_tx_added(self.tx, self.added, &mut out);
        out
    }
}

fn encode_tx_added(tx: TxId, added: bool, out: &mut Vec<u8>) {
    ((tx.raw() << 1) | u64::from(added)).encode_into(out);
}
