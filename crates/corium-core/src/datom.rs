//! Datoms and covering index key composition.

use crate::{
    AttrId, EntityId, TxId, Value,
    encoding::{DecodeError, Encodable, decode_entity_id, decode_u64, decode_value},
};

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

    /// Decodes a complete covering-index key back into its datom.
    ///
    /// # Errors
    /// Returns [`DecodeError`] when a component is truncated or malformed.
    pub fn from_key(order: IndexOrder, input: &[u8]) -> Result<Self, DecodeError> {
        let mut offset = 0;
        let entity = |offset: &mut usize| {
            let (value, used) = decode_entity_id(&input[*offset..])?;
            *offset += used;
            Ok::<_, DecodeError>(value)
        };
        let (e, a, v) = match order {
            IndexOrder::Eavt => {
                let e = entity(&mut offset)?;
                let a = entity(&mut offset)?;
                let (v, used) = decode_value(&input[offset..])?;
                offset += used;
                (e, a, v)
            }
            IndexOrder::Aevt => {
                let a = entity(&mut offset)?;
                let e = entity(&mut offset)?;
                let (v, used) = decode_value(&input[offset..])?;
                offset += used;
                (e, a, v)
            }
            IndexOrder::Avet => {
                let a = entity(&mut offset)?;
                let (v, used) = decode_value(&input[offset..])?;
                offset += used;
                let e = entity(&mut offset)?;
                (e, a, v)
            }
            IndexOrder::Vaet => {
                let (v, used) = decode_value(&input[offset..])?;
                offset += used;
                let a = entity(&mut offset)?;
                let e = entity(&mut offset)?;
                (e, a, v)
            }
        };
        let (tx_added, used) = decode_u64(&input[offset..])?;
        offset += used;
        if offset != input.len() {
            return Err(DecodeError::Trailing);
        }
        Ok(Self {
            e,
            a,
            v,
            tx: EntityId::from_raw(tx_added >> 1),
            added: tx_added & 1 == 1,
        })
    }
}

fn encode_tx_added(tx: TxId, added: bool, out: &mut Vec<u8>) {
    ((tx.raw() << 1) | u64::from(added)).encode_into(out);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covering_keys_round_trip() {
        let datom = Datom {
            e: EntityId::from_raw(42),
            a: EntityId::from_raw(7),
            v: Value::Str("a\0z".into()),
            tx: EntityId::from_raw(19),
            added: true,
        };
        for order in [
            IndexOrder::Eavt,
            IndexOrder::Aevt,
            IndexOrder::Avet,
            IndexOrder::Vaet,
        ] {
            let key = datom.key(order);
            assert_eq!(Datom::from_key(order, &key), Ok(datom.clone()));
        }
    }

    #[test]
    fn covering_key_rejects_trailing_bytes() {
        let datom = Datom {
            e: EntityId::from_raw(1),
            a: EntityId::from_raw(2),
            v: Value::Long(3),
            tx: EntityId::from_raw(4),
            added: false,
        };
        let mut key = datom.key(IndexOrder::Eavt);
        key.push(0);
        assert_eq!(
            Datom::from_key(IndexOrder::Eavt, &key),
            Err(DecodeError::Trailing)
        );
    }
}
