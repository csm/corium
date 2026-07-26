//! Attributes the engine installs in every database.
//!
//! Corium reserves the low `:db.part/db` sequence range — everything below the
//! first user-installable attribute id — for attributes the engine owns. Today
//! that is `:db/txInstant`, the commit timestamp the transactor asserts on
//! every transaction entity. Storing it as an ordinary datom is what makes
//! transaction time queryable data (`[?tx :db/txInstant ?inst]`) rather than
//! log-only metadata, and what lets [`Db::as_of_instant`](crate::Db::as_of_instant)
//! resolve a wall-clock instant to a basis.

use corium_core::{
    AttrId, Attribute, Cardinality, Datom, EntityId, Keyword, Partition, Schema, Value, ValueType,
};

use crate::Idents;

/// Entity id of `:db/txInstant`, which is 50 as it is in Datomic.
pub const TX_INSTANT: AttrId = EntityId::new(Partition::Db as u32, 50);

/// The `:db/txInstant` keyword.
#[must_use]
pub fn tx_instant_ident() -> Keyword {
    Keyword::new(Some("db"), "txInstant")
}

/// Schema record for `:db/txInstant`.
///
/// It is AVET-indexed so an instant resolves to a basis by index seek, and it
/// keeps history: a transaction's timestamp is never retracted.
#[must_use]
pub const fn tx_instant_attribute() -> Attribute {
    Attribute {
        id: TX_INSTANT,
        value_type: ValueType::Instant,
        cardinality: Cardinality::One,
        unique: None,
        is_component: false,
        indexed: true,
        no_history: false,
    }
}

/// Installs the engine's attributes into a schema and ident registry.
///
/// Idempotent: installing over a registry that already carries them (a schema
/// decoded from a transactor handshake, say) leaves it unchanged.
pub fn install(schema: &mut Schema, idents: &mut Idents) {
    schema.insert(tx_instant_attribute());
    idents.insert(tx_instant_ident(), TX_INSTANT);
}

/// Installs the engine's attributes into a schema alone.
pub fn install_schema(schema: &mut Schema) {
    schema.insert(tx_instant_attribute());
}

/// The `:db/txInstant` datom for transaction `t`.
#[must_use]
pub fn tx_instant_datom(t: u64, instant: i64) -> Datom {
    let tx = EntityId::new(Partition::Tx as u32, t);
    Datom {
        e: tx,
        a: TX_INSTANT,
        v: Value::Instant(instant),
        tx,
        added: true,
    }
}

/// The instant asserted for the transaction entity of `t` within `datoms`.
///
/// Commit paths use this to honour a `:db/txInstant` supplied as transaction
/// metadata instead of stamping their own.
#[must_use]
pub fn asserted_instant(t: u64, datoms: &[Datom]) -> Option<i64> {
    let tx = EntityId::new(Partition::Tx as u32, t);
    let mut asserted = None;
    for datom in datoms {
        if let Datom {
            e,
            a,
            v: Value::Instant(instant),
            added,
            ..
        } = datom
            && *e == tx
            && *a == TX_INSTANT
        {
            if *added {
                asserted = Some(*instant);
            } else if asserted == Some(*instant) {
                asserted = None;
            }
        }
    }
    asserted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_is_idempotent() {
        let mut schema = Schema::default();
        let mut idents = Idents::default();
        install(&mut schema, &mut idents);
        install(&mut schema, &mut idents);
        assert_eq!(schema.iter().count(), 1);
        assert_eq!(idents.iter().count(), 1);
        assert_eq!(idents.entid(&tx_instant_ident()), Some(TX_INSTANT));
    }

    #[test]
    fn asserted_instant_finds_transaction_metadata() {
        let datoms = vec![tx_instant_datom(7, 1_700_000_000_000)];
        assert_eq!(asserted_instant(7, &datoms), Some(1_700_000_000_000));
        assert_eq!(asserted_instant(8, &datoms), None);
    }

    #[test]
    fn asserted_instant_uses_the_final_cardinality_one_value() {
        let mut datoms = vec![tx_instant_datom(7, 2_000)];
        let mut retract = tx_instant_datom(7, 2_000);
        retract.added = false;
        datoms.push(retract);
        datoms.push(tx_instant_datom(7, 1_000));
        assert_eq!(asserted_instant(7, &datoms), Some(1_000));
    }
}
