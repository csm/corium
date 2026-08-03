//! Attributes the engine installs in every database.
//!
//! Corium reserves the low `:db.part/db` sequence range — everything below the
//! first user-installable attribute id — for attributes the engine owns. Two
//! groups live there.
//!
//! `:db/txInstant` is the commit timestamp the transactor asserts on every
//! transaction entity. Storing it as an ordinary datom is what makes
//! transaction time queryable data (`[?tx :db/txInstant ?inst]`) rather than
//! log-only metadata, and what lets [`Db::as_of_instant`](crate::Db::as_of_instant)
//! resolve a wall-clock instant to a basis.
//!
//! The rest are the **schema vocabulary**: the attributes an attribute's own
//! metadata is stored under. A schema change is a transaction asserting these
//! on the attribute entity, which is what makes schema basis-versioned data
//! rather than a fixed property of a database
//! (`docs/design/schema-migrations.md`). [`crate::schemadatoms`] derives a
//! [`Schema`] from them; nothing outside the engine may write them.
//!
//! Ids match Datomic's where Datomic has the same attribute, so a database
//! ported between the two keeps its attribute entities. `:db/protection` and
//! `:db/retired` are Corium's own and sit above Datomic's reserved range.

use corium_core::{
    AttrId, Attribute, Cardinality, Datom, EntityId, Keyword, Partition, Schema, Unique, Value,
    ValueType,
};

use crate::Idents;

/// Entity id of `:db/ident`, which is 10 as it is in Datomic.
pub const IDENT: AttrId = EntityId::new(Partition::Db as u32, 10);
/// Entity id of `:db/valueType`.
pub const VALUE_TYPE: AttrId = EntityId::new(Partition::Db as u32, 40);
/// Entity id of `:db/cardinality`.
pub const CARDINALITY: AttrId = EntityId::new(Partition::Db as u32, 41);
/// Entity id of `:db/unique`.
pub const UNIQUE: AttrId = EntityId::new(Partition::Db as u32, 42);
/// Entity id of `:db/isComponent`.
pub const IS_COMPONENT: AttrId = EntityId::new(Partition::Db as u32, 43);
/// Entity id of `:db/index`.
pub const INDEX: AttrId = EntityId::new(Partition::Db as u32, 44);
/// Entity id of `:db/noHistory`.
pub const NO_HISTORY: AttrId = EntityId::new(Partition::Db as u32, 45);
/// Entity id of `:db/txInstant`, which is 50 as it is in Datomic.
pub const TX_INSTANT: AttrId = EntityId::new(Partition::Db as u32, 50);
/// Entity id of `:db/doc`.
pub const DOC: AttrId = EntityId::new(Partition::Db as u32, 62);
/// Entity id of `:db/protection`, Corium's attribute protection class
/// reference (`docs/design/encryption.md`).
pub const PROTECTION: AttrId = EntityId::new(Partition::Db as u32, 80);
/// Entity id of `:db/retired`, the retirement marker: an attribute that
/// refuses new assertions while every existing fact stays readable and
/// retractable.
pub const RETIRED: AttrId = EntityId::new(Partition::Db as u32, 81);

/// Entity id of `:db.schemaUpdate/requester`.
pub const UPDATE_REQUESTER: AttrId = EntityId::new(Partition::Db as u32, 90);
/// Entity id of `:db.schemaUpdate/desiredDigest`.
pub const UPDATE_DESIRED_DIGEST: AttrId = EntityId::new(Partition::Db as u32, 91);
/// Entity id of `:db.schemaUpdate/planDigest`.
pub const UPDATE_PLAN_DIGEST: AttrId = EntityId::new(Partition::Db as u32, 92);
/// Entity id of `:db.schemaUpdate/observedBasis`.
pub const UPDATE_OBSERVED_BASIS: AttrId = EntityId::new(Partition::Db as u32, 93);
/// Entity id of `:db.schemaUpdate/tool`.
pub const UPDATE_TOOL: AttrId = EntityId::new(Partition::Db as u32, 94);
/// Entity id of `:db.schemaUpdate/class`, cardinality many.
pub const UPDATE_CLASS: AttrId = EntityId::new(Partition::Db as u32, 95);
/// Entity id of `:db.schemaUpdate/ack`, cardinality many.
pub const UPDATE_ACK: AttrId = EntityId::new(Partition::Db as u32, 96);

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

/// Every engine attribute, as `(id, keyword, record)`.
///
/// The schema vocabulary is cardinality-one and keeps history: an attribute's
/// metadata timeline is exactly what an `as-of` view needs to decode the facts
/// stored under it. `:db/ident` is `unique identity`, so two attributes can
/// never claim one name.
#[must_use]
pub fn attributes() -> Vec<(Keyword, Attribute)> {
    let simple = |id: AttrId, value_type: ValueType| Attribute {
        id,
        value_type,
        cardinality: Cardinality::One,
        unique: None,
        is_component: false,
        indexed: false,
        no_history: false,
    };
    vec![
        (
            Keyword::new(Some("db"), "ident"),
            Attribute {
                unique: Some(Unique::Identity),
                indexed: true,
                ..simple(IDENT, ValueType::Keyword)
            },
        ),
        (
            Keyword::new(Some("db"), "valueType"),
            simple(VALUE_TYPE, ValueType::Keyword),
        ),
        (
            Keyword::new(Some("db"), "cardinality"),
            simple(CARDINALITY, ValueType::Keyword),
        ),
        (
            Keyword::new(Some("db"), "unique"),
            simple(UNIQUE, ValueType::Keyword),
        ),
        (
            Keyword::new(Some("db"), "isComponent"),
            simple(IS_COMPONENT, ValueType::Bool),
        ),
        (
            Keyword::new(Some("db"), "index"),
            simple(INDEX, ValueType::Bool),
        ),
        (
            Keyword::new(Some("db"), "noHistory"),
            simple(NO_HISTORY, ValueType::Bool),
        ),
        (tx_instant_ident(), tx_instant_attribute()),
        (Keyword::new(Some("db"), "doc"), simple(DOC, ValueType::Str)),
        (
            Keyword::new(Some("db"), "protection"),
            simple(PROTECTION, ValueType::Ref),
        ),
        (
            Keyword::new(Some("db"), "retired"),
            simple(RETIRED, ValueType::Bool),
        ),
        (
            Keyword::new(Some("db.schemaUpdate"), "requester"),
            simple(UPDATE_REQUESTER, ValueType::Str),
        ),
        (
            Keyword::new(Some("db.schemaUpdate"), "desiredDigest"),
            simple(UPDATE_DESIRED_DIGEST, ValueType::Str),
        ),
        (
            Keyword::new(Some("db.schemaUpdate"), "planDigest"),
            simple(UPDATE_PLAN_DIGEST, ValueType::Str),
        ),
        (
            Keyword::new(Some("db.schemaUpdate"), "observedBasis"),
            simple(UPDATE_OBSERVED_BASIS, ValueType::Long),
        ),
        (
            Keyword::new(Some("db.schemaUpdate"), "tool"),
            simple(UPDATE_TOOL, ValueType::Str),
        ),
        (
            Keyword::new(Some("db.schemaUpdate"), "class"),
            Attribute {
                cardinality: Cardinality::Many,
                ..simple(UPDATE_CLASS, ValueType::Str)
            },
        ),
        (
            Keyword::new(Some("db.schemaUpdate"), "ack"),
            Attribute {
                cardinality: Cardinality::Many,
                ..simple(UPDATE_ACK, ValueType::Str)
            },
        ),
    ]
}

/// Whether `a` records the audit trail of an applied schema update.
///
/// These are ordinary attributes on the transaction entity — queryable like
/// any other fact, which is the point of keeping the audit trail in the
/// database. They are written only by the schema-update path, so that a
/// transaction cannot claim to have been one.
#[must_use]
pub fn is_audit_attribute(a: AttrId) -> bool {
    matches!(
        a,
        UPDATE_REQUESTER
            | UPDATE_DESIRED_DIGEST
            | UPDATE_PLAN_DIGEST
            | UPDATE_OBSERVED_BASIS
            | UPDATE_TOOL
            | UPDATE_CLASS
            | UPDATE_ACK
    )
}

/// Whether `a` is an attribute of the schema vocabulary.
///
/// These carry an attribute's own metadata, so writing one is a schema change
/// and not an ordinary assertion. `corium-tx` refuses them; the schema-update
/// path builds them directly.
#[must_use]
pub fn is_schema_attribute(a: AttrId) -> bool {
    matches!(
        a,
        IDENT
            | VALUE_TYPE
            | CARDINALITY
            | UNIQUE
            | IS_COMPONENT
            | INDEX
            | NO_HISTORY
            | DOC
            | PROTECTION
            | RETIRED
    )
}

/// Installs the engine's attributes into a schema and ident registry.
///
/// Idempotent: installing over a registry that already carries them (a schema
/// decoded from a transactor handshake, say) leaves it unchanged. Installing
/// them over a database created before the vocabulary existed is the
/// compatibility upgrade the design calls for — it labels what the creation
/// metadata already implies and changes no basis, transaction number, or
/// attribute id.
pub fn install(schema: &mut Schema, idents: &mut Idents) {
    for (keyword, attribute) in attributes() {
        idents.insert(keyword, attribute.id);
        schema.insert(attribute);
    }
}

/// Installs the engine's attributes into a schema alone.
pub fn install_schema(schema: &mut Schema) {
    for (_, attribute) in attributes() {
        schema.insert(attribute);
    }
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
        assert_eq!(schema.iter().count(), attributes().len());
        assert_eq!(idents.iter().count(), attributes().len());
        assert_eq!(idents.entid(&tx_instant_ident()), Some(TX_INSTANT));
        assert_eq!(
            idents.entid(&Keyword::new(Some("db"), "valueType")),
            Some(VALUE_TYPE)
        );
    }

    #[test]
    fn the_schema_vocabulary_is_engine_owned_and_distinctly_numbered() {
        let ids: std::collections::BTreeSet<AttrId> =
            attributes().iter().map(|(_, attr)| attr.id).collect();
        assert_eq!(ids.len(), attributes().len(), "ids must not collide");
        for (_, attribute) in attributes() {
            assert_eq!(attribute.id.partition(), Partition::Db as u32);
            assert!(attribute.id.sequence() < 100, "{:?}", attribute.id);
        }
        // `:db/txInstant` is engine data but not schema metadata: it describes
        // a transaction, not an attribute, so ordinary transactions keep
        // asserting it.
        assert!(!is_schema_attribute(TX_INSTANT));
        assert!(is_schema_attribute(VALUE_TYPE));
        assert!(!is_schema_attribute(EntityId::new(
            Partition::Db as u32,
            100
        )));
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
