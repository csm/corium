//! Deriving a [`Schema`] and [`Idents`] from schema datoms.
//!
//! Schema is data. An attribute's metadata is stored as ordinary datoms on the
//! attribute entity under the vocabulary in [`crate::bootstrap`], so a schema
//! change travels through the transaction log like any other fact: the
//! transactor appends it once, and every replay, recovery, and peer tx report
//! reconstructs the same schema by folding it here
//! (`docs/design/schema-migrations.md`).
//!
//! Derivation is deliberately independent of datom order within a
//! transaction. A transaction that installs an attribute and uses it is legal
//! because [`Db::with_transaction_at`](crate::Db::with_transaction_at) derives
//! the schema effects of the whole record before it indexes any of its user
//! datoms — not because the schema datoms happened to be listed first.
//!
//! Protection is forward-only, so a `:db/protection` change *appends* to the
//! attribute's timeline at `t` rather than replacing it. A datom keeps the
//! form it was asserted in, and the timeline is what lets a keyless transactor
//! still decide which forms a later retraction or `:db/cas` operand may name.

use std::collections::BTreeMap;

use corium_core::{
    AttrId, Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, ProtectionTimeline,
    Schema, Unique, Value, ValueType,
};

use crate::{Idents, bootstrap};

/// Whether `datoms` needs a schema derivation at all.
///
/// The overwhelmingly common transaction touches no vocabulary attribute and
/// pays one scan for the answer.
#[must_use]
pub fn touches_naming(datoms: &[Datom]) -> bool {
    datoms
        .iter()
        .any(|datom| bootstrap::is_schema_attribute(datom.a))
}

/// Whether `datoms` changes *attribute metadata*, as opposed to naming.
///
/// `:db/ident` does double duty: on an attribute entity it is schema, and on
/// an ordinary entity it is just a name — the way a database function or an
/// enumerated value is given one. The two are told apart by partition, since
/// `:db.part/db` is where schema entities live and the schema-update path is
/// its only writer.
///
/// The distinction is what keeps the schema generation meaningful. Naming a
/// database function must not claim the schema changed, because every holder
/// of a database value would then throw away a correct covering-index fold.
#[must_use]
pub fn changes_schema(datoms: &[Datom]) -> bool {
    datoms.iter().any(|datom| {
        bootstrap::is_schema_attribute(datom.a)
            && datom.e.partition() == corium_core::Partition::Db as u32
    })
}

/// The final value each `(entity, attribute)` pair holds after `datoms`.
///
/// A cardinality-one assertion is recorded as a retraction of the old value
/// followed by an assertion of the new one, so reading the datoms in order
/// would see a spurious clear. Collapsing per pair — assertion wins, otherwise
/// the pair is cleared — makes the result independent of the order the
/// transaction happens to list its datoms in.
fn final_values(datoms: &[Datom]) -> BTreeMap<(EntityId, AttrId), Option<Value>> {
    let mut resolved: BTreeMap<(EntityId, AttrId), Option<Value>> = BTreeMap::new();
    for datom in datoms {
        if !bootstrap::is_schema_attribute(datom.a) {
            continue;
        }
        let slot = resolved.entry((datom.e, datom.a)).or_insert(None);
        if datom.added {
            *slot = Some(datom.v.clone());
        } else if slot.as_ref() == Some(&datom.v) {
            *slot = None;
        }
    }
    resolved
}

/// Folds the schema effects of one transaction into `schema` and `idents`.
///
/// `t` is the basis the transaction commits at, which is where a protection
/// change enters the attribute's timeline. `interner` resolves the keyword
/// values the vocabulary stores (`:db.type/string`, `:db.cardinality/many`).
///
/// Unknown or malformed values are ignored rather than panicking: this runs on
/// replay of a durable log, where refusing to open a database would be a worse
/// answer than declining to apply one uninterpretable fact. The transactor
/// validates before it appends, so nothing valid reaches here malformed.
pub fn derive(
    schema: &mut Schema,
    idents: &mut Idents,
    interner: &KeywordInterner,
    t: u64,
    datoms: &[Datom],
) {
    let resolved = final_values(datoms);
    let keyword = |value: &Value| match value {
        Value::Keyword(id) => interner.resolve(*id),
        _ => None,
    };

    // Names first: an attribute installed and named in one transaction must be
    // reachable by ident from the value this transaction produces.
    for ((entity, attribute), value) in &resolved {
        if *attribute == bootstrap::IDENT
            && let Some(value) = value
            && let Some(name) = keyword(value)
        {
            idents.insert(name, *entity);
        }
    }

    for ((entity, attribute), value) in &resolved {
        if *attribute == bootstrap::IDENT {
            continue;
        }
        // Every other vocabulary attribute describes an attribute entity. The
        // first `:db/valueType` is what installs one; a property landing on an
        // entity with no attribute record yet gets a typed placeholder so the
        // properties of one transaction never depend on each other's order.
        let existing = schema.get(*entity).cloned();
        let mut record = existing.clone().unwrap_or(Attribute {
            id: *entity,
            value_type: ValueType::Str,
            cardinality: Cardinality::One,
            unique: None,
            is_component: false,
            indexed: false,
            no_history: false,
        });
        let mut installed = existing.is_some();
        match *attribute {
            bootstrap::VALUE_TYPE => {
                if let Some(value_type) = value
                    .as_ref()
                    .and_then(&keyword)
                    .as_ref()
                    .and_then(value_type_of)
                {
                    record.value_type = value_type;
                    installed = true;
                }
            }
            bootstrap::CARDINALITY => {
                if let Some(cardinality) = value
                    .as_ref()
                    .and_then(&keyword)
                    .as_ref()
                    .and_then(cardinality_of)
                {
                    record.cardinality = cardinality;
                }
            }
            bootstrap::UNIQUE => {
                record.unique = match value {
                    None => None,
                    Some(value) => match keyword(value).as_ref().and_then(unique_of) {
                        Some(unique) => Some(unique),
                        // An unreadable mode is not "no constraint": leaving
                        // the installed one in place is the safe reading.
                        None => record.unique,
                    },
                };
            }
            bootstrap::INDEX => record.indexed = value.as_ref() == Some(&Value::Bool(true)),
            bootstrap::IS_COMPONENT => {
                record.is_component = value.as_ref() == Some(&Value::Bool(true));
            }
            bootstrap::NO_HISTORY => {
                record.no_history = value.as_ref() == Some(&Value::Bool(true));
            }
            bootstrap::DOC => schema.set_doc(
                *entity,
                match value {
                    Some(Value::Str(doc)) => Some(doc.to_string()),
                    _ => None,
                },
            ),
            bootstrap::RETIRED => {
                schema.set_retired(*entity, value.as_ref() == Some(&Value::Bool(true)));
            }
            bootstrap::PROTECTION => {
                let class = match value {
                    Some(Value::Ref(class)) => Some(*class),
                    _ => None,
                };
                let mut timeline: Vec<(u64, Option<EntityId>)> =
                    schema.protection(*entity).entries().to_vec();
                // Forward-only: the entry is appended at `t`, never edited in
                // place, so every fact already stored keeps the form it was
                // asserted under and stays retractable by naming it.
                if timeline.last().and_then(|(_, class)| *class) != class {
                    timeline.push((t, class));
                    schema.set_protection(*entity, ProtectionTimeline::from_entries(timeline));
                }
            }
            _ => {}
        }
        // Uniqueness implies AVET coverage, exactly as it does at creation.
        record.indexed = record.indexed || record.unique.is_some();
        if installed {
            schema.insert(record);
        }
    }
}

/// The value type a `:db.type/…` keyword names.
#[must_use]
pub fn value_type_of(keyword: &Keyword) -> Option<ValueType> {
    Some(match keyword.name.as_str() {
        "string" => ValueType::Str,
        "long" => ValueType::Long,
        "double" => ValueType::Double,
        "boolean" => ValueType::Bool,
        "instant" => ValueType::Instant,
        "uuid" => ValueType::Uuid,
        "keyword" => ValueType::Keyword,
        "bytes" => ValueType::Bytes,
        "ref" => ValueType::Ref,
        _ => return None,
    })
}

/// The cardinality a `:db.cardinality/…` keyword names.
#[must_use]
pub fn cardinality_of(keyword: &Keyword) -> Option<Cardinality> {
    Some(match keyword.name.as_str() {
        "one" => Cardinality::One,
        "many" => Cardinality::Many,
        _ => return None,
    })
}

/// The uniqueness mode a `:db.unique/…` keyword names.
#[must_use]
pub fn unique_of(keyword: &Keyword) -> Option<Unique> {
    Some(match keyword.name.as_str() {
        "identity" => Unique::Identity,
        "value" => Unique::Value,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::Partition;

    fn attr(sequence: u64) -> AttrId {
        EntityId::new(Partition::Db as u32, sequence)
    }

    struct Fixture {
        schema: Schema,
        idents: Idents,
        interner: KeywordInterner,
    }

    impl Fixture {
        fn new() -> Self {
            let mut schema = Schema::default();
            let mut idents = Idents::default();
            bootstrap::install(&mut schema, &mut idents);
            Self {
                schema,
                idents,
                interner: KeywordInterner::default(),
            }
        }

        fn keyword(&mut self, text: &str) -> Value {
            Value::Keyword(self.interner.intern(Keyword::parse(text)))
        }

        fn apply(&mut self, t: u64, datoms: &[Datom]) {
            derive(
                &mut self.schema,
                &mut self.idents,
                &self.interner,
                t,
                datoms,
            );
        }
    }

    fn datom(e: EntityId, a: AttrId, v: Value, t: u64, added: bool) -> Datom {
        Datom {
            e,
            a,
            v,
            tx: EntityId::new(Partition::Tx as u32, t),
            added,
        }
    }

    #[test]
    fn an_attribute_is_installed_from_its_datoms() {
        let mut fixture = Fixture::new();
        let email = attr(100);
        let name = fixture.keyword("person/email");
        let string = fixture.keyword("db.type/string");
        let identity = fixture.keyword("db.unique/identity");
        fixture.apply(
            1,
            &[
                datom(email, bootstrap::IDENT, name, 1, true),
                datom(email, bootstrap::VALUE_TYPE, string, 1, true),
                datom(email, bootstrap::UNIQUE, identity, 1, true),
                datom(
                    email,
                    bootstrap::DOC,
                    Value::Str("primary contact".into()),
                    1,
                    true,
                ),
            ],
        );
        let installed = fixture.schema.get(email).expect("installed");
        assert_eq!(installed.value_type, ValueType::Str);
        assert_eq!(installed.cardinality, Cardinality::One);
        assert_eq!(installed.unique, Some(Unique::Identity));
        assert!(installed.indexed, "uniqueness implies AVET coverage");
        assert_eq!(fixture.schema.doc(email), Some("primary contact"));
        assert_eq!(
            fixture.idents.entid(&Keyword::parse("person/email")),
            Some(email)
        );
    }

    #[test]
    fn derivation_does_not_depend_on_datom_order() {
        let email = attr(100);
        let mut forward = Fixture::new();
        let name = forward.keyword("person/email");
        let string = forward.keyword("db.type/string");
        let many = forward.keyword("db.cardinality/many");
        let datoms = vec![
            datom(email, bootstrap::IDENT, name, 1, true),
            datom(email, bootstrap::VALUE_TYPE, string, 1, true),
            datom(email, bootstrap::CARDINALITY, many, 1, true),
        ];
        forward.apply(1, &datoms);

        let mut reverse = Fixture::new();
        reverse.interner = forward.interner.clone();
        let mut reversed = datoms;
        reversed.reverse();
        reverse.apply(1, &reversed);
        assert_eq!(forward.schema, reverse.schema);
        assert_eq!(
            forward.schema.get(email).expect("installed").cardinality,
            Cardinality::Many
        );
    }

    #[test]
    fn a_cardinality_one_replacement_is_not_read_as_a_clear() {
        let mut fixture = Fixture::new();
        let email = attr(100);
        let string = fixture.keyword("db.type/string");
        let one = fixture.keyword("db.cardinality/one");
        let many = fixture.keyword("db.cardinality/many");
        fixture.apply(
            1,
            &[
                datom(email, bootstrap::VALUE_TYPE, string, 1, true),
                datom(email, bootstrap::CARDINALITY, one.clone(), 1, true),
            ],
        );
        // The retraction of the old value and the assertion of the new one
        // arrive together, exactly as a commit records them.
        fixture.apply(
            2,
            &[
                datom(email, bootstrap::CARDINALITY, one, 2, false),
                datom(email, bootstrap::CARDINALITY, many, 2, true),
            ],
        );
        assert_eq!(
            fixture.schema.get(email).expect("installed").cardinality,
            Cardinality::Many
        );
    }

    #[test]
    fn retracting_a_property_restores_its_default() {
        let mut fixture = Fixture::new();
        let email = attr(100);
        let string = fixture.keyword("db.type/string");
        let identity = fixture.keyword("db.unique/identity");
        fixture.apply(
            1,
            &[
                datom(email, bootstrap::VALUE_TYPE, string, 1, true),
                datom(email, bootstrap::UNIQUE, identity.clone(), 1, true),
                datom(email, bootstrap::DOC, Value::Str("why".into()), 1, true),
            ],
        );
        fixture.apply(
            2,
            &[
                datom(email, bootstrap::UNIQUE, identity, 2, false),
                datom(email, bootstrap::DOC, Value::Str("why".into()), 2, false),
                datom(email, bootstrap::INDEX, Value::Bool(false), 2, true),
            ],
        );
        let installed = fixture.schema.get(email).expect("installed");
        assert_eq!(installed.unique, None);
        assert!(!installed.indexed);
        assert_eq!(fixture.schema.doc(email), None);
    }

    #[test]
    fn protection_changes_append_to_the_timeline() {
        let mut fixture = Fixture::new();
        let email = attr(100);
        let pii = EntityId::new(Partition::Db as u32, 200);
        let string = fixture.keyword("db.type/string");
        fixture.apply(1, &[datom(email, bootstrap::VALUE_TYPE, string, 1, true)]);
        fixture.apply(
            7,
            &[datom(
                email,
                bootstrap::PROTECTION,
                Value::Ref(pii),
                7,
                true,
            )],
        );
        fixture.apply(
            9,
            &[datom(
                email,
                bootstrap::PROTECTION,
                Value::Ref(pii),
                9,
                false,
            )],
        );
        let timeline = fixture.schema.protection(email);
        assert_eq!(timeline.entries(), &[(7, Some(pii)), (9, None)]);
        // Unprotecting does not un-seal what is already stored, so the
        // attribute can never later gain value-ordered coverage.
        assert!(timeline.ever_protected());
        assert_eq!(timeline.at(8), Some(pii));
        assert_eq!(timeline.at(6), None);
        assert_eq!(timeline.current(), None);
    }

    #[test]
    fn retirement_is_recorded_and_reversible_as_metadata() {
        let mut fixture = Fixture::new();
        let email = attr(100);
        let string = fixture.keyword("db.type/string");
        fixture.apply(1, &[datom(email, bootstrap::VALUE_TYPE, string, 1, true)]);
        assert!(!fixture.schema.is_retired(email));
        fixture.apply(
            2,
            &[datom(email, bootstrap::RETIRED, Value::Bool(true), 2, true)],
        );
        assert!(fixture.schema.is_retired(email));
        assert!(
            fixture.schema.get(email).is_some(),
            "a retired attribute stays readable"
        );
    }

    #[test]
    fn touching_no_schema_attribute_is_recognized() {
        let user = EntityId::new(Partition::User as u32, 1);
        let ordinary = datom(user, attr(100), Value::Long(1), 1, true);
        assert!(!touches_naming(std::slice::from_ref(&ordinary)));
        assert!(!changes_schema(&[ordinary]));
        // Transaction time is engine data about a transaction, not metadata
        // about an attribute.
        let instant = datom(
            EntityId::new(Partition::Tx as u32, 1),
            bootstrap::TX_INSTANT,
            Value::Instant(1),
            1,
            true,
        );
        assert!(!touches_naming(std::slice::from_ref(&instant)));
        assert!(!changes_schema(&[instant]));
        let metadata = datom(attr(100), bootstrap::INDEX, Value::Bool(true), 1, true);
        assert!(touches_naming(std::slice::from_ref(&metadata)));
        assert!(changes_schema(&[metadata]));
    }

    #[test]
    fn naming_an_ordinary_entity_is_not_a_schema_change() {
        let mut fixture = Fixture::new();
        let function = EntityId::new(Partition::User as u32, 1_000);
        let name = fixture.keyword("acct/withdraw");
        let naming = datom(function, bootstrap::IDENT, name, 1, true);
        // The registry has to learn the name...
        assert!(touches_naming(std::slice::from_ref(&naming)));
        // ...but no attribute's metadata moved, so nothing that was built
        // under the old schema is invalidated.
        assert!(!changes_schema(std::slice::from_ref(&naming)));

        fixture.apply(1, &[naming]);
        assert_eq!(
            fixture.idents.entid(&Keyword::parse("acct/withdraw")),
            Some(function)
        );
        assert!(
            fixture.schema.get(function).is_none(),
            "naming an entity does not install an attribute"
        );
    }
}
