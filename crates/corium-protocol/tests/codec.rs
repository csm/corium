//! Round-trip and cross-variant properties for the composite wire codec.

use corium_core::{
    Datom, EntityId, Keyword, KeywordInterner, Partition, Schema, TotalF64, Unique, Value,
    ValueType,
    encoding::{decode_value, encode_value},
};
use corium_db::{Idents, attribute};
use corium_protocol::codec;
use corium_query::edn::Edn;
use proptest::prelude::*;

fn arb_keyword() -> impl Strategy<Value = Keyword> {
    (
        proptest::option::of("[a-z][a-z0-9.-]{0,8}"),
        "[a-z][a-z0-9-]{0,8}",
    )
        .prop_map(|(namespace, name)| Keyword::new(namespace.as_deref(), &name))
}

fn arb_edn() -> impl Strategy<Value = Edn> {
    let leaf = prop_oneof![
        Just(Edn::Nil),
        any::<bool>().prop_map(Edn::Bool),
        any::<i64>().prop_map(Edn::Long),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(|f| Edn::Double(TotalF64(f))),
        "[ -~]{0,12}".prop_map(Edn::Str),
        arb_keyword().prop_map(Edn::Keyword),
        "[a-z][a-z0-9]{0,6}".prop_map(Edn::Symbol),
    ];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Edn::List),
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Edn::Vector),
            proptest::collection::vec(inner.clone(), 0..4).prop_map(|items| {
                let mut items = items;
                items.sort();
                items.dedup();
                Edn::Set(items)
            }),
            proptest::collection::vec((inner.clone(), inner.clone()), 0..3).prop_map(|pairs| {
                let mut pairs: Vec<_> = pairs
                    .into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>()
                    .into_iter()
                    .collect();
                pairs.sort_by(|left, right| left.0.cmp(&right.0));
                Edn::Map(pairs)
            }),
            ("[a-z]{1,6}", inner).prop_map(|(tag, value)| Edn::Tagged(tag, Box::new(value))),
        ]
    })
}

fn arb_value(interner: &mut KeywordInterner) -> Vec<Value> {
    let kw = interner.intern(Keyword::new(Some("wire"), "kw"));
    vec![
        Value::Bool(true),
        Value::Long(-42),
        Value::Long(i64::MIN),
        Value::Long(i64::MAX),
        Value::Double(TotalF64(-0.5)),
        Value::Instant(1_700_000_000_000),
        Value::Uuid(0x1234_5678_9abc_def0_1122_3344_5566_7788),
        Value::Keyword(kw),
        Value::Str("hello \u{0} world".into()),
        Value::Bytes(vec![0, 1, 2, 0xff, 0].into()),
        Value::Ref(EntityId::new(Partition::User as u32, 77)),
    ]
}

proptest! {
    #[test]
    fn edn_round_trips(form in arb_edn()) {
        let bytes = codec::encode_edn(&form);
        prop_assert_eq!(codec::decode_edn(&bytes).expect("decode"), form);
    }
}

#[test]
fn values_round_trip_and_match_core_variant() {
    let mut interner = KeywordInterner::default();
    for value in arb_value(&mut interner) {
        // Composite variant round trip.
        let mut writer = codec::Writer::new();
        writer.value(&value, &interner).expect("encode");
        let bytes = writer.finish();
        let mut reader = codec::Reader::new(&bytes);
        let mut peer_interner = KeywordInterner::default();
        let decoded = reader.value(&mut peer_interner).expect("decode");
        reader.expect_end().expect("no trailing bytes");
        // Keywords resolve by name, not id, across interners.
        match (&value, &decoded) {
            (Value::Keyword(ours), Value::Keyword(theirs)) => {
                assert_eq!(
                    interner.resolve(*ours).expect("ours"),
                    peer_interner.resolve(*theirs).expect("theirs")
                );
            }
            _ => assert_eq!(value, decoded),
        }
        // Cross-variant: the core sortable encoding must agree for the same
        // logical value (keywords excluded: they use interner ids there).
        if !matches!(value, Value::Keyword(_)) {
            let core = encode_value(&value);
            let (core_decoded, used) = decode_value(&core).expect("core decode");
            assert_eq!(used, core.len());
            assert_eq!(core_decoded, value);
        }
    }
}

#[test]
fn interning_table_compresses_repeats() {
    let repeated = Edn::Vector(vec![Edn::Str("repeat-me".into()); 10]);
    let bytes = codec::encode_edn(&repeated);
    assert!(bytes.len() < 10 * 9, "interning did not compress repeats");
    assert_eq!(codec::decode_edn(&bytes).expect("decode"), repeated);
}

#[test]
fn datoms_round_trip_with_keyword_renumbering() {
    let mut server = KeywordInterner::default();
    let _pad = server.intern(Keyword::new(None, "pad"));
    let color = server.intern(Keyword::new(Some("palette"), "blue"));
    let datoms = vec![
        Datom {
            e: EntityId::new(Partition::User as u32, 1000),
            a: EntityId::new(Partition::Db as u32, 100),
            v: Value::Keyword(color),
            tx: EntityId::new(Partition::Tx as u32, 1),
            added: true,
        },
        Datom {
            e: EntityId::new(Partition::User as u32, 1001),
            a: EntityId::new(Partition::Db as u32, 101),
            v: Value::Str("plain".into()),
            tx: EntityId::new(Partition::Tx as u32, 1),
            added: false,
        },
    ];
    let bytes = codec::encode_datoms(&datoms, &server).expect("encode");
    let mut peer = KeywordInterner::default();
    let decoded = codec::decode_datoms(&bytes, &mut peer).expect("decode");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[1], datoms[1]);
    let Value::Keyword(id) = decoded[0].v else {
        panic!("keyword value expected")
    };
    // The peer assigned its own id, but the same name.
    assert_eq!(
        peer.resolve(id),
        Some(&Keyword::new(Some("palette"), "blue"))
    );
    assert_eq!(decoded[0].e, datoms[0].e);
}

#[test]
fn schema_and_naming_round_trip() {
    let mut schema = Schema::default();
    schema.insert(attribute(
        100,
        ValueType::Str,
        corium_core::Cardinality::One,
        None,
    ));
    schema.insert(attribute(
        101,
        ValueType::Long,
        corium_core::Cardinality::Many,
        Some(Unique::Identity),
    ));
    let mut idents = Idents::default();
    idents.insert(
        Keyword::new(Some("p"), "name"),
        EntityId::new(Partition::Db as u32, 100),
    );
    idents.insert(
        Keyword::new(Some("p"), "score"),
        EntityId::new(Partition::Db as u32, 101),
    );
    let bytes = codec::encode_schema(&schema, &idents);
    let (schema2, idents2) = codec::decode_schema(&bytes).expect("decode");
    assert_eq!(schema, schema2);
    assert_eq!(
        idents.iter().collect::<Vec<_>>(),
        idents2.iter().collect::<Vec<_>>()
    );

    let mut interner = KeywordInterner::default();
    interner.intern(Keyword::new(None, "zeta"));
    interner.intern(Keyword::new(Some("a"), "alpha"));
    let bytes = codec::encode_naming(&interner);
    let decoded = codec::decode_naming(&bytes).expect("decode");
    assert_eq!(
        interner.iter().collect::<Vec<_>>(),
        decoded.iter().collect::<Vec<_>>()
    );
}
