//! Property tests for M0 sortable encoding and datom keys.

use std::sync::Arc;

use corium_core::{
    Datom, EntityId, IndexOrder, KEY_TX_SUFFIX_LEN, Keyword, KeywordInterner, Sealed, TotalF64,
    Value, ValueType,
    encoding::{
        SealPlaintextError, decode_seal_plaintext, decode_value, encode_seal_plaintext,
        encode_value,
    },
    key_components,
};
use proptest::prelude::*;

fn arb_value_type() -> impl Strategy<Value = ValueType> {
    prop_oneof![
        Just(ValueType::Bool),
        Just(ValueType::Long),
        Just(ValueType::Double),
        Just(ValueType::Instant),
        Just(ValueType::Uuid),
        Just(ValueType::Keyword),
        Just(ValueType::Str),
        Just(ValueType::Bytes),
        Just(ValueType::Ref),
    ]
}

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::Long),
        any::<i64>().prop_map(Value::Instant),
        any::<u128>().prop_map(Value::Uuid),
        any::<u64>().prop_map(Value::Keyword),
        "[a-zA-Z0-9\u{0}]{0,16}".prop_map(|s| Value::Str(Arc::from(s))),
        prop::collection::vec(any::<u8>(), 0..16).prop_map(|b| Value::Bytes(Arc::from(b))),
        any::<u64>().prop_map(|raw| Value::Ref(EntityId::from_raw(raw))),
        any::<u64>().prop_map(|bits| Value::Double(TotalF64(f64::from_bits(bits)))),
        (
            any::<u64>(),
            any::<u32>(),
            arb_value_type(),
            prop::collection::vec(any::<u8>(), 0..24),
        )
            .prop_map(|(class, epoch, vtype, body)| Value::Sealed(Sealed {
                class: EntityId::from_raw(class),
                epoch,
                vtype,
                body: Arc::from(body),
            })),
    ]
}

proptest! {
    #[test]
    fn value_round_trips(v in arb_value()) {
        let encoded = encode_value(&v);
        let (decoded, used) = decode_value(&encoded).unwrap();
        prop_assert_eq!(used, encoded.len());
        prop_assert_eq!(decoded, v);
    }

    #[test]
    fn value_encoding_preserves_order(a in arb_value(), b in arb_value()) {
        prop_assert_eq!(a.cmp(&b), encode_value(&a).cmp(&encode_value(&b)));
    }

    /// Seal plaintext survives zero padding: a protection class pads to a
    /// fixed multiple and records no length, so the decoder must recover the
    /// value from the self-delimiting encoding alone.
    #[test]
    fn seal_plaintext_round_trips_through_padding(
        v in arb_value(),
        pad in 0_usize..64,
    ) {
        prop_assume!(!matches!(v, Value::Sealed(_) | Value::Keyword(_)));
        let interner = KeywordInterner::default();
        let mut bytes = encode_seal_plaintext(&v, &interner).unwrap();
        bytes.resize(bytes.len() + pad, 0);
        let vtype = declared_type(&v);
        prop_assert_eq!(decode_seal_plaintext(&bytes, vtype, &interner).unwrap(), v);
    }
}

fn declared_type(value: &Value) -> ValueType {
    for vtype in [
        ValueType::Bool,
        ValueType::Long,
        ValueType::Double,
        ValueType::Instant,
        ValueType::Uuid,
        ValueType::Keyword,
        ValueType::Str,
        ValueType::Bytes,
        ValueType::Ref,
    ] {
        if value.has_type(vtype) {
            return vtype;
        }
    }
    unreachable!("every value has a declared type")
}

#[test]
fn seal_plaintext_carries_keyword_text_not_its_id() {
    let mut writer = KeywordInterner::default();
    let id = writer.intern(Keyword::new(Some("status"), "active"));
    let bytes = encode_seal_plaintext(&Value::Keyword(id), &writer).unwrap();
    // The id never appears; the text does, under the string tag.
    assert!(bytes.windows(6).any(|w| w == b"active"));

    // A reader that interned nothing still recovers the keyword, and does not
    // pollute the durable naming table doing so.
    let reader = KeywordInterner::default();
    let decoded = decode_seal_plaintext(&bytes, ValueType::Keyword, &reader).unwrap();
    let Value::Keyword(local) = decoded else {
        panic!("keyword expected")
    };
    assert_eq!(
        reader.resolve(local),
        Some(Keyword::new(Some("status"), "active"))
    );
    assert_eq!(reader.len(), 0);
    assert!(local >= corium_core::FIRST_LOCAL_KW_ID);

    // A reader that already knows the keyword durably keeps the durable id,
    // so hydrated values and query literals compare equal.
    let mut known = KeywordInterner::default();
    let durable = known.intern(Keyword::new(Some("status"), "active"));
    assert_eq!(
        decode_seal_plaintext(&bytes, ValueType::Keyword, &known).unwrap(),
        Value::Keyword(durable)
    );
}

#[test]
fn seal_plaintext_rejects_nesting_and_type_confusion() {
    let interner = KeywordInterner::default();
    let sealed = Value::Sealed(Sealed {
        class: EntityId::from_raw(100),
        epoch: 1,
        vtype: ValueType::Str,
        body: Arc::from(vec![0_u8; 16]),
    });
    assert_eq!(
        encode_seal_plaintext(&sealed, &interner),
        Err(SealPlaintextError::Nested)
    );

    let bytes = encode_seal_plaintext(&Value::Long(7), &interner).unwrap();
    assert_eq!(
        decode_seal_plaintext(&bytes, ValueType::Str, &interner),
        Err(SealPlaintextError::TypeMismatch {
            expected: ValueType::Str,
            actual: ValueType::Long,
        })
    );
}

#[test]
fn retracting_a_sealed_fact_shares_the_assertion_prefix() {
    let assertion = Datom {
        e: EntityId::from_raw(42),
        a: EntityId::from_raw(7),
        v: Value::Sealed(Sealed {
            class: EntityId::from_raw(100),
            epoch: 3,
            // A body with an embedded zero exercises the escape encoding
            // inside the sealed layout.
            body: Arc::from(vec![1_u8, 0, 2, 0, 0, 3]),
            vtype: ValueType::Str,
        }),
        tx: EntityId::from_raw(19),
        added: true,
    };
    let retraction = Datom {
        tx: EntityId::from_raw(20),
        added: false,
        ..assertion.clone()
    };
    for order in [
        IndexOrder::Eavt,
        IndexOrder::Aevt,
        IndexOrder::Avet,
        IndexOrder::Vaet,
    ] {
        let key = assertion.key(order);
        assert_eq!(Datom::from_key(order, &key), Ok(assertion.clone()));
        assert_eq!(key_components(&key).len(), key.len() - KEY_TX_SUFFIX_LEN);
        assert_eq!(
            key_components(&key),
            key_components(&retraction.key(order)),
            "a keyless transactor pairs a sealed retraction bytewise"
        );
    }
}

#[test]
fn datom_key_composition_for_all_index_orders() {
    let d = Datom {
        e: EntityId::new(2, 10),
        a: EntityId::new(0, 20),
        v: Value::Long(30),
        tx: EntityId::new(1, 40),
        added: true,
    };
    for order in [
        IndexOrder::Eavt,
        IndexOrder::Aevt,
        IndexOrder::Avet,
        IndexOrder::Vaet,
    ] {
        assert!(!d.key(order).is_empty());
    }
    assert!(
        d.key(IndexOrder::Eavt)
            .starts_with(&EntityId::new(2, 10).raw().to_be_bytes())
    );
    assert!(
        d.key(IndexOrder::Aevt)
            .starts_with(&EntityId::new(0, 20).raw().to_be_bytes())
    );
    assert!(
        d.key(IndexOrder::Avet)
            .starts_with(&EntityId::new(0, 20).raw().to_be_bytes())
    );
    assert!(
        d.key(IndexOrder::Vaet)
            .starts_with(&encode_value(&Value::Long(30)))
    );
}
