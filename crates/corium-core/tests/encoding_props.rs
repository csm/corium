//! Property tests for M0 sortable encoding and datom keys.

use std::sync::Arc;

use corium_core::{
    Datom, EntityId, IndexOrder, TotalF64, Value,
    encoding::{decode_value, encode_value},
};
use proptest::prelude::*;

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
        any::<u64>().prop_map(|bits| Value::Double(TotalF64(f64::from_bits(bits))))
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
