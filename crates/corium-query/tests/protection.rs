//! What a reader that cannot hydrate a sealed value gets from the query
//! engine: never a match by accident, never a value-ordered answer.

use std::sync::Arc;

use corium_core::{EntityId, Partition, Sealed, Value, ValueType};
use corium_query::{
    QueryError,
    aggregate::{AggOut, apply},
    builtins::{CallResult, call},
};

const CLASS: EntityId = EntityId::new(Partition::Db as u32, 100);

fn sealed(body: &[u8]) -> Value {
    Value::Sealed(Sealed {
        class: CLASS,
        epoch: 1,
        vtype: ValueType::Long,
        body: Arc::from(body),
    })
}

#[test]
fn no_predicate_is_satisfied_by_a_value_the_reader_cannot_open() {
    for name in ["<", "<=", ">", ">=", "=", "==", "!=", "not="] {
        assert_eq!(
            call(name, &[sealed(b"aaaa"), Value::Long(5)]),
            Ok(CallResult::Test(false)),
            "{name} must not be satisfied by a sealed operand"
        );
        assert_eq!(
            call(name, &[Value::Long(5), sealed(b"aaaa")]),
            Ok(CallResult::Test(false)),
            "{name} must not be satisfied by a sealed operand"
        );
    }
    // Two identical ciphertexts are not declared equal either: the reader
    // knows the bytes match, not the values, and saying so is the caller's
    // question to ask by joining, not by asserting equality.
    assert_eq!(
        call("=", &[sealed(b"aaaa"), sealed(b"aaaa")]),
        Ok(CallResult::Test(false))
    );
}

#[test]
fn value_ordered_aggregates_refuse_sealed_values() {
    let values = vec![sealed(b"aaaa"), sealed(b"bbbb")];
    for op in ["min", "max", "sum", "avg", "median", "variance", "stddev"] {
        assert_eq!(
            apply(op, None, &values),
            Err(QueryError::Protected(CLASS)),
            "{op} must refuse sealed values"
        );
    }
    assert_eq!(
        apply("min", Some(1), &values),
        Err(QueryError::Protected(CLASS))
    );
    assert_eq!(
        call("min", &values),
        Err(QueryError::Protected(CLASS)),
        "the min function refuses them too"
    );
}

#[test]
fn counting_and_grouping_still_work() {
    let values = vec![sealed(b"aaaa"), sealed(b"bbbb"), sealed(b"aaaa")];
    assert_eq!(
        apply("count", None, &values),
        Ok(AggOut::Value(Value::Long(3)))
    );
    // Determinism already tells a keyless reader which values repeat; this
    // is the documented equality leak, not a new one.
    assert_eq!(
        apply("count-distinct", None, &values),
        Ok(AggOut::Value(Value::Long(2)))
    );
    let AggOut::Set(distinct) = apply("distinct", None, &values).expect("distinct works") else {
        panic!("distinct returns a set")
    };
    assert_eq!(distinct.len(), 2);
}

#[test]
fn a_sealed_value_renders_redacted_in_string_context() {
    assert_eq!(
        call("str", &[sealed(b"aaaa")]),
        Ok(CallResult::Scalar(Value::Str("#corium/redacted".into())))
    );
}
