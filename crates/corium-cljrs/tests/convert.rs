//! Boundary conversion round-trips between cljrs values and engine EDN,
//! including the `cljrs-reader` text bridge.

use corium_cljrs::convert::{ConvertError, from_edn, read_edn, to_edn};
use corium_query::edn::{Edn, read_one};

fn round_trip(text: &str) {
    let _mutator = cljrs_gc::register_mutator();
    let form = read_one(text).expect("edn");
    let value = from_edn(&form);
    let back = to_edn(&value).expect("convertible");
    assert_eq!(back, form, "round trip of {text}");
}

#[test]
fn scalars_round_trip() {
    for text in [
        "nil",
        "true",
        "false",
        "0",
        "-42",
        "3.25",
        "\"hello\"",
        ":kw",
        ":ns/kw",
        "sym",
    ] {
        round_trip(text);
    }
}

#[test]
fn collections_round_trip() {
    for text in [
        "[1 2 3]",
        "(1 (2) [3])",
        "{:a 1 :b {:c 2}}",
        "#{1 2 3}",
        "[{:a [1]} #{:x} \"s\"]",
    ] {
        round_trip(text);
    }
}

#[test]
fn tagged_values_round_trip() {
    // Native cljrs shapes.
    round_trip("#uuid \"0123456789abcdef0123456789abcdef\"");
    round_trip("#bytes \"deadbeef\"");
    // Metadata-carried tags (no native cljrs shape).
    round_trip("#inst 1700000000000");
    round_trip("#eid 17");
}

#[test]
fn instants_compare_transparently_with_longs() {
    let _mutator = cljrs_gc::register_mutator();
    let tagged = from_edn(&read_one("#inst 1700000000000").expect("edn"));
    let plain = from_edn(&Edn::Long(1_700_000_000_000));
    // cljrs metadata is equality-transparent, matching the boundary table.
    assert_eq!(tagged, plain);
}

#[test]
fn unrepresentable_values_error() {
    let _mutator = cljrs_gc::register_mutator();
    let globals = cljrs_interp::standard_env_minimal(None, None, None);
    let mut env = cljrs_env::env::Env::new(globals, "user");
    let mut parser = cljrs_reader::Parser::new("(fn [x] x)".to_owned(), "<t>".to_owned());
    let forms = parser.parse_all().expect("parse");
    let value = cljrs_interp::eval::eval(&forms[0], &mut env).expect("eval");
    assert!(matches!(to_edn(&value), Err(ConvertError::Unsupported(_))));
}

#[test]
fn reader_bridge_parses_edn_text() {
    let forms = read_edn("{:a 1} [1 2] :kw ; comment\n \"str\"").expect("read");
    assert_eq!(
        forms,
        vec![
            read_one("{:a 1}").expect("edn"),
            read_one("[1 2]").expect("edn"),
            read_one(":kw").expect("edn"),
            read_one("\"str\"").expect("edn"),
        ]
    );
}

#[test]
fn lazy_sequences_realize_on_conversion() {
    let _mutator = cljrs_gc::register_mutator();
    let globals = cljrs_interp::standard_env_minimal(None, None, None);
    let mut env = cljrs_env::env::Env::new(globals, "user");
    let mut parser = cljrs_reader::Parser::new("(map inc [1 2 3])".to_owned(), "<t>".to_owned());
    let forms = parser.parse_all().expect("parse");
    let value = cljrs_interp::eval::eval(&forms[0], &mut env).expect("eval");
    assert_eq!(
        to_edn(&value).expect("edn"),
        read_one("(2 3 4)").expect("edn")
    );
}
