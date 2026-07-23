//! Mapping between Corium's SQL row model and `PostgreSQL` wire types.
//!
//! The server speaks the text wire format exclusively, so a value becomes a
//! `RowDescription` type OID plus a UTF-8 rendering placed in a `DataRow`.

use corium_sql::{SqlType, SqlValue};

// PostgreSQL built-in type OIDs (from `pg_type`).
const OID_BOOL: i32 = 16;
const OID_BYTEA: i32 = 17;
const OID_INT8: i32 = 20;
const OID_INT2: i32 = 21;
const OID_INT4: i32 = 23;
const OID_TEXT: i32 = 25;
const OID_FLOAT4: i32 = 700;
const OID_FLOAT8: i32 = 701;
const OID_TIMESTAMPTZ: i32 = 1184;
const OID_NUMERIC: i32 = 1700;

// Array type OIDs.
const OID_BOOL_ARRAY: i32 = 1000;
const OID_BYTEA_ARRAY: i32 = 1001;
const OID_INT2_ARRAY: i32 = 1005;
const OID_INT4_ARRAY: i32 = 1007;
const OID_TEXT_ARRAY: i32 = 1009;
const OID_INT8_ARRAY: i32 = 1016;
const OID_FLOAT4_ARRAY: i32 = 1021;
const OID_FLOAT8_ARRAY: i32 = 1022;
const OID_TIMESTAMPTZ_ARRAY: i32 = 1185;
const OID_NUMERIC_ARRAY: i32 = 1231;

/// The `PostgreSQL` type OID advertised for a Corium SQL column type.
///
/// Distinct Corium types intentionally share an OID (for example both signed
/// and unsigned 32-bit integers map to `int4`), so the arms are kept separate
/// for documentation rather than merged.
#[must_use]
#[allow(clippy::match_same_arms)]
pub(crate) fn type_oid(sql_type: &SqlType) -> i32 {
    match sql_type {
        SqlType::Null => OID_TEXT,
        SqlType::Boolean => OID_BOOL,
        SqlType::SignedInteger(8 | 16) => OID_INT2,
        SqlType::SignedInteger(32) => OID_INT4,
        SqlType::SignedInteger(_) => OID_INT8,
        // PostgreSQL has no unsigned integers. 8/16/32-bit values fit an int4
        // or int8; 64-bit unsigned values (e.g. entity ids) may exceed
        // int8, so numeric keeps them lossless in the text format.
        SqlType::UnsignedInteger(8 | 16) => OID_INT4,
        SqlType::UnsignedInteger(32) => OID_INT8,
        SqlType::UnsignedInteger(_) => OID_NUMERIC,
        SqlType::Float(16 | 32) => OID_FLOAT4,
        SqlType::Float(_) => OID_FLOAT8,
        SqlType::TimestampMillis(_) => OID_TIMESTAMPTZ,
        SqlType::Text => OID_TEXT,
        SqlType::Bytes => OID_BYTEA,
        SqlType::List(inner) => array_oid(inner),
        SqlType::Other(_) => OID_TEXT,
    }
}

/// The advertised type length in bytes, or -1 for variable-length types.
#[must_use]
pub(crate) fn type_len(oid: i32) -> i16 {
    match oid {
        OID_BOOL => 1,
        OID_INT2 => 2,
        OID_INT4 | OID_FLOAT4 => 4,
        OID_INT8 | OID_FLOAT8 | OID_TIMESTAMPTZ => 8,
        _ => -1,
    }
}

/// The array type OID whose element type is `element`.
fn array_oid(element: &SqlType) -> i32 {
    match type_oid(element) {
        OID_BOOL => OID_BOOL_ARRAY,
        OID_BYTEA => OID_BYTEA_ARRAY,
        OID_INT2 => OID_INT2_ARRAY,
        OID_INT4 => OID_INT4_ARRAY,
        OID_INT8 => OID_INT8_ARRAY,
        OID_FLOAT4 => OID_FLOAT4_ARRAY,
        OID_FLOAT8 => OID_FLOAT8_ARRAY,
        OID_TIMESTAMPTZ => OID_TIMESTAMPTZ_ARRAY,
        OID_NUMERIC => OID_NUMERIC_ARRAY,
        _ => OID_TEXT_ARRAY,
    }
}

/// Encodes one value in the `PostgreSQL` text wire format, or `None` for NULL.
#[must_use]
pub(crate) fn encode_value(value: &SqlValue) -> Option<Vec<u8>> {
    match value {
        SqlValue::Null => None,
        SqlValue::Boolean(true) => Some(b"t".to_vec()),
        SqlValue::Boolean(false) => Some(b"f".to_vec()),
        SqlValue::Integer(value) => Some(value.to_string().into_bytes()),
        SqlValue::Unsigned(value) => Some(value.to_string().into_bytes()),
        SqlValue::Float(value) => Some(format_float(*value).into_bytes()),
        SqlValue::TimestampMillis(millis) => Some(format_timestamp(*millis).into_bytes()),
        SqlValue::Text(text) | SqlValue::Other(text) => Some(text.clone().into_bytes()),
        SqlValue::Bytes(bytes) => Some(format_bytea(bytes).into_bytes()),
        SqlValue::List(values) => Some(format_array(values).into_bytes()),
    }
}

/// Formats a float the way `PostgreSQL` renders `float8`/`float4` text.
fn format_float(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() {
        if value.is_sign_negative() {
            "-Infinity".to_owned()
        } else {
            "Infinity".to_owned()
        }
    } else {
        // Rust's default float formatting is the shortest round-trippable
        // representation, matching PostgreSQL's `extra_float_digits = 1`.
        value.to_string()
    }
}

/// Formats bytes as `PostgreSQL` hex `bytea` output (`\x` prefix).
fn format_bytea(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Formats a list as a `PostgreSQL` array literal (`{a,b,c}`).
fn format_array(values: &[SqlValue]) -> String {
    let mut out = String::from("{");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        push_array_element(&mut out, value);
    }
    out.push('}');
    out
}

/// Appends one array element, quoting and escaping where `PostgreSQL` requires.
fn push_array_element(out: &mut String, value: &SqlValue) {
    match value {
        SqlValue::Null => out.push_str("NULL"),
        SqlValue::List(values) => out.push_str(&format_array(values)),
        SqlValue::Boolean(_)
        | SqlValue::Integer(_)
        | SqlValue::Unsigned(_)
        | SqlValue::Float(_) => {
            if let Some(bytes) = encode_value(value) {
                out.push_str(&String::from_utf8_lossy(&bytes));
            }
        }
        SqlValue::TimestampMillis(_)
        | SqlValue::Text(_)
        | SqlValue::Bytes(_)
        | SqlValue::Other(_) => {
            if let Some(bytes) = encode_value(value) {
                out.push('"');
                for character in String::from_utf8_lossy(&bytes).chars() {
                    if character == '"' || character == '\\' {
                        out.push('\\');
                    }
                    out.push(character);
                }
                out.push('"');
            }
        }
    }
}

/// Formats epoch milliseconds as a `timestamptz` in UTC (`YYYY-MM-DD HH:MM:SS[.mmm]+00`).
fn format_timestamp(millis: i64) -> String {
    let days = millis.div_euclid(86_400_000);
    let time_of_day = millis.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    let hours = time_of_day / 3_600_000;
    let minutes = (time_of_day / 60_000) % 60;
    let seconds = (time_of_day / 1_000) % 60;
    let sub_millis = time_of_day % 1_000;
    let mut out = format!("{year:04}-{month:02}-{day:02} {hours:02}:{minutes:02}:{seconds:02}");
    if sub_millis != 0 {
        use std::fmt::Write as _;
        let _ = write!(out, ".{sub_millis:03}");
    }
    out.push_str("+00");
    out
}

/// Converts a count of days since the Unix epoch to a `(year, month, day)`
/// civil date, using Howard Hinnant's `civil_from_days` algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = u32::try_from(day_of_year - (153 * month_position + 2) / 5 + 1).unwrap_or(1);
    let month = u32::try_from(if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    })
    .unwrap_or(1);
    let year = year + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_map_to_narrowest_pg_type() {
        assert_eq!(type_oid(&SqlType::SignedInteger(16)), OID_INT2);
        assert_eq!(type_oid(&SqlType::SignedInteger(32)), OID_INT4);
        assert_eq!(type_oid(&SqlType::SignedInteger(64)), OID_INT8);
        assert_eq!(type_oid(&SqlType::UnsignedInteger(64)), OID_NUMERIC);
    }

    #[test]
    fn list_of_text_maps_to_text_array() {
        let list = SqlType::List(Box::new(SqlType::Text));
        assert_eq!(type_oid(&list), OID_TEXT_ARRAY);
    }

    #[test]
    fn booleans_render_as_single_letters() {
        assert_eq!(encode_value(&SqlValue::Boolean(true)), Some(b"t".to_vec()));
        assert_eq!(encode_value(&SqlValue::Boolean(false)), Some(b"f".to_vec()));
    }

    #[test]
    fn null_encodes_as_none() {
        assert_eq!(encode_value(&SqlValue::Null), None);
    }

    #[test]
    fn bytea_uses_hex_output() {
        let value = SqlValue::Bytes(vec![0x00, 0xff, 0x42]);
        assert_eq!(encode_value(&value), Some(b"\\x00ff42".to_vec()));
    }

    #[test]
    fn text_array_quotes_and_escapes() {
        let value = SqlValue::List(vec![
            SqlValue::Text("ambient".into()),
            SqlValue::Text("a\"b\\c".into()),
        ]);
        assert_eq!(
            encode_value(&value),
            Some(br#"{"ambient","a\"b\\c"}"#.to_vec())
        );
    }

    #[test]
    fn integer_array_is_unquoted() {
        let value = SqlValue::List(vec![SqlValue::Integer(1), SqlValue::Integer(2)]);
        assert_eq!(encode_value(&value), Some(b"{1,2}".to_vec()));
    }

    #[test]
    fn timestamp_formats_as_utc() {
        // 2021-01-01T00:00:00Z is 1_609_459_200_000 ms.
        assert_eq!(
            format_timestamp(1_609_459_200_000),
            "2021-01-01 00:00:00+00"
        );
        // With sub-second milliseconds.
        assert_eq!(
            format_timestamp(1_609_459_200_123),
            "2021-01-01 00:00:00.123+00"
        );
        // The Unix epoch itself.
        assert_eq!(format_timestamp(0), "1970-01-01 00:00:00+00");
    }

    #[test]
    fn special_floats_use_postgres_spelling() {
        assert_eq!(format_float(f64::NAN), "NaN");
        assert_eq!(format_float(f64::INFINITY), "Infinity");
        assert_eq!(format_float(f64::NEG_INFINITY), "-Infinity");
    }
}
