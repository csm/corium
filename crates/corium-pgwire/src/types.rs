//! Mapping between Corium's SQL row model and `PostgreSQL` wire types.
//!
//! A value becomes a `RowDescription` type OID plus either `PostgreSQL` text or
//! binary data in a `DataRow`. Bound inputs likewise accept supported text and
//! binary parameter encodings.

use std::fmt::Write as _;

use chrono::DateTime;
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
const OID_VARCHAR: i32 = 1043;
const OID_UUID: i32 = 2950;

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
        SqlValue::Unspecified(text) | SqlValue::Text(text) | SqlValue::Other(text) => {
            Some(text.clone().into_bytes())
        }
        SqlValue::Bytes(bytes) => Some(format_bytea(bytes).into_bytes()),
        SqlValue::List(values) => Some(format_array(values).into_bytes()),
    }
}

/// Encodes one value in the requested `PostgreSQL` result format.
pub(crate) fn encode_result(
    value: &SqlValue,
    sql_type: &SqlType,
    format: i16,
) -> Result<Option<Vec<u8>>, String> {
    match format {
        0 => Ok(encode_value(value)),
        1 => encode_binary_result(value, sql_type),
        other => Err(format!("unknown result format code {other}")),
    }
}

fn encode_binary_result(value: &SqlValue, sql_type: &SqlType) -> Result<Option<Vec<u8>>, String> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let oid = type_oid(sql_type);
    let bytes = match (oid, value) {
        (OID_BOOL, SqlValue::Boolean(value)) => vec![u8::from(*value)],
        (OID_INT2, SqlValue::Integer(value)) => i16::try_from(*value)
            .map_err(|_| "value does not fit PostgreSQL int2")?
            .to_be_bytes()
            .to_vec(),
        (OID_INT4, SqlValue::Integer(value)) => i32::try_from(*value)
            .map_err(|_| "value does not fit PostgreSQL int4")?
            .to_be_bytes()
            .to_vec(),
        (OID_INT4, SqlValue::Unsigned(value)) => i32::try_from(*value)
            .map_err(|_| "value does not fit PostgreSQL int4")?
            .to_be_bytes()
            .to_vec(),
        (OID_INT8, SqlValue::Integer(value)) => value.to_be_bytes().to_vec(),
        (OID_INT8, SqlValue::Unsigned(value)) => i64::try_from(*value)
            .map_err(|_| "value does not fit PostgreSQL int8")?
            .to_be_bytes()
            .to_vec(),
        (OID_FLOAT4, SqlValue::Float(value)) => {
            #[allow(clippy::cast_possible_truncation)]
            let value = *value as f32;
            value.to_be_bytes().to_vec()
        }
        (OID_FLOAT8, SqlValue::Float(value)) => value.to_be_bytes().to_vec(),
        (OID_TIMESTAMPTZ, SqlValue::TimestampMillis(value)) => value
            .checked_sub(946_684_800_000)
            .and_then(|value| value.checked_mul(1_000))
            .ok_or("timestamp is outside PostgreSQL binary range")?
            .to_be_bytes()
            .to_vec(),
        (OID_NUMERIC, SqlValue::Unsigned(value)) => encode_numeric_u64(*value),
        (OID_NUMERIC, SqlValue::Integer(value)) => encode_numeric_i64(*value),
        (OID_BYTEA, SqlValue::Bytes(value)) => value.clone(),
        (
            OID_TEXT | OID_VARCHAR,
            SqlValue::Unspecified(value) | SqlValue::Text(value) | SqlValue::Other(value),
        ) => value.as_bytes().to_vec(),
        (_, SqlValue::List(values)) if matches!(sql_type, SqlType::List(_)) => {
            let SqlType::List(element_type) = sql_type else {
                unreachable!("guarded")
            };
            encode_binary_array(values, element_type)?
        }
        _ => {
            return Err(format!(
                "cannot encode {value:?} as binary PostgreSQL type OID {oid}"
            ));
        }
    };
    Ok(Some(bytes))
}

fn encode_numeric_i64(value: i64) -> Vec<u8> {
    let negative = value.is_negative();
    let magnitude = value.unsigned_abs();
    encode_numeric(magnitude, negative)
}

fn encode_numeric_u64(value: u64) -> Vec<u8> {
    encode_numeric(value, false)
}

fn encode_numeric(mut magnitude: u64, negative: bool) -> Vec<u8> {
    if magnitude == 0 {
        return vec![0; 8];
    }
    let mut digits = Vec::new();
    while magnitude != 0 {
        digits.push(u16::try_from(magnitude % 10_000).expect("base-10000 digit"));
        magnitude /= 10_000;
    }
    digits.reverse();
    let mut out = Vec::with_capacity(8 + digits.len() * 2);
    out.extend_from_slice(
        &i16::try_from(digits.len())
            .unwrap_or(i16::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(
        &i16::try_from(digits.len() - 1)
            .unwrap_or(i16::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(&(if negative { 0x4000u16 } else { 0 }).to_be_bytes());
    out.extend_from_slice(&0i16.to_be_bytes());
    for digit in digits {
        out.extend_from_slice(&digit.to_be_bytes());
    }
    out
}

fn encode_binary_array(values: &[SqlValue], element_type: &SqlType) -> Result<Vec<u8>, String> {
    if values
        .iter()
        .any(|value| matches!(value, SqlValue::List(_)))
    {
        return Err("nested binary arrays are not supported".into());
    }
    let mut out = Vec::new();
    let dimensions = i32::from(!values.is_empty());
    out.extend_from_slice(&dimensions.to_be_bytes());
    out.extend_from_slice(
        &i32::from(values.iter().any(|value| matches!(value, SqlValue::Null))).to_be_bytes(),
    );
    out.extend_from_slice(&type_oid(element_type).to_be_bytes());
    if !values.is_empty() {
        out.extend_from_slice(
            &i32::try_from(values.len())
                .map_err(|_| "array has too many elements")?
                .to_be_bytes(),
        );
        out.extend_from_slice(&1i32.to_be_bytes());
        for value in values {
            match encode_binary_result(value, element_type)? {
                Some(bytes) => {
                    out.extend_from_slice(
                        &i32::try_from(bytes.len())
                            .map_err(|_| "array element is too large")?
                            .to_be_bytes(),
                    );
                    out.extend_from_slice(&bytes);
                }
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
    }
    Ok(out)
}

/// Decodes one bound `PostgreSQL` parameter.
pub(crate) fn decode_parameter(
    oid: i32,
    format: i16,
    bytes: Option<&[u8]>,
) -> Result<SqlValue, String> {
    let Some(bytes) = bytes else {
        return Ok(SqlValue::Null);
    };
    match format {
        0 => decode_text_parameter(oid, bytes),
        1 => decode_binary_parameter(oid, bytes),
        other => Err(format!("unknown parameter format code {other}")),
    }
}

/// Representative typed value used to plan a statement-level `Describe`
/// before a portal has supplied real parameter values.
pub(crate) fn describe_parameter(oid: i32) -> Result<SqlValue, String> {
    match oid {
        0 => Ok(SqlValue::Unspecified(String::new())),
        OID_TEXT | OID_VARCHAR | OID_UUID => Ok(SqlValue::Text(String::new())),
        OID_BOOL => Ok(SqlValue::Boolean(false)),
        OID_INT2 | OID_INT4 | OID_INT8 | OID_NUMERIC => Ok(SqlValue::Integer(0)),
        OID_FLOAT4 | OID_FLOAT8 => Ok(SqlValue::Float(0.0)),
        OID_TIMESTAMPTZ => Ok(SqlValue::TimestampMillis(0)),
        OID_BYTEA => Ok(SqlValue::Bytes(Vec::new())),
        oid if is_array_oid(oid) => {
            Err("array parameters are not supported yet; use an ARRAY expression".into())
        }
        other => Err(format!("parameter type OID {other} is not supported")),
    }
}

fn decode_text_parameter(oid: i32, bytes: &[u8]) -> Result<SqlValue, String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "parameter is not valid UTF-8")?;
    match oid {
        0 => Ok(SqlValue::Unspecified(text.to_owned())),
        OID_TEXT | OID_VARCHAR | OID_UUID => Ok(SqlValue::Text(text.to_owned())),
        OID_BOOL => match text {
            "t" | "true" | "TRUE" | "1" => Ok(SqlValue::Boolean(true)),
            "f" | "false" | "FALSE" | "0" => Ok(SqlValue::Boolean(false)),
            _ => Err(format!("invalid boolean parameter {text:?}")),
        },
        OID_INT2 | OID_INT4 | OID_INT8 => text
            .parse::<i64>()
            .map(SqlValue::Integer)
            .map_err(|error| format!("invalid integer parameter: {error}")),
        OID_NUMERIC => text
            .parse::<i64>()
            .map(SqlValue::Integer)
            .or_else(|_| text.parse::<u64>().map(SqlValue::Unsigned))
            .or_else(|_| text.parse::<f64>().map(SqlValue::Float))
            .map_err(|error| format!("invalid numeric parameter: {error}")),
        OID_FLOAT4 | OID_FLOAT8 => text
            .parse::<f64>()
            .map(SqlValue::Float)
            .map_err(|error| format!("invalid floating-point parameter: {error}")),
        OID_BYTEA => decode_bytea(text).map(SqlValue::Bytes),
        OID_TIMESTAMPTZ => decode_text_timestamp(text).map(SqlValue::TimestampMillis),
        oid if is_array_oid(oid) => {
            Err("array parameters are not supported yet; use an ARRAY expression".into())
        }
        other => Err(format!("parameter type OID {other} is not supported")),
    }
}

fn decode_text_timestamp(text: &str) -> Result<i64, String> {
    DateTime::parse_from_rfc3339(text)
        .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f%#z"))
        .map(|timestamp| timestamp.timestamp_millis())
        .map_err(|_| {
            format!(
                "invalid timestamptz parameter {text:?}; expected a timestamp with an \
                 explicit UTC offset, e.g. 2021-01-01T00:00:00Z"
            )
        })
}

fn decode_binary_parameter(oid: i32, bytes: &[u8]) -> Result<SqlValue, String> {
    let exact = |expected: usize| {
        (bytes.len() == expected)
            .then_some(())
            .ok_or_else(|| format!("binary parameter requires {expected} bytes"))
    };
    match oid {
        OID_BOOL => {
            exact(1)?;
            match bytes[0] {
                0 => Ok(SqlValue::Boolean(false)),
                1 => Ok(SqlValue::Boolean(true)),
                _ => Err("binary boolean parameter must be zero or one".into()),
            }
        }
        OID_INT2 => {
            exact(2)?;
            Ok(SqlValue::Integer(i64::from(i16::from_be_bytes([
                bytes[0], bytes[1],
            ]))))
        }
        OID_INT4 => {
            exact(4)?;
            Ok(SqlValue::Integer(i64::from(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))))
        }
        OID_INT8 => {
            exact(8)?;
            Ok(SqlValue::Integer(i64::from_be_bytes(
                bytes.try_into().expect("length checked"),
            )))
        }
        OID_FLOAT4 => {
            exact(4)?;
            Ok(SqlValue::Float(f64::from(f32::from_bits(
                u32::from_be_bytes(bytes.try_into().expect("length checked")),
            ))))
        }
        OID_FLOAT8 => {
            exact(8)?;
            Ok(SqlValue::Float(f64::from_bits(u64::from_be_bytes(
                bytes.try_into().expect("length checked"),
            ))))
        }
        OID_NUMERIC => decode_binary_numeric(bytes),
        0 | OID_TEXT | OID_VARCHAR => std::str::from_utf8(bytes)
            .map(|text| {
                if oid == 0 {
                    SqlValue::Unspecified(text.to_owned())
                } else {
                    SqlValue::Text(text.to_owned())
                }
            })
            .map_err(|_| "binary text parameter is not valid UTF-8".into()),
        OID_BYTEA => Ok(SqlValue::Bytes(bytes.to_vec())),
        OID_TIMESTAMPTZ => {
            exact(8)?;
            let micros = i64::from_be_bytes(bytes.try_into().expect("length checked"));
            let millis = micros
                .div_euclid(1_000)
                .checked_add(946_684_800_000)
                .ok_or("binary timestamptz parameter is outside the supported range")?;
            Ok(SqlValue::TimestampMillis(millis))
        }
        OID_UUID => {
            exact(16)?;
            let text = bytes
                .iter()
                .fold(String::with_capacity(32), |mut text, byte| {
                    let _ = write!(text, "{byte:02x}");
                    text
                });
            Ok(SqlValue::Text(text))
        }
        other => Err(format!(
            "binary parameter type OID {other} is not supported"
        )),
    }
}

fn decode_binary_numeric(bytes: &[u8]) -> Result<SqlValue, String> {
    if bytes.len() < 8 || !(bytes.len() - 8).is_multiple_of(2) {
        return Err("binary numeric parameter has an invalid length".into());
    }
    let ndigits = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
    if bytes.len() != 8 + ndigits * 2 {
        return Err("binary numeric digit count does not match its length".into());
    }
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = u16::from_be_bytes([bytes[6], bytes[7]]);
    let digits = bytes[8..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u16::from_be_bytes(*pair))
        .collect::<Vec<_>>();
    if digits.iter().any(|digit| *digit >= 10_000) {
        return Err("binary numeric contains an invalid base-10000 digit".into());
    }
    match sign {
        0xc000 => return Ok(SqlValue::Float(f64::NAN)),
        0xd000 => return Ok(SqlValue::Float(f64::INFINITY)),
        0xf000 => return Ok(SqlValue::Float(f64::NEG_INFINITY)),
        0x0000 | 0x4000 => {}
        _ => return Err("binary numeric contains an invalid sign".into()),
    }

    if dscale == 0 && weight >= 0 {
        let groups = usize::try_from(weight).unwrap_or(usize::MAX) + 1;
        if digits.iter().skip(groups).any(|digit| *digit != 0) {
            return Err("scale-zero binary numeric contains fractional digits".into());
        }
        let mut magnitude = 0u128;
        for index in 0..groups {
            magnitude = magnitude
                .checked_mul(10_000)
                .and_then(|value| {
                    value.checked_add(u128::from(digits.get(index).copied().unwrap_or(0)))
                })
                .ok_or("binary numeric is outside the supported integer range")?;
        }
        if sign == 0x4000 {
            let limit = u128::from(i64::MAX.unsigned_abs()) + 1;
            if magnitude > limit {
                return Err("binary numeric is outside the supported signed range".into());
            }
            let value = if magnitude == limit {
                i64::MIN
            } else {
                -i64::try_from(magnitude).expect("range checked")
            };
            return Ok(SqlValue::Integer(value));
        }
        if let Ok(value) = i64::try_from(magnitude) {
            return Ok(SqlValue::Integer(value));
        }
        return u64::try_from(magnitude)
            .map(SqlValue::Unsigned)
            .map_err(|_| "binary numeric is outside the supported unsigned range".into());
    }

    let mut value = 0.0;
    for (index, digit) in digits.iter().enumerate() {
        let exponent = i32::from(weight)
            - i32::try_from(index).map_err(|_| "binary numeric has too many digits")?;
        value += f64::from(*digit) * 10_000f64.powi(exponent);
    }
    if sign == 0x4000 {
        value = -value;
    }
    Ok(SqlValue::Float(value))
}

fn decode_bytea(text: &str) -> Result<Vec<u8>, String> {
    let Some(hex) = text.strip_prefix("\\x") else {
        return Err("only hexadecimal bytea input is supported".into());
    };
    let hex = hex.as_bytes();
    if !hex.len().is_multiple_of(2) {
        return Err("bytea hex input has odd length".into());
    }
    hex.as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|digits| u8::from_str_radix(digits, 16).ok())
                .ok_or_else(|| "invalid bytea hex".to_owned())
        })
        .collect()
}

const fn is_array_oid(oid: i32) -> bool {
    matches!(
        oid,
        OID_BOOL_ARRAY
            | OID_BYTEA_ARRAY
            | OID_INT2_ARRAY
            | OID_INT4_ARRAY
            | OID_TEXT_ARRAY
            | OID_INT8_ARRAY
            | OID_FLOAT4_ARRAY
            | OID_FLOAT8_ARRAY
            | OID_TIMESTAMPTZ_ARRAY
            | OID_NUMERIC_ARRAY
    )
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
        | SqlValue::Unspecified(_)
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
    // Keep the integer calendar conversion rather than routing output through
    // chrono: SqlValue can hold the full i64 millisecond range, which is wider
    // than chrono's representable DateTime range.
    let days = millis.div_euclid(86_400_000);
    let time_of_day = millis.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    let hours = time_of_day / 3_600_000;
    let minutes = (time_of_day / 60_000) % 60;
    let seconds = (time_of_day / 1_000) % 60;
    let sub_millis = time_of_day % 1_000;
    let mut out = format!("{year:04}-{month:02}-{day:02} {hours:02}:{minutes:02}:{seconds:02}");
    if sub_millis != 0 {
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
    fn text_and_binary_parameters_decode_to_typed_values() {
        assert_eq!(
            decode_parameter(OID_INT8, 0, Some(b"2001")).unwrap(),
            SqlValue::Integer(2001)
        );
        assert_eq!(
            decode_parameter(OID_INT8, 1, Some(&2001i64.to_be_bytes())).unwrap(),
            SqlValue::Integer(2001)
        );
        assert_eq!(decode_parameter(OID_TEXT, 0, None).unwrap(), SqlValue::Null);
        assert_eq!(
            decode_parameter(0, 0, Some(b"2001")).unwrap(),
            SqlValue::Unspecified("2001".into())
        );
    }

    #[test]
    fn numeric_text_prefers_signed_integer_before_unsigned() {
        assert_eq!(
            decode_parameter(OID_NUMERIC, 0, Some(b"42")).unwrap(),
            SqlValue::Integer(42)
        );
        assert_eq!(
            decode_parameter(OID_NUMERIC, 0, Some(b"18446744073709551615")).unwrap(),
            SqlValue::Unsigned(u64::MAX)
        );
    }

    #[test]
    fn text_timestamptz_accepts_iso_8601_and_postgresql_offsets() {
        assert_eq!(
            decode_parameter(OID_TIMESTAMPTZ, 0, Some(b"2021-01-01T00:00:00.123Z")).unwrap(),
            SqlValue::TimestampMillis(1_609_459_200_123)
        );
        assert_eq!(
            decode_parameter(OID_TIMESTAMPTZ, 0, Some(b"2021-01-01 01:30:00+01:30")).unwrap(),
            SqlValue::TimestampMillis(1_609_459_200_000)
        );
        for text in [
            "2021-01-01T00:00:00+00",
            "2021-01-01T00:00:00+0000",
            "2021-01-01 00:00:00Z",
        ] {
            assert_eq!(decode_text_timestamp(text), Ok(1_609_459_200_000), "{text}");
        }
    }

    #[test]
    fn text_timestamptz_round_trips_output_and_handles_pre_epoch_values() {
        for millis in [0, 1_609_459_200_123, -500, i64::from(i32::MAX) * 1_000] {
            let text = format_timestamp(millis);
            assert_eq!(decode_text_timestamp(&text), Ok(millis), "{text}");
        }
        assert_eq!(
            decode_text_timestamp("1970-01-01T00:00:00.999999Z"),
            Ok(999)
        );
    }

    #[test]
    fn text_timestamptz_errors_explain_that_an_offset_is_required() {
        for text in ["not-a-timestamp", "2021-01-01 00:00:00"] {
            let error = decode_text_timestamp(text).unwrap_err();
            assert!(error.contains(text), "{error}");
            assert!(error.contains("explicit UTC offset"), "{error}");
        }
    }

    #[test]
    fn binary_parameter_decoders_cover_supported_scalar_types() {
        assert_eq!(
            decode_parameter(OID_BOOL, 1, Some(&[1])).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            decode_parameter(OID_INT2, 1, Some(&(-7i16).to_be_bytes())).unwrap(),
            SqlValue::Integer(-7)
        );
        assert_eq!(
            decode_parameter(OID_INT4, 1, Some(&42i32.to_be_bytes())).unwrap(),
            SqlValue::Integer(42)
        );
        assert_eq!(
            decode_parameter(OID_FLOAT4, 1, Some(&1.5f32.to_be_bytes())).unwrap(),
            SqlValue::Float(1.5)
        );
        assert_eq!(
            decode_parameter(OID_FLOAT8, 1, Some(&2.5f64.to_be_bytes())).unwrap(),
            SqlValue::Float(2.5)
        );
        assert_eq!(
            decode_parameter(OID_TEXT, 1, Some(b"hello")).unwrap(),
            SqlValue::Text("hello".into())
        );
        assert_eq!(
            decode_parameter(OID_BYTEA, 1, Some(&[0, 255])).unwrap(),
            SqlValue::Bytes(vec![0, 255])
        );
        assert_eq!(
            decode_parameter(OID_TIMESTAMPTZ, 1, Some(&0i64.to_be_bytes())).unwrap(),
            SqlValue::TimestampMillis(946_684_800_000)
        );
        assert!(matches!(
            decode_parameter(OID_UUID, 1, Some(&[0x12; 16])).unwrap(),
            SqlValue::Text(value) if value == "12121212121212121212121212121212"
        ));
        assert!(decode_parameter(OID_INT8, 1, Some(&[0; 7])).is_err());
        assert!(decode_parameter(OID_BOOL, 1, Some(&[2])).is_err());
    }

    #[test]
    fn binary_results_encode_numeric_timestamp_and_arrays() {
        let numeric = encode_result(
            &SqlValue::Unsigned(18_446_744_073_709_551_615),
            &SqlType::UnsignedInteger(64),
            1,
        )
        .unwrap()
        .unwrap();
        assert_eq!(i16::from_be_bytes([numeric[0], numeric[1]]), 5);
        assert_eq!(i16::from_be_bytes([numeric[2], numeric[3]]), 4);
        assert_eq!(
            decode_parameter(OID_NUMERIC, 1, Some(&numeric)).unwrap(),
            SqlValue::Unsigned(u64::MAX)
        );

        let timestamp = encode_result(
            &SqlValue::TimestampMillis(946_684_800_001),
            &SqlType::TimestampMillis(Some("UTC".into())),
            1,
        )
        .unwrap()
        .unwrap();
        assert_eq!(i64::from_be_bytes(timestamp.try_into().unwrap()), 1_000);

        let array = encode_result(
            &SqlValue::List(vec![SqlValue::Integer(1), SqlValue::Integer(2)]),
            &SqlType::List(Box::new(SqlType::SignedInteger(64))),
            1,
        )
        .unwrap()
        .unwrap();
        assert_eq!(i32::from_be_bytes(array[0..4].try_into().unwrap()), 1);
        assert_eq!(i32::from_be_bytes(array[12..16].try_into().unwrap()), 2);
    }

    #[test]
    fn bytea_uses_hex_output() {
        let value = SqlValue::Bytes(vec![0x00, 0xff, 0x42]);
        assert_eq!(encode_value(&value), Some(b"\\x00ff42".to_vec()));
    }

    #[test]
    fn bytea_rejects_non_ascii_hex_without_panicking() {
        assert_eq!(
            decode_parameter(OID_BYTEA, 0, Some("\\x€0".as_bytes())),
            Err("invalid bytea hex".to_owned())
        );
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
