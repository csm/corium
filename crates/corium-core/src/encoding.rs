//! Sortable binary encoding for values and datom key components.

use std::sync::Arc;

use thiserror::Error;

use crate::{EntityId, TotalF64, Value};

const BOOL: u8 = 0x10;
const LONG: u8 = 0x20;
const DOUBLE: u8 = 0x30;
const INSTANT: u8 = 0x40;
const UUID: u8 = 0x50;
const KEYWORD: u8 = 0x60;
const STR: u8 = 0x70;
const BYTES: u8 = 0x80;
const REF: u8 = 0x90;

/// Decoding failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    /// Input ended before a complete value was read.
    #[error("truncated sortable value")]
    Truncated,
    /// Type tag is not known.
    #[error("unknown value tag {0:#x}")]
    UnknownTag(u8),
    /// Escaped byte sequence is invalid.
    #[error("invalid escaped bytes")]
    InvalidEscape,
    /// UTF-8 string payload is invalid.
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    /// A complete value was followed by unexpected bytes.
    #[error("trailing bytes after sortable value")]
    Trailing,
}

/// Trait for types with Corium sortable encodings.
pub trait Encodable {
    /// Appends this value's encoding to `out`.
    fn encode_into(&self, out: &mut Vec<u8>);
}

/// Encodes a value into a fresh vector.
#[must_use]
pub fn encode_value(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    value.encode_into(&mut out);
    out
}

impl Encodable for EntityId {
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.raw().to_be_bytes());
    }
}
impl Encodable for u64 {
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_be_bytes());
    }
}
impl Encodable for i64 {
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(
            &(u64::from_be_bytes(self.to_be_bytes()) ^ (1_u64 << 63)).to_be_bytes(),
        );
    }
}

impl Encodable for Value {
    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Self::Bool(v) => out.extend_from_slice(&[BOOL, u8::from(*v)]),
            Self::Long(v) => {
                out.push(LONG);
                v.encode_into(out);
            }
            Self::Double(v) => {
                out.push(DOUBLE);
                out.extend_from_slice(&v.sortable_bits().to_be_bytes());
            }
            Self::Instant(v) => {
                out.push(INSTANT);
                v.encode_into(out);
            }
            Self::Uuid(v) => {
                out.push(UUID);
                out.extend_from_slice(&v.to_be_bytes());
            }
            Self::Keyword(v) => {
                out.push(KEYWORD);
                v.encode_into(out);
            }
            Self::Str(v) => {
                out.push(STR);
                encode_escaped(v.as_bytes(), out);
            }
            Self::Bytes(v) => {
                out.push(BYTES);
                encode_escaped(v, out);
            }
            Self::Ref(v) => {
                out.push(REF);
                v.encode_into(out);
            }
        }
    }
}

/// Decodes one complete value and returns the value plus bytes consumed.
///
/// # Errors
///
/// Returns [`DecodeError`] when input is truncated, has an unknown tag, contains
/// malformed escape sequences, or carries invalid UTF-8 for strings.
pub fn decode_value(input: &[u8]) -> Result<(Value, usize), DecodeError> {
    let Some((&tag, rest)) = input.split_first() else {
        return Err(DecodeError::Truncated);
    };
    let fixed =
        |n: usize| -> Result<&[u8], DecodeError> { rest.get(..n).ok_or(DecodeError::Truncated) };
    Ok(match tag {
        BOOL => (
            Value::Bool(*fixed(1)?.first().ok_or(DecodeError::Truncated)? != 0),
            2,
        ),
        LONG => (Value::Long(decode_i64(fixed(8)?)), 9),
        DOUBLE => (
            Value::Double(TotalF64(f64::from_bits(decode_f64_bits(fixed(8)?)))),
            9,
        ),
        INSTANT => (Value::Instant(decode_i64(fixed(8)?)), 9),
        UUID => (Value::Uuid(u128::from_be_bytes(array_16(fixed(16)?))), 17),
        KEYWORD => (Value::Keyword(u64::from_be_bytes(array_8(fixed(8)?))), 9),
        REF => (
            Value::Ref(EntityId::from_raw(u64::from_be_bytes(array_8(fixed(8)?)))),
            9,
        ),
        STR | BYTES => {
            let (bytes, used) = decode_escaped(rest)?;
            if tag == STR {
                (
                    Value::Str(
                        std::str::from_utf8(&bytes)
                            .map_err(|_| DecodeError::InvalidUtf8)?
                            .into(),
                    ),
                    used + 1,
                )
            } else {
                (Value::Bytes(Arc::from(bytes)), used + 1)
            }
        }
        other => return Err(DecodeError::UnknownTag(other)),
    })
}
fn decode_i64(bytes: &[u8]) -> i64 {
    i64::from_be_bytes((u64::from_be_bytes(array_8(bytes)) ^ (1_u64 << 63)).to_be_bytes())
}
fn decode_f64_bits(bytes: &[u8]) -> u64 {
    let s = u64::from_be_bytes(array_8(bytes));
    if (s & (1_u64 << 63)) == 0 {
        !s
    } else {
        s ^ (1_u64 << 63)
    }
}
fn encode_escaped(bytes: &[u8], out: &mut Vec<u8>) {
    for b in bytes {
        if *b == 0 {
            out.extend_from_slice(&[0, 0xff]);
        } else {
            out.push(*b);
        }
    }
    out.extend_from_slice(&[0, 0]);
}
fn decode_escaped(input: &[u8]) -> Result<(Vec<u8>, usize), DecodeError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < input.len() {
        match input[i] {
            0 if input.get(i + 1) == Some(&0) => return Ok((out, i + 2)),
            0 if input.get(i + 1) == Some(&0xff) => {
                out.push(0);
                i += 2;
            }
            0 => return Err(DecodeError::InvalidEscape),
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Err(DecodeError::Truncated)
}

fn array_8(bytes: &[u8]) -> [u8; 8] {
    let mut out = [0; 8];
    out.copy_from_slice(bytes);
    out
}

/// Decodes one fixed-width entity-id component.
///
/// # Errors
/// Returns [`DecodeError::Truncated`] when fewer than eight bytes remain.
pub fn decode_entity_id(input: &[u8]) -> Result<(EntityId, usize), DecodeError> {
    let bytes = input.get(..8).ok_or(DecodeError::Truncated)?;
    Ok((EntityId::from_raw(u64::from_be_bytes(array_8(bytes))), 8))
}

/// Decodes one fixed-width unsigned integer component.
///
/// # Errors
/// Returns [`DecodeError::Truncated`] when fewer than eight bytes remain.
pub fn decode_u64(input: &[u8]) -> Result<(u64, usize), DecodeError> {
    let bytes = input.get(..8).ok_or(DecodeError::Truncated)?;
    Ok((u64::from_be_bytes(array_8(bytes)), 8))
}

fn array_16(bytes: &[u8]) -> [u8; 16] {
    let mut out = [0; 16];
    out.copy_from_slice(bytes);
    out
}
