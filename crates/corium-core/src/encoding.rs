//! Sortable binary encoding for values and datom key components.

use std::sync::Arc;

use thiserror::Error;

use crate::{EntityId, Keyword, KeywordInterner, KwId, Sealed, TotalF64, Value, ValueType};

/// Encodes a value type as its tag byte in [`ValueType`] declaration order.
#[must_use]
pub(crate) fn value_type_tag(value_type: ValueType) -> u8 {
    match value_type {
        ValueType::Bool => 0,
        ValueType::Long => 1,
        ValueType::Double => 2,
        ValueType::Instant => 3,
        ValueType::Uuid => 4,
        ValueType::Keyword => 5,
        ValueType::Str => 6,
        ValueType::Bytes => 7,
        ValueType::Ref => 8,
    }
}

/// Decodes a value type tag byte written by [`value_type_tag`].
pub(crate) fn value_type_from_tag(tag: u8) -> Result<ValueType, DecodeError> {
    Ok(match tag {
        0 => ValueType::Bool,
        1 => ValueType::Long,
        2 => ValueType::Double,
        3 => ValueType::Instant,
        4 => ValueType::Uuid,
        5 => ValueType::Keyword,
        6 => ValueType::Str,
        7 => ValueType::Bytes,
        8 => ValueType::Ref,
        other => return Err(DecodeError::InvalidValueType(other)),
    })
}

const BOOL: u8 = 0x10;
const LONG: u8 = 0x20;
const DOUBLE: u8 = 0x30;
const INSTANT: u8 = 0x40;
const UUID: u8 = 0x50;
const KEYWORD: u8 = 0x60;
const STR: u8 = 0x70;
const BYTES: u8 = 0x80;
const REF: u8 = 0x90;
const SEALED: u8 = 0xA0;

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
    /// A sealed value's declared value type byte is not known.
    #[error("unknown value type tag {0:#x}")]
    InvalidValueType(u8),
}

/// Seal plaintext encoding failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SealPlaintextError {
    /// A sealed value cannot itself be sealed again.
    #[error("sealed values cannot nest")]
    Nested,
    /// The value's keyword id does not resolve in the interner.
    #[error("unresolvable keyword id {0}")]
    UnresolvableKeyword(KwId),
    /// The opened plaintext is not a well-formed value encoding.
    #[error("malformed seal plaintext: {0}")]
    Malformed(#[from] DecodeError),
    /// The opened plaintext does not carry the value type the sealed header
    /// declares.
    #[error("seal plaintext holds a {actual:?} where the header declares {expected:?}")]
    TypeMismatch {
        /// Type named by the sealed header.
        expected: ValueType,
        /// Type actually found in the plaintext.
        actual: ValueType,
    },
}

/// Encodes a value as the plaintext of a sealed value.
///
/// This is [`encode_value`] with one difference: a keyword is encoded as its
/// *text*, under the string tag, rather than as its interner id. Ids are
/// assigned by the transactor's naming table, and a protected attribute's
/// vocabulary must never reach it — an id would leak the very value the seal
/// is meant to hide, and would not survive a reader that interns differently.
/// [`decode_seal_plaintext`] reverses this using the declared value type.
///
/// # Errors
///
/// Returns [`SealPlaintextError::Nested`] for an already-sealed value and
/// [`SealPlaintextError::UnresolvableKeyword`] when a keyword id has no text
/// in `interner`.
pub fn encode_seal_plaintext(
    value: &Value,
    interner: &KeywordInterner,
) -> Result<Vec<u8>, SealPlaintextError> {
    match value {
        Value::Sealed(_) => Err(SealPlaintextError::Nested),
        Value::Keyword(id) => {
            let keyword = interner
                .resolve(*id)
                .ok_or(SealPlaintextError::UnresolvableKeyword(*id))?;
            let mut out = vec![STR];
            encode_escaped(keyword_text(&keyword).as_bytes(), &mut out);
            Ok(out)
        }
        other => Ok(encode_value(other)),
    }
}

/// Decodes plaintext produced by [`encode_seal_plaintext`].
///
/// `expected` is the value type from the sealed header, which the AEAD has
/// already authenticated. Bytes after the first complete value are ignored:
/// every value encoding is self-delimiting, which is what lets a protection
/// class pad plaintext to a fixed multiple without recording the true length.
///
/// A keyword is re-interned locally ([`KeywordInterner::intern_local`]), so
/// the durable naming table stays free of the protected vocabulary.
///
/// # Errors
///
/// Returns [`SealPlaintextError::Malformed`] when the bytes are not a value
/// encoding, and [`SealPlaintextError::TypeMismatch`] when the value found is
/// not of the declared type.
pub fn decode_seal_plaintext(
    bytes: &[u8],
    expected: ValueType,
    interner: &KeywordInterner,
) -> Result<Value, SealPlaintextError> {
    let (value, _) = decode_value(bytes)?;
    if expected == ValueType::Keyword {
        let Value::Str(text) = value else {
            return Err(SealPlaintextError::TypeMismatch {
                expected,
                actual: value_type_of(&value)?,
            });
        };
        return Ok(Value::Keyword(interner.intern_local(Keyword::parse(&text))));
    }
    if value.has_type(expected) {
        Ok(value)
    } else {
        Err(SealPlaintextError::TypeMismatch {
            expected,
            actual: value_type_of(&value)?,
        })
    }
}

/// Renders a keyword the way [`Keyword::parse`] reads it back: no leading
/// colon, namespace and name joined by `/`.
fn keyword_text(keyword: &Keyword) -> String {
    keyword.namespace.as_ref().map_or_else(
        || keyword.name.clone(),
        |namespace| format!("{namespace}/{}", keyword.name),
    )
}

fn value_type_of(value: &Value) -> Result<ValueType, SealPlaintextError> {
    Ok(match value {
        Value::Bool(_) => ValueType::Bool,
        Value::Long(_) => ValueType::Long,
        Value::Double(_) => ValueType::Double,
        Value::Instant(_) => ValueType::Instant,
        Value::Uuid(_) => ValueType::Uuid,
        Value::Keyword(_) => ValueType::Keyword,
        Value::Str(_) => ValueType::Str,
        Value::Bytes(_) => ValueType::Bytes,
        Value::Ref(_) => ValueType::Ref,
        Value::Sealed(_) => return Err(SealPlaintextError::Nested),
    })
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
            Self::Sealed(v) => {
                out.push(SEALED);
                v.class.encode_into(out);
                out.extend_from_slice(&v.epoch.to_be_bytes());
                out.push(value_type_tag(v.vtype));
                encode_escaped(&v.body, out);
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
        SEALED => {
            let class = EntityId::from_raw(u64::from_be_bytes(array_8(fixed(8)?)));
            let epoch = u32::from_be_bytes(array_4(rest.get(8..12).ok_or(DecodeError::Truncated)?));
            let vtype = value_type_from_tag(*rest.get(12).ok_or(DecodeError::Truncated)?)?;
            let (body, used) = decode_escaped(rest.get(13..).ok_or(DecodeError::Truncated)?)?;
            (
                Value::Sealed(Sealed {
                    class,
                    epoch,
                    vtype,
                    body: Arc::from(body),
                }),
                1 + 13 + used,
            )
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

fn array_4(bytes: &[u8]) -> [u8; 4] {
    let mut out = [0; 4];
    out.copy_from_slice(bytes);
    out
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
