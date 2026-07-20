//! Composite wire encoding for values, EDN forms, datoms, and schema.
//!
//! Single values reuse the sortable tag space from `corium-core`; composite
//! payloads extend it with container tags and a per-message interning table
//! for keywords and repeated strings (see `docs/design/protocol.md`). The
//! composite variant is length-prefixed rather than escaped, and keywords
//! travel by name so no shared interner state is required across processes.

use std::collections::HashMap;
use std::sync::Arc;

use corium_core::{
    Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Schema, TotalF64, Unique,
    Value, ValueType,
};
use corium_db::Idents;
use corium_query::edn::Edn;
use thiserror::Error;

// Scalar tags shared with `corium_core::encoding`.
const BOOL: u8 = 0x10;
const LONG: u8 = 0x20;
const DOUBLE: u8 = 0x30;
const INSTANT: u8 = 0x40;
const UUID: u8 = 0x50;
const REF: u8 = 0x90;
// Composite-variant tags.
const NIL: u8 = 0x00;
const KEYWORD_NAME: u8 = 0x61;
const STR_INTERNED: u8 = 0x71;
const BYTES_PREFIXED: u8 = 0x81;
const LIST: u8 = 0xA0;
const VECTOR: u8 = 0xA1;
const MAP: u8 = 0xA2;
const SET: u8 = 0xA3;
const TAGGED: u8 = 0xA4;
const SYMBOL: u8 = 0xA5;

/// Codec failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum CodecError {
    /// Input ended before a complete item was read.
    #[error("truncated wire payload")]
    Truncated,
    /// Unknown tag byte.
    #[error("unknown wire tag {0:#x}")]
    UnknownTag(u8),
    /// Interning table reference out of range.
    #[error("invalid intern reference {0}")]
    InvalidIntern(u64),
    /// String payload is not UTF-8.
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    /// A keyword id has no entry in the supplied interner.
    #[error("keyword id {0} is not interned")]
    UnknownKeyword(u64),
    /// A count or length does not fit the platform.
    #[error("wire length out of range")]
    Length,
    /// Payload decoded but trailing bytes remain.
    #[error("trailing bytes after wire payload")]
    Trailing,
    /// Field value outside its legal range.
    #[error("invalid wire field: {0}")]
    InvalidField(&'static str),
}

/// Streaming writer with a per-message string interning table.
#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
    table: HashMap<String, u64>,
}

impl Writer {
    /// Creates an empty writer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Consumes the writer, returning the message bytes.
    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }

    fn varint(&mut self, mut n: u64) {
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                self.buf.push(byte);
                return;
            }
            self.buf.push(byte | 0x80);
        }
    }

    /// Writes an interned string: `0 len bytes` defines the next table
    /// index on first use; later uses write `index` (1-based).
    fn intern(&mut self, text: &str) {
        if let Some(&index) = self.table.get(text) {
            self.varint(index);
            return;
        }
        let index = self.table.len() as u64 + 1;
        self.table.insert(text.to_owned(), index);
        self.varint(0);
        self.varint(text.len() as u64);
        self.buf.extend_from_slice(text.as_bytes());
    }

    fn keyword(&mut self, keyword: &Keyword) {
        self.buf.push(KEYWORD_NAME);
        match &keyword.namespace {
            Some(namespace) => self.intern(&format!("{namespace}/{}", keyword.name)),
            None => self.intern(&keyword.name),
        }
    }

    /// Writes a `u64` as a varint (for counts and ids).
    pub fn u64(&mut self, n: u64) {
        self.varint(n);
    }

    /// Writes an `i64` (zigzag varint).
    pub fn i64(&mut self, n: i64) {
        self.varint(zigzag(n));
    }

    /// Writes a raw byte.
    pub fn byte(&mut self, b: u8) {
        self.buf.push(b);
    }

    /// Writes one EDN form.
    pub fn edn(&mut self, form: &Edn) {
        match form {
            Edn::Nil => self.buf.push(NIL),
            Edn::Bool(v) => {
                self.buf.push(BOOL);
                self.buf.push(u8::from(*v));
            }
            Edn::Long(v) => {
                self.buf.push(LONG);
                self.i64(*v);
            }
            Edn::Double(v) => {
                self.buf.push(DOUBLE);
                self.buf.extend_from_slice(&v.sortable_bits().to_be_bytes());
            }
            Edn::Str(v) => {
                self.buf.push(STR_INTERNED);
                self.intern(v);
            }
            Edn::Keyword(k) => self.keyword(k),
            Edn::Symbol(s) => {
                self.buf.push(SYMBOL);
                self.intern(s);
            }
            Edn::List(items) => self.seq(LIST, items),
            Edn::Vector(items) => self.seq(VECTOR, items),
            Edn::Set(items) => self.seq(SET, items),
            Edn::Map(pairs) => {
                self.buf.push(MAP);
                self.varint(pairs.len() as u64);
                for (key, value) in pairs {
                    self.edn(key);
                    self.edn(value);
                }
            }
            Edn::Tagged(tag, value) => {
                self.buf.push(TAGGED);
                self.intern(tag);
                self.edn(value);
            }
        }
    }

    fn seq(&mut self, tag: u8, items: &[Edn]) {
        self.buf.push(tag);
        self.varint(items.len() as u64);
        for item in items {
            self.edn(item);
        }
    }

    /// Writes one engine value. Keywords travel by name via `interner`.
    ///
    /// # Errors
    /// Returns [`CodecError::UnknownKeyword`] for an unresolvable keyword id.
    pub fn value(&mut self, value: &Value, interner: &KeywordInterner) -> Result<(), CodecError> {
        match value {
            Value::Bool(v) => {
                self.buf.push(BOOL);
                self.buf.push(u8::from(*v));
            }
            Value::Long(v) => {
                self.buf.push(LONG);
                self.i64(*v);
            }
            Value::Double(v) => {
                self.buf.push(DOUBLE);
                self.buf.extend_from_slice(&v.sortable_bits().to_be_bytes());
            }
            Value::Instant(v) => {
                self.buf.push(INSTANT);
                self.i64(*v);
            }
            Value::Uuid(v) => {
                self.buf.push(UUID);
                self.buf.extend_from_slice(&v.to_be_bytes());
            }
            Value::Keyword(id) => {
                let keyword = interner
                    .resolve(*id)
                    .ok_or(CodecError::UnknownKeyword(*id))?
                    .clone();
                self.keyword(&keyword);
            }
            Value::Str(v) => {
                self.buf.push(STR_INTERNED);
                self.intern(v);
            }
            Value::Bytes(v) => {
                self.buf.push(BYTES_PREFIXED);
                self.varint(v.len() as u64);
                self.buf.extend_from_slice(v);
            }
            Value::Ref(e) => {
                self.buf.push(REF);
                self.varint(e.raw());
            }
        }
        Ok(())
    }
}

/// Streaming reader over one wire message.
pub struct Reader<'a> {
    input: &'a [u8],
    table: Vec<String>,
}

impl<'a> Reader<'a> {
    /// Creates a reader over message bytes.
    #[must_use]
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            table: Vec::new(),
        }
    }

    /// Fails unless every input byte was consumed.
    ///
    /// # Errors
    /// Returns [`CodecError::Trailing`] when bytes remain.
    pub fn expect_end(&self) -> Result<(), CodecError> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(CodecError::Trailing)
        }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let bytes = self.input.get(..n).ok_or(CodecError::Truncated)?;
        self.input = &self.input[n..];
        Ok(bytes)
    }

    fn tag(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    /// Reads a varint `u64`.
    ///
    /// # Errors
    /// Returns an error for truncated input.
    pub fn u64(&mut self) -> Result<u64, CodecError> {
        let mut out = 0_u64;
        let mut shift = 0_u32;
        loop {
            let byte = self.take(1)?[0];
            out |= u64::from(byte & 0x7f)
                .checked_shl(shift)
                .ok_or(CodecError::Length)?;
            if byte & 0x80 == 0 {
                return Ok(out);
            }
            shift += 7;
            if shift > 63 {
                return Err(CodecError::Length);
            }
        }
    }

    /// Reads a zigzag varint `i64`.
    ///
    /// # Errors
    /// Returns an error for truncated input.
    pub fn i64(&mut self) -> Result<i64, CodecError> {
        Ok(unzigzag(self.u64()?))
    }

    /// Reads a raw byte.
    ///
    /// # Errors
    /// Returns an error for truncated input.
    pub fn byte(&mut self) -> Result<u8, CodecError> {
        self.tag()
    }

    fn count(&mut self) -> Result<usize, CodecError> {
        usize::try_from(self.u64()?).map_err(|_| CodecError::Length)
    }

    fn intern(&mut self) -> Result<String, CodecError> {
        let index = self.u64()?;
        if index == 0 {
            let len = self.count()?;
            let text = std::str::from_utf8(self.take(len)?)
                .map_err(|_| CodecError::InvalidUtf8)?
                .to_owned();
            self.table.push(text.clone());
            return Ok(text);
        }
        let position = usize::try_from(index - 1).map_err(|_| CodecError::Length)?;
        self.table
            .get(position)
            .cloned()
            .ok_or(CodecError::InvalidIntern(index))
    }

    fn double(&mut self) -> Result<TotalF64, CodecError> {
        let sortable = u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CodecError::Truncated)?,
        );
        let bits = if sortable & (1_u64 << 63) == 0 {
            !sortable
        } else {
            sortable ^ (1_u64 << 63)
        };
        Ok(TotalF64(f64::from_bits(bits)))
    }

    /// Reads one EDN form.
    ///
    /// # Errors
    /// Returns [`CodecError`] for malformed input.
    pub fn edn(&mut self) -> Result<Edn, CodecError> {
        Ok(match self.tag()? {
            NIL => Edn::Nil,
            BOOL => Edn::Bool(self.take(1)?[0] != 0),
            LONG => Edn::Long(self.i64()?),
            DOUBLE => Edn::Double(self.double()?),
            STR_INTERNED => Edn::Str(self.intern()?),
            KEYWORD_NAME => Edn::Keyword(Keyword::parse(&self.intern()?)),
            SYMBOL => Edn::Symbol(self.intern()?),
            LIST => Edn::List(self.items()?),
            VECTOR => Edn::Vector(self.items()?),
            SET => {
                let mut items = self.items()?;
                items.sort();
                items.dedup();
                Edn::Set(items)
            }
            MAP => {
                let count = self.count()?;
                let mut pairs = Vec::with_capacity(count.min(4096));
                for _ in 0..count {
                    let key = self.edn()?;
                    let value = self.edn()?;
                    pairs.push((key, value));
                }
                pairs.sort_by(|left, right| left.0.cmp(&right.0));
                Edn::Map(pairs)
            }
            TAGGED => {
                let tag = self.intern()?;
                Edn::Tagged(tag, Box::new(self.edn()?))
            }
            other => return Err(CodecError::UnknownTag(other)),
        })
    }

    fn items(&mut self) -> Result<Vec<Edn>, CodecError> {
        let count = self.count()?;
        let mut items = Vec::with_capacity(count.min(4096));
        for _ in 0..count {
            items.push(self.edn()?);
        }
        Ok(items)
    }

    /// Reads one engine value, interning keyword names into `interner`.
    ///
    /// # Errors
    /// Returns [`CodecError`] for malformed input.
    pub fn value(&mut self, interner: &mut KeywordInterner) -> Result<Value, CodecError> {
        Ok(match self.tag()? {
            BOOL => Value::Bool(self.take(1)?[0] != 0),
            LONG => Value::Long(self.i64()?),
            DOUBLE => Value::Double(self.double()?),
            INSTANT => Value::Instant(self.i64()?),
            UUID => Value::Uuid(u128::from_be_bytes(
                self.take(16)?
                    .try_into()
                    .map_err(|_| CodecError::Truncated)?,
            )),
            KEYWORD_NAME => {
                let keyword = Keyword::parse(&self.intern()?);
                Value::Keyword(interner.intern(keyword))
            }
            STR_INTERNED => Value::Str(Arc::from(self.intern()?.as_str())),
            BYTES_PREFIXED => {
                let len = self.count()?;
                Value::Bytes(Arc::from(self.take(len)?))
            }
            REF => Value::Ref(EntityId::from_raw(self.u64()?)),
            other => return Err(CodecError::UnknownTag(other)),
        })
    }
}

#[allow(clippy::cast_sign_loss)]
const fn zigzag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

#[allow(clippy::cast_possible_wrap)]
const fn unzigzag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// Encodes one EDN form as a standalone message.
#[must_use]
pub fn encode_edn(form: &Edn) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.edn(form);
    writer.finish()
}

/// Decodes one EDN form from a standalone message.
///
/// # Errors
/// Returns [`CodecError`] for malformed or trailing input.
pub fn decode_edn(bytes: &[u8]) -> Result<Edn, CodecError> {
    let mut reader = Reader::new(bytes);
    let form = reader.edn()?;
    reader.expect_end()?;
    Ok(form)
}

/// Encodes a datom list; keyword values travel by name via `interner`.
///
/// # Errors
/// Returns [`CodecError::UnknownKeyword`] for unresolvable keyword ids.
pub fn encode_datoms(datoms: &[Datom], interner: &KeywordInterner) -> Result<Vec<u8>, CodecError> {
    let mut writer = Writer::new();
    writer.u64(datoms.len() as u64);
    for datom in datoms {
        writer.u64(datom.e.raw());
        writer.u64(datom.a.raw());
        writer.u64(datom.tx.raw());
        writer.byte(u8::from(datom.added));
        writer.value(&datom.v, interner)?;
    }
    Ok(writer.finish())
}

/// Decodes a datom list, interning keyword names into `interner`.
///
/// # Errors
/// Returns [`CodecError`] for malformed input.
pub fn decode_datoms(
    bytes: &[u8],
    interner: &mut KeywordInterner,
) -> Result<Vec<Datom>, CodecError> {
    let mut reader = Reader::new(bytes);
    let count = usize::try_from(reader.u64()?).map_err(|_| CodecError::Length)?;
    let mut datoms = Vec::with_capacity(count.min(65_536));
    for _ in 0..count {
        let e = EntityId::from_raw(reader.u64()?);
        let a = EntityId::from_raw(reader.u64()?);
        let tx = EntityId::from_raw(reader.u64()?);
        let added = reader.byte()? != 0;
        let v = reader.value(interner)?;
        datoms.push(Datom { e, a, v, tx, added });
    }
    reader.expect_end()?;
    Ok(datoms)
}

/// Encodes schema attributes plus the ident registry (handshake payload).
#[must_use]
pub fn encode_schema(schema: &Schema, idents: &Idents) -> Vec<u8> {
    let mut writer = Writer::new();
    let attrs: Vec<_> = schema.iter().collect();
    writer.u64(attrs.len() as u64);
    for (_, attr) in attrs {
        writer.u64(attr.id.raw());
        writer.byte(value_type_tag(attr.value_type));
        writer.byte(match attr.cardinality {
            Cardinality::One => 0,
            Cardinality::Many => 1,
        });
        writer.byte(match attr.unique {
            None => 0,
            Some(Unique::Identity) => 1,
            Some(Unique::Value) => 2,
        });
        writer.byte(
            u8::from(attr.is_component)
                | (u8::from(attr.indexed) << 1)
                | (u8::from(attr.no_history) << 2),
        );
    }
    let idents: Vec<_> = idents.iter().collect();
    writer.u64(idents.len() as u64);
    for (keyword, id) in idents {
        writer.edn(&Edn::Keyword(keyword.clone()));
        writer.u64(id.raw());
    }
    writer.finish()
}

/// Decodes a schema/ident handshake payload.
///
/// # Errors
/// Returns [`CodecError`] for malformed input.
pub fn decode_schema(bytes: &[u8]) -> Result<(Schema, Idents), CodecError> {
    let mut reader = Reader::new(bytes);
    let mut schema = Schema::default();
    let attr_count = usize::try_from(reader.u64()?).map_err(|_| CodecError::Length)?;
    for _ in 0..attr_count {
        let id = EntityId::from_raw(reader.u64()?);
        let value_type = value_type_from(reader.byte()?)?;
        let cardinality = match reader.byte()? {
            0 => Cardinality::One,
            1 => Cardinality::Many,
            _ => return Err(CodecError::InvalidField("cardinality")),
        };
        let unique = match reader.byte()? {
            0 => None,
            1 => Some(Unique::Identity),
            2 => Some(Unique::Value),
            _ => return Err(CodecError::InvalidField("unique")),
        };
        let flags = reader.byte()?;
        schema.insert(Attribute {
            id,
            value_type,
            cardinality,
            unique,
            is_component: flags & 1 != 0,
            indexed: flags & 2 != 0,
            no_history: flags & 4 != 0,
        });
    }
    let mut idents = Idents::default();
    let ident_count = usize::try_from(reader.u64()?).map_err(|_| CodecError::Length)?;
    for _ in 0..ident_count {
        let Edn::Keyword(keyword) = reader.edn()? else {
            return Err(CodecError::InvalidField("ident keyword"));
        };
        let id = EntityId::from_raw(reader.u64()?);
        idents.insert(keyword, id);
    }
    reader.expect_end()?;
    Ok((schema, idents))
}

/// Encodes an interner snapshot (keywords in dense id order).
#[must_use]
pub fn encode_naming(interner: &KeywordInterner) -> Vec<u8> {
    let mut writer = Writer::new();
    let entries: Vec<_> = interner.iter().collect();
    writer.u64(entries.len() as u64);
    for (_, keyword) in entries {
        writer.edn(&Edn::Keyword(keyword.clone()));
    }
    writer.finish()
}

/// Decodes an interner snapshot; re-interning in order reproduces dense ids.
///
/// # Errors
/// Returns [`CodecError`] for malformed input.
pub fn decode_naming(bytes: &[u8]) -> Result<KeywordInterner, CodecError> {
    let mut reader = Reader::new(bytes);
    let mut interner = KeywordInterner::default();
    let count = usize::try_from(reader.u64()?).map_err(|_| CodecError::Length)?;
    for _ in 0..count {
        let Edn::Keyword(keyword) = reader.edn()? else {
            return Err(CodecError::InvalidField("interner keyword"));
        };
        interner.intern(keyword);
    }
    reader.expect_end()?;
    Ok(interner)
}

/// Encodes the durable schema, ident, and keyword-interner metadata record.
#[must_use]
pub fn encode_metadata(schema: &Schema, idents: &Idents, interner: &KeywordInterner) -> Vec<u8> {
    let schema_bytes = encode_schema(schema, idents);
    let naming_bytes = encode_naming(interner);
    let mut out = Vec::with_capacity(8 + schema_bytes.len() + naming_bytes.len());
    out.extend_from_slice(&u32::try_from(schema_bytes.len()).unwrap_or(0).to_be_bytes());
    out.extend_from_slice(&schema_bytes);
    out.extend_from_slice(&u32::try_from(naming_bytes.len()).unwrap_or(0).to_be_bytes());
    out.extend_from_slice(&naming_bytes);
    out
}

/// Decodes a durable schema, ident, and keyword-interner metadata record.
///
/// # Errors
/// Returns [`CodecError`] for malformed input.
pub fn decode_metadata(bytes: &[u8]) -> Result<(Schema, Idents, KeywordInterner), CodecError> {
    fn take(input: &mut &[u8]) -> Result<Vec<u8>, CodecError> {
        let len_bytes = input.get(..4).ok_or(CodecError::Truncated)?;
        let len = usize::try_from(u32::from_be_bytes(len_bytes.try_into().unwrap_or_default()))
            .map_err(|_| CodecError::Length)?;
        let end = 4_usize.checked_add(len).ok_or(CodecError::Length)?;
        let payload = input.get(4..end).ok_or(CodecError::Truncated)?.to_vec();
        *input = &input[end..];
        Ok(payload)
    }

    let mut input = bytes;
    let schema_bytes = take(&mut input)?;
    let naming_bytes = take(&mut input)?;
    if !input.is_empty() {
        return Err(CodecError::Trailing);
    }
    let (schema, idents) = decode_schema(&schema_bytes)?;
    let interner = decode_naming(&naming_bytes)?;
    Ok((schema, idents, interner))
}

const fn value_type_tag(value_type: ValueType) -> u8 {
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

const fn value_type_from(tag: u8) -> Result<ValueType, CodecError> {
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
        _ => return Err(CodecError::InvalidField("value type")),
    })
}
