//! Bidirectional conversion between Clojurust values and Corium boundary EDN.
//!
//! The engine's boundary language is [`corium_query::edn::Edn`]; every value
//! crossing between a cljrs runtime and the engine converts through it once,
//! per ADR-0002. Tagged engine values map to natural cljrs shapes where one
//! exists (`#uuid` → native UUID, `#bytes` → byte blob); tags with no native
//! cljrs shape (`#inst`, `#eid`, `#tx`) ride as metadata on the wrapped
//! value, which cljrs treats as equality-transparent.

use cljrs_gc::GcPtr;
use cljrs_value::value::SetValue;
use cljrs_value::{Keyword as CljKeyword, MapValue, Symbol, Value};
use corium_core::{Keyword, TotalF64};
use corium_query::edn::Edn;
use thiserror::Error;

/// Conversion failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConvertError {
    /// The cljrs value has no engine representation.
    #[error("value has no Corium representation: {0}")]
    Unsupported(String),
    /// A lazy sequence failed to realize.
    #[error("lazy sequence failed to realize")]
    LazyError,
}

/// Metadata key marking a tagged engine value carried as a wrapped cljrs
/// value (`{:corium/tag :inst}` on a long, for example).
const TAG_KEY: &str = "corium/tag";

fn tag_meta(tag: &str) -> Value {
    Value::Map(MapValue::from_pairs(vec![(
        Value::keyword(CljKeyword::parse(TAG_KEY)),
        Value::keyword(CljKeyword::parse(tag)),
    )]))
}

fn meta_tag(meta: &Value) -> Option<String> {
    if let Value::Map(map) = meta
        && let Some(Value::Keyword(keyword)) = map.get(&Value::keyword(CljKeyword::parse(TAG_KEY)))
    {
        return Some(keyword.get().full_name());
    }
    None
}

/// Converts a Corium boundary EDN form to a cljrs value.
///
/// Must be called on the thread owning the target cljrs isolate.
#[must_use]
pub fn from_edn(form: &Edn) -> Value {
    match form {
        Edn::Nil => Value::Nil,
        Edn::Bool(v) => Value::Bool(*v),
        Edn::Long(v) => Value::Long(*v),
        Edn::Double(v) => Value::Double(v.0),
        Edn::Str(v) => Value::string(v.clone()),
        Edn::Keyword(k) => Value::keyword(match &k.namespace {
            Some(namespace) => CljKeyword::qualified(namespace.clone(), k.name.clone()),
            None => CljKeyword::simple(k.name.clone()),
        }),
        Edn::Symbol(s) => Value::symbol(Symbol::parse(s)),
        Edn::List(items) => Value::List(GcPtr::new(items.iter().map(from_edn).collect())),
        Edn::Vector(items) => Value::Vector(GcPtr::new(items.iter().map(from_edn).collect())),
        Edn::Map(pairs) => Value::Map(MapValue::from_pairs(
            pairs
                .iter()
                .map(|(k, v)| (from_edn(k), from_edn(v)))
                .collect(),
        )),
        Edn::Set(items) => {
            let mut set = SetValue::empty();
            for item in items {
                set.conj_mut(from_edn(item));
            }
            Value::Set(set)
        }
        Edn::Tagged(tag, inner) => match (tag.as_str(), inner.as_ref()) {
            ("uuid", Edn::Str(hex)) => u128::from_str_radix(hex, 16)
                .map_or_else(|_| tagged_fallback(tag, inner), Value::Uuid),
            ("bytes", Edn::Str(hex)) => decode_hex(hex).map_or_else(
                || tagged_fallback(tag, inner),
                |bytes| Value::ByteBlob(bytes.into()),
            ),
            _ => tagged_fallback(tag, inner),
        },
    }
}

/// Unknown or non-native tags ride as `{:corium/tag <tag>}` metadata on the
/// converted inner value; cljrs equality/printing sees through the wrapper.
fn tagged_fallback(tag: &str, inner: &Edn) -> Value {
    Value::WithMeta(Box::new(from_edn(inner)), Box::new(tag_meta(tag)))
}

/// Converts a cljrs value to a Corium boundary EDN form.
///
/// Lazy sequences and cons cells are fully realized; maps and sets are
/// normalized to the engine's sorted representation.
///
/// # Errors
/// Returns [`ConvertError::Unsupported`] for values with no engine
/// representation (functions, atoms, big numbers, native objects, …).
pub fn to_edn(value: &Value) -> Result<Edn, ConvertError> {
    match value {
        Value::Nil => Ok(Edn::Nil),
        Value::Bool(v) => Ok(Edn::Bool(*v)),
        Value::Long(v) => Ok(Edn::Long(*v)),
        Value::Double(v) => Ok(Edn::Double(TotalF64(*v))),
        Value::Char(c) => Ok(Edn::Str(c.to_string())),
        Value::Str(s) => Ok(Edn::Str(s.get().clone())),
        Value::Uuid(v) => Ok(Edn::Tagged(
            "uuid".into(),
            Box::new(Edn::Str(format!("{v:032x}"))),
        )),
        Value::Keyword(k) => Ok(Edn::Keyword(Keyword::parse(&k.get().full_name()))),
        Value::Symbol(s) => Ok(Edn::Symbol(s.get().full_name())),
        Value::ByteBlob(bytes) => Ok(Edn::Tagged(
            "bytes".into(),
            Box::new(Edn::Str(encode_hex(bytes))),
        )),
        Value::List(items) => Ok(Edn::List(
            items.get().iter().map(to_edn).collect::<Result<_, _>>()?,
        )),
        Value::Vector(items) => Ok(Edn::Vector(
            items.get().iter().map(to_edn).collect::<Result<_, _>>()?,
        )),
        Value::Queue(items) => Ok(Edn::List(
            items.get().iter().map(to_edn).collect::<Result<_, _>>()?,
        )),
        Value::Map(map) => {
            let mut pairs = Vec::with_capacity(map.count());
            for (k, v) in map.iter() {
                pairs.push((to_edn(k)?, to_edn(v)?));
            }
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(Edn::Map(pairs))
        }
        Value::Set(set) => {
            let mut items = set.iter().map(to_edn).collect::<Result<Vec<_>, _>>()?;
            items.sort();
            items.dedup();
            Ok(Edn::Set(items))
        }
        Value::LazySeq(_) | Value::Cons(_) => {
            let mut items = Vec::new();
            realize_seq(value, &mut items)?;
            Ok(Edn::List(items))
        }
        Value::WithMeta(inner, meta) => {
            let converted = to_edn(inner)?;
            Ok(match meta_tag(meta) {
                Some(tag) => Edn::Tagged(tag, Box::new(converted)),
                None => converted,
            })
        }
        other => Err(ConvertError::Unsupported(format!("{other}"))),
    }
}

/// Walks a realized-or-lazy sequence into `out`.
fn realize_seq(value: &Value, out: &mut Vec<Edn>) -> Result<(), ConvertError> {
    let mut current = value.clone();
    loop {
        match current {
            Value::Nil => return Ok(()),
            Value::LazySeq(seq) => {
                let realized = seq.get().realize();
                if seq.get().error().is_some() {
                    return Err(ConvertError::LazyError);
                }
                current = realized;
            }
            Value::Cons(cons) => {
                out.push(to_edn(&cons.get().head)?);
                current = cons.get().tail.clone();
            }
            Value::List(items) => {
                for item in items.get().iter() {
                    out.push(to_edn(item)?);
                }
                return Ok(());
            }
            Value::Vector(items) => {
                for item in items.get().iter() {
                    out.push(to_edn(item)?);
                }
                return Ok(());
            }
            other => {
                out.push(to_edn(&other)?);
                return Ok(());
            }
        }
    }
}

/// Parses EDN text with the Clojurust reader, producing boundary EDN forms.
///
/// This is the single-EDN-implementation seam from
/// `docs/design/clojurust-integration.md`: text entering through a cljrs
/// boundary is read by `cljrs-reader` and only then mapped onto the engine's
/// boundary representation.
///
/// # Errors
/// Returns [`ConvertError::Unsupported`] for unparseable text or reader
/// forms with no EDN data representation.
pub fn read_edn(text: &str) -> Result<Vec<Edn>, ConvertError> {
    let mut parser = cljrs_reader::Parser::new(text.to_owned(), "<corium>".to_owned());
    let forms = parser
        .parse_all()
        .map_err(|error| ConvertError::Unsupported(format!("reader error: {error:?}")))?;
    forms.iter().map(form_to_edn).collect()
}

/// Maps a parsed reader form to boundary EDN (data positions only).
///
/// # Errors
/// Returns [`ConvertError::Unsupported`] for forms that are not plain data
/// (reader macros other than quote-free data, anonymous functions, …).
pub fn form_to_edn(form: &cljrs_reader::Form) -> Result<Edn, ConvertError> {
    use cljrs_reader::form::FormKind;
    match &form.kind {
        FormKind::Nil => Ok(Edn::Nil),
        FormKind::Bool(b) => Ok(Edn::Bool(*b)),
        FormKind::Int(n) => Ok(Edn::Long(*n)),
        FormKind::Float(f) => Ok(Edn::Double(TotalF64(*f))),
        FormKind::Str(s) => Ok(Edn::Str(s.clone())),
        FormKind::Char(c) => Ok(Edn::Str(c.to_string())),
        FormKind::Symbol(s) => Ok(Edn::Symbol(s.clone())),
        FormKind::Keyword(s) => Ok(Edn::keyword(s)),
        FormKind::List(items) => Ok(Edn::List(
            items.iter().map(form_to_edn).collect::<Result<_, _>>()?,
        )),
        FormKind::Vector(items) => Ok(Edn::Vector(
            items.iter().map(form_to_edn).collect::<Result<_, _>>()?,
        )),
        FormKind::Map(entries) => {
            let mut pairs = Vec::with_capacity(entries.len() / 2);
            for pair in entries.chunks(2) {
                let [k, v] = pair else {
                    return Err(ConvertError::Unsupported("odd map literal".into()));
                };
                pairs.push((form_to_edn(k)?, form_to_edn(v)?));
            }
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(Edn::Map(pairs))
        }
        FormKind::Set(items) => {
            let mut converted = items
                .iter()
                .map(form_to_edn)
                .collect::<Result<Vec<_>, _>>()?;
            converted.sort();
            converted.dedup();
            Ok(Edn::Set(converted))
        }
        FormKind::TaggedLiteral(tag, inner) => {
            Ok(Edn::Tagged(tag.clone(), Box::new(form_to_edn(inner)?)))
        }
        FormKind::Quote(inner) => form_to_edn(inner),
        other => Err(ConvertError::Unsupported(format!("reader form {other:?}"))),
    }
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut acc, b| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{b:02x}");
        acc
    })
}
