//! Minimal EDN reader/printer for the query boundary.
//!
//! The engine carries this small self-contained reader covering the subset
//! used by queries, transaction forms, and the conformance corpus:
//! collections, keywords, symbols, strings, numbers, booleans, `nil`, sets,
//! comments, `#_` discards, and tagged elements. Text arriving through a
//! cljrs boundary is read by `cljrs-reader` instead and bridged onto this
//! representation (`corium_cljrs::convert::read_edn`, M5), keeping one EDN
//! implementation at the boundary while the engine core stays dependency-free.

use std::fmt;

use corium_core::{Keyword, TotalF64};
use thiserror::Error;

/// An EDN value with total ordering (maps/sets are normalized sorted).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Edn {
    /// `nil`.
    Nil,
    /// Boolean.
    Bool(bool),
    /// Integer.
    Long(i64),
    /// Floating point (total order).
    Double(TotalF64),
    /// String.
    Str(String),
    /// Keyword.
    Keyword(Keyword),
    /// Symbol, printed verbatim.
    Symbol(String),
    /// List `( … )`.
    List(Vec<Edn>),
    /// Vector `[ … ]`.
    Vector(Vec<Edn>),
    /// Map `{ … }` as sorted key/value pairs.
    Map(Vec<(Edn, Edn)>),
    /// Set `#{ … }` as sorted, deduplicated elements.
    Set(Vec<Edn>),
    /// Tagged element `#tag value`.
    Tagged(String, Box<Edn>),
}

impl Edn {
    /// Builds a symbol.
    #[must_use]
    pub fn symbol(text: &str) -> Self {
        Self::Symbol(text.to_owned())
    }

    /// Builds a keyword from `"ns/name"` or `"name"` text (no leading colon).
    #[must_use]
    pub fn keyword(text: &str) -> Self {
        Self::Keyword(Keyword::parse(text))
    }

    /// Returns the symbol text if this is a symbol.
    #[must_use]
    pub fn as_symbol(&self) -> Option<&str> {
        match self {
            Self::Symbol(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the keyword if this is a keyword.
    #[must_use]
    pub const fn as_keyword(&self) -> Option<&Keyword> {
        match self {
            Self::Keyword(k) => Some(k),
            _ => None,
        }
    }

    /// Returns the elements if this is a list or vector.
    #[must_use]
    pub fn as_seq(&self) -> Option<&[Edn]> {
        match self {
            Self::List(items) | Self::Vector(items) => Some(items),
            _ => None,
        }
    }

    /// Looks up a map value by key.
    #[must_use]
    pub fn get(&self, key: &Self) -> Option<&Self> {
        match self {
            Self::Map(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }
}

/// EDN read failure.
#[derive(Debug, Error, Eq, PartialEq)]
#[error("EDN parse error at offset {offset}: {message}")]
pub struct EdnError {
    /// Byte offset of the failure.
    pub offset: usize,
    /// Failure description.
    pub message: String,
}

/// Parses exactly one EDN form from `input` (trailing whitespace allowed).
///
/// # Errors
/// Returns [`EdnError`] on malformed input or trailing data.
pub fn read_one(input: &str) -> Result<Edn, EdnError> {
    let mut reader = Reader::new(input);
    let form = reader.read_form()?;
    reader.skip_ws();
    if reader.peek().is_some() {
        return Err(reader.error("trailing data after form"));
    }
    Ok(form)
}

/// Parses every top-level EDN form in `input`.
///
/// # Errors
/// Returns [`EdnError`] on malformed input.
pub fn read_all(input: &str) -> Result<Vec<Edn>, EdnError> {
    let mut reader = Reader::new(input);
    let mut forms = Vec::new();
    loop {
        reader.skip_ws();
        if reader.peek().is_none() {
            return Ok(forms);
        }
        forms.push(reader.read_form()?);
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

const DELIMITERS: &[u8] = b"()[]{}\"; \t\r\n,";

impl<'a> Reader<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            pos: 0,
        }
    }

    fn error(&self, message: &str) -> EdnError {
        EdnError {
            offset: self.pos,
            message: message.to_owned(),
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Some(b)
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            match b {
                b' ' | b'\t' | b'\r' | b'\n' | b',' => {
                    self.pos += 1;
                }
                b';' => {
                    while self.peek().is_some_and(|b| b != b'\n') {
                        self.pos += 1;
                    }
                }
                _ => return,
            }
        }
    }

    fn read_form(&mut self) -> Result<Edn, EdnError> {
        self.skip_ws();
        match self.peek().ok_or_else(|| self.error("unexpected end"))? {
            b'(' => {
                self.pos += 1;
                Ok(Edn::List(self.read_until(b')')?))
            }
            b'[' => {
                self.pos += 1;
                Ok(Edn::Vector(self.read_until(b']')?))
            }
            b'{' => {
                self.pos += 1;
                let items = self.read_until(b'}')?;
                if items.len() % 2 != 0 {
                    return Err(self.error("map requires an even number of forms"));
                }
                let mut pairs: Vec<(Edn, Edn)> = Vec::new();
                let mut iter = items.into_iter();
                while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                    pairs.push((k, v));
                }
                pairs.sort_by(|left, right| left.0.cmp(&right.0));
                Ok(Edn::Map(pairs))
            }
            b'"' => self.read_string(),
            b'#' => self.read_dispatch(),
            _ => self.read_atom(),
        }
    }

    fn read_until(&mut self, close: u8) -> Result<Vec<Edn>, EdnError> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                None => return Err(self.error("unterminated collection")),
                Some(b) if b == close => {
                    self.pos += 1;
                    return Ok(items);
                }
                Some(_) => items.push(self.read_form()?),
            }
        }
    }

    fn read_dispatch(&mut self) -> Result<Edn, EdnError> {
        self.pos += 1;
        match self.peek() {
            Some(b'{') => {
                self.pos += 1;
                let mut items = self.read_until(b'}')?;
                items.sort();
                items.dedup();
                Ok(Edn::Set(items))
            }
            Some(b'_') => {
                self.pos += 1;
                let _discarded = self.read_form()?;
                self.read_form()
            }
            _ => {
                let tag = self.read_token()?;
                if tag.is_empty() {
                    return Err(self.error("empty dispatch tag"));
                }
                let value = self.read_form()?;
                Ok(Edn::Tagged(tag, Box::new(value)))
            }
        }
    }

    fn read_string(&mut self) -> Result<Edn, EdnError> {
        self.pos += 1;
        let mut out = String::new();
        loop {
            match self
                .bump()
                .ok_or_else(|| self.error("unterminated string"))?
            {
                b'"' => return Ok(Edn::Str(out)),
                b'\\' => {
                    let escape = self
                        .bump()
                        .ok_or_else(|| self.error("unterminated escape"))?;
                    out.push(match escape {
                        b'n' => '\n',
                        b't' => '\t',
                        b'r' => '\r',
                        b'"' => '"',
                        b'\\' => '\\',
                        _ => return Err(self.error("unknown string escape")),
                    });
                }
                b => {
                    // Re-decode multi-byte UTF-8 starting at this byte.
                    let start = self.pos - 1;
                    let mut end = self.pos;
                    while end < self.bytes.len() && (self.bytes[end] & 0xC0) == 0x80 {
                        end += 1;
                    }
                    if b < 0x80 {
                        out.push(char::from(b));
                    } else {
                        let text = std::str::from_utf8(&self.bytes[start..end])
                            .map_err(|_| self.error("invalid UTF-8"))?;
                        out.push_str(text);
                        self.pos = end;
                    }
                }
            }
        }
    }

    fn read_token(&mut self) -> Result<String, EdnError> {
        let start = self.pos;
        while self.peek().is_some_and(|b| !DELIMITERS.contains(&b)) {
            self.pos += 1;
        }
        std::str::from_utf8(&self.bytes[start..self.pos])
            .map(str::to_owned)
            .map_err(|_| self.error("invalid UTF-8 token"))
    }

    fn read_atom(&mut self) -> Result<Edn, EdnError> {
        let token = self.read_token()?;
        if token.is_empty() {
            return Err(self.error("unexpected character"));
        }
        if let Some(name) = token.strip_prefix(':') {
            if name.is_empty() {
                return Err(self.error("empty keyword"));
            }
            return Ok(Edn::keyword(name));
        }
        match token.as_str() {
            "nil" => return Ok(Edn::Nil),
            "true" => return Ok(Edn::Bool(true)),
            "false" => return Ok(Edn::Bool(false)),
            _ => {}
        }
        let numeric_start = token.starts_with(|c: char| c.is_ascii_digit())
            || (token.len() > 1
                && (token.starts_with('-') || token.starts_with('+'))
                && token[1..].starts_with(|c: char| c.is_ascii_digit()));
        if numeric_start {
            if token.contains('.') || token.contains('e') || token.contains('E') {
                return token
                    .parse::<f64>()
                    .map(|v| Edn::Double(TotalF64(v)))
                    .map_err(|_| self.error("malformed float"));
            }
            return token
                .parse::<i64>()
                .map(Edn::Long)
                .map_err(|_| self.error("malformed integer"));
        }
        Ok(Edn::Symbol(token))
    }
}

impl fmt::Display for Edn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn write_seq(f: &mut fmt::Formatter<'_>, items: &[Edn]) -> fmt::Result {
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    write!(f, " ")?;
                }
                write!(f, "{item}")?;
            }
            Ok(())
        }
        match self {
            Self::Nil => write!(f, "nil"),
            Self::Bool(v) => write!(f, "{v}"),
            Self::Long(v) => write!(f, "{v}"),
            Self::Double(v) => write!(f, "{:?}", v.0),
            Self::Str(v) => write!(f, "{v:?}"),
            Self::Keyword(v) => write!(f, "{v}"),
            Self::Symbol(v) => write!(f, "{v}"),
            Self::List(items) => {
                write!(f, "(")?;
                write_seq(f, items)?;
                write!(f, ")")
            }
            Self::Vector(items) => {
                write!(f, "[")?;
                write_seq(f, items)?;
                write!(f, "]")
            }
            Self::Set(items) => {
                write!(f, "#{{")?;
                write_seq(f, items)?;
                write!(f, "}}")
            }
            Self::Map(pairs) => {
                write!(f, "{{")?;
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k} {v}")?;
                }
                write!(f, "}}")
            }
            Self::Tagged(tag, value) => write!(f, "#{tag} {value}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_query_shapes() {
        let form = read_one(
            "{:find [?e (count ?x)] ; comment\n :where [[?e :person/name \"A\"] [(< ?x 3)]]}",
        )
        .expect("parse");
        assert!(form.get(&Edn::keyword("find")).is_some());
        assert!(form.to_string().contains(":person/name"));
    }

    #[test]
    fn reads_sets_tags_and_discards() {
        assert_eq!(
            read_one("#{3 1 2 1}").expect("set"),
            Edn::Set(vec![Edn::Long(1), Edn::Long(2), Edn::Long(3)])
        );
        assert_eq!(
            read_one("#tempid \"a\"").expect("tag"),
            Edn::Tagged("tempid".into(), Box::new(Edn::Str("a".into())))
        );
        assert_eq!(
            read_one("[#_ 1 2]").expect("discard"),
            Edn::Vector(vec![Edn::Long(2)])
        );
    }

    #[test]
    fn reads_numbers_and_negative_symbols() {
        assert_eq!(read_one("-42").expect("int"), Edn::Long(-42));
        assert_eq!(read_one("1.5").expect("float"), Edn::Double(TotalF64(1.5)));
        assert_eq!(read_one("-").expect("minus"), Edn::symbol("-"));
        assert_eq!(read_one("?e").expect("var"), Edn::symbol("?e"));
    }
}
