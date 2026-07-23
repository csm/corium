//! Conversions between Rust values and boundary [`Edn`], used throughout the
//! fluent API for query constants, transaction values, and typed result
//! extraction.

use corium_core::{EntityId, Keyword, TotalF64};
use corium_query::edn::Edn;

use crate::ClientError;

/// A Rust value that can be lowered into boundary [`Edn`].
///
/// Implemented for the scalar types that appear as query constants,
/// transaction values, and pull/lookup arguments. This is the seam the
/// builders use so callers write `lit(42)` or `attr("person/name")` instead
/// of constructing [`Edn`] by hand.
pub trait IntoEdn {
    /// Lowers `self` into its boundary [`Edn`] form.
    fn into_edn(self) -> Edn;
}

impl IntoEdn for Edn {
    fn into_edn(self) -> Edn {
        self
    }
}

impl IntoEdn for &Edn {
    fn into_edn(self) -> Edn {
        self.clone()
    }
}

impl IntoEdn for bool {
    fn into_edn(self) -> Edn {
        Edn::Bool(self)
    }
}

impl IntoEdn for i64 {
    fn into_edn(self) -> Edn {
        Edn::Long(self)
    }
}

impl IntoEdn for i32 {
    fn into_edn(self) -> Edn {
        Edn::Long(i64::from(self))
    }
}

impl IntoEdn for u32 {
    fn into_edn(self) -> Edn {
        Edn::Long(i64::from(self))
    }
}

impl IntoEdn for f64 {
    fn into_edn(self) -> Edn {
        Edn::Double(TotalF64(self))
    }
}

impl IntoEdn for &str {
    fn into_edn(self) -> Edn {
        Edn::Str(self.to_owned())
    }
}

impl IntoEdn for String {
    fn into_edn(self) -> Edn {
        Edn::Str(self)
    }
}

impl IntoEdn for &String {
    fn into_edn(self) -> Edn {
        Edn::Str(self.clone())
    }
}

impl IntoEdn for Keyword {
    fn into_edn(self) -> Edn {
        Edn::Keyword(self)
    }
}

impl IntoEdn for &Keyword {
    fn into_edn(self) -> Edn {
        Edn::Keyword(self.clone())
    }
}

impl IntoEdn for EntityId {
    fn into_edn(self) -> Edn {
        Edn::Long(i64::try_from(self.raw()).unwrap_or(i64::MAX))
    }
}

/// A Rust value that can be read back out of boundary [`Edn`], for typed
/// extraction from query rows and pull results.
///
/// # Examples
/// ```no_run
/// # use corium_client::{FromEdn, QueryResult};
/// # fn demo(result: QueryResult) -> Result<(), corium_client::ClientError> {
/// for row in result.rows() {
///     let name: String = row.get(0)?;
///     let age: i64 = row.get(1)?;
///     println!("{name} is {age}");
/// }
/// # Ok(())
/// # }
/// ```
pub trait FromEdn: Sized {
    /// Reads `self` from a boundary [`Edn`] form.
    ///
    /// # Errors
    /// Returns [`ClientError::Decode`] when the form is not the expected
    /// shape.
    fn from_edn(form: &Edn) -> Result<Self, ClientError>;
}

fn decode_err(what: &str, form: &Edn) -> ClientError {
    ClientError::Decode(format!("expected {what}, got {form}"))
}

impl FromEdn for Edn {
    fn from_edn(form: &Edn) -> Result<Self, ClientError> {
        Ok(form.clone())
    }
}

impl FromEdn for bool {
    fn from_edn(form: &Edn) -> Result<Self, ClientError> {
        match form {
            Edn::Bool(value) => Ok(*value),
            other => Err(decode_err("bool", other)),
        }
    }
}

impl FromEdn for i64 {
    fn from_edn(form: &Edn) -> Result<Self, ClientError> {
        match form {
            Edn::Long(value) => Ok(*value),
            other => Err(decode_err("long", other)),
        }
    }
}

impl FromEdn for f64 {
    #[allow(clippy::cast_precision_loss)]
    fn from_edn(form: &Edn) -> Result<Self, ClientError> {
        match form {
            Edn::Double(value) => Ok(value.0),
            // A whole-number long widens for convenience; very large
            // magnitudes lose precision, as with any i64-to-f64 conversion.
            Edn::Long(value) => Ok(*value as Self),
            other => Err(decode_err("double", other)),
        }
    }
}

impl FromEdn for String {
    fn from_edn(form: &Edn) -> Result<Self, ClientError> {
        match form {
            Edn::Str(value) => Ok(value.clone()),
            other => Err(decode_err("string", other)),
        }
    }
}

impl FromEdn for Keyword {
    fn from_edn(form: &Edn) -> Result<Self, ClientError> {
        match form {
            Edn::Keyword(value) => Ok(value.clone()),
            other => Err(decode_err("keyword", other)),
        }
    }
}

impl FromEdn for EntityId {
    fn from_edn(form: &Edn) -> Result<Self, ClientError> {
        match form {
            // Entity ids surface as longs at the boundary (as in Datomic).
            Edn::Long(value) => u64::try_from(*value)
                .map(EntityId::from_raw)
                .map_err(|_| decode_err("entity id", form)),
            other => Err(decode_err("entity id", other)),
        }
    }
}

impl<T: FromEdn> FromEdn for Option<T> {
    fn from_edn(form: &Edn) -> Result<Self, ClientError> {
        match form {
            Edn::Nil => Ok(None),
            other => T::from_edn(other).map(Some),
        }
    }
}

impl<T: FromEdn> FromEdn for Vec<T> {
    fn from_edn(form: &Edn) -> Result<Self, ClientError> {
        match form {
            Edn::Vector(items) | Edn::List(items) | Edn::Set(items) => {
                items.iter().map(T::from_edn).collect()
            }
            other => Err(decode_err("collection", other)),
        }
    }
}
