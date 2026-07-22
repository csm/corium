//! Query results and typed row access.

use corium_query::edn::Edn;

use crate::ClientError;
use crate::value::FromEdn;

/// The shape of a query result, fixed by the query's `:find` clause.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultShape {
    /// A set of tuples (`:find ?a ?b`).
    Relation,
    /// A flat collection (`:find [?x ...]`).
    Collection,
    /// A single tuple (`:find [?a ?b]`).
    Tuple,
    /// A single value (`:find ?x .`).
    Scalar,
}

/// A query result: the boundary [`Edn`] value plus its [`ResultShape`], with
/// typed accessors keyed to the shape.
#[derive(Clone, Debug)]
pub struct QueryResult {
    shape: ResultShape,
    value: Edn,
}

impl QueryResult {
    /// Wraps a boundary result value with its shape.
    #[must_use]
    pub fn new(shape: ResultShape, value: Edn) -> Self {
        Self { shape, value }
    }

    /// The result shape.
    #[must_use]
    pub fn shape(&self) -> ResultShape {
        self.shape
    }

    /// The raw boundary [`Edn`] value.
    #[must_use]
    pub fn edn(&self) -> &Edn {
        &self.value
    }

    /// Consumes the result, yielding the raw boundary [`Edn`] value.
    #[must_use]
    pub fn into_edn(self) -> Edn {
        self.value
    }

    /// Rows of a relation result. Empty for other shapes.
    #[must_use]
    pub fn rows(&self) -> Vec<Row<'_>> {
        match (&self.shape, &self.value) {
            (ResultShape::Relation, Edn::Vector(rows)) => rows
                .iter()
                .filter_map(|row| match row {
                    Edn::Vector(cells) => Some(Row(cells)),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Values of a collection result. Empty for other shapes.
    #[must_use]
    pub fn values(&self) -> Vec<&Edn> {
        match (&self.shape, &self.value) {
            (ResultShape::Collection, Edn::Vector(items)) => items.iter().collect(),
            _ => Vec::new(),
        }
    }

    /// Typed values of a collection result.
    ///
    /// # Errors
    /// Returns [`ClientError::Decode`] if any value is not of type `T`.
    pub fn values_as<T: FromEdn>(&self) -> Result<Vec<T>, ClientError> {
        self.values().into_iter().map(T::from_edn).collect()
    }

    /// The single tuple of a tuple result, or `None` when empty.
    #[must_use]
    pub fn tuple(&self) -> Option<Row<'_>> {
        match (&self.shape, &self.value) {
            (ResultShape::Tuple, Edn::Vector(cells)) => Some(Row(cells)),
            _ => None,
        }
    }

    /// The single value of a scalar result, or `None` when empty (`nil`).
    #[must_use]
    pub fn scalar(&self) -> Option<&Edn> {
        match (&self.shape, &self.value) {
            (ResultShape::Scalar, Edn::Nil) => None,
            (ResultShape::Scalar, value) => Some(value),
            _ => None,
        }
    }

    /// The typed scalar value of a scalar result.
    ///
    /// # Errors
    /// Returns [`ClientError::Decode`] if the value is not of type `T`.
    pub fn scalar_as<T: FromEdn>(&self) -> Result<Option<T>, ClientError> {
        self.scalar().map(T::from_edn).transpose()
    }

    /// Whether the result is empty (no rows/values, or a `nil` tuple/scalar).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match &self.value {
            Edn::Nil => true,
            Edn::Vector(items) => items.is_empty(),
            _ => false,
        }
    }
}

/// A borrowed view of one result row (a relation row or the tuple result),
/// with typed cell access.
#[derive(Clone, Copy, Debug)]
pub struct Row<'a>(&'a [Edn]);

impl<'a> Row<'a> {
    /// The number of cells.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the row has no cells.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The raw cell at `index`.
    #[must_use]
    pub fn edn(&self, index: usize) -> Option<&'a Edn> {
        self.0.get(index)
    }

    /// The cells as a slice.
    #[must_use]
    pub fn cells(&self) -> &'a [Edn] {
        self.0
    }

    /// The typed cell at `index`.
    ///
    /// # Errors
    /// Returns [`ClientError::Decode`] if the cell is missing or not of type
    /// `T`.
    pub fn get<T: FromEdn>(&self, index: usize) -> Result<T, ClientError> {
        let cell = self
            .0
            .get(index)
            .ok_or_else(|| ClientError::Decode(format!("row has no cell at index {index}")))?;
        T::from_edn(cell)
    }
}
