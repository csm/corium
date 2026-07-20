use std::fmt;

use arrow::array::Array as _;
use arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::ScalarValue;

use crate::SqlError;

/// SQL result type independent of `DataFusion` and Arrow versions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlType {
    /// SQL NULL.
    Null,
    /// Boolean.
    Boolean,
    /// Signed integer, with its bit width.
    SignedInteger(u8),
    /// Unsigned integer, with its bit width.
    UnsignedInteger(u8),
    /// Floating-point value, with its bit width.
    Float(u8),
    /// Milliseconds since the Unix epoch, with an optional timezone.
    TimestampMillis(Option<String>),
    /// UTF-8 text.
    Text,
    /// Arbitrary bytes.
    Bytes,
    /// Variable-length list.
    List(Box<Self>),
    /// An engine type not yet represented structurally by this API.
    Other(String),
}

impl SqlType {
    pub(crate) fn from_arrow(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::SignedInteger(8),
            DataType::Int16 => Self::SignedInteger(16),
            DataType::Int32 => Self::SignedInteger(32),
            DataType::Int64 => Self::SignedInteger(64),
            DataType::UInt8 => Self::UnsignedInteger(8),
            DataType::UInt16 => Self::UnsignedInteger(16),
            DataType::UInt32 => Self::UnsignedInteger(32),
            DataType::UInt64 => Self::UnsignedInteger(64),
            DataType::Float16 => Self::Float(16),
            DataType::Float32 => Self::Float(32),
            DataType::Float64 => Self::Float(64),
            DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
                Self::TimestampMillis(timezone.as_ref().map(ToString::to_string))
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Self::Text,
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => Self::Bytes,
            DataType::List(field) | DataType::LargeList(field) => {
                Self::List(Box::new(Self::from_arrow(field.data_type())))
            }
            other => Self::Other(other.to_string()),
        }
    }
}

/// One result column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlColumn {
    /// Projected column name.
    pub name: String,
    /// SQL result type.
    pub data_type: SqlType,
    /// Whether the result may contain NULL.
    pub nullable: bool,
}

impl SqlColumn {
    pub(crate) fn from_arrow(field: &Field) -> Self {
        Self {
            name: field.name().clone(),
            data_type: SqlType::from_arrow(field.data_type()),
            nullable: field.is_nullable(),
        }
    }
}

/// One SQL value independent of `DataFusion` and Arrow versions.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    /// SQL NULL.
    Null,
    /// Boolean.
    Boolean(bool),
    /// Signed integer normalized to 64 bits.
    Integer(i64),
    /// Unsigned integer normalized to 64 bits.
    Unsigned(u64),
    /// Floating-point value normalized to 64 bits.
    Float(f64),
    /// Milliseconds since the Unix epoch.
    TimestampMillis(i64),
    /// UTF-8 text.
    Text(String),
    /// Arbitrary bytes.
    Bytes(Vec<u8>),
    /// Variable-length list. Corium cardinality-many columns use unique,
    /// deterministically ordered lists with set semantics.
    List(Vec<Self>),
    /// Text rendering of an engine value not yet represented structurally.
    Other(String),
}

/// One SQL result row.
pub type SqlRow = Vec<SqlValue>;

impl SqlValue {
    pub(crate) fn from_scalar(value: ScalarValue) -> Result<Self, SqlError> {
        Ok(match value {
            ScalarValue::Null
            | ScalarValue::Boolean(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Float16(None)
            | ScalarValue::Float32(None)
            | ScalarValue::Float64(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Binary(None)
            | ScalarValue::BinaryView(None)
            | ScalarValue::LargeBinary(None)
            | ScalarValue::TimestampMillisecond(None, _)
            | ScalarValue::FixedSizeBinary(_, None) => Self::Null,
            ScalarValue::Boolean(Some(value)) => Self::Boolean(value),
            ScalarValue::Int8(Some(value)) => Self::Integer(i64::from(value)),
            ScalarValue::Int16(Some(value)) => Self::Integer(i64::from(value)),
            ScalarValue::Int32(Some(value)) => Self::Integer(i64::from(value)),
            ScalarValue::Int64(Some(value)) => Self::Integer(value),
            ScalarValue::UInt8(Some(value)) => Self::Unsigned(u64::from(value)),
            ScalarValue::UInt16(Some(value)) => Self::Unsigned(u64::from(value)),
            ScalarValue::UInt32(Some(value)) => Self::Unsigned(u64::from(value)),
            ScalarValue::UInt64(Some(value)) => Self::Unsigned(value),
            ScalarValue::Float16(Some(value)) => Self::Float(f64::from(value)),
            ScalarValue::Float32(Some(value)) => Self::Float(f64::from(value)),
            ScalarValue::Float64(Some(value)) => Self::Float(value),
            ScalarValue::TimestampMillisecond(Some(value), _) => Self::TimestampMillis(value),
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)) => Self::Text(value),
            ScalarValue::Binary(Some(value))
            | ScalarValue::BinaryView(Some(value))
            | ScalarValue::LargeBinary(Some(value))
            | ScalarValue::FixedSizeBinary(_, Some(value)) => Self::Bytes(value),
            ScalarValue::List(array) => {
                if array.is_null(0) {
                    Self::Null
                } else {
                    let values = array.value(0);
                    let mut result = Vec::with_capacity(values.len());
                    for index in 0..values.len() {
                        result.push(Self::from_scalar(ScalarValue::try_from_array(
                            values.as_ref(),
                            index,
                        )?)?);
                    }
                    Self::List(result)
                }
            }
            other => Self::Other(other.to_string()),
        })
    }
}

impl fmt::Display for SqlValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => formatter.write_str("NULL"),
            Self::Boolean(value) => write!(formatter, "{value}"),
            Self::Integer(value) | Self::TimestampMillis(value) => write!(formatter, "{value}"),
            Self::Unsigned(value) => write!(formatter, "{value}"),
            Self::Float(value) => write!(formatter, "{value}"),
            Self::Text(value) | Self::Other(value) => formatter.write_str(value),
            Self::Bytes(value) => {
                for byte in value {
                    write!(formatter, "{byte:02x}")?;
                }
                Ok(())
            }
            Self::List(values) => {
                formatter.write_str("[")?;
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{value}")?;
                }
                formatter.write_str("]")
            }
        }
    }
}
