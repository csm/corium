//! Aggregate evaluation for `:find` specifications.
//!
//! The engine is deterministic: `sample` returns the first `n` distinct
//! values in sort order rather than a random selection, and `rand` is not
//! provided. `median` returns the lower middle element for even counts.

use corium_core::{TotalF64, Value};

use crate::QueryError;
use crate::builtins::compare;

/// An aggregate outcome, distinguishing scalar, ordered-collection, and
/// set-valued results for output conversion.
#[derive(Clone, Debug, PartialEq)]
pub enum AggOut {
    /// A scalar value.
    Value(Value),
    /// An ordered collection (n-arity `min`/`max`, `sample`).
    Coll(Vec<Value>),
    /// A set of distinct values (`distinct`).
    Set(Vec<Value>),
}

fn distinct_sorted(values: &[Value]) -> Result<Vec<Value>, QueryError> {
    let mut out: Vec<Value> = Vec::new();
    for value in values {
        if !out.iter().any(|v| v == value) {
            out.push(value.clone());
        }
    }
    sort_values(&mut out)?;
    Ok(out)
}

fn sort_values(values: &mut [Value]) -> Result<(), QueryError> {
    // Validate comparability first so sort_by can stay total.
    for window in values.windows(2) {
        compare(&window[0], &window[1])?;
    }
    values.sort_by(|left, right| compare(left, right).unwrap_or(std::cmp::Ordering::Equal));
    Ok(())
}

#[allow(clippy::cast_precision_loss)]
fn numeric_sum(values: &[Value]) -> Result<Value, QueryError> {
    let mut long_sum: i64 = 0;
    let mut double_sum: f64 = 0.0;
    let mut any_double = false;
    for value in values {
        match value {
            Value::Long(v) => {
                long_sum = long_sum
                    .checked_add(*v)
                    .ok_or_else(|| QueryError::Type("sum overflowed".into()))?;
            }
            Value::Double(v) => {
                any_double = true;
                double_sum += v.0;
            }
            _ => return Err(QueryError::Type("sum requires numbers".into())),
        }
    }
    if any_double {
        Ok(Value::Double(TotalF64(double_sum + long_sum as f64)))
    } else {
        Ok(Value::Long(long_sum))
    }
}

#[allow(clippy::cast_precision_loss)]
fn as_f64(value: &Value) -> Result<f64, QueryError> {
    match value {
        Value::Long(v) => Ok(*v as f64),
        Value::Double(v) => Ok(v.0),
        _ => Err(QueryError::Type("aggregate requires numbers".into())),
    }
}

/// Applies aggregate `op` (with optional constant argument `n`) to the bag
/// of `values` collected for one group.
///
/// # Errors
/// Returns [`QueryError`] for unknown operations or unsupported operand
/// types.
#[allow(clippy::cast_precision_loss)]
pub fn apply(op: &str, n: Option<i64>, values: &[Value]) -> Result<AggOut, QueryError> {
    let take = |n: Option<i64>| -> Result<usize, QueryError> {
        n.and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| QueryError::Arity(format!("({op} n ?x) requires a positive count")))
    };
    match (op, n) {
        ("count", None) => Ok(AggOut::Value(Value::Long(
            i64::try_from(values.len()).unwrap_or(i64::MAX),
        ))),
        ("count-distinct", None) => Ok(AggOut::Value(Value::Long(
            i64::try_from(distinct_sorted(values)?.len()).unwrap_or(i64::MAX),
        ))),
        ("sum", None) => numeric_sum(values).map(AggOut::Value),
        ("avg", None) => {
            if values.is_empty() {
                return Err(QueryError::Type("avg of no values".into()));
            }
            let sum: f64 = values.iter().map(as_f64).sum::<Result<f64, _>>()?;
            Ok(AggOut::Value(Value::Double(TotalF64(
                sum / values.len() as f64,
            ))))
        }
        ("median", None) => {
            if values.is_empty() {
                return Err(QueryError::Type("median of no values".into()));
            }
            let mut sorted = values.to_vec();
            sort_values(&mut sorted)?;
            Ok(AggOut::Value(sorted[(sorted.len() - 1) / 2].clone()))
        }
        ("min" | "max", None) => {
            if values.is_empty() {
                return Err(QueryError::Type(format!("{op} of no values")));
            }
            let mut best = values[0].clone();
            for value in &values[1..] {
                let ordering = compare(value, &best)?;
                if (op == "min" && ordering.is_lt()) || (op == "max" && ordering.is_gt()) {
                    best = value.clone();
                }
            }
            Ok(AggOut::Value(best))
        }
        ("min" | "sample", Some(_)) => {
            let count = take(n)?;
            let sorted = distinct_sorted(values)?;
            Ok(AggOut::Coll(sorted.into_iter().take(count).collect()))
        }
        ("max", Some(_)) => {
            let count = take(n)?;
            let sorted = distinct_sorted(values)?;
            let skip = sorted.len().saturating_sub(count);
            Ok(AggOut::Coll(sorted.into_iter().skip(skip).collect()))
        }
        ("distinct", None) => Ok(AggOut::Set(distinct_sorted(values)?)),
        ("variance" | "stddev", None) => {
            if values.is_empty() {
                return Err(QueryError::Type(format!("{op} of no values")));
            }
            let nums: Vec<f64> = values.iter().map(as_f64).collect::<Result<_, _>>()?;
            let mean = nums.iter().sum::<f64>() / nums.len() as f64;
            let variance = nums.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / nums.len() as f64;
            let out = if op == "variance" {
                variance
            } else {
                variance.sqrt()
            };
            Ok(AggOut::Value(Value::Double(TotalF64(out))))
        }
        _ => Err(QueryError::Unsupported(format!(
            "unknown aggregate ({op} …)"
        ))),
    }
}
