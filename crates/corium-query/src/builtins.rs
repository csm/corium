//! The native predicate/function set for query call clauses.
//!
//! This is resolution step (1) from `docs/design/query-engine.md`; the
//! sandboxed cljrs resolution seam (step 2) plugs in post-v1. `tuple` and
//! `untuple` are deliberately absent until tuple value types land
//! (deferred by ADR-0009).

use std::cmp::Ordering;

use corium_core::{TotalF64, Value};

use crate::QueryError;

/// Result of evaluating a native call.
#[derive(Clone, Debug, PartialEq)]
pub enum CallResult {
    /// A predicate outcome.
    Test(bool),
    /// A scalar value.
    Scalar(Value),
    /// A collection of values.
    Coll(Vec<Value>),
    /// A relation of tuples.
    Rel(Vec<Vec<Value>>),
}

fn type_error(name: &str, args: &[Value]) -> QueryError {
    QueryError::Type(format!("{name} cannot be applied to {args:?}"))
}

fn arity_error(name: &str) -> QueryError {
    QueryError::Arity(format!("wrong number of arguments to {name}"))
}

/// Numeric view of a value for arithmetic and comparisons.
enum Num {
    Long(i64),
    Double(f64),
}

fn as_num(value: &Value) -> Option<Num> {
    match value {
        Value::Long(v) | Value::Instant(v) => Some(Num::Long(*v)),
        Value::Double(v) => Some(Num::Double(v.0)),
        // Entity ids compare as their numeric representation, as in Datomic.
        Value::Ref(e) => i64::try_from(e.raw()).ok().map(Num::Long),
        _ => None,
    }
}

/// Total comparison across the value types queries can meet, with numeric
/// coercion between longs, doubles, instants, and entity ids.
///
/// # Errors
/// Returns [`QueryError::Type`] for incomparable operand types.
pub fn compare(left: &Value, right: &Value) -> Result<Ordering, QueryError> {
    if let (Some(l), Some(r)) = (as_num(left), as_num(right)) {
        return Ok(match (l, r) {
            (Num::Long(l), Num::Long(r)) => l.cmp(&r),
            (Num::Long(l), Num::Double(r)) => TotalF64(to_f64(l)).cmp(&TotalF64(r)),
            (Num::Double(l), Num::Long(r)) => TotalF64(l).cmp(&TotalF64(to_f64(r))),
            (Num::Double(l), Num::Double(r)) => TotalF64(l).cmp(&TotalF64(r)),
        });
    }
    match (left, right) {
        (Value::Str(l), Value::Str(r)) => Ok(l.cmp(r)),
        (Value::Bool(l), Value::Bool(r)) => Ok(l.cmp(r)),
        (Value::Keyword(l), Value::Keyword(r)) => Ok(l.cmp(r)),
        (Value::Uuid(l), Value::Uuid(r)) => Ok(l.cmp(r)),
        (Value::Bytes(l), Value::Bytes(r)) => Ok(l.cmp(r)),
        _ => Err(QueryError::Type(format!(
            "cannot compare {left:?} with {right:?}"
        ))),
    }
}

/// Loose equality used by `=`/`!=`: numeric coercion, no error on
/// cross-type operands (they are simply unequal).
#[must_use]
pub fn loose_eq(left: &Value, right: &Value) -> bool {
    compare(left, right).map(Ordering::is_eq).unwrap_or(false)
}

#[allow(clippy::cast_precision_loss)]
fn to_f64(v: i64) -> f64 {
    v as f64
}

fn fold_numeric(
    name: &str,
    args: &[Value],
    long_op: impl Fn(i64, i64) -> Option<i64>,
    double_op: impl Fn(f64, f64) -> f64,
) -> Result<Value, QueryError> {
    let mut iter = args.iter();
    let first = iter.next().ok_or_else(|| arity_error(name))?;
    let mut acc = match first {
        Value::Long(v) => Num::Long(*v),
        Value::Double(v) => Num::Double(v.0),
        _ => return Err(type_error(name, args)),
    };
    for arg in iter {
        let rhs = match arg {
            Value::Long(v) => Num::Long(*v),
            Value::Double(v) => Num::Double(v.0),
            _ => return Err(type_error(name, args)),
        };
        acc = match (acc, rhs) {
            (Num::Long(l), Num::Long(r)) => match long_op(l, r) {
                Some(v) => Num::Long(v),
                None => return Err(QueryError::Type(format!("{name} overflowed"))),
            },
            (l, r) => {
                let l = match l {
                    Num::Long(v) => to_f64(v),
                    Num::Double(v) => v,
                };
                let r = match r {
                    Num::Long(v) => to_f64(v),
                    Num::Double(v) => v,
                };
                Num::Double(double_op(l, r))
            }
        };
    }
    Ok(match acc {
        Num::Long(v) => Value::Long(v),
        Num::Double(v) => Value::Double(TotalF64(v)),
    })
}

fn string_arg<'a>(name: &str, args: &'a [Value], index: usize) -> Result<&'a str, QueryError> {
    match args.get(index) {
        Some(Value::Str(s)) => Ok(s),
        _ => Err(type_error(name, args)),
    }
}

fn long_arg(name: &str, args: &[Value], index: usize) -> Result<i64, QueryError> {
    match args.get(index) {
        Some(Value::Long(v)) => Ok(*v),
        _ => Err(type_error(name, args)),
    }
}

fn one_long(name: &str, args: &[Value]) -> Result<i64, QueryError> {
    if args.len() != 1 {
        return Err(arity_error(name));
    }
    long_arg(name, args, 0)
}

fn two_longs(name: &str, args: &[Value]) -> Result<(i64, i64), QueryError> {
    if args.len() != 2 {
        return Err(arity_error(name));
    }
    Ok((long_arg(name, args, 0)?, long_arg(name, args, 1)?))
}

fn chain_compare(
    args: &[Value],
    accept: impl Fn(Ordering) -> bool,
) -> Result<CallResult, QueryError> {
    for window in args.windows(2) {
        if !accept(compare(&window[0], &window[1])?) {
            return Ok(CallResult::Test(false));
        }
    }
    Ok(CallResult::Test(true))
}

/// Renders a value the way `str` concatenation sees it.
#[must_use]
pub fn value_to_display(value: &Value) -> String {
    match value {
        Value::Bool(v) => v.to_string(),
        Value::Long(v) | Value::Instant(v) => v.to_string(),
        Value::Double(v) => v.0.to_string(),
        Value::Str(s) => s.to_string(),
        Value::Uuid(v) => format!("{v:032x}"),
        Value::Keyword(id) => format!(":kw{id}"),
        Value::Ref(e) => e.raw().to_string(),
        Value::Bytes(b) => format!("{b:02x?}"),
    }
}

/// Whether `name` is in the native call set (excluding the db-context
/// builtins `get-else`, `missing?`, and `ground`, which the executor
/// handles itself).
#[must_use]
pub fn is_native(name: &str) -> bool {
    matches!(
        name,
        "<" | "<="
            | ">"
            | ">="
            | "="
            | "=="
            | "!="
            | "not="
            | "even?"
            | "odd?"
            | "zero?"
            | "pos?"
            | "neg?"
            | "true?"
            | "false?"
            | "starts-with?"
            | "ends-with?"
            | "includes?"
            | "+"
            | "-"
            | "*"
            | "/"
            | "quot"
            | "rem"
            | "mod"
            | "inc"
            | "dec"
            | "abs"
            | "min"
            | "max"
            | "str"
            | "count"
            | "subs"
            | "upper-case"
            | "lower-case"
            | "identity"
    )
}

/// Evaluates a native call over fully bound arguments.
///
/// # Errors
/// Returns [`QueryError`] for unknown names, wrong arity, or operand types
/// the operation does not support.
#[allow(clippy::too_many_lines)]
pub fn call(name: &str, args: &[Value]) -> Result<CallResult, QueryError> {
    match name {
        "<" => chain_compare(args, Ordering::is_lt),
        "<=" => chain_compare(args, Ordering::is_le),
        ">" => chain_compare(args, Ordering::is_gt),
        ">=" => chain_compare(args, Ordering::is_ge),
        "=" | "==" => Ok(CallResult::Test(
            args.windows(2).all(|w| loose_eq(&w[0], &w[1])),
        )),
        "!=" | "not=" => Ok(CallResult::Test(
            args.windows(2).any(|w| !loose_eq(&w[0], &w[1])),
        )),
        "even?" => Ok(CallResult::Test(one_long(name, args)? % 2 == 0)),
        "odd?" => Ok(CallResult::Test(one_long(name, args)? % 2 != 0)),
        "zero?" => Ok(CallResult::Test(one_long(name, args)? == 0)),
        "pos?" => Ok(CallResult::Test(one_long(name, args)? > 0)),
        "neg?" => Ok(CallResult::Test(one_long(name, args)? < 0)),
        "true?" => Ok(CallResult::Test(args == [Value::Bool(true)])),
        "false?" => Ok(CallResult::Test(args == [Value::Bool(false)])),
        "starts-with?" => Ok(CallResult::Test(
            string_arg(name, args, 0)?.starts_with(string_arg(name, args, 1)?),
        )),
        "ends-with?" => Ok(CallResult::Test(
            string_arg(name, args, 0)?.ends_with(string_arg(name, args, 1)?),
        )),
        "includes?" => Ok(CallResult::Test(
            string_arg(name, args, 0)?.contains(string_arg(name, args, 1)?),
        )),
        "+" => Ok(CallResult::Scalar(fold_numeric(
            name,
            args,
            i64::checked_add,
            |l, r| l + r,
        )?)),
        "-" => {
            if args.len() == 1 {
                return call("-", &[Value::Long(0), args[0].clone()]);
            }
            Ok(CallResult::Scalar(fold_numeric(
                name,
                args,
                i64::checked_sub,
                |l, r| l - r,
            )?))
        }
        "*" => Ok(CallResult::Scalar(fold_numeric(
            name,
            args,
            i64::checked_mul,
            |l, r| l * r,
        )?)),
        "/" => {
            // Long division stays exact when it divides evenly, otherwise
            // falls to a double (there is no ratio type).
            if args.len() != 2 {
                return Err(arity_error(name));
            }
            match (&args[0], &args[1]) {
                (Value::Long(_), Value::Long(0)) => {
                    Err(QueryError::Type("division by zero".into()))
                }
                (Value::Long(l), Value::Long(r)) if l % r == 0 => {
                    Ok(CallResult::Scalar(Value::Long(l / r)))
                }
                (left, right) => {
                    let (Some(l), Some(r)) = (as_num(left), as_num(right)) else {
                        return Err(type_error(name, args));
                    };
                    let l = match l {
                        Num::Long(v) => to_f64(v),
                        Num::Double(v) => v,
                    };
                    let r = match r {
                        Num::Long(v) => to_f64(v),
                        Num::Double(v) => v,
                    };
                    Ok(CallResult::Scalar(Value::Double(TotalF64(l / r))))
                }
            }
        }
        "quot" => {
            let (l, r) = two_longs(name, args)?;
            if r == 0 {
                return Err(QueryError::Type("division by zero".into()));
            }
            Ok(CallResult::Scalar(Value::Long(l / r)))
        }
        "rem" => {
            let (l, r) = two_longs(name, args)?;
            if r == 0 {
                return Err(QueryError::Type("division by zero".into()));
            }
            Ok(CallResult::Scalar(Value::Long(l % r)))
        }
        "mod" => {
            let (l, r) = two_longs(name, args)?;
            if r == 0 {
                return Err(QueryError::Type("division by zero".into()));
            }
            Ok(CallResult::Scalar(Value::Long(l.rem_euclid(r))))
        }
        "inc" => Ok(CallResult::Scalar(Value::Long(
            one_long(name, args)?
                .checked_add(1)
                .ok_or_else(|| QueryError::Type("inc overflowed".into()))?,
        ))),
        "dec" => Ok(CallResult::Scalar(Value::Long(
            one_long(name, args)?
                .checked_sub(1)
                .ok_or_else(|| QueryError::Type("dec overflowed".into()))?,
        ))),
        "abs" => match args {
            [Value::Long(v)] => Ok(CallResult::Scalar(Value::Long(v.abs()))),
            [Value::Double(v)] => Ok(CallResult::Scalar(Value::Double(TotalF64(v.0.abs())))),
            _ => Err(type_error(name, args)),
        },
        "min" | "max" => {
            if args.is_empty() {
                return Err(arity_error(name));
            }
            let mut best = args[0].clone();
            for arg in &args[1..] {
                let ordering = compare(arg, &best)?;
                let better = if name == "min" {
                    ordering.is_lt()
                } else {
                    ordering.is_gt()
                };
                if better {
                    best = arg.clone();
                }
            }
            Ok(CallResult::Scalar(best))
        }
        "str" => Ok(CallResult::Scalar(Value::Str(
            args.iter().map(value_to_display).collect::<String>().into(),
        ))),
        "count" => Ok(CallResult::Scalar(Value::Long(
            i64::try_from(string_arg(name, args, 0)?.chars().count())
                .map_err(|_| QueryError::Type("count overflowed".into()))?,
        ))),
        "subs" => {
            let text = string_arg(name, args, 0)?;
            let start =
                usize::try_from(long_arg(name, args, 1)?).map_err(|_| type_error(name, args))?;
            let end = match args.get(2) {
                Some(Value::Long(v)) => usize::try_from(*v).map_err(|_| type_error(name, args))?,
                None => text.chars().count(),
                Some(_) => return Err(type_error(name, args)),
            };
            let out: String = text
                .chars()
                .skip(start)
                .take(end.saturating_sub(start))
                .collect();
            Ok(CallResult::Scalar(Value::Str(out.into())))
        }
        "upper-case" => Ok(CallResult::Scalar(Value::Str(
            string_arg(name, args, 0)?.to_uppercase().into(),
        ))),
        "lower-case" => Ok(CallResult::Scalar(Value::Str(
            string_arg(name, args, 0)?.to_lowercase().into(),
        ))),
        "identity" => match args {
            [value] => Ok(CallResult::Scalar(value.clone())),
            _ => Err(arity_error(name)),
        },
        _ => Err(QueryError::Unsupported(format!(
            "unknown function or predicate {name}"
        ))),
    }
}
