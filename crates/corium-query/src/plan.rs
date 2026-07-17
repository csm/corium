//! Clause ordering and index selection.
//!
//! Ordering is greedy selectivity ordering: start from the most selective
//! bound pattern and repeatedly pick the cheapest runnable clause, using
//! per-attribute statistics from the database. Explicit clause order is the
//! tiebreak, matching the Datomic performance model users know.

use std::collections::BTreeSet;

use corium_core::IndexOrder;

use crate::ast::{Clause, Term, Var, clause_vars};
use crate::exec::ExecCtx;

/// How a pattern scan is keyed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanChoice {
    /// Chosen covering index.
    pub order: IndexOrder,
    /// Number of leading components bound into the scan prefix.
    pub prefix_len: usize,
}

/// Picks the covering index and prefix length for one pattern lookup.
///
/// `avet_ok` says whether the bound attribute participates in AVET;
/// `v_is_ref` whether the bound value is an entity reference.
///
/// A bound `a` never yields a full scan: AEVT (or AVET) always gives at
/// least a one-component prefix — the property the roadmap requires.
#[must_use]
#[allow(clippy::fn_params_excessive_bools)]
pub fn choose_index(
    e_bound: bool,
    a_bound: bool,
    v_bound: bool,
    avet_ok: bool,
    v_is_ref: bool,
) -> ScanChoice {
    if e_bound {
        let prefix_len = 1 + usize::from(a_bound) + usize::from(a_bound && v_bound);
        return ScanChoice {
            order: IndexOrder::Eavt,
            prefix_len,
        };
    }
    if a_bound {
        if v_bound && avet_ok {
            return ScanChoice {
                order: IndexOrder::Avet,
                prefix_len: 2,
            };
        }
        return ScanChoice {
            order: IndexOrder::Aevt,
            prefix_len: 1,
        };
    }
    if v_bound && v_is_ref {
        return ScanChoice {
            order: IndexOrder::Vaet,
            prefix_len: 1,
        };
    }
    ScanChoice {
        order: IndexOrder::Eavt,
        prefix_len: 0,
    }
}

/// Cost class, compared before magnitude: runnable filters first, then
/// patterns by estimated fan-out, then rule calls, then `not`/`or` subplans
/// and filters whose inputs are not yet bound. Classes keep semantics safe:
/// negation and disjunction always run after every clause that could bind
/// their variables.
type Cost = (u8, usize);

const CLASS_FILTER: u8 = 0;
const CLASS_PATTERN: u8 = 1;
const CLASS_RULE: u8 = 2;
const CLASS_SUBPLAN: u8 = 3;

/// Orders `clauses` for execution given the initially bound variables.
///
/// Returns indices into `clauses`. Predicates and functions run as soon as
/// their inputs are bound; patterns are chosen by estimated fan-out;
/// `not`/`or` run once their shared variables have had a chance to bind.
#[must_use]
pub fn order_clauses(
    clauses: &[Clause],
    initially_bound: &BTreeSet<Var>,
    ctx: &ExecCtx<'_>,
) -> Vec<usize> {
    let mut bound = initially_bound.clone();
    let mut remaining: Vec<usize> = (0..clauses.len()).collect();
    let mut ordered = Vec::with_capacity(clauses.len());
    while !remaining.is_empty() {
        let best = remaining
            .iter()
            .enumerate()
            .min_by_key(|(position, index)| (cost(&clauses[**index], &bound, ctx), *position))
            .map_or(0, |(position, _)| position);
        let index = remaining.remove(best);
        bound.extend(clause_vars(&clauses[index]));
        ordered.push(index);
    }
    ordered
}

fn term_bound(term: &Term, bound: &BTreeSet<Var>) -> bool {
    match term {
        Term::Const(_) => true,
        Term::Var(v) => bound.contains(v),
        Term::Blank => false,
    }
}

fn cost(clause: &Clause, bound: &BTreeSet<Var>, ctx: &ExecCtx<'_>) -> Cost {
    match clause {
        Clause::Pattern(pattern) => {
            let Some(db) = ctx.db(&pattern.src) else {
                return (CLASS_PATTERN, usize::MAX);
            };
            let stats = db.planner_stats();
            let a = match &pattern.a {
                Term::Const(form) => ctx.attr_of(db, form),
                _ => None,
            };
            let a_known = matches!(&pattern.a, Term::Const(_))
                || matches!(&pattern.a, Term::Var(v) if bound.contains(v));
            let e_known = term_bound(&pattern.e, bound);
            let v_known = term_bound(&pattern.v, bound);
            if a_known && a.is_none() && matches!(&pattern.a, Term::Var(_)) {
                // Attribute known only at runtime: assume a per-attribute scan.
                let estimate = (stats.total_datoms / stats.per_attr.len().max(1)).max(1);
                return (CLASS_PATTERN, estimate);
            }
            (CLASS_PATTERN, stats.estimate(e_known, a, v_known))
        }
        Clause::Pred { args, .. } | Clause::Fn { args, .. } => {
            let runnable = args
                .iter()
                .all(|t| term_bound(t, bound) || matches!(t, Term::Blank));
            if runnable {
                (CLASS_FILTER, 0)
            } else {
                (CLASS_SUBPLAN, 0)
            }
        }
        Clause::RuleCall { .. } => (CLASS_RULE, 0),
        Clause::Not { .. } | Clause::Or { .. } => (CLASS_SUBPLAN, 0),
    }
}
