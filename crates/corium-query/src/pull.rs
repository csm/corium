//! The Pull API: declarative hierarchical selection from an entity.
//!
//! Supports the full v1 grammar: attribute specs, `*`, reverse refs
//! (`:ns/_name`), nested maps, `:as`/`:limit`/`:default` options, bounded
//! and unbounded recursion (`{:friend 6}` / `'...'`), and component
//! auto-recursion. Results are EDN maps keyed by attribute ident.

use std::collections::BTreeSet;

use corium_core::{AttrId, EntityId, IndexOrder, Keyword, Value, ValueType};
use corium_db::{Db, key_prefix};

use crate::QueryError;
use crate::boundary::value_to_edn;
use crate::edn::Edn;

/// Datomic's default limit on cardinality-many results.
const DEFAULT_LIMIT: usize = 1000;

#[derive(Clone, Debug)]
enum PullAttr {
    Forward(AttrId, Keyword),
    Reverse(AttrId, Keyword),
}

#[derive(Clone, Debug)]
enum SubSelect {
    None,
    Pattern(Box<PullPattern>),
    Recur(Option<usize>),
}

/// Cardinality-many result limit: unset (Datomic's default of 1000),
/// explicitly unlimited (`:limit nil`), or a bound.
#[derive(Clone, Copy, Debug)]
enum Limit {
    Default,
    Unlimited,
    At(usize),
}

impl Limit {
    fn bound(self) -> Option<usize> {
        match self {
            Self::Default => Some(DEFAULT_LIMIT),
            Self::Unlimited => None,
            Self::At(bound) => Some(bound),
        }
    }
}

#[derive(Clone, Debug)]
struct PullSpec {
    attr: PullAttr,
    as_key: Option<Edn>,
    limit: Limit,
    default: Option<Edn>,
    sub: SubSelect,
}

#[derive(Clone, Debug, Default)]
struct PullPattern {
    wildcard: bool,
    db_id: bool,
    specs: Vec<PullSpec>,
}

fn parse_error(message: impl Into<String>) -> QueryError {
    QueryError::Parse(message.into())
}

fn resolve_attr(db: &Db, keyword: &Keyword) -> Result<PullAttr, QueryError> {
    if let Some(reverse_name) = keyword.name.strip_prefix('_') {
        let forward = Keyword::new(keyword.namespace.as_deref(), reverse_name);
        let attr = db
            .idents()
            .entid(&forward)
            .ok_or_else(|| QueryError::UnknownIdent(forward.clone()))?;
        return Ok(PullAttr::Reverse(attr, keyword.clone()));
    }
    let attr = db
        .idents()
        .entid(keyword)
        .ok_or_else(|| QueryError::UnknownIdent(keyword.clone()))?;
    Ok(PullAttr::Forward(attr, keyword.clone()))
}

fn parse_pattern(db: &Db, form: &Edn) -> Result<PullPattern, QueryError> {
    let items = form
        .as_seq()
        .ok_or_else(|| parse_error("pull pattern must be a vector"))?;
    let mut pattern = PullPattern::default();
    for item in items {
        match item {
            Edn::Symbol(sym) if sym == "*" => pattern.wildcard = true,
            Edn::Keyword(k) if k.namespace.as_deref() == Some("db") && k.name == "id" => {
                pattern.db_id = true;
            }
            Edn::Keyword(k) => pattern.specs.push(PullSpec {
                attr: resolve_attr(db, k)?,
                as_key: None,
                limit: Limit::Default,
                default: None,
                sub: SubSelect::None,
            }),
            Edn::Vector(_) => pattern
                .specs
                .push(parse_attr_spec(db, item, SubSelect::None)?),
            Edn::Map(pairs) => {
                for (key, sub_form) in pairs {
                    let sub = match sub_form {
                        Edn::Long(depth) => SubSelect::Recur(Some(
                            usize::try_from(*depth)
                                .map_err(|_| parse_error("bad recursion depth"))?,
                        )),
                        Edn::Symbol(sym) if sym == "..." => SubSelect::Recur(None),
                        _ => SubSelect::Pattern(Box::new(parse_pattern(db, sub_form)?)),
                    };
                    pattern.specs.push(parse_attr_spec(db, key, sub)?);
                }
            }
            _ => return Err(parse_error(format!("bad pull spec {item}"))),
        }
    }
    Ok(pattern)
}

/// Parses a keyword or `[attr opts…]` attribute spec.
fn parse_attr_spec(db: &Db, form: &Edn, sub: SubSelect) -> Result<PullSpec, QueryError> {
    match form {
        Edn::Keyword(k) => Ok(PullSpec {
            attr: resolve_attr(db, k)?,
            as_key: None,
            limit: Limit::Default,
            default: None,
            sub,
        }),
        Edn::Vector(items) => {
            let (attr_form, opts) = items
                .split_first()
                .ok_or_else(|| parse_error("empty attribute spec"))?;
            let Edn::Keyword(k) = attr_form else {
                return Err(parse_error(format!("bad attribute spec {form}")));
            };
            let mut spec = PullSpec {
                attr: resolve_attr(db, k)?,
                as_key: None,
                limit: Limit::Default,
                default: None,
                sub,
            };
            let mut opts = opts.iter();
            while let Some(opt) = opts.next() {
                let value = opts
                    .next()
                    .ok_or_else(|| parse_error("attribute option requires a value"))?;
                match opt.as_keyword().map(|k| k.name.as_str()) {
                    Some("as") => spec.as_key = Some(value.clone()),
                    Some("limit") => {
                        spec.limit = match value {
                            Edn::Nil => Limit::Unlimited,
                            Edn::Long(n) => Limit::At(
                                usize::try_from(*n).map_err(|_| parse_error("bad :limit value"))?,
                            ),
                            _ => return Err(parse_error("bad :limit value")),
                        };
                    }
                    Some("default") => spec.default = Some(value.clone()),
                    _ => return Err(parse_error(format!("unknown attribute option {opt}"))),
                }
            }
            Ok(spec)
        }
        _ => Err(parse_error(format!("bad attribute spec {form}"))),
    }
}

/// Pulls `pattern` for one entity, producing an EDN map (or `nil` when the
/// entity has no matching datoms).
///
/// # Errors
/// Returns [`QueryError`] for malformed patterns or unknown attribute idents.
pub fn pull(db: &Db, pattern: &Edn, eid: EntityId) -> Result<Edn, QueryError> {
    let parsed = parse_pattern(db, pattern)?;
    // The root is on the recursion path: a cycle back to it stops.
    let mut path = BTreeSet::from([eid]);
    pull_entity(db, &parsed, eid, &mut path)
}

/// Pulls `pattern` for each entity, preserving order.
///
/// # Errors
/// Returns [`QueryError`] for malformed patterns or unknown attribute idents.
pub fn pull_many(db: &Db, pattern: &Edn, eids: &[EntityId]) -> Result<Edn, QueryError> {
    let parsed = parse_pattern(db, pattern)?;
    let results = eids
        .iter()
        .map(|eid| {
            let mut path = BTreeSet::from([*eid]);
            pull_entity(db, &parsed, *eid, &mut path)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Edn::Vector(results))
}

fn entity_datoms(db: &Db, eid: EntityId) -> Vec<(AttrId, Value)> {
    let prefix = key_prefix(IndexOrder::Eavt, Some(eid), None, None);
    db.datoms_prefix(IndexOrder::Eavt, &prefix)
        .map(|datom| (datom.a, datom.v.clone()))
        .collect()
}

fn reverse_refs(db: &Db, attr: AttrId, eid: EntityId) -> Vec<EntityId> {
    let value = Value::Ref(eid);
    let prefix = key_prefix(IndexOrder::Vaet, None, Some(attr), Some(&value));
    db.datoms_prefix(IndexOrder::Vaet, &prefix)
        .map(|datom| datom.e)
        .collect()
}

fn pull_entity(
    db: &Db,
    pattern: &PullPattern,
    eid: EntityId,
    path: &mut BTreeSet<EntityId>,
) -> Result<Edn, QueryError> {
    let own = entity_datoms(db, eid);
    let mut pairs: Vec<(Edn, Edn)> = Vec::new();
    if pattern.db_id || pattern.wildcard {
        pairs.push((
            Edn::keyword("db/id"),
            Edn::Long(i64::try_from(eid.raw()).unwrap_or(i64::MAX)),
        ));
    }
    if pattern.wildcard {
        let explicit: BTreeSet<AttrId> = pattern
            .specs
            .iter()
            .filter_map(|spec| match &spec.attr {
                PullAttr::Forward(attr, _) => Some(*attr),
                PullAttr::Reverse(_, _) => None,
            })
            .collect();
        let mut attrs: Vec<AttrId> = own.iter().map(|(a, _)| *a).collect();
        attrs.dedup();
        for attr in attrs {
            if explicit.contains(&attr) {
                continue;
            }
            let Some(ident) = db.idents().ident(attr) else {
                continue;
            };
            let spec = PullSpec {
                attr: PullAttr::Forward(attr, ident.clone()),
                as_key: None,
                limit: Limit::Default,
                default: None,
                // Wildcard recursively pulls component entities.
                sub: SubSelect::None,
            };
            if let Some((key, value)) = pull_spec(db, &spec, pattern, eid, &own, path)? {
                pairs.push((key, value));
            }
        }
    }
    for spec in &pattern.specs {
        if let Some((key, value)) = pull_spec(db, spec, pattern, eid, &own, path)? {
            pairs.push((key, value));
        }
    }
    if pairs.is_empty() {
        return Ok(Edn::Nil);
    }
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    pairs.dedup_by(|left, right| left.0 == right.0);
    Ok(Edn::Map(pairs))
}

#[allow(clippy::too_many_lines)]
fn pull_spec(
    db: &Db,
    spec: &PullSpec,
    enclosing: &PullPattern,
    eid: EntityId,
    own: &[(AttrId, Value)],
    path: &mut BTreeSet<EntityId>,
) -> Result<Option<(Edn, Edn)>, QueryError> {
    match &spec.attr {
        PullAttr::Forward(attr, ident) => {
            let meta = db.schema().get(*attr);
            let is_ref = meta.is_some_and(|m| m.value_type == ValueType::Ref);
            let is_component = meta.is_some_and(|m| m.is_component);
            let many = meta.is_none_or(|m| m.cardinality == corium_core::Cardinality::Many);
            let limit = spec.limit.bound();
            let mut values: Vec<Value> = own
                .iter()
                .filter(|(a, _)| a == attr)
                .map(|(_, v)| v.clone())
                .collect();
            if let Some(limit) = limit {
                values.truncate(limit);
            }
            if values.is_empty() {
                let default = spec.default.clone().map(|form| {
                    (
                        spec.as_key
                            .clone()
                            .unwrap_or_else(|| Edn::Keyword(ident.clone())),
                        form,
                    )
                });
                return Ok(default);
            }
            let render =
                |value: &Value, path: &mut BTreeSet<EntityId>| -> Result<Edn, QueryError> {
                    if is_ref {
                        if let Value::Ref(child) = value {
                            return render_ref(db, spec, enclosing, *child, is_component, path);
                        }
                    }
                    Ok(value_to_edn(db, value))
                };
            let rendered = if many {
                let items = values
                    .iter()
                    .map(|value| render(value, path))
                    .collect::<Result<Vec<_>, _>>()?;
                Edn::Vector(items)
            } else {
                render(&values[0], path)?
            };
            let key = spec
                .as_key
                .clone()
                .unwrap_or_else(|| Edn::Keyword(ident.clone()));
            Ok(Some((key, rendered)))
        }
        PullAttr::Reverse(attr, reverse_ident) => {
            let is_component = db.schema().get(*attr).is_some_and(|m| m.is_component);
            let parents = reverse_refs(db, *attr, eid);
            if parents.is_empty() {
                let default = spec.default.clone().map(|form| {
                    (
                        spec.as_key
                            .clone()
                            .unwrap_or_else(|| Edn::Keyword(reverse_ident.clone())),
                        form,
                    )
                });
                return Ok(default);
            }
            let limit = spec.limit.bound();
            let mut parents = parents;
            if let Some(limit) = limit {
                parents.truncate(limit);
            }
            let render = |parent: EntityId, path: &mut BTreeSet<EntityId>| match &spec.sub {
                SubSelect::None => Ok(Edn::Map(vec![(
                    Edn::keyword("db/id"),
                    Edn::Long(i64::try_from(parent.raw()).unwrap_or(i64::MAX)),
                )])),
                SubSelect::Pattern(sub) => pull_entity(db, sub, parent, path),
                SubSelect::Recur(_) => Err(QueryError::Parse(
                    "recursion is not supported on reverse refs".into(),
                )),
            };
            // A component's parent is unique: reverse component refs are scalar.
            let rendered = if is_component {
                render(parents[0], path)?
            } else {
                Edn::Vector(
                    parents
                        .into_iter()
                        .map(|parent| render(parent, path))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            };
            let key = spec
                .as_key
                .clone()
                .unwrap_or_else(|| Edn::Keyword(reverse_ident.clone()));
            Ok(Some((key, rendered)))
        }
    }
}

fn render_ref(
    db: &Db,
    spec: &PullSpec,
    enclosing: &PullPattern,
    child: EntityId,
    is_component: bool,
    path: &mut BTreeSet<EntityId>,
) -> Result<Edn, QueryError> {
    let db_id_map = |child: EntityId| {
        Edn::Map(vec![(
            Edn::keyword("db/id"),
            Edn::Long(i64::try_from(child.raw()).unwrap_or(i64::MAX)),
        )])
    };
    let spec_attr = |candidate: &PullSpec| match (&candidate.attr, &spec.attr) {
        (PullAttr::Forward(a, _), PullAttr::Forward(b, _))
        | (PullAttr::Reverse(a, _), PullAttr::Reverse(b, _)) => a == b,
        _ => false,
    };
    match &spec.sub {
        SubSelect::Pattern(sub) => {
            if !path.insert(child) {
                return Ok(db_id_map(child));
            }
            let result = pull_entity(db, sub, child, path);
            path.remove(&child);
            result
        }
        SubSelect::Recur(depth) => {
            if depth == &Some(0) || !path.insert(child) {
                return Ok(db_id_map(child));
            }
            // Recursion re-applies the enclosing pattern, with this spec's
            // remaining depth decremented.
            let mut sub = enclosing.clone();
            for candidate in &mut sub.specs {
                if spec_attr(candidate) {
                    if let SubSelect::Recur(d) = &candidate.sub {
                        candidate.sub = SubSelect::Recur(d.map(|d| d.saturating_sub(1)));
                    }
                }
            }
            let result = pull_entity(db, &sub, child, path);
            path.remove(&child);
            result
        }
        SubSelect::None => {
            if is_component {
                if !path.insert(child) {
                    return Ok(db_id_map(child));
                }
                // Component auto-recursion: pull the whole component entity.
                let wildcard = PullPattern {
                    wildcard: true,
                    db_id: true,
                    specs: Vec::new(),
                };
                let result = pull_entity(db, &wildcard, child, path);
                path.remove(&child);
                result
            } else {
                Ok(db_id_map(child))
            }
        }
    }
}
