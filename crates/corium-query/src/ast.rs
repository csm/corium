//! Query AST and parsing from EDN forms (map and vector shapes).

use std::collections::BTreeSet;

use crate::QueryError;
use crate::edn::Edn;

/// A logic variable (`?name`).
pub type Var = String;

/// Default database source name.
pub const DEFAULT_SRC: &str = "$";

/// A parsed query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Query {
    /// The `:find` specification.
    pub find: FindSpec,
    /// Extra grouping variables (`:with`).
    pub with: Vec<Var>,
    /// Input bindings (`:in`), defaulting to `[$]`.
    pub inputs: Vec<InSpec>,
    /// The `:where` clauses.
    pub wheres: Vec<Clause>,
}

/// The shape of `:find`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FindSpec {
    /// Relation of tuples.
    Rel(Vec<FindElem>),
    /// Collection `[?x …]`.
    Coll(FindElem),
    /// Single tuple `[?x ?y]`.
    Tuple(Vec<FindElem>),
    /// Scalar `?x .`.
    Scalar(FindElem),
}

impl FindSpec {
    /// All find elements in order.
    #[must_use]
    pub fn elems(&self) -> Vec<&FindElem> {
        match self {
            Self::Rel(elems) | Self::Tuple(elems) => elems.iter().collect(),
            Self::Coll(elem) | Self::Scalar(elem) => vec![elem],
        }
    }
}

/// One element of a find specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FindElem {
    /// A plain variable.
    Var(Var),
    /// An aggregate call `(op args… ?var)`.
    Aggregate(Aggregate),
    /// A pull expression `(pull ?e pattern)`.
    Pull(Var, Edn),
}

impl FindElem {
    /// The variable this element projects.
    #[must_use]
    pub fn var(&self) -> &Var {
        match self {
            Self::Var(v) | Self::Pull(v, _) => v,
            Self::Aggregate(agg) => &agg.var,
        }
    }
}

/// An aggregate find element.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Aggregate {
    /// Operation name (`count`, `sum`, …).
    pub op: String,
    /// Optional leading constant argument (`(min 3 ?x)`).
    pub n: Option<i64>,
    /// Aggregated variable.
    pub var: Var,
}

/// One `:in` binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InSpec {
    /// A database source (`$`, `$hist`, …).
    Db(String),
    /// A rule set (`%`).
    Rules,
    /// Scalar binding `?x`.
    Scalar(Var),
    /// Tuple binding `[?x ?y]`.
    Tuple(Vec<Var>),
    /// Collection binding `[?x …]`.
    Coll(Var),
    /// Relation binding `[[?x ?y]]`.
    Rel(Vec<Var>),
}

/// A term in a pattern or call position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Term {
    /// A variable.
    Var(Var),
    /// The blank `_`.
    Blank,
    /// A constant EDN form, resolved against a database at execution.
    Const(Edn),
}

impl Term {
    fn parse(form: &Edn) -> Self {
        match form.as_symbol() {
            Some("_") => Self::Blank,
            Some(sym) if sym.starts_with('?') => Self::Var(sym.to_owned()),
            _ => Self::Const(form.clone()),
        }
    }
}

/// A data pattern with up to five positions (`added` binds on history views).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pattern {
    /// Database source name.
    pub src: String,
    /// Entity position.
    pub e: Term,
    /// Attribute position.
    pub a: Term,
    /// Value position.
    pub v: Term,
    /// Transaction position.
    pub tx: Term,
    /// Assertion flag position.
    pub added: Term,
}

/// A `:where` clause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Clause {
    /// A data pattern.
    Pattern(Pattern),
    /// A predicate `[(pred args…)]`.
    Pred {
        /// Predicate name.
        name: String,
        /// Argument terms.
        args: Vec<Term>,
    },
    /// A function `[(f args…) binding]`.
    Fn {
        /// Function name.
        name: String,
        /// Argument terms.
        args: Vec<Term>,
        /// Output binding.
        binding: Binding,
    },
    /// `not` / `not-join`.
    Not {
        /// Database source name.
        src: String,
        /// Join variables for `not-join`; `None` for plain `not`.
        vars: Option<Vec<Var>>,
        /// Negated clauses.
        clauses: Vec<Clause>,
    },
    /// `or` / `or-join`.
    Or {
        /// Database source name.
        src: String,
        /// Join variables for `or-join`; `None` for plain `or`.
        vars: Option<Vec<Var>>,
        /// Alternative clause groups.
        branches: Vec<Vec<Clause>>,
    },
    /// A rule invocation `(rule-name args…)`.
    RuleCall {
        /// Rule name.
        name: String,
        /// Argument terms.
        args: Vec<Term>,
    },
}

/// One rule definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleDef {
    /// Rule name.
    pub name: String,
    /// Head variables that must be bound at invocation.
    pub required: Vec<Var>,
    /// Remaining head variables.
    pub free: Vec<Var>,
    /// Body clauses.
    pub clauses: Vec<Clause>,
}

impl RuleDef {
    /// All head variables in argument order.
    #[must_use]
    pub fn head_vars(&self) -> Vec<&Var> {
        self.required.iter().chain(self.free.iter()).collect()
    }
}

fn parse_error(message: impl Into<String>) -> QueryError {
    QueryError::Parse(message.into())
}

fn expect_var(form: &Edn) -> Result<Var, QueryError> {
    match form.as_symbol() {
        Some(sym) if sym.starts_with('?') => Ok(sym.to_owned()),
        _ => Err(parse_error(format!("expected variable, got {form}"))),
    }
}

fn is_src_symbol(form: &Edn) -> bool {
    form.as_symbol().is_some_and(|s| s.starts_with('$'))
}

/// Parses a query from its EDN map form or vector form.
///
/// # Errors
/// Returns [`QueryError::Parse`] for malformed queries and
/// [`QueryError::Unbound`] when find/with variables never appear in `:where`
/// or `:in`.
pub fn parse_query(form: &Edn) -> Result<Query, QueryError> {
    let map = normalize_to_map(form)?;
    let find_form = map
        .get(&Edn::keyword("find"))
        .ok_or_else(|| parse_error("query requires :find"))?;
    let find_items = find_form
        .as_seq()
        .ok_or_else(|| parse_error(":find must be a vector"))?;
    let find = parse_find(find_items)?;

    let with = match map.get(&Edn::keyword("with")) {
        None => Vec::new(),
        Some(form) => form
            .as_seq()
            .ok_or_else(|| parse_error(":with must be a vector"))?
            .iter()
            .map(expect_var)
            .collect::<Result<_, _>>()?,
    };

    let inputs = match map.get(&Edn::keyword("in")) {
        None => vec![InSpec::Db(DEFAULT_SRC.to_owned())],
        Some(form) => form
            .as_seq()
            .ok_or_else(|| parse_error(":in must be a vector"))?
            .iter()
            .map(parse_in_spec)
            .collect::<Result<_, _>>()?,
    };

    let wheres = match map.get(&Edn::keyword("where")) {
        None => Vec::new(),
        Some(form) => parse_clauses(
            form.as_seq()
                .ok_or_else(|| parse_error(":where must be a vector"))?,
        )?,
    };

    let query = Query {
        find,
        with,
        inputs,
        wheres,
    };
    validate(&query)?;
    Ok(query)
}

/// Rewrites the vector form `[:find … :in … :where …]` into map shape.
fn normalize_to_map(form: &Edn) -> Result<Edn, QueryError> {
    match form {
        Edn::Map(_) => Ok(form.clone()),
        Edn::Vector(items) => {
            let mut pairs: Vec<(Edn, Edn)> = Vec::new();
            let mut current: Option<(Edn, Vec<Edn>)> = None;
            for item in items {
                if let Edn::Keyword(k) = item
                    && k.namespace.is_none()
                {
                    if let Some((key, values)) = current.take() {
                        pairs.push((key, Edn::Vector(values)));
                    }
                    current = Some((Edn::Keyword(k.clone()), Vec::new()));
                    continue;
                }
                match &mut current {
                    Some((_, values)) => values.push(item.clone()),
                    None => return Err(parse_error("vector query must start with a keyword")),
                }
            }
            if let Some((key, values)) = current.take() {
                pairs.push((key, Edn::Vector(values)));
            }
            pairs.sort_by(|left, right| left.0.cmp(&right.0));
            Ok(Edn::Map(pairs))
        }
        _ => Err(parse_error("query must be a map or vector")),
    }
}

fn parse_find(items: &[Edn]) -> Result<FindSpec, QueryError> {
    if items.is_empty() {
        return Err(parse_error(":find requires at least one element"));
    }
    // Scalar: `?x .`
    if items.len() == 2 && items[1].as_symbol() == Some(".") {
        return Ok(FindSpec::Scalar(parse_find_elem(&items[0])?));
    }
    // Collection `[?x …]` or single tuple `[?x ?y]`.
    if items.len() == 1
        && let Edn::Vector(inner) = &items[0]
    {
        if inner.last().and_then(Edn::as_symbol) == Some("...") {
            if inner.len() != 2 {
                return Err(parse_error("collection find takes one element"));
            }
            return Ok(FindSpec::Coll(parse_find_elem(&inner[0])?));
        }
        return Ok(FindSpec::Tuple(
            inner
                .iter()
                .map(parse_find_elem)
                .collect::<Result<_, _>>()?,
        ));
    }
    Ok(FindSpec::Rel(
        items
            .iter()
            .map(parse_find_elem)
            .collect::<Result<_, _>>()?,
    ))
}

fn parse_find_elem(form: &Edn) -> Result<FindElem, QueryError> {
    match form {
        Edn::Symbol(sym) if sym.starts_with('?') => Ok(FindElem::Var(sym.clone())),
        Edn::List(items) => {
            let (op, rest) = items
                .split_first()
                .ok_or_else(|| parse_error("empty find call"))?;
            let op = op
                .as_symbol()
                .ok_or_else(|| parse_error("find call must start with a symbol"))?;
            if op == "pull" {
                let [entity, pattern] = rest else {
                    return Err(parse_error("pull takes an entity variable and a pattern"));
                };
                return Ok(FindElem::Pull(expect_var(entity)?, pattern.clone()));
            }
            match rest {
                [var] => Ok(FindElem::Aggregate(Aggregate {
                    op: op.to_owned(),
                    n: None,
                    var: expect_var(var)?,
                })),
                [Edn::Long(n), var] => Ok(FindElem::Aggregate(Aggregate {
                    op: op.to_owned(),
                    n: Some(*n),
                    var: expect_var(var)?,
                })),
                _ => Err(parse_error(format!("malformed aggregate ({op} …)"))),
            }
        }
        _ => Err(parse_error(format!("bad find element {form}"))),
    }
}

fn parse_in_spec(form: &Edn) -> Result<InSpec, QueryError> {
    match form {
        Edn::Symbol(sym) if sym.starts_with('$') => Ok(InSpec::Db(sym.clone())),
        Edn::Symbol(sym) if sym == "%" => Ok(InSpec::Rules),
        Edn::Symbol(sym) if sym.starts_with('?') => Ok(InSpec::Scalar(sym.clone())),
        Edn::Vector(items) => {
            if items.last().and_then(Edn::as_symbol) == Some("...") {
                if items.len() != 2 {
                    return Err(parse_error("collection binding takes one variable"));
                }
                return Ok(InSpec::Coll(expect_var(&items[0])?));
            }
            if items.len() == 1
                && let Edn::Vector(inner) = &items[0]
            {
                return Ok(InSpec::Rel(
                    inner.iter().map(expect_var).collect::<Result<_, _>>()?,
                ));
            }
            Ok(InSpec::Tuple(
                items.iter().map(expect_var).collect::<Result<_, _>>()?,
            ))
        }
        _ => Err(parse_error(format!("bad :in binding {form}"))),
    }
}

fn parse_clauses(forms: &[Edn]) -> Result<Vec<Clause>, QueryError> {
    forms.iter().map(parse_clause).collect()
}

/// Parses one `:where` clause.
///
/// # Errors
/// Returns [`QueryError::Parse`] for malformed clause forms.
pub fn parse_clause(form: &Edn) -> Result<Clause, QueryError> {
    match form {
        Edn::Vector(items) => parse_vector_clause(items),
        Edn::List(items) => parse_list_clause(items),
        _ => Err(parse_error(format!("bad clause {form}"))),
    }
}

fn parse_vector_clause(items: &[Edn]) -> Result<Clause, QueryError> {
    // `[(f …)]` predicate, `[(f …) binding]` function.
    if let Some(Edn::List(call)) = items.first() {
        let (name, args) = call
            .split_first()
            .ok_or_else(|| parse_error("empty call clause"))?;
        let name = name
            .as_symbol()
            .ok_or_else(|| parse_error("call must start with a symbol"))?
            .to_owned();
        let args = args.iter().map(Term::parse).collect();
        return match &items[1..] {
            [] => Ok(Clause::Pred { name, args }),
            [binding] => Ok(Clause::Fn {
                name,
                args,
                binding: parse_binding(binding)?,
            }),
            _ => Err(parse_error("function clause takes one binding form")),
        };
    }
    // Data pattern with optional leading src.
    let (src, rest) = match items.split_first() {
        Some((first, rest)) if is_src_symbol(first) => {
            (first.as_symbol().unwrap_or(DEFAULT_SRC).to_owned(), rest)
        }
        _ => (DEFAULT_SRC.to_owned(), items),
    };
    if rest.is_empty() || rest.len() > 5 {
        return Err(parse_error("data pattern takes one to five positions"));
    }
    let term = |i: usize| rest.get(i).map_or(Term::Blank, Term::parse);
    Ok(Clause::Pattern(Pattern {
        src,
        e: term(0),
        a: term(1),
        v: term(2),
        tx: term(3),
        added: term(4),
    }))
}

fn parse_list_clause(items: &[Edn]) -> Result<Clause, QueryError> {
    let (src, items) = match items.split_first() {
        Some((first, rest)) if is_src_symbol(first) => {
            (first.as_symbol().unwrap_or(DEFAULT_SRC).to_owned(), rest)
        }
        _ => (DEFAULT_SRC.to_owned(), items),
    };
    let (head, rest) = items
        .split_first()
        .ok_or_else(|| parse_error("empty list clause"))?;
    let head = head
        .as_symbol()
        .ok_or_else(|| parse_error("list clause must start with a symbol"))?;
    match head {
        "not" => Ok(Clause::Not {
            src,
            vars: None,
            clauses: parse_clauses(rest)?,
        }),
        "not-join" => {
            let (vars, clauses) = parse_join_head(rest, "not-join")?;
            Ok(Clause::Not {
                src,
                vars: Some(vars),
                clauses,
            })
        }
        "or" => Ok(Clause::Or {
            src,
            vars: None,
            branches: parse_branches(rest)?,
        }),
        "or-join" => {
            let (vars, branch_forms) = rest
                .split_first()
                .ok_or_else(|| parse_error("or-join requires a variable vector"))?;
            let vars = vars
                .as_seq()
                .ok_or_else(|| parse_error("or-join requires a variable vector"))?
                .iter()
                .map(expect_var)
                .collect::<Result<_, _>>()?;
            Ok(Clause::Or {
                src,
                vars: Some(vars),
                branches: parse_branches(branch_forms)?,
            })
        }
        "and" => Err(parse_error("(and …) is only valid inside or")),
        _ => Ok(Clause::RuleCall {
            name: head.to_owned(),
            args: rest.iter().map(Term::parse).collect(),
        }),
    }
}

fn parse_join_head(forms: &[Edn], what: &str) -> Result<(Vec<Var>, Vec<Clause>), QueryError> {
    let (vars, rest) = forms
        .split_first()
        .ok_or_else(|| parse_error(format!("{what} requires a variable vector")))?;
    let vars = vars
        .as_seq()
        .ok_or_else(|| parse_error(format!("{what} requires a variable vector")))?
        .iter()
        .map(expect_var)
        .collect::<Result<_, _>>()?;
    Ok((vars, parse_clauses(rest)?))
}

fn parse_branches(forms: &[Edn]) -> Result<Vec<Vec<Clause>>, QueryError> {
    forms
        .iter()
        .map(|form| {
            if let Edn::List(items) = form
                && items.first().and_then(Edn::as_symbol) == Some("and")
            {
                return parse_clauses(&items[1..]);
            }
            Ok(vec![parse_clause(form)?])
        })
        .collect()
}

fn parse_binding(form: &Edn) -> Result<Binding, QueryError> {
    let target = |form: &Edn| -> Result<BindTarget, QueryError> {
        match form.as_symbol() {
            Some("_") => Ok(BindTarget::Blank),
            Some(sym) if sym.starts_with('?') => Ok(BindTarget::Var(sym.to_owned())),
            _ => Err(parse_error(format!("bad binding target {form}"))),
        }
    };
    match form {
        Edn::Symbol(sym) if sym.starts_with('?') => Ok(Binding::Scalar(sym.clone())),
        Edn::Vector(items) => {
            if items.last().and_then(Edn::as_symbol) == Some("...") {
                if items.len() != 2 {
                    return Err(parse_error("collection binding takes one target"));
                }
                return Ok(Binding::Coll(target(&items[0])?));
            }
            if items.len() == 1
                && let Edn::Vector(inner) = &items[0]
            {
                return Ok(Binding::Rel(
                    inner.iter().map(target).collect::<Result<_, _>>()?,
                ));
            }
            Ok(Binding::Tuple(
                items.iter().map(target).collect::<Result<_, _>>()?,
            ))
        }
        _ => Err(parse_error(format!("bad binding form {form}"))),
    }
}

/// A function clause output binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Binding {
    /// `?x`.
    Scalar(Var),
    /// `[?x ?y]`.
    Tuple(Vec<BindTarget>),
    /// `[?x …]`.
    Coll(BindTarget),
    /// `[[?x ?y]]`.
    Rel(Vec<BindTarget>),
}

/// A binding target: a variable or the blank.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindTarget {
    /// A variable.
    Var(Var),
    /// `_`.
    Blank,
}

/// Parses a rule set from its EDN form: `[[(name head…) clause…]…]`.
///
/// # Errors
/// Returns [`QueryError::Parse`] for malformed rule definitions.
pub fn parse_rules(form: &Edn) -> Result<Vec<RuleDef>, QueryError> {
    let defs = form
        .as_seq()
        .ok_or_else(|| parse_error("rule set must be a vector"))?;
    defs.iter()
        .map(|def| {
            let items = def
                .as_seq()
                .ok_or_else(|| parse_error("rule definition must be a vector"))?;
            let (head, body) = items
                .split_first()
                .ok_or_else(|| parse_error("rule definition requires a head"))?;
            let Edn::List(head_items) = head else {
                return Err(parse_error("rule head must be a list"));
            };
            let (name, head_args) = head_items
                .split_first()
                .ok_or_else(|| parse_error("rule head requires a name"))?;
            let name = name
                .as_symbol()
                .ok_or_else(|| parse_error("rule name must be a symbol"))?
                .to_owned();
            let (required, free) = match head_args.split_first() {
                Some((Edn::Vector(required), rest)) => (
                    required.iter().map(expect_var).collect::<Result<_, _>>()?,
                    rest.iter().map(expect_var).collect::<Result<Vec<_>, _>>()?,
                ),
                _ => (
                    Vec::new(),
                    head_args
                        .iter()
                        .map(expect_var)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            };
            Ok(RuleDef {
                name,
                required,
                free,
                clauses: parse_clauses(body)?,
            })
        })
        .collect()
}

/// Variables bound by a clause (produced, not merely consumed).
#[must_use]
pub fn clause_vars(clause: &Clause) -> BTreeSet<Var> {
    fn term_var(term: &Term, out: &mut BTreeSet<Var>) {
        if let Term::Var(v) = term {
            out.insert(v.clone());
        }
    }
    let mut out = BTreeSet::new();
    match clause {
        Clause::Pattern(p) => {
            for term in [&p.e, &p.a, &p.v, &p.tx, &p.added] {
                term_var(term, &mut out);
            }
        }
        Clause::Pred { args, .. } | Clause::RuleCall { args, .. } => {
            for term in args {
                term_var(term, &mut out);
            }
        }
        Clause::Fn { args, binding, .. } => {
            for term in args {
                term_var(term, &mut out);
            }
            let mut push = |target: &BindTarget| {
                if let BindTarget::Var(v) = target {
                    out.insert(v.clone());
                }
            };
            match binding {
                Binding::Scalar(v) => {
                    out.insert(v.clone());
                }
                Binding::Coll(t) => push(t),
                Binding::Tuple(ts) | Binding::Rel(ts) => ts.iter().for_each(&mut push),
            }
        }
        Clause::Not { vars, clauses, .. } => match vars {
            Some(vars) => out.extend(vars.iter().cloned()),
            None => {
                for clause in clauses {
                    out.extend(clause_vars(clause));
                }
            }
        },
        Clause::Or { vars, branches, .. } => match vars {
            Some(vars) => out.extend(vars.iter().cloned()),
            None => {
                for branch in branches {
                    for clause in branch {
                        out.extend(clause_vars(clause));
                    }
                }
            }
        },
    }
    out
}

fn validate(query: &Query) -> Result<(), QueryError> {
    let mut available: BTreeSet<Var> = BTreeSet::new();
    for spec in &query.inputs {
        match spec {
            InSpec::Scalar(v) | InSpec::Coll(v) => {
                available.insert(v.clone());
            }
            InSpec::Tuple(vs) | InSpec::Rel(vs) => available.extend(vs.iter().cloned()),
            InSpec::Db(_) | InSpec::Rules => {}
        }
    }
    for clause in &query.wheres {
        available.extend(clause_vars(clause));
    }
    for elem in query.find.elems() {
        if !available.contains(elem.var()) {
            return Err(QueryError::Unbound(elem.var().clone()));
        }
    }
    for var in &query.with {
        if !available.contains(var) {
            return Err(QueryError::Unbound(var.clone()));
        }
    }
    // `or` branches must agree on the variables they bind.
    for clause in &query.wheres {
        validate_or(clause)?;
    }
    Ok(())
}

fn validate_or(clause: &Clause) -> Result<(), QueryError> {
    match clause {
        Clause::Or { vars, branches, .. } => {
            if vars.is_none() {
                let mut expected: Option<BTreeSet<Var>> = None;
                for branch in branches {
                    let mut bound = BTreeSet::new();
                    for clause in branch {
                        bound.extend(clause_vars(clause));
                    }
                    match &expected {
                        None => expected = Some(bound),
                        Some(prev) if *prev == bound => {}
                        Some(_) => {
                            return Err(QueryError::Parse(
                                "or branches must bind the same variables".into(),
                            ));
                        }
                    }
                }
            }
            for branch in branches {
                for clause in branch {
                    validate_or(clause)?;
                }
            }
            Ok(())
        }
        Clause::Not { clauses, .. } => {
            for clause in clauses {
                validate_or(clause)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
