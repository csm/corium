//! A typesafe, builder-patterned Datalog query value.
//!
//! Every type here is a plain immutable value that lowers to the boundary
//! [`Edn`] the corium query engine parses. Nothing is stringly-typed: a
//! [`Query`] is assembled from [`Var`]s, [`Term`]s, and [`Clause`]s and
//! rendered with [`Query::to_edn`], so a malformed query is a type error, not
//! a parse error at execution time.
//!
//! ```
//! use corium_client::query::{Query, data, var, attr, gte, lit};
//!
//! // [:find ?name ?age
//! //  :in $ ?min
//! //  :where [?e :person/name ?name]
//! //         [?e :person/age ?age]
//! //         [(>= ?age ?min)]]
//! let q = Query::find([var("name"), var("age")])
//!     .in_scalar(var("min"))
//!     .where_(data(var("e"), attr("person/name"), var("name")))
//!     .and(data(var("e"), attr("person/age"), var("age")))
//!     .and(gte(var("age"), var("min")))
//!     .to_edn();
//! ```

use corium_query::edn::Edn;

use crate::pull::Pull;
use crate::value::IntoEdn;

/// A logic variable such as `?e`. Constructed with or without the leading
/// `?`: `var("e")` and `var("?e")` are equal.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Var(String);

impl Var {
    /// Builds a variable, adding the leading `?` if the caller omits it.
    #[must_use]
    pub fn new(name: impl AsRef<str>) -> Self {
        let name = name.as_ref();
        if name.starts_with('?') {
            Self(name.to_owned())
        } else {
            Self(format!("?{name}"))
        }
    }

    /// The variable text including the leading `?`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn to_edn(&self) -> Edn {
        Edn::Symbol(self.0.clone())
    }
}

/// Shorthand for [`Var::new`].
#[must_use]
pub fn var(name: impl AsRef<str>) -> Var {
    Var::new(name)
}

/// An attribute keyword constant, e.g. `attr("person/name")` for
/// `:person/name`. Returns an [`Edn`] usable in any [`Term`] position.
#[must_use]
pub fn attr(name: &str) -> Edn {
    Edn::keyword(name)
}

/// Alias for [`attr`]: a keyword constant from `"ns/name"` text.
#[must_use]
pub fn kw(name: &str) -> Edn {
    Edn::keyword(name)
}

/// A constant term from any [`IntoEdn`] scalar, e.g. `lit(42)` or
/// `lit("hello")`.
#[must_use]
pub fn lit(value: impl IntoEdn) -> Term {
    Term::Const(value.into_edn())
}

/// The blank term `_`.
#[must_use]
pub fn blank() -> Term {
    Term::Blank
}

/// A term in a pattern, predicate, function, or rule position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Term {
    /// A logic variable.
    Var(Var),
    /// The blank `_`.
    Blank,
    /// A constant, resolved against the database at execution time.
    Const(Edn),
}

impl Term {
    fn to_edn(&self) -> Edn {
        match self {
            Self::Var(variable) => variable.to_edn(),
            Self::Blank => Edn::symbol("_"),
            Self::Const(form) => form.clone(),
        }
    }
}

impl From<Var> for Term {
    fn from(value: Var) -> Self {
        Self::Var(value)
    }
}

impl From<Edn> for Term {
    fn from(value: Edn) -> Self {
        Self::Const(value)
    }
}

/// A `:find` element: a variable, a pull expression, or an aggregate call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FindElem {
    /// A plain variable `?x`.
    Var(Var),
    /// A pull expression `(pull ?e pattern)`.
    Pull(Var, Pull),
    /// An aggregate call such as `(count ?x)` or `(min 3 ?x)`.
    Aggregate {
        /// Operator name (`count`, `sum`, `avg`, `min`, `max`, ...).
        op: String,
        /// Optional leading constant argument, as in `(min 3 ?x)`.
        n: Option<i64>,
        /// The aggregated variable.
        var: Var,
    },
}

impl FindElem {
    fn to_edn(&self) -> Edn {
        match self {
            Self::Var(variable) => variable.to_edn(),
            Self::Pull(entity, pattern) => {
                Edn::List(vec![Edn::symbol("pull"), entity.to_edn(), pattern.to_edn()])
            }
            Self::Aggregate { op, n, var } => {
                let mut items = vec![Edn::symbol(op)];
                if let Some(n) = n {
                    items.push(Edn::Long(*n));
                }
                items.push(var.to_edn());
                Edn::List(items)
            }
        }
    }
}

impl From<Var> for FindElem {
    fn from(value: Var) -> Self {
        Self::Var(value)
    }
}

/// A pull expression find element `(pull ?e pattern)`.
#[must_use]
pub fn pull_expr(entity: Var, pattern: Pull) -> FindElem {
    FindElem::Pull(entity, pattern)
}

macro_rules! aggregate_fn {
    ($(#[$meta:meta])* $name:ident => $op:literal) => {
        $(#[$meta])*
        #[must_use]
        pub fn $name(var: Var) -> FindElem {
            FindElem::Aggregate { op: $op.to_owned(), n: None, var }
        }
    };
}

aggregate_fn!(/// The `(count ?x)` aggregate.
    count => "count");
aggregate_fn!(/// The `(count-distinct ?x)` aggregate.
    count_distinct => "count-distinct");
aggregate_fn!(/// The `(sum ?x)` aggregate.
    sum => "sum");
aggregate_fn!(/// The `(avg ?x)` aggregate.
    avg => "avg");
aggregate_fn!(/// The `(min ?x)` aggregate.
    min => "min");
aggregate_fn!(/// The `(max ?x)` aggregate.
    max => "max");

/// A bounded aggregate such as `(min 3 ?x)` or `(max 5 ?x)`.
#[must_use]
pub fn agg_n(op: &str, n: i64, var: Var) -> FindElem {
    FindElem::Aggregate {
        op: op.to_owned(),
        n: Some(n),
        var,
    }
}

/// The shape of the `:find` clause, which fixes the result shape.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Find {
    /// Relation of tuples: `:find ?a ?b`.
    Rel(Vec<FindElem>),
    /// Collection: `:find [?x ...]`.
    Coll(FindElem),
    /// Single tuple: `:find [?a ?b]`.
    Tuple(Vec<FindElem>),
    /// Scalar: `:find ?x .`.
    Scalar(FindElem),
}

impl Find {
    fn to_find_items(&self) -> Vec<Edn> {
        match self {
            Self::Rel(elems) => elems.iter().map(FindElem::to_edn).collect(),
            Self::Coll(elem) => {
                vec![Edn::Vector(vec![elem.to_edn(), Edn::symbol("...")])]
            }
            Self::Tuple(elems) => {
                vec![Edn::Vector(elems.iter().map(FindElem::to_edn).collect())]
            }
            Self::Scalar(elem) => vec![elem.to_edn(), Edn::symbol(".")],
        }
    }
}

/// A function-clause output binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Binding {
    /// `?x`.
    Scalar(Var),
    /// `[?x ?y]`.
    Tuple(Vec<Var>),
    /// `[?x ...]`.
    Coll(Var),
    /// `[[?x ?y]]`.
    Rel(Vec<Var>),
}

impl Binding {
    fn to_edn(&self) -> Edn {
        match self {
            Self::Scalar(var) => var.to_edn(),
            Self::Tuple(vars) => Edn::Vector(vars.iter().map(Var::to_edn).collect()),
            Self::Coll(var) => Edn::Vector(vec![var.to_edn(), Edn::symbol("...")]),
            Self::Rel(vars) => {
                Edn::Vector(vec![Edn::Vector(vars.iter().map(Var::to_edn).collect())])
            }
        }
    }
}

/// A `:where` clause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Clause {
    /// A data pattern `[src? e a v tx? added?]`.
    Pattern(Pattern),
    /// A predicate `[(name args...)]`.
    Predicate {
        /// Predicate name (`>`, `<`, a rule predicate, ...).
        name: String,
        /// Argument terms.
        args: Vec<Term>,
    },
    /// A function `[(name args...) binding]`.
    Function {
        /// Function name.
        name: String,
        /// Argument terms.
        args: Vec<Term>,
        /// Output binding.
        binding: Binding,
    },
    /// `(not clauses...)` or `(not-join [vars] clauses...)`.
    Not {
        /// Join variables for `not-join`; `None` for plain `not`.
        vars: Option<Vec<Var>>,
        /// Negated clauses.
        clauses: Vec<Clause>,
    },
    /// `(or branches...)` or `(or-join [vars] branches...)`.
    Or {
        /// Join variables for `or-join`; `None` for plain `or`.
        vars: Option<Vec<Var>>,
        /// Alternative clause groups; each inner vector is an `and` branch.
        branches: Vec<Vec<Clause>>,
    },
    /// A rule invocation `(rule-name args...)`.
    Rule {
        /// Rule name.
        name: String,
        /// Argument terms.
        args: Vec<Term>,
    },
}

impl Clause {
    fn to_edn(&self) -> Edn {
        match self {
            Self::Pattern(pattern) => pattern.to_edn(),
            Self::Predicate { name, args } => Edn::Vector(vec![call_form(name, args)]),
            Self::Function {
                name,
                args,
                binding,
            } => Edn::Vector(vec![call_form(name, args), binding.to_edn()]),
            Self::Not { vars, clauses } => {
                let mut items = Vec::new();
                match vars {
                    Some(vars) => {
                        items.push(Edn::symbol("not-join"));
                        items.push(Edn::Vector(vars.iter().map(Var::to_edn).collect()));
                    }
                    None => items.push(Edn::symbol("not")),
                }
                items.extend(clauses.iter().map(Clause::to_edn));
                Edn::List(items)
            }
            Self::Or { vars, branches } => {
                let mut items = Vec::new();
                match vars {
                    Some(vars) => {
                        items.push(Edn::symbol("or-join"));
                        items.push(Edn::Vector(vars.iter().map(Var::to_edn).collect()));
                    }
                    None => items.push(Edn::symbol("or")),
                }
                for branch in branches {
                    if branch.len() == 1 {
                        items.push(branch[0].to_edn());
                    } else {
                        let mut and = vec![Edn::symbol("and")];
                        and.extend(branch.iter().map(Clause::to_edn));
                        items.push(Edn::List(and));
                    }
                }
                Edn::List(items)
            }
            Self::Rule { name, args } => {
                let mut items = vec![Edn::symbol(name)];
                items.extend(args.iter().map(Term::to_edn));
                Edn::List(items)
            }
        }
    }
}

fn call_form(name: &str, args: &[Term]) -> Edn {
    let mut items = vec![Edn::symbol(name)];
    items.extend(args.iter().map(Term::to_edn));
    Edn::List(items)
}

/// A data pattern, built by [`data`] and refined with [`Pattern::src`],
/// [`Pattern::tx`], and [`Pattern::added`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pattern {
    src: Option<String>,
    e: Term,
    a: Term,
    v: Term,
    tx: Option<Term>,
    added: Option<Term>,
}

impl Pattern {
    /// Reads the pattern from a non-default database source, e.g. `"$hist"`.
    #[must_use]
    pub fn src(mut self, src: impl Into<String>) -> Self {
        self.src = Some(src.into());
        self
    }

    /// Binds the transaction position.
    #[must_use]
    pub fn tx(mut self, tx: impl Into<Term>) -> Self {
        self.tx = Some(tx.into());
        self
    }

    /// Binds the assertion-flag position (`?added` on history views).
    #[must_use]
    pub fn added(mut self, added: impl Into<Term>) -> Self {
        self.added = Some(added.into());
        self
    }

    fn to_edn(&self) -> Edn {
        let mut items = Vec::with_capacity(6);
        if let Some(src) = &self.src {
            items.push(Edn::symbol(src));
        }
        items.push(self.e.to_edn());
        items.push(self.a.to_edn());
        items.push(self.v.to_edn());
        // tx/added are trailing positions; emit `added` only when `tx` is set
        // so positions stay aligned.
        if self.tx.is_some() || self.added.is_some() {
            items.push(self.tx.as_ref().map_or(Edn::symbol("_"), Term::to_edn));
        }
        if let Some(added) = &self.added {
            items.push(added.to_edn());
        }
        Edn::Vector(items)
    }
}

impl From<Pattern> for Clause {
    fn from(value: Pattern) -> Self {
        Self::Pattern(value)
    }
}

/// A data pattern `[?e :attr ?v]`. Refine it with [`Pattern::src`],
/// [`Pattern::tx`], or [`Pattern::added`] before adding it to a query.
#[must_use]
pub fn data(e: impl Into<Term>, a: impl Into<Term>, v: impl Into<Term>) -> Pattern {
    Pattern {
        src: None,
        e: e.into(),
        a: a.into(),
        v: v.into(),
        tx: None,
        added: None,
    }
}

/// A predicate clause `[(name args...)]`.
#[must_use]
pub fn pred(name: impl Into<String>, args: Vec<Term>) -> Clause {
    Clause::Predicate {
        name: name.into(),
        args,
    }
}

macro_rules! comparison_fn {
    ($(#[$meta:meta])* $name:ident => $op:literal) => {
        $(#[$meta])*
        #[must_use]
        pub fn $name(left: impl Into<Term>, right: impl Into<Term>) -> Clause {
            Clause::Predicate { name: $op.to_owned(), args: vec![left.into(), right.into()] }
        }
    };
}

comparison_fn!(/// The predicate `[(> a b)]`.
    gt => ">");
comparison_fn!(/// The predicate `[(< a b)]`.
    lt => "<");
comparison_fn!(/// The predicate `[(>= a b)]`.
    gte => ">=");
comparison_fn!(/// The predicate `[(<= a b)]`.
    lte => "<=");
comparison_fn!(/// The predicate `[(= a b)]`.
    eq => "=");
comparison_fn!(/// The predicate `[(not= a b)]`.
    neq => "not=");

/// A function call, built by [`call`] and completed with [`Call::bind`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Call {
    name: String,
    args: Vec<Term>,
}

impl Call {
    /// Binds the call's output, producing a function clause
    /// `[(name args...) binding]`.
    #[must_use]
    pub fn bind(self, binding: Binding) -> Clause {
        Clause::Function {
            name: self.name,
            args: self.args,
            binding,
        }
    }

    /// Binds the call's output to a single scalar variable.
    #[must_use]
    pub fn bind_scalar(self, var: Var) -> Clause {
        self.bind(Binding::Scalar(var))
    }
}

/// A function call `(name args...)`, completed with [`Call::bind`].
#[must_use]
pub fn call(name: impl Into<String>, args: Vec<Term>) -> Call {
    Call {
        name: name.into(),
        args,
    }
}

fn collect_clauses<C: Into<Clause>>(clauses: impl IntoIterator<Item = C>) -> Vec<Clause> {
    clauses.into_iter().map(Into::into).collect()
}

fn collect_branches<B, C>(branches: impl IntoIterator<Item = B>) -> Vec<Vec<Clause>>
where
    B: IntoIterator<Item = C>,
    C: Into<Clause>,
{
    branches.into_iter().map(collect_clauses).collect()
}

/// A `(not clauses...)` clause.
#[must_use]
pub fn not<C: Into<Clause>>(clauses: impl IntoIterator<Item = C>) -> Clause {
    Clause::Not {
        vars: None,
        clauses: collect_clauses(clauses),
    }
}

/// A `(not-join [vars] clauses...)` clause.
#[must_use]
pub fn not_join<C: Into<Clause>>(vars: Vec<Var>, clauses: impl IntoIterator<Item = C>) -> Clause {
    Clause::Not {
        vars: Some(vars),
        clauses: collect_clauses(clauses),
    }
}

/// An `(or branches...)` clause. Each branch is a group of clauses joined by
/// an implicit `and`.
#[must_use]
pub fn or<B, C>(branches: impl IntoIterator<Item = B>) -> Clause
where
    B: IntoIterator<Item = C>,
    C: Into<Clause>,
{
    Clause::Or {
        vars: None,
        branches: collect_branches(branches),
    }
}

/// An `(or-join [vars] branches...)` clause.
#[must_use]
pub fn or_join<B, C>(vars: Vec<Var>, branches: impl IntoIterator<Item = B>) -> Clause
where
    B: IntoIterator<Item = C>,
    C: Into<Clause>,
{
    Clause::Or {
        vars: Some(vars),
        branches: collect_branches(branches),
    }
}

/// A rule invocation `(rule-name args...)`.
#[must_use]
pub fn rule(name: impl Into<String>, args: Vec<Term>) -> Clause {
    Clause::Rule {
        name: name.into(),
        args,
    }
}

/// A `:in` binding after the implicit default database `$`.
#[derive(Clone, Debug, Eq, PartialEq)]
enum InSpec {
    Db(String),
    Rules,
    Scalar(Var),
    Tuple(Vec<Var>),
    Coll(Var),
    Rel(Vec<Var>),
}

impl InSpec {
    fn to_edn(&self) -> Edn {
        match self {
            Self::Db(name) => Edn::symbol(name),
            Self::Rules => Edn::symbol("%"),
            Self::Scalar(var) => var.to_edn(),
            Self::Tuple(vars) => Edn::Vector(vars.iter().map(Var::to_edn).collect()),
            Self::Coll(var) => Edn::Vector(vec![var.to_edn(), Edn::symbol("...")]),
            Self::Rel(vars) => {
                Edn::Vector(vec![Edn::Vector(vars.iter().map(Var::to_edn).collect())])
            }
        }
    }
}

/// An immutable Datalog query value.
///
/// Start from one of the `find*` constructors, chain `:in` and `:where`
/// builders, and render with [`Query::to_edn`]. The query shape
/// (relation/collection/tuple/scalar) is fixed by which constructor is used
/// and determines the shape of the [`crate::QueryResult`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Query {
    find: Find,
    with: Vec<Var>,
    inputs: Vec<InSpec>,
    wheres: Vec<Clause>,
}

impl Query {
    fn from_find(find: Find) -> Self {
        Self {
            find,
            with: Vec::new(),
            inputs: Vec::new(),
            wheres: Vec::new(),
        }
    }

    /// A relation query `:find ?a ?b` yielding a set of tuples.
    #[must_use]
    pub fn find<E: Into<FindElem>>(elems: impl IntoIterator<Item = E>) -> Self {
        Self::from_find(Find::Rel(elems.into_iter().map(Into::into).collect()))
    }

    /// A relation query from an explicit list of find elements. Use this when
    /// mixing element kinds, e.g. a variable and a pull expression.
    #[must_use]
    pub fn find_rel(elems: Vec<FindElem>) -> Self {
        Self::from_find(Find::Rel(elems))
    }

    /// A collection query `:find [?x ...]` yielding a flat list of values.
    #[must_use]
    pub fn find_coll(elem: impl Into<FindElem>) -> Self {
        Self::from_find(Find::Coll(elem.into()))
    }

    /// A tuple query `:find [?a ?b]` yielding a single tuple.
    #[must_use]
    pub fn find_tuple<E: Into<FindElem>>(elems: impl IntoIterator<Item = E>) -> Self {
        Self::from_find(Find::Tuple(elems.into_iter().map(Into::into).collect()))
    }

    /// A scalar query `:find ?x .` yielding a single value.
    #[must_use]
    pub fn find_scalar(elem: impl Into<FindElem>) -> Self {
        Self::from_find(Find::Scalar(elem.into()))
    }

    /// Adds `:with` grouping variables (kept out of `:find` but preserved for
    /// aggregate cardinality).
    #[must_use]
    pub fn with(mut self, vars: impl IntoIterator<Item = Var>) -> Self {
        self.with.extend(vars);
        self
    }

    /// Declares an additional database source input such as `$hist`, bound
    /// positionally after the default `$`.
    #[must_use]
    pub fn in_db(mut self, name: impl Into<String>) -> Self {
        self.inputs.push(InSpec::Db(name.into()));
        self
    }

    /// Declares a rule-set input `%`.
    #[must_use]
    pub fn in_rules(mut self) -> Self {
        self.inputs.push(InSpec::Rules);
        self
    }

    /// Declares a scalar input `?x`.
    #[must_use]
    pub fn in_scalar(mut self, var: Var) -> Self {
        self.inputs.push(InSpec::Scalar(var));
        self
    }

    /// Declares a tuple input `[?x ?y]`.
    #[must_use]
    pub fn in_tuple(mut self, vars: Vec<Var>) -> Self {
        self.inputs.push(InSpec::Tuple(vars));
        self
    }

    /// Declares a collection input `[?x ...]`.
    #[must_use]
    pub fn in_coll(mut self, var: Var) -> Self {
        self.inputs.push(InSpec::Coll(var));
        self
    }

    /// Declares a relation input `[[?x ?y]]`.
    #[must_use]
    pub fn in_rel(mut self, vars: Vec<Var>) -> Self {
        self.inputs.push(InSpec::Rel(vars));
        self
    }

    /// Adds the first `:where` clause (or another one; identical to
    /// [`Query::and`]).
    #[must_use]
    pub fn where_(mut self, clause: impl Into<Clause>) -> Self {
        self.wheres.push(clause.into());
        self
    }

    /// Adds another `:where` clause.
    #[must_use]
    pub fn and(mut self, clause: impl Into<Clause>) -> Self {
        self.wheres.push(clause.into());
        self
    }

    /// Whether this query declares any non-database `:in` inputs (rules,
    /// scalars, tuples, collections, or relations), which the caller must
    /// supply as arguments.
    #[must_use]
    pub fn has_inputs(&self) -> bool {
        self.inputs
            .iter()
            .any(|spec| !matches!(spec, InSpec::Db(_)))
    }

    /// Renders the query to its boundary [`Edn`] map form.
    #[must_use]
    pub fn to_edn(&self) -> Edn {
        let mut pairs = Vec::with_capacity(4);
        pairs.push((Edn::keyword("find"), Edn::Vector(self.find.to_find_items())));
        if !self.with.is_empty() {
            pairs.push((
                Edn::keyword("with"),
                Edn::Vector(self.with.iter().map(Var::to_edn).collect()),
            ));
        }
        if !self.inputs.is_empty() {
            let mut ins = vec![Edn::symbol("$")];
            ins.extend(self.inputs.iter().map(InSpec::to_edn));
            pairs.push((Edn::keyword("in"), Edn::Vector(ins)));
        }
        pairs.push((
            Edn::keyword("where"),
            Edn::Vector(self.wheres.iter().map(Clause::to_edn).collect()),
        ));
        Edn::Map(pairs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_query::ast::parse_query;

    /// Every builder output must parse cleanly in the engine.
    fn assert_parses(query: &Query) {
        let edn = query.to_edn();
        parse_query(&edn).unwrap_or_else(|error| panic!("query did not parse: {error}\n{edn}"));
    }

    #[test]
    fn relation_with_predicate_and_input() {
        let query = Query::find([var("name"), var("age")])
            .in_scalar(var("min"))
            .where_(data(var("e"), attr("person/name"), var("name")))
            .and(data(var("e"), attr("person/age"), var("age")))
            .and(gte(var("age"), var("min")));
        assert_parses(&query);
        assert_eq!(
            query.to_edn().to_string(),
            "{:find [?name ?age], :in [$ ?min], :where [[?e :person/name ?name] \
             [?e :person/age ?age] [(>= ?age ?min)]]}"
        );
    }

    #[test]
    fn scalar_coll_tuple_shapes_parse() {
        assert_parses(&Query::find_scalar(var("e")).where_(data(
            var("e"),
            attr("db/ident"),
            blank(),
        )));
        assert_parses(&Query::find_coll(var("e")).where_(data(
            var("e"),
            attr("db/ident"),
            blank(),
        )));
        assert_parses(&Query::find_tuple([var("e"), var("a")]).where_(data(
            var("e"),
            attr("db/ident"),
            var("a"),
        )));
    }

    #[test]
    fn aggregate_and_function_clauses_parse() {
        assert_parses(
            &Query::find_rel(vec![min(var("age")), max(var("age"))]).where_(data(
                var("e"),
                attr("person/age"),
                var("age"),
            )),
        );
        let with_count = Query::find_rel(vec![FindElem::Var(var("name")), count(var("e"))])
            .where_(data(var("e"), attr("person/name"), var("name")));
        assert_parses(&with_count);
        assert_parses(
            &Query::find([var("total")])
                .where_(data(var("e"), attr("order/subtotal"), var("sub")))
                .and(data(var("e"), attr("order/tax"), var("tax")))
                .and(
                    call("+", vec![var("sub").into(), var("tax").into()]).bind_scalar(var("total")),
                ),
        );
    }

    #[test]
    fn not_or_and_rules_parse() {
        assert_parses(
            &Query::find([var("e")])
                .where_(data(var("e"), attr("person/name"), blank()))
                .and(not(vec![data(
                    var("e"),
                    attr("person/deceased"),
                    lit(true),
                )])),
        );
        assert_parses(&Query::find([var("e")]).where_(or(vec![
            vec![data(var("e"), attr("person/name"), lit("Alice"))],
            vec![data(var("e"), attr("person/name"), lit("Bob"))],
        ])));
        assert_parses(
            &Query::find([var("e")])
                .in_rules()
                .where_(rule("ancestor", vec![var("e").into(), var("x").into()])),
        );
    }
}
