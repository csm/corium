//! The bounded relationship search that answers one check.
//!
//! `Check(subject, action, object, database, at_authz_t)` reduces to: resolve
//! the action to candidate relations, then ask whether any of the request's
//! subjects reaches the target object through one of them. The search is a
//! breadth-first walk over `(relation, object)` goals with three hard bounds —
//! maximum depth, maximum visited goals, and a visited set that also serves as
//! cycle detection — because policy data is user-authored and a cyclic
//! `parent` chain must cost a bounded amount, not hang a request.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use corium_protocol::authz::ViewFilter;

use crate::model::{ObjectRef, SubjectRef};
use crate::policy::Policy;
use crate::view;

/// Bounds on one check's search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Maximum number of relation hops from a target object.
    pub max_depth: usize,
    /// Maximum `(relation, object)` goals visited across the whole check.
    pub max_visited: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: 8,
            max_visited: 10_000,
        }
    }
}

/// One edge of a matched relationship path, outermost (the target) first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathStep {
    /// Relation walked.
    pub relation: String,
    /// Object it was walked on.
    pub object: ObjectRef,
}

impl std::fmt::Display for PathStep {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}#{}", self.object, self.relation)
    }
}

/// A successful path: the relation on the target that was satisfied, the
/// subject that satisfied it, and the goals walked to get there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Match {
    /// Relation on the target object that the action required.
    pub relation: String,
    /// The target object.
    pub object: ObjectRef,
    /// Subject that satisfied it.
    pub subject: SubjectRef,
    /// Goals walked from the target to the matching tuple.
    pub path: Vec<PathStep>,
}

impl Match {
    /// Renders the path as `object#relation -> object#relation -> subject`,
    /// the form audit lines carry.
    #[must_use]
    pub fn render_path(&self) -> String {
        let mut parts: Vec<String> = self.path.iter().map(ToString::to_string).collect();
        parts.push(self.subject.to_string());
        parts.join(" -> ")
    }
}

/// Why a check failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Denial {
    /// No permission entity maps the action onto any relation.
    NoPermission {
        /// Object type checked.
        object_type: String,
        /// Action name checked.
        action: String,
    },
    /// Relations were known but no path reached the subject.
    NoPath {
        /// Relations that would have satisfied the action.
        relations: Vec<String>,
        /// Objects that were checked.
        objects: Vec<String>,
    },
    /// The search hit its bounds before finding a path. Reported separately
    /// from `NoPath` because it is an operational signal, not a policy answer.
    Exhausted {
        /// Goals visited before giving up.
        visited: usize,
    },
}

impl std::fmt::Display for Denial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPermission {
                object_type,
                action,
            } => write!(
                formatter,
                "no permission maps action {action:?} on object type {object_type:?}"
            ),
            Self::NoPath { relations, objects } => write!(
                formatter,
                "no relationship path grants {relations:?} on {objects:?}"
            ),
            Self::Exhausted { visited } => write!(
                formatter,
                "relationship search exhausted its budget after {visited} goals"
            ),
        }
    }
}

/// The outcome of one bounded check.
#[derive(Clone, Debug)]
pub enum Outcome {
    /// Permitted with full visibility.
    Allow {
        /// Every relation that granted the access.
        matches: Vec<Match>,
    },
    /// Permitted through a view filter.
    AllowFiltered {
        /// Every relation that granted the access.
        matches: Vec<Match>,
        /// The combined filter.
        filter: Arc<dyn ViewFilter>,
        /// Names of the views that were combined.
        views: Vec<String>,
    },
    /// Refused.
    Deny(Denial),
}

impl Outcome {
    /// Whether the access was permitted (filtered or not).
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        !matches!(self, Self::Deny(_))
    }

    /// The matched paths, when the check succeeded.
    #[must_use]
    pub fn matches(&self) -> &[Match] {
        match self {
            Self::Allow { matches } | Self::AllowFiltered { matches, .. } => matches,
            Self::Deny(_) => &[],
        }
    }
}

/// The request side of a check: the subjects a principal expands to, the
/// relations to look for, and the objects to look on.
#[derive(Clone, Debug)]
pub struct Query {
    /// Subjects the principal expands to (`user:alice`, `role:admin`, …).
    pub subjects: BTreeSet<ObjectRef>,
    /// Relations that satisfy the action, from the permission map.
    pub relations: BTreeSet<String>,
    /// Action name, for the denial message.
    pub action: String,
    /// Objects the access targets; any one granting is enough.
    pub objects: Vec<ObjectRef>,
    /// Relations a plain (non-userset) group subject is expanded through.
    pub expand_relations: Vec<String>,
    /// Search bounds.
    pub limits: Limits,
}

/// Runs the bounded search for `query` against `policy`.
#[must_use]
pub fn check(policy: &Policy, query: &Query) -> Outcome {
    if query.relations.is_empty() {
        return Outcome::Deny(Denial::NoPermission {
            object_type: query
                .objects
                .first()
                .map_or_else(|| "*".to_owned(), |object| object.kind.clone()),
            action: query.action.clone(),
        });
    }

    let mut budget = Budget {
        visited: 0,
        max_visited: query.limits.max_visited,
    };
    let mut matches = Vec::new();
    let mut exhausted = false;
    // One root goal per (relation, object) pair: a view binding attaches to
    // the relation that satisfied the action on the target, so successes are
    // collected per root goal rather than per leaf tuple.
    'roots: for object in &query.objects {
        for relation in &query.relations {
            match Walk::new(policy, query).run(relation, object, &mut budget) {
                Search::Found(found) => matches.push(found),
                Search::NotFound => {}
                Search::Exhausted => {
                    exhausted = true;
                    break 'roots;
                }
            }
        }
    }

    if matches.is_empty() {
        return Outcome::Deny(if exhausted {
            Denial::Exhausted {
                visited: budget.visited,
            }
        } else {
            Denial::NoPath {
                relations: query.relations.iter().cloned().collect(),
                objects: query.objects.iter().map(ToString::to_string).collect(),
            }
        });
    }

    combine_views(policy, matches)
}

/// Applies the view bindings of every successful path.
///
/// Conservative by construction: filters intersect, and a path that declares
/// no binding neither widens nor narrows. Only a binding explicitly marked
/// `:authz.binding/unfiltered` grants full visibility, which is the documented
/// escape hatch for relations like `owner` that must see everything.
fn combine_views(policy: &Policy, matches: Vec<Match>) -> Outcome {
    let mut filters = Vec::new();
    let mut names = Vec::new();
    for found in &matches {
        let Some(binding) = policy.binding_for(&found.relation, &found.object) else {
            continue;
        };
        if binding.unfiltered {
            return Outcome::Allow { matches };
        }
        if let Some(name) = &binding.view
            && let Some(filter) = policy.view(name)
        {
            filters.push(Arc::clone(filter));
            names.push(name.clone());
        }
    }
    match view::combine(filters) {
        None => Outcome::Allow { matches },
        Some(filter) => Outcome::AllowFiltered {
            matches,
            filter,
            views: names,
        },
    }
}

/// The shared visit budget for one check, spanning every root goal.
struct Budget {
    visited: usize,
    max_visited: usize,
}

impl Budget {
    fn spend(&mut self) -> bool {
        if self.visited >= self.max_visited {
            return false;
        }
        self.visited += 1;
        true
    }
}

enum Search {
    Found(Match),
    NotFound,
    Exhausted,
}

/// A queued goal, with a parent pointer so the matched path can be rebuilt
/// without cloning a path per queued entry.
struct Node {
    step: PathStep,
    parent: Option<usize>,
    depth: usize,
}

/// Breadth-first walk for one `(relation, object)` root goal.
struct Walk<'a> {
    policy: &'a Policy,
    query: &'a Query,
    nodes: Vec<Node>,
    queue: VecDeque<usize>,
    visited: BTreeSet<(String, ObjectRef)>,
}

impl<'a> Walk<'a> {
    fn new(policy: &'a Policy, query: &'a Query) -> Self {
        Self {
            policy,
            query,
            nodes: Vec::new(),
            queue: VecDeque::new(),
            visited: BTreeSet::new(),
        }
    }

    fn run(mut self, relation: &str, object: &ObjectRef, budget: &mut Budget) -> Search {
        self.push(relation.to_owned(), object.clone(), None, 0);
        while let Some(index) = self.queue.pop_front() {
            if !budget.spend() {
                return Search::Exhausted;
            }
            let goal = PathStep {
                relation: self.nodes[index].step.relation.clone(),
                object: self.nodes[index].step.object.clone(),
            };
            let depth = self.nodes[index].depth;

            for subject in self.policy.subjects_for(&goal.object, &goal.relation) {
                let subject = subject.clone();
                match &subject.relation {
                    // `group:eng#member writer database:music`: the relation
                    // named on the subject side becomes the next goal.
                    Some(userset) => {
                        self.push(
                            userset.clone(),
                            subject.object.clone(),
                            Some(index),
                            depth + 1,
                        );
                    }
                    None if self.holds(&subject) => {
                        return Search::Found(Match {
                            relation: relation.to_owned(),
                            object: object.clone(),
                            subject,
                            path: self.path_of(index),
                        });
                    }
                    // `group:eng writer database:music` written without the
                    // `#member` suffix: expand the named object through the
                    // configured membership relations.
                    None => {
                        for expansion in &self.query.expand_relations {
                            self.push(
                                expansion.clone(),
                                subject.object.clone(),
                                Some(index),
                                depth + 1,
                            );
                        }
                    }
                }
            }

            for rewrite in self.policy.rewrites_for(&goal.relation, &goal.object.kind) {
                let on_relation = rewrite.on_relation.clone();
                let parents: Vec<ObjectRef> = self
                    .policy
                    .subjects_for(&goal.object, &rewrite.via_relation)
                    .into_iter()
                    .map(|parent| parent.object.clone())
                    .collect();
                for parent in parents {
                    self.push(on_relation.clone(), parent, Some(index), depth + 1);
                }
            }
        }
        Search::NotFound
    }

    /// Whether the request's subject set satisfies a tuple's subject. A tuple
    /// written against `type:*` matches any held subject of that type, which is
    /// how "everyone" and "every authenticated caller" are expressed.
    fn holds(&self, subject: &SubjectRef) -> bool {
        if self.query.subjects.contains(&subject.object) {
            return true;
        }
        subject.object.is_wildcard()
            && self
                .query
                .subjects
                .iter()
                .any(|held| held.kind == subject.object.kind)
    }

    fn push(&mut self, relation: String, object: ObjectRef, parent: Option<usize>, depth: usize) {
        if depth > self.query.limits.max_depth {
            return;
        }
        // The visited set is both the cycle breaker and the work deduplicator:
        // a `parent`-loop in policy data revisits nothing.
        if !self.visited.insert((relation.clone(), object.clone())) {
            return;
        }
        self.nodes.push(Node {
            step: PathStep { relation, object },
            parent,
            depth,
        });
        self.queue.push_back(self.nodes.len() - 1);
    }

    fn path_of(&self, mut index: usize) -> Vec<PathStep> {
        let mut path = Vec::new();
        loop {
            path.push(self.nodes[index].step.clone());
            match self.nodes[index].parent {
                Some(parent) => index = parent,
                None => break,
            }
        }
        path.reverse();
        path
    }
}
