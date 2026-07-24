//! The policy vocabulary: object references, relationship tuples, permission
//! maps, rewrite rules, and view bindings.
//!
//! Every name here is *data* read out of the authz database, never a Rust
//! enum: a deployment invents its own relations (`owner`, `writer`, `viewer`,
//! `member`, `parent`, `impersonator`) and the evaluator only knows how to
//! walk them. The one fixed vocabulary is the coarse action classes of
//! [`corium_protocol::authz::Action`], which are the API contract.

use std::fmt;

use corium_protocol::authz::{Action, ActionClass};

/// Wildcard id (and object type) — `database:*` is "every database".
pub const WILDCARD: &str = "*";

/// A subject or protected object: a `type` and an `id`, written `type:id`.
///
/// Both halves may be the wildcard `*`. The type is everything before the
/// first `:`, so ids may themselves contain colons (`user:oidc:alice`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectRef {
    /// Object type, e.g. `database`, `group`, `tenant`, `user`.
    pub kind: String,
    /// Id within the type, or [`WILDCARD`].
    pub id: String,
}

impl ObjectRef {
    /// Builds a reference from its parts.
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
        }
    }

    /// Parses `type:id`. A string with no `:` is read as an id of type
    /// `user`, the common case in hand-written policy.
    #[must_use]
    pub fn parse(text: &str) -> Self {
        match text.split_once(':') {
            Some((kind, id)) if !kind.is_empty() && !id.is_empty() => Self::new(kind, id),
            _ => Self::new("user", text),
        }
    }

    /// Every object of this reference's type: `type:*`.
    #[must_use]
    pub fn wildcard_of_type(&self) -> Self {
        Self::new(self.kind.clone(), WILDCARD)
    }

    /// Whether this reference names every object of its type.
    #[must_use]
    pub fn is_wildcard(&self) -> bool {
        self.id == WILDCARD
    }
}

impl fmt::Display for ObjectRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.kind, self.id)
    }
}

/// The subject half of a tuple: either a concrete object, or the *userset*
/// `object#relation` — "everyone who holds `relation` on `object`".
///
/// The userset form is what makes group nesting explicit
/// (`group:eng#member writer database:music`); the plain form is expanded
/// through [`crate::AuthzConfig::expand_relations`] so the shorter, more
/// common spelling (`group:eng writer database:music`) also works.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubjectRef {
    /// The object named on the subject side.
    pub object: ObjectRef,
    /// Relation on that object, when the subject is a userset.
    pub relation: Option<String>,
}

impl SubjectRef {
    /// Parses `type:id` or `type:id#relation`.
    #[must_use]
    pub fn parse(text: &str) -> Self {
        match text.split_once('#') {
            Some((object, relation)) if !relation.is_empty() => Self {
                object: ObjectRef::parse(object),
                relation: Some(relation.to_owned()),
            },
            _ => Self {
                object: ObjectRef::parse(text),
                relation: None,
            },
        }
    }
}

impl fmt::Display for SubjectRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.relation {
            Some(relation) => write!(formatter, "{}#{relation}", self.object),
            None => write!(formatter, "{}", self.object),
        }
    }
}

/// A relationship fact: `subject relation object`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tuple {
    /// Who (or which userset) the relation holds for.
    pub subject: SubjectRef,
    /// The relation name.
    pub relation: String,
    /// The object the relation is held on.
    pub object: ObjectRef,
}

/// A derived-relation rule: `relation` holds on an object when `on_relation`
/// holds on whatever that object's `via_relation` points at.
///
/// This is Zanzibar's tuple-to-userset rewrite. With
/// `{relation: viewer, via: parent, on: viewer}` and the tuples
/// `tenant:acme parent database:music` and `user:alice viewer tenant:acme`,
/// alice is a `viewer` of `database:music` without a tuple naming it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rewrite {
    /// The relation being derived.
    pub relation: String,
    /// Relation walked from the object to its parent.
    pub via_relation: String,
    /// Relation required on the parent.
    pub on_relation: String,
    /// Object type this rule is restricted to, or `None` for any type.
    pub object_type: Option<String>,
}

/// Maps a Corium action onto the relations that satisfy it.
///
/// `action` matches an exact action name (`query`, `transact`), an action
/// class (`read`, `write`, `admin`), or [`WILDCARD`]; `object_type` matches an
/// object type or [`WILDCARD`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Permission {
    /// Object type the permission applies to.
    pub object_type: String,
    /// Action name, action class, or wildcard.
    pub action: String,
    /// Relations that satisfy it; any one is enough.
    pub relations: Vec<String>,
}

/// The kind of visibility restriction a [`ViewDef`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterKind {
    /// Only the named attributes are visible.
    AttributeAllowlist,
    /// Every attribute except the named ones is visible.
    AttributeDenylist,
}

impl FilterKind {
    /// Parses the `:authz.view/filter-type` value.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "attribute-allowlist" | "allowlist" => Some(Self::AttributeAllowlist),
            "attribute-denylist" | "denylist" => Some(Self::AttributeDenylist),
            _ => None,
        }
    }
}

/// A named, reusable [`ViewFilter`](corium_protocol::authz::ViewFilter)
/// definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewDef {
    /// Name bindings refer to.
    pub name: String,
    /// What the filter does.
    pub kind: FilterKind,
    /// Attribute idents (e.g. `:person/email`) the filter names.
    pub attributes: Vec<String>,
}

/// Attaches a view (or explicit full visibility) to a successful relation on
/// an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    /// Relation the binding applies to.
    pub relation: String,
    /// Object it applies on; `type:*` covers every object of a type.
    pub object: ObjectRef,
    /// View name, when the binding restricts visibility.
    pub view: Option<String>,
    /// Marks the relation as granting full visibility, widening any filter
    /// another successful path would otherwise impose.
    pub unfiltered: bool,
}

/// A principal registration: binds a subject id to the provider allowed to
/// vouch for it, and grants roles from policy data.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrincipalDef {
    /// Subject id, matching [`corium_protocol::authz::Principal::subject`].
    pub id: String,
    /// Provider that must have vouched for it, or `*`/`None` for any.
    pub provider: Option<String>,
    /// Roles this principal holds regardless of what the provider asserted.
    pub roles: Vec<String>,
}

/// An object registration: metadata that names a Corium database an object
/// stands for, so a check on `database:music` can also consider, say,
/// `tenant:acme` when that tenant is registered against `music`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectDef {
    /// The object.
    pub object: ObjectRef,
    /// Corium database this object stands for, when it names one.
    pub database: Option<String>,
}

/// The wire name of an [`Action`], used in `:authz.permission/action`.
#[must_use]
pub fn action_name(action: Action) -> &'static str {
    match action {
        Action::Query => "query",
        Action::Pull => "pull",
        Action::Datoms => "datoms",
        Action::TxRange => "tx-range",
        Action::Subscribe => "subscribe",
        Action::Inspect => "inspect",
        Action::Transact => "transact",
        Action::CreateDatabase => "create-database",
        Action::DeleteDatabase => "delete-database",
        Action::ForkDatabase => "fork-database",
        Action::ListDatabases => "list-databases",
        Action::GarbageCollect => "garbage-collect",
        Action::ManageIndex => "manage-index",
    }
}

/// The [`Action`] a wire name denotes, the inverse of [`action_name`]. Used by
/// `corium authz check`, which takes the action to test on the command line.
#[must_use]
pub fn action_from_name(name: &str) -> Option<Action> {
    const ACTIONS: [Action; 13] = [
        Action::Query,
        Action::Pull,
        Action::Datoms,
        Action::TxRange,
        Action::Subscribe,
        Action::Inspect,
        Action::Transact,
        Action::CreateDatabase,
        Action::DeleteDatabase,
        Action::ForkDatabase,
        Action::ListDatabases,
        Action::GarbageCollect,
        Action::ManageIndex,
    ];
    ACTIONS
        .into_iter()
        .find(|action| action_name(*action) == name)
}

/// Every action name, for help text and error messages.
#[must_use]
pub fn action_names() -> Vec<&'static str> {
    [
        Action::Query,
        Action::Pull,
        Action::Datoms,
        Action::TxRange,
        Action::Subscribe,
        Action::Inspect,
        Action::Transact,
        Action::CreateDatabase,
        Action::DeleteDatabase,
        Action::ForkDatabase,
        Action::ListDatabases,
        Action::GarbageCollect,
        Action::ManageIndex,
    ]
    .into_iter()
    .map(action_name)
    .collect()
}

/// The wire name of an [`ActionClass`], used in `:authz.permission/action`.
#[must_use]
pub fn action_class_name(class: ActionClass) -> &'static str {
    match class {
        ActionClass::Read => "read",
        ActionClass::Write => "write",
        ActionClass::Admin => "admin",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_refs_round_trip() {
        let reference = ObjectRef::parse("database:music");
        assert_eq!(reference, ObjectRef::new("database", "music"));
        assert_eq!(reference.to_string(), "database:music");
        assert_eq!(
            reference.wildcard_of_type(),
            ObjectRef::new("database", "*")
        );
        assert!(reference.wildcard_of_type().is_wildcard());
        // A bare name is a user id, and a colon inside the id is preserved.
        assert_eq!(ObjectRef::parse("alice"), ObjectRef::new("user", "alice"));
        assert_eq!(
            ObjectRef::parse("user:oidc:alice"),
            ObjectRef::new("user", "oidc:alice")
        );
    }

    #[test]
    fn subject_refs_carry_usersets() {
        let plain = SubjectRef::parse("group:eng");
        assert_eq!(plain.relation, None);
        let userset = SubjectRef::parse("group:eng#member");
        assert_eq!(userset.object, ObjectRef::new("group", "eng"));
        assert_eq!(userset.relation.as_deref(), Some("member"));
        assert_eq!(userset.to_string(), "group:eng#member");
    }

    #[test]
    fn action_names_are_stable() {
        assert_eq!(action_name(Action::Query), "query");
        assert_eq!(action_name(Action::CreateDatabase), "create-database");
        assert_eq!(action_class_name(ActionClass::Admin), "admin");
    }

    #[test]
    fn action_names_round_trip() {
        for name in action_names() {
            let action = action_from_name(name).expect("every listed name parses");
            assert_eq!(action_name(action), name);
        }
        assert_eq!(action_names().len(), 13);
        assert!(action_from_name("nonsense").is_none());
    }
}
