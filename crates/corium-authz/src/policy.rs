//! Compiling a snapshot of the authz database into an immutable policy value.
//!
//! Compilation happens off the request path (see [`crate::SystemDbAuthorizer`]);
//! what a check touches is only the indexes built here, so the hot path is a
//! bounded in-memory graph walk with no locking beyond reading the current
//! snapshot pointer.
//!
//! The compiled value is keyed by the authz database's basis `t`. That single
//! number is what makes decisions reproducible (re-read the database `as-of`
//! that `t` and the same policy comes back) and cache invalidation trivial
//! (entries keyed by a stale `t` can never be hit again).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use corium_core::{AttrId, Keyword, Value};
use corium_db::Db;
use corium_protocol::authz::{Action, ActionClass, ViewFilter};

use crate::model::{
    Binding, FilterKind, ObjectDef, ObjectRef, Permission, PrincipalDef, Rewrite, SubjectRef,
    Tuple, ViewDef, WILDCARD, action_class_name, action_name,
};
use crate::schema;
use crate::view;

/// Failure to compile a policy from a database snapshot.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PolicyError {
    /// The snapshot does not carry the reserved authz schema.
    #[error("database is not an authorization database: {0} is not installed")]
    NotAnAuthzDatabase(String),
    /// A policy entity is missing a required attribute.
    #[error("{entity} at entity {eid} is missing {attribute}")]
    Incomplete {
        /// Kind of policy entity (`tuple`, `permission`, …).
        entity: &'static str,
        /// Entity id in the authz database.
        eid: String,
        /// The attribute that is missing.
        attribute: &'static str,
    },
    /// A value could not be read as the schema's type.
    #[error("{attribute} at entity {eid} is not a {expected}")]
    BadValue {
        /// The attribute.
        attribute: &'static str,
        /// Entity id in the authz database.
        eid: String,
        /// The expected shape.
        expected: &'static str,
    },
    /// A binding names a view the policy does not define. Fails compilation
    /// rather than silently dropping a restriction.
    #[error("binding {relation} on {object} names undefined view {view:?}")]
    UndefinedView {
        /// Relation the binding attaches to.
        relation: String,
        /// Object the binding attaches to.
        object: String,
        /// The missing view name.
        view: String,
    },
    /// A view names a filter type this build does not implement.
    #[error("view {view:?} has unsupported filter type {kind:?}")]
    UnsupportedFilter {
        /// The view name.
        view: String,
        /// The filter type read from policy data.
        kind: String,
    },
}

/// Counts describing a compiled policy, for logs and `authz status`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PolicyStats {
    /// Registered principals.
    pub principals: usize,
    /// Registered objects.
    pub objects: usize,
    /// Relationship tuples.
    pub tuples: usize,
    /// Permission mappings.
    pub permissions: usize,
    /// Rewrite rules.
    pub rewrites: usize,
    /// View definitions.
    pub views: usize,
    /// View bindings.
    pub bindings: usize,
}

/// An immutable, compiled policy snapshot at one authz basis `t`.
#[derive(Clone)]
pub struct Policy {
    basis_t: u64,
    /// `(object, relation)` → subjects holding it.
    by_object: BTreeMap<(ObjectRef, String), Vec<SubjectRef>>,
    /// Relation → rewrite rules deriving it.
    rewrites: BTreeMap<String, Vec<Rewrite>>,
    /// `(object type, action-or-class-or-*)` → relations satisfying it.
    permissions: BTreeMap<(String, String), BTreeSet<String>>,
    /// View name → compiled filter.
    views: BTreeMap<String, Arc<dyn ViewFilter>>,
    /// `(relation, object)` → binding.
    bindings: BTreeMap<(String, ObjectRef), Binding>,
    /// Principal id → registration.
    principals: BTreeMap<String, PrincipalDef>,
    /// Corium database name → objects registered against it.
    objects_by_database: BTreeMap<String, Vec<ObjectRef>>,
    stats: PolicyStats,
}

impl std::fmt::Debug for Policy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Policy")
            .field("basis_t", &self.basis_t)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

impl Policy {
    /// The empty policy at basis 0: no permission maps, so it denies
    /// everything. This is the fail-closed value, not a default to run on.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            basis_t: 0,
            by_object: BTreeMap::new(),
            rewrites: BTreeMap::new(),
            permissions: BTreeMap::new(),
            views: BTreeMap::new(),
            bindings: BTreeMap::new(),
            principals: BTreeMap::new(),
            objects_by_database: BTreeMap::new(),
            stats: PolicyStats::default(),
        }
    }

    /// Compiles the policy recorded in `db`.
    ///
    /// # Errors
    /// Returns [`PolicyError`] when the snapshot is not an authz database or a
    /// policy entity is malformed. Compilation is all-or-nothing on purpose: a
    /// half-read policy would silently under- or over-grant.
    pub fn compile(db: &Db) -> Result<Self, PolicyError> {
        let reader = Reader::new(db)?;
        let mut policy = Self::empty();
        policy.basis_t = db.basis_t();
        policy.read_principals(&reader)?;
        policy.read_objects(&reader)?;
        policy.read_tuples(&reader)?;
        policy.read_permissions(&reader)?;
        policy.read_rewrites(&reader)?;
        // Views before bindings: a binding that names a view the policy does
        // not define is a compile error, not a silently dropped restriction.
        policy.read_views(&reader)?;
        policy.read_bindings(&reader)?;
        policy.stats.principals = policy.principals.len();
        Ok(policy)
    }

    fn read_principals(&mut self, reader: &Reader) -> Result<(), PolicyError> {
        for (eid, values) in reader.rows(schema::PRINCIPAL_ID) {
            let id = reader.one_string("principal", eid, schema::PRINCIPAL_ID, values)?;
            let provider = reader
                .value(eid, schema::PRINCIPAL_PROVIDER)
                .map(|values| {
                    reader.one_string("principal", eid, schema::PRINCIPAL_PROVIDER, values)
                })
                .transpose()?
                .filter(|provider| provider != WILDCARD);
            let roles = reader.strings(eid, schema::PRINCIPAL_ROLE);
            self.principals.insert(
                id.clone(),
                PrincipalDef {
                    id,
                    provider,
                    roles,
                },
            );
        }
        Ok(())
    }

    fn read_objects(&mut self, reader: &Reader) -> Result<(), PolicyError> {
        for (eid, values) in reader.rows(schema::OBJECT_ID) {
            let id = reader.one_string("object", eid, schema::OBJECT_ID, values)?;
            let kind = reader.required_string("object", eid, schema::OBJECT_TYPE)?;
            let database = reader
                .value(eid, schema::OBJECT_DATABASE)
                .map(|values| reader.one_string("object", eid, schema::OBJECT_DATABASE, values))
                .transpose()?;
            let definition = ObjectDef {
                object: ObjectRef::new(kind, id),
                database,
            };
            if let Some(database) = &definition.database {
                self.objects_by_database
                    .entry(database.clone())
                    .or_default()
                    .push(definition.object.clone());
            }
            self.stats.objects += 1;
        }
        Ok(())
    }

    fn read_tuples(&mut self, reader: &Reader) -> Result<(), PolicyError> {
        for (eid, values) in reader.rows(schema::TUPLE_RELATION) {
            let relation = reader.one_string("tuple", eid, schema::TUPLE_RELATION, values)?;
            let subject = reader.required_string("tuple", eid, schema::TUPLE_SUBJECT)?;
            let object = reader.required_string("tuple", eid, schema::TUPLE_OBJECT)?;
            let tuple = Tuple {
                subject: SubjectRef::parse(&subject),
                relation,
                object: ObjectRef::parse(&object),
            };
            self.by_object
                .entry((tuple.object.clone(), tuple.relation.clone()))
                .or_default()
                .push(tuple.subject);
            self.stats.tuples += 1;
        }
        Ok(())
    }

    fn read_permissions(&mut self, reader: &Reader) -> Result<(), PolicyError> {
        for (eid, values) in reader.rows(schema::PERMISSION_ACTION) {
            let action = reader.one_string("permission", eid, schema::PERMISSION_ACTION, values)?;
            let object_type = reader
                .value(eid, schema::PERMISSION_OBJECT_TYPE)
                .map(|values| {
                    reader.one_string("permission", eid, schema::PERMISSION_OBJECT_TYPE, values)
                })
                .transpose()?
                .unwrap_or_else(|| WILDCARD.to_owned());
            let relations = reader.strings(eid, schema::PERMISSION_RELATION);
            if relations.is_empty() {
                return Err(PolicyError::Incomplete {
                    entity: "permission",
                    eid: format!("{eid:?}"),
                    attribute: schema::PERMISSION_RELATION,
                });
            }
            let permission = Permission {
                object_type,
                action,
                relations,
            };
            self.permissions
                .entry((permission.object_type.clone(), permission.action.clone()))
                .or_default()
                .extend(permission.relations);
            self.stats.permissions += 1;
        }
        Ok(())
    }

    fn read_rewrites(&mut self, reader: &Reader) -> Result<(), PolicyError> {
        for (eid, values) in reader.rows(schema::REWRITE_RELATION) {
            let relation = reader.one_string("rewrite", eid, schema::REWRITE_RELATION, values)?;
            let via_relation =
                reader.required_string("rewrite", eid, schema::REWRITE_VIA_RELATION)?;
            let on_relation =
                reader.required_string("rewrite", eid, schema::REWRITE_ON_RELATION)?;
            let object_type = reader
                .value(eid, schema::REWRITE_OBJECT_TYPE)
                .map(|values| {
                    reader.one_string("rewrite", eid, schema::REWRITE_OBJECT_TYPE, values)
                })
                .transpose()?
                .filter(|kind| kind != WILDCARD);
            self.rewrites
                .entry(relation.clone())
                .or_default()
                .push(Rewrite {
                    relation,
                    via_relation,
                    on_relation,
                    object_type,
                });
            self.stats.rewrites += 1;
        }
        Ok(())
    }

    fn read_views(&mut self, reader: &Reader) -> Result<(), PolicyError> {
        for (eid, values) in reader.rows(schema::VIEW_NAME) {
            let name = reader.one_string("view", eid, schema::VIEW_NAME, values)?;
            let kind_text = reader.required_string("view", eid, schema::VIEW_FILTER_TYPE)?;
            let kind =
                FilterKind::parse(&kind_text).ok_or_else(|| PolicyError::UnsupportedFilter {
                    view: name.clone(),
                    kind: kind_text,
                })?;
            let definition = ViewDef {
                name: name.clone(),
                kind,
                attributes: reader.strings(eid, schema::VIEW_ATTRIBUTE),
            };
            self.views.insert(name, view::build(&definition));
            self.stats.views += 1;
        }
        Ok(())
    }

    fn read_bindings(&mut self, reader: &Reader) -> Result<(), PolicyError> {
        for (eid, values) in reader.rows(schema::BINDING_RELATION) {
            let relation = reader.one_string("binding", eid, schema::BINDING_RELATION, values)?;
            let object = ObjectRef::parse(&reader.required_string(
                "binding",
                eid,
                schema::BINDING_OBJECT,
            )?);
            let view_name = reader
                .value(eid, schema::BINDING_VIEW)
                .map(|values| reader.one_string("binding", eid, schema::BINDING_VIEW, values))
                .transpose()?;
            let unfiltered = reader
                .value(eid, schema::BINDING_UNFILTERED)
                .and_then(|values| values.first())
                .is_some_and(|value| matches!(value, Value::Bool(true)));
            if let Some(name) = &view_name
                && !self.views.contains_key(name)
            {
                return Err(PolicyError::UndefinedView {
                    relation,
                    object: object.to_string(),
                    view: name.clone(),
                });
            }
            self.bindings.insert(
                (relation.clone(), object.clone()),
                Binding {
                    relation,
                    object,
                    view: view_name,
                    unfiltered,
                },
            );
            self.stats.bindings += 1;
        }
        Ok(())
    }

    /// The authz database basis this policy was compiled from.
    #[must_use]
    pub const fn basis_t(&self) -> u64 {
        self.basis_t
    }

    /// Counts of the policy entities compiled.
    #[must_use]
    pub const fn stats(&self) -> PolicyStats {
        self.stats
    }

    /// The registration for `id`, when policy data names it.
    #[must_use]
    pub fn principal(&self, id: &str) -> Option<&PrincipalDef> {
        self.principals.get(id)
    }

    /// Objects registered as standing for the Corium database `name`.
    #[must_use]
    pub fn objects_for_database(&self, name: &str) -> &[ObjectRef] {
        self.objects_by_database
            .get(name)
            .map_or(&[], Vec::as_slice)
    }

    /// Relations that satisfy `action` on an object of `object_type`.
    ///
    /// Matching widens in three steps — the exact action name, then its class
    /// (`read`/`write`/`admin`), then `*` — each against both the object's own
    /// type and `*`, so a policy can be as specific or as coarse as it likes.
    #[must_use]
    pub fn relations_for(&self, object_type: &str, action: Action) -> BTreeSet<String> {
        let names = [
            action_name(action),
            action_class_name(ActionClass::of(action)),
            WILDCARD,
        ];
        let mut relations = BTreeSet::new();
        for name in names {
            for kind in [object_type, WILDCARD] {
                if let Some(found) = self.permissions.get(&(kind.to_owned(), name.to_owned())) {
                    relations.extend(found.iter().cloned());
                }
            }
        }
        relations
    }

    /// Subjects holding `relation` on `object`, including tuples written
    /// against the type wildcard `type:*`.
    pub(crate) fn subjects_for(&self, object: &ObjectRef, relation: &str) -> Vec<&SubjectRef> {
        let mut subjects = Vec::new();
        for key in [object.clone(), object.wildcard_of_type()] {
            if let Some(found) = self.by_object.get(&(key, relation.to_owned())) {
                subjects.extend(found.iter());
            }
            if object.is_wildcard() {
                break;
            }
        }
        subjects
    }

    /// Rewrite rules deriving `relation` on an object of `object_type`.
    pub(crate) fn rewrites_for(&self, relation: &str, object_type: &str) -> Vec<&Rewrite> {
        self.rewrites.get(relation).map_or_else(Vec::new, |rules| {
            rules
                .iter()
                .filter(|rule| {
                    rule.object_type
                        .as_ref()
                        .is_none_or(|kind| kind == object_type)
                })
                .collect()
        })
    }

    /// The binding attached to `relation` on `object`, falling back to the
    /// type wildcard.
    pub(crate) fn binding_for(&self, relation: &str, object: &ObjectRef) -> Option<&Binding> {
        self.bindings
            .get(&(relation.to_owned(), object.clone()))
            .or_else(|| {
                self.bindings
                    .get(&(relation.to_owned(), object.wildcard_of_type()))
            })
    }

    /// The compiled filter a binding names.
    pub(crate) fn view(&self, name: &str) -> Option<&Arc<dyn ViewFilter>> {
        self.views.get(name)
    }
}

/// Reads the reserved attributes out of a snapshot, grouped by entity.
struct Reader {
    /// Attribute ident → entity → values, in entity order.
    rows: BTreeMap<&'static str, BTreeMap<corium_core::EntityId, Vec<Value>>>,
    interner: corium_core::KeywordInterner,
}

impl Reader {
    fn new(db: &Db) -> Result<Self, PolicyError> {
        let mut rows: BTreeMap<&'static str, BTreeMap<corium_core::EntityId, Vec<Value>>> =
            BTreeMap::new();
        let mut installed = 0usize;
        for attribute in schema::ATTRIBUTES {
            let Some(id) = attribute_id(db, attribute) else {
                continue;
            };
            installed += 1;
            let mut by_entity: BTreeMap<corium_core::EntityId, Vec<Value>> = BTreeMap::new();
            for datom in db.datoms_for_attribute(id) {
                by_entity.entry(datom.e).or_default().push(datom.v.clone());
            }
            rows.insert(attribute, by_entity);
        }
        // A misconfigured authz database (pointed at an application database,
        // say) must fail loudly: an empty policy would deny every request while
        // looking like a legitimately restrictive one.
        if installed == 0 {
            return Err(PolicyError::NotAnAuthzDatabase(
                schema::TUPLE_SUBJECT.to_owned(),
            ));
        }
        Ok(Self {
            rows,
            interner: db.interner().clone(),
        })
    }

    /// Entities carrying `attribute`, with their values.
    fn rows(&self, attribute: &'static str) -> Vec<(corium_core::EntityId, &Vec<Value>)> {
        self.rows.get(attribute).map_or_else(Vec::new, |by_entity| {
            by_entity
                .iter()
                .map(|(eid, values)| (*eid, values))
                .collect()
        })
    }

    fn value(&self, eid: corium_core::EntityId, attribute: &'static str) -> Option<&Vec<Value>> {
        self.rows.get(attribute).and_then(|rows| rows.get(&eid))
    }

    fn one_string(
        &self,
        entity: &'static str,
        eid: corium_core::EntityId,
        attribute: &'static str,
        values: &[Value],
    ) -> Result<String, PolicyError> {
        let value = values.first().ok_or_else(|| PolicyError::Incomplete {
            entity,
            eid: format!("{eid:?}"),
            attribute,
        })?;
        self.text(value).ok_or_else(|| PolicyError::BadValue {
            attribute,
            eid: format!("{eid:?}"),
            expected: "string",
        })
    }

    fn required_string(
        &self,
        entity: &'static str,
        eid: corium_core::EntityId,
        attribute: &'static str,
    ) -> Result<String, PolicyError> {
        let values = self.value(eid, attribute).ok_or(PolicyError::Incomplete {
            entity,
            eid: format!("{eid:?}"),
            attribute,
        })?;
        self.one_string(entity, eid, attribute, values)
    }

    fn strings(&self, eid: corium_core::EntityId, attribute: &'static str) -> Vec<String> {
        self.value(eid, attribute).map_or_else(Vec::new, |values| {
            values.iter().filter_map(|value| self.text(value)).collect()
        })
    }

    /// Reads a value as text. Keywords are accepted and rendered with their
    /// leading colon, so `:person/name` may be written either way.
    fn text(&self, value: &Value) -> Option<String> {
        match value {
            Value::Str(text) => Some(text.to_string()),
            Value::Keyword(id) => self
                .interner
                .resolve(*id)
                .map(std::string::ToString::to_string),
            _ => None,
        }
    }
}

fn attribute_id(db: &Db, attribute: &str) -> Option<AttrId> {
    db.idents().entid(&Keyword::parse(attribute))
}
