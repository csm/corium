//! The reserved schema of the authorization system database.
//!
//! The authz database is an ordinary Corium database: the schema below is
//! installed with `CreateDatabase` like any other, and policy is transacted
//! with ordinary transactions. Keeping it ordinary is deliberate — backup,
//! restore, fork, `as-of`, and the log API all work on policy data for free,
//! which is what makes an authz decision reproducible from its basis `t`.
//!
//! Every attribute is a string (or boolean) so the tuple shape stays flat and
//! importable from, or exportable to, an `OpenFGA`-style service. References
//! between policy entities are by *name* (`"database:music"`, `"reader-view"`)
//! rather than by entity ref, so a policy fragment can be transacted without
//! first resolving the entities it points at.

use corium_query::edn::Edn;

/// Default name of the system authorization database.
///
/// `docs/design/auth.md` writes this as `_corium/authz`; database names are
/// restricted to `[A-Za-z0-9_-]`, so the shipped default spells the same idea
/// as `corium_authz`.
pub const DEFAULT_AUTHZ_DB: &str = "corium_authz";

/// `:authz.principal/id` — the subject id an [`IdentityProvider`] produced.
///
/// [`IdentityProvider`]: corium_protocol::authz::IdentityProvider
pub const PRINCIPAL_ID: &str = "authz.principal/id";
/// `:authz.principal/provider` — provider that must vouch for the id (`*` = any).
pub const PRINCIPAL_PROVIDER: &str = "authz.principal/provider";
/// `:authz.principal/role` — roles granted to the principal by policy data.
pub const PRINCIPAL_ROLE: &str = "authz.principal/role";

/// `:authz.object/type` — object type, e.g. `database`, `tenant`, `catalog`.
pub const OBJECT_TYPE: &str = "authz.object/type";
/// `:authz.object/id` — object id within its type.
pub const OBJECT_ID: &str = "authz.object/id";
/// `:authz.object/database` — Corium database this object stands for.
pub const OBJECT_DATABASE: &str = "authz.object/database";

/// `:authz.tuple/subject` — `type:id`, or the userset `type:id#relation`.
pub const TUPLE_SUBJECT: &str = "authz.tuple/subject";
/// `:authz.tuple/relation` — relation name, e.g. `member`, `viewer`.
pub const TUPLE_RELATION: &str = "authz.tuple/relation";
/// `:authz.tuple/object` — `type:id`, or `type:*` for every object of a type.
pub const TUPLE_OBJECT: &str = "authz.tuple/object";

/// `:authz.permission/object-type` — object type the permission applies to (`*` = any).
pub const PERMISSION_OBJECT_TYPE: &str = "authz.permission/object-type";
/// `:authz.permission/action` — action name, action class, or `*`.
pub const PERMISSION_ACTION: &str = "authz.permission/action";
/// `:authz.permission/relation` — relation(s) that satisfy the action.
pub const PERMISSION_RELATION: &str = "authz.permission/relation";

/// `:authz.rewrite/relation` — derived relation this rule defines.
pub const REWRITE_RELATION: &str = "authz.rewrite/relation";
/// `:authz.rewrite/via-relation` — relation walked from the object to its parent.
pub const REWRITE_VIA_RELATION: &str = "authz.rewrite/via-relation";
/// `:authz.rewrite/on-relation` — relation required on the parent.
pub const REWRITE_ON_RELATION: &str = "authz.rewrite/on-relation";
/// `:authz.rewrite/object-type` — restricts the rule to one object type.
pub const REWRITE_OBJECT_TYPE: &str = "authz.rewrite/object-type";

/// `:authz.view/name` — name a binding refers to.
pub const VIEW_NAME: &str = "authz.view/name";
/// `:authz.view/filter-type` — `attribute-allowlist` or `attribute-denylist`.
pub const VIEW_FILTER_TYPE: &str = "authz.view/filter-type";
/// `:authz.view/attribute` — attribute idents the filter names.
pub const VIEW_ATTRIBUTE: &str = "authz.view/attribute";

/// `:authz.binding/relation` — relation the view is attached to.
pub const BINDING_RELATION: &str = "authz.binding/relation";
/// `:authz.binding/object` — object the view is attached to (`type:*` allowed).
pub const BINDING_OBJECT: &str = "authz.binding/object";
/// `:authz.binding/view` — name of the view this binding applies.
pub const BINDING_VIEW: &str = "authz.binding/view";
/// `:authz.binding/unfiltered` — marks the relation as granting full visibility.
pub const BINDING_UNFILTERED: &str = "authz.binding/unfiltered";

/// Every attribute ident of the reserved schema, in installation order.
pub const ATTRIBUTES: &[&str] = &[
    PRINCIPAL_ID,
    PRINCIPAL_PROVIDER,
    PRINCIPAL_ROLE,
    OBJECT_TYPE,
    OBJECT_ID,
    OBJECT_DATABASE,
    TUPLE_SUBJECT,
    TUPLE_RELATION,
    TUPLE_OBJECT,
    PERMISSION_OBJECT_TYPE,
    PERMISSION_ACTION,
    PERMISSION_RELATION,
    REWRITE_RELATION,
    REWRITE_VIA_RELATION,
    REWRITE_ON_RELATION,
    REWRITE_OBJECT_TYPE,
    VIEW_NAME,
    VIEW_FILTER_TYPE,
    VIEW_ATTRIBUTE,
    BINDING_RELATION,
    BINDING_OBJECT,
    BINDING_VIEW,
    BINDING_UNFILTERED,
];

/// The reserved schema as EDN attribute maps, ready for `CreateDatabase`.
///
/// `:db/index true` is set on every attribute the compiler scans by value so
/// AVET covers the lookups a policy import performs; the compiler itself reads
/// whole attributes in AEVT order, which covers every attribute.
pub const SCHEMA_EDN: &str = r"
[{:db/ident :authz.principal/id           :db/valueType :db.type/string  :db/unique :db.unique/identity}
 {:db/ident :authz.principal/provider     :db/valueType :db.type/string}
 {:db/ident :authz.principal/role         :db/valueType :db.type/string  :db/cardinality :db.cardinality/many}

 {:db/ident :authz.object/type            :db/valueType :db.type/string  :db/index true}
 {:db/ident :authz.object/id              :db/valueType :db.type/string  :db/index true}
 {:db/ident :authz.object/database        :db/valueType :db.type/string  :db/index true}

 {:db/ident :authz.tuple/subject          :db/valueType :db.type/string  :db/index true}
 {:db/ident :authz.tuple/relation         :db/valueType :db.type/string  :db/index true}
 {:db/ident :authz.tuple/object           :db/valueType :db.type/string  :db/index true}

 {:db/ident :authz.permission/object-type :db/valueType :db.type/string  :db/index true}
 {:db/ident :authz.permission/action      :db/valueType :db.type/string  :db/index true}
 {:db/ident :authz.permission/relation    :db/valueType :db.type/string  :db/cardinality :db.cardinality/many}

 {:db/ident :authz.rewrite/relation       :db/valueType :db.type/string  :db/index true}
 {:db/ident :authz.rewrite/via-relation   :db/valueType :db.type/string}
 {:db/ident :authz.rewrite/on-relation    :db/valueType :db.type/string}
 {:db/ident :authz.rewrite/object-type    :db/valueType :db.type/string}

 {:db/ident :authz.view/name              :db/valueType :db.type/string  :db/unique :db.unique/identity}
 {:db/ident :authz.view/filter-type       :db/valueType :db.type/string}
 {:db/ident :authz.view/attribute         :db/valueType :db.type/string  :db/cardinality :db.cardinality/many}

 {:db/ident :authz.binding/relation       :db/valueType :db.type/string  :db/index true}
 {:db/ident :authz.binding/object         :db/valueType :db.type/string  :db/index true}
 {:db/ident :authz.binding/view           :db/valueType :db.type/string}
 {:db/ident :authz.binding/unfiltered     :db/valueType :db.type/boolean}]
";

/// The default permission map an `authz init` installs: the coarse action
/// classes of the [`Guard`] API bound to conventional relation names, so a
/// fresh authz database answers something sensible before an operator writes
/// any policy of their own.
///
/// [`Guard`]: corium_protocol::authz::Guard
pub const DEFAULT_PERMISSIONS_EDN: &str = r#"
[{:db/id "p-db-read"    :authz.permission/object-type "database" :authz.permission/action "read"  :authz.permission/relation ["viewer" "writer" "owner"]}
 {:db/id "p-db-write"   :authz.permission/object-type "database" :authz.permission/action "write" :authz.permission/relation ["writer" "owner"]}
 {:db/id "p-db-admin"   :authz.permission/object-type "database" :authz.permission/action "admin" :authz.permission/relation ["owner"]}
 {:db/id "p-cat-read"   :authz.permission/object-type "catalog"  :authz.permission/action "read"  :authz.permission/relation ["viewer" "owner"]}
 {:db/id "p-cat-write"  :authz.permission/object-type "catalog"  :authz.permission/action "write" :authz.permission/relation ["owner"]}
 {:db/id "p-cat-admin"  :authz.permission/object-type "catalog"  :authz.permission/action "admin" :authz.permission/relation ["owner"]}]
"#;

/// Parses [`SCHEMA_EDN`] into the attribute maps `CreateDatabase` expects.
///
/// # Panics
/// Never: the source is a crate constant and is covered by a unit test.
#[must_use]
pub fn schema_forms() -> Vec<Edn> {
    forms_of(SCHEMA_EDN)
}

/// Parses [`DEFAULT_PERMISSIONS_EDN`] into transaction forms.
///
/// # Panics
/// Never: the source is a crate constant and is covered by a unit test.
#[must_use]
pub fn default_permission_forms() -> Vec<Edn> {
    forms_of(DEFAULT_PERMISSIONS_EDN)
}

fn forms_of(text: &str) -> Vec<Edn> {
    match corium_query::edn::read_all(text)
        .expect("reserved authz EDN parses")
        .pop()
    {
        Some(Edn::Vector(items) | Edn::List(items)) => items,
        other => panic!("reserved authz EDN must be one vector, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_forms::schemaform::schema_from_edn;

    #[test]
    fn reserved_schema_installs() {
        let forms = schema_forms();
        assert_eq!(forms.len(), ATTRIBUTES.len());
        let (schema, idents) = schema_from_edn(&forms).expect("reserved schema is well formed");
        for attribute in ATTRIBUTES {
            let keyword = corium_core::Keyword::parse(attribute);
            let id = idents
                .entid(&keyword)
                .unwrap_or_else(|| panic!("{attribute} is installed"));
            assert!(schema.get(id).is_some(), "{attribute} has schema metadata");
        }
    }

    #[test]
    fn default_permissions_parse() {
        assert_eq!(default_permission_forms().len(), 6);
    }
}
