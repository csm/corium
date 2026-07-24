//! Building and editing an authorization database.
//!
//! Two jobs live here. The first is turning policy text into a database value
//! without a transactor: [`PolicyBuilder`] installs the reserved schema and
//! applies transaction forms in process, which is what tests, `authz check
//! --policy`, and embedded deployments use. The second is emitting the
//! transaction forms an operator's `authz grant` / `authz revoke` sends to a
//! real authz database, so both paths speak exactly the same EDN.

use corium_core::{EntityId, Keyword, KeywordInterner, Partition, Value};
use corium_db::{Db, FIRST_USER_ID};
use corium_forms::schemaform::schema_from_edn;
use corium_forms::txforms::tx_items_from_edn;
use corium_query::edn::{Edn, read_all};

use crate::model::{ObjectRef, SubjectRef};
use crate::schema;

/// Failure to build or edit an authz database in process.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// The policy text is not valid EDN.
    #[error("cannot read policy EDN: {0}")]
    Edn(#[from] corium_query::edn::EdnError),
    /// A form is not a valid transaction form.
    #[error("bad policy form: {0}")]
    Form(#[from] corium_forms::txforms::TxFormError),
    /// The transaction does not apply.
    #[error("cannot apply policy transaction: {0}")]
    Transact(#[from] corium_tx::TxError),
}

/// Applies policy transactions to an in-memory authz database.
pub struct PolicyBuilder {
    db: Db,
    interner: KeywordInterner,
    next_user: u64,
}

impl Default for PolicyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PolicyBuilder {
    /// An empty authz database with the reserved schema installed.
    ///
    /// # Panics
    /// Never: the reserved schema is a crate constant covered by tests.
    #[must_use]
    pub fn new() -> Self {
        let (schema, idents) =
            schema_from_edn(&schema::schema_forms()).expect("reserved schema is well formed");
        let interner = KeywordInterner::default();
        Self {
            db: Db::new(schema).with_naming(idents, interner.clone()),
            interner,
            next_user: FIRST_USER_ID,
        }
    }

    /// Applies EDN transaction forms (a vector of maps, or bare forms).
    ///
    /// # Errors
    /// Returns [`BootstrapError`] when the text is malformed or the
    /// transaction does not apply.
    pub fn transact_edn(&mut self, text: &str) -> Result<&mut Self, BootstrapError> {
        let forms = read_all(text)?;
        let forms = match forms.as_slice() {
            [Edn::Vector(items) | Edn::List(items)] => items.clone(),
            _ => forms,
        };
        self.transact(&forms)
    }

    /// Applies transaction forms.
    ///
    /// # Errors
    /// Returns [`BootstrapError`] when a form is malformed or the transaction
    /// does not apply.
    pub fn transact(&mut self, forms: &[Edn]) -> Result<&mut Self, BootstrapError> {
        let items = tx_items_from_edn(&self.db, &mut self.interner, forms)?;
        let t = self.db.basis_t() + 1;
        let tx = EntityId::new(Partition::Tx as u32, t);
        let prepared = corium_tx::prepare(&self.db, items, tx, self.next_user)?;
        self.next_user = prepared
            .tempids
            .values()
            .filter(|entity| entity.partition() == Partition::User as u32)
            .map(|entity| entity.sequence() + 1)
            .max()
            .unwrap_or(self.next_user)
            .max(self.next_user);
        self.db = self
            .db
            .clone()
            .with_naming(self.db.idents().clone(), self.interner.clone())
            .with_transaction(t, &prepared.datoms);
        Ok(self)
    }

    /// The current database value.
    #[must_use]
    pub fn db(&self) -> Db {
        self.db.clone()
    }
}

/// Builds an authz database from policy EDN.
///
/// # Errors
/// Returns [`BootstrapError`] when the text is malformed or does not apply.
pub fn policy_db(text: &str) -> Result<Db, BootstrapError> {
    let mut builder = PolicyBuilder::new();
    builder.transact_edn(text)?;
    Ok(builder.db())
}

/// A transaction form asserting the tuple `subject relation object`.
#[must_use]
pub fn tuple_form(subject: &str, relation: &str, object: &str) -> Edn {
    // Round-tripping through the parsers normalizes what an operator typed
    // (`alice` becomes `user:alice`) so the stored tuple matches what a check
    // derives from a principal.
    entity(
        "tuple",
        &[
            (
                schema::TUPLE_SUBJECT,
                SubjectRef::parse(subject).to_string(),
            ),
            (schema::TUPLE_RELATION, relation.to_owned()),
            (schema::TUPLE_OBJECT, ObjectRef::parse(object).to_string()),
        ],
    )
}

/// A transaction form registering a principal.
#[must_use]
pub fn principal_form(id: &str, provider: Option<&str>, roles: &[String]) -> Edn {
    let mut pairs = vec![(Edn::keyword(schema::PRINCIPAL_ID), Edn::Str(id.to_owned()))];
    if let Some(provider) = provider {
        pairs.push((
            Edn::keyword(schema::PRINCIPAL_PROVIDER),
            Edn::Str(provider.to_owned()),
        ));
    }
    if !roles.is_empty() {
        pairs.push((
            Edn::keyword(schema::PRINCIPAL_ROLE),
            Edn::Vector(roles.iter().cloned().map(Edn::Str).collect()),
        ));
    }
    map_with_id(format!("principal-{id}"), pairs)
}

/// A transaction form mapping an action (or action class) onto relations.
#[must_use]
pub fn permission_form(object_type: &str, action: &str, relations: &[String]) -> Edn {
    map_with_id(
        format!("permission-{object_type}-{action}"),
        vec![
            (
                Edn::keyword(schema::PERMISSION_OBJECT_TYPE),
                Edn::Str(object_type.to_owned()),
            ),
            (
                Edn::keyword(schema::PERMISSION_ACTION),
                Edn::Str(action.to_owned()),
            ),
            (
                Edn::keyword(schema::PERMISSION_RELATION),
                Edn::Vector(relations.iter().cloned().map(Edn::Str).collect()),
            ),
        ],
    )
}

/// The entity id of the tuple `subject relation object`, when the database
/// holds it. `authz revoke` retracts what this finds.
#[must_use]
pub fn find_tuple(db: &Db, subject: &str, relation: &str, object: &str) -> Option<EntityId> {
    let subject = SubjectRef::parse(subject).to_string();
    let object = ObjectRef::parse(object).to_string();
    let attribute = |ident: &str| db.idents().entid(&Keyword::parse(ident));
    let (subject_attr, relation_attr, object_attr) = (
        attribute(schema::TUPLE_SUBJECT)?,
        attribute(schema::TUPLE_RELATION)?,
        attribute(schema::TUPLE_OBJECT)?,
    );
    let matches = |entity: EntityId, attribute, expected: &str| {
        db.values(entity, attribute)
            .iter()
            .any(|value| matches!(value, Value::Str(text) if &**text == expected))
    };
    db.datoms_for_attribute(subject_attr)
        .filter(|datom| matches!(&datom.v, Value::Str(text) if **text == subject))
        .map(|datom| datom.e)
        .find(|entity| {
            matches(*entity, relation_attr, relation) && matches(*entity, object_attr, &object)
        })
}

/// A `[:db/retractEntity …]` form.
#[must_use]
pub fn retract_entity_form(entity: EntityId) -> Edn {
    Edn::Vector(vec![
        Edn::keyword("db/retractEntity"),
        Edn::Long(i64::try_from(entity.raw()).unwrap_or_default()),
    ])
}

fn entity(prefix: &str, attributes: &[(&str, String)]) -> Edn {
    let pairs = attributes
        .iter()
        .map(|(attribute, value)| (Edn::keyword(attribute), Edn::Str(value.clone())))
        .collect();
    let id = format!(
        "{prefix}-{}",
        attributes
            .iter()
            .map(|(_, value)| value.as_str())
            .collect::<Vec<_>>()
            .join("-")
    );
    map_with_id(id, pairs)
}

fn map_with_id(temp_id: String, mut pairs: Vec<(Edn, Edn)>) -> Edn {
    pairs.push((Edn::keyword("db/id"), Edn::Str(temp_id)));
    pairs.sort();
    Edn::Map(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::Policy;

    #[test]
    fn builds_and_edits_a_policy_database() {
        let mut builder = PolicyBuilder::new();
        builder
            .transact(&[
                tuple_form("alice", "writer", "database:music"),
                permission_form("database", "write", &["writer".to_owned()]),
            ])
            .expect("policy applies");
        let db = builder.db();
        let policy = Policy::compile(&db).expect("policy compiles");
        assert_eq!(policy.stats().tuples, 1);
        assert_eq!(policy.stats().permissions, 1);

        let entity = find_tuple(&db, "alice", "writer", "database:music").expect("tuple is stored");
        builder
            .transact(&[retract_entity_form(entity)])
            .expect("retraction applies");
        let policy = Policy::compile(&builder.db()).expect("policy compiles");
        assert_eq!(policy.stats().tuples, 0);
    }

    #[test]
    fn normalizes_operator_shorthand() {
        let Edn::Map(pairs) = tuple_form("alice", "member", "group:eng") else {
            panic!("tuple form is a map");
        };
        let subject = pairs
            .iter()
            .find(|(key, _)| *key == Edn::keyword(schema::TUPLE_SUBJECT))
            .map(|(_, value)| value.clone());
        assert_eq!(subject, Some(Edn::Str("user:alice".to_owned())));
    }
}
