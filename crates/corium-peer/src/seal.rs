//! Peer-side sealing of transaction values on protected attributes.
//!
//! Sealing happens here, in the writing peer, before tx-data is encoded onto
//! the wire: the peer already holds the schema, so it knows every attribute's
//! class, and the transactor never needs a key (`docs/design/encryption.md`,
//! "Write path").
//!
//! The rewrite is structural rather than a re-render of the expanded
//! transaction: only the value positions of protected attributes change, and
//! every other form — tempids, lookup refs, map forms, `:db/retractEntity` —
//! reaches the transactor exactly as the caller wrote it.

use corium_core::{KeywordInterner, Value};
use corium_db::{
    Db,
    protect::{ClassKeys, seal_value},
};
use corium_forms::txforms::{SEALED_TAG, sealed_to_edn, tx_value};
use corium_query::edn::Edn;

use crate::PeerError;

/// Value positions each list form carries, by operation.
///
/// `:db/cas` seals both: the old value must name the bytes the fact was
/// asserted as, and under create-time protection those are exactly what
/// re-sealing the plaintext produces.
const fn value_positions(op: &str) -> &'static [usize] {
    match op.as_bytes() {
        b"db/add" | b"db/retract" => &[3],
        b"db/cas" => &[3, 4],
        _ => &[],
    }
}

/// Rewrites every value on a protected attribute into its sealed form.
///
/// Returns the forms unchanged when the database declares no protection
/// class, which is the overwhelming majority of transactions.
///
/// # Errors
///
/// Returns [`PeerError::MissingKey`] when the transaction writes a class this
/// peer holds no key for; the whole transaction is refused rather than
/// committed in part.
pub fn seal_forms(db: &Db, keys: &ClassKeys, forms: Vec<Edn>) -> Result<Vec<Edn>, PeerError> {
    if db.schema().protections().next().is_none() {
        return Ok(forms);
    }
    // A scratch interner: a keyword value seals its text, so nothing it
    // interns needs to outlive the rewrite or reach the naming table.
    let mut interner = db.interner().clone();
    forms
        .into_iter()
        .map(|form| match form {
            Edn::Vector(items) => list_form(db, keys, &mut interner, items),
            Edn::Map(pairs) => map_form(db, keys, &mut interner, pairs),
            other => Ok(other),
        })
        .collect()
}

fn list_form(
    db: &Db,
    keys: &ClassKeys,
    interner: &mut KeywordInterner,
    mut items: Vec<Edn>,
) -> Result<Edn, PeerError> {
    let Some(op) = items.first().and_then(Edn::as_keyword) else {
        return Ok(Edn::Vector(items));
    };
    let op = format!(
        "{}/{}",
        op.namespace.as_deref().unwrap_or_default(),
        op.name
    );
    let positions = value_positions(&op);
    if positions.is_empty() {
        return Ok(Edn::Vector(items));
    }
    // An attribute position that does not resolve is the transactor's to
    // reject, with the error it already has for it.
    let Some(attr) = items
        .get(2)
        .and_then(Edn::as_keyword)
        .and_then(|keyword| db.idents().entid(keyword))
    else {
        return Ok(Edn::Vector(items));
    };
    if !db.schema().is_protected(attr) {
        return Ok(Edn::Vector(items));
    }
    for position in positions {
        if let Some(form) = items.get(*position) {
            items[*position] = seal_one(db, keys, interner, attr, form)?;
        }
    }
    Ok(Edn::Vector(items))
}

fn map_form(
    db: &Db,
    keys: &ClassKeys,
    interner: &mut KeywordInterner,
    pairs: Vec<(Edn, Edn)>,
) -> Result<Edn, PeerError> {
    pairs
        .into_iter()
        .map(|(key, value)| {
            let Some(attr) = key
                .as_keyword()
                .and_then(|keyword| db.idents().entid(keyword))
                .filter(|attr| db.schema().is_protected(*attr))
            else {
                return Ok((key, value));
            };
            let sealed = match value {
                // Cardinality many: every element seals on its own.
                Edn::Vector(values) => Edn::Vector(
                    values
                        .iter()
                        .map(|form| seal_one(db, keys, interner, attr, form))
                        .collect::<Result<_, _>>()?,
                ),
                ref form => seal_one(db, keys, interner, attr, form)?,
            };
            Ok((key, sealed))
        })
        .collect::<Result<Vec<_>, PeerError>>()
        .map(Edn::Map)
}

fn seal_one(
    db: &Db,
    keys: &ClassKeys,
    interner: &mut KeywordInterner,
    attr: corium_core::AttrId,
    form: &Edn,
) -> Result<Edn, PeerError> {
    // A caller that supplied a sealed value already knows what it is doing —
    // retracting a historical form, say — so it passes through untouched.
    if matches!(form, Edn::Tagged(tag, _) if tag == SEALED_TAG) {
        return Ok(form.clone());
    }
    let value = tx_value(db, interner, attr, form)?;
    // The entity is not known here: a tempid has no id until the transactor
    // resolves one. Attribute scope does not bind it, and entity scope is
    // refused rather than sealed against the wrong subject.
    let Value::Sealed(sealed) = seal_value(db.schema(), keys, attr, None, &value, interner)? else {
        unreachable!("seal_value returns a sealed value")
    };
    Ok(sealed_to_edn(&sealed))
}
