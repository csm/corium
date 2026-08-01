//! Attribute-level sealing and hydration.
//!
//! A value on a protected attribute is sealed by the writing peer before the
//! transaction leaves it, and opened only by a reader whose keyring resolves
//! the class key. Everything in between — the transactor, the log, the
//! indexes, a peer without the key — handles the ciphertext bytewise and
//! keeps working (`docs/design/encryption.md`, ADR-0018).
//!
//! Key resolution is async and happens once, into a [`ClassKeys`] snapshot,
//! so the sync transact and query paths never touch a keyring.

use std::collections::BTreeMap;
use std::sync::Arc;

use corium_core::{
    AttrId, EntityId, KeywordInterner, MissingKeyPolicy, ProtectionClass, ProtectionScope, Schema,
    Sealed, Value, ValueType, decode_seal_plaintext, encode_seal_plaintext,
};
use corium_crypt::{KeyError, Keyring, SecretKey, open_deterministic, seal_deterministic};

/// Domain separator opening every sealing AAD.
const SEAL_AAD_PREFIX: &[u8] = b"corium/seal-v1";

/// Sealing or hydration failure.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ProtectError {
    /// The attribute is not protected at this basis.
    #[error("attribute {0} is not protected")]
    NotProtected(AttrId),
    /// The schema names a class the class table does not define.
    #[error("protection class {0} is not installed")]
    UnknownClass(EntityId),
    /// No key for the class and epoch this process needs.
    #[error("no key for protection class {class} at epoch {epoch}")]
    MissingKey {
        /// Class whose key is absent.
        class: EntityId,
        /// Epoch that was wanted.
        epoch: u32,
    },
    /// The body did not authenticate under the class key.
    ///
    /// Distinct from a decode failure on purpose: a wrong key, a moved
    /// ciphertext, and a tampered body must all look the same.
    #[error("sealed value for class {class} epoch {epoch} did not authenticate")]
    Authentication {
        /// Class named by the sealed header.
        class: EntityId,
        /// Epoch named by the sealed header.
        epoch: u32,
    },
    /// Entity-scoped classes are declared but not yet sealable.
    #[error("protection class {0} uses :db.protect.scope/entity, which this build cannot seal")]
    UnsupportedScope(EntityId),
    /// The plaintext could not be encoded or decoded.
    #[error("seal plaintext: {0}")]
    Plaintext(#[from] corium_core::SealPlaintextError),
    /// The sealed header names a class the attribute has never used.
    #[error("attribute {attr} was never sealed under class {class}")]
    ForeignClass {
        /// Attribute the value was found on.
        attr: AttrId,
        /// Class the sealed header names.
        class: EntityId,
    },
}

/// Every class key one process can resolve, snapshotted once.
///
/// Resolution is async and a class key is stable for the life of a
/// connection, so the snapshot is taken at connect time and the sync paths
/// only look keys up. A class whose key this process cannot resolve is simply
/// absent, which is the ordinary case for a keyless peer rather than an
/// error.
#[derive(Clone, Debug, Default)]
pub struct ClassKeys {
    keys: BTreeMap<(EntityId, u32), Arc<SecretKey>>,
}

impl ClassKeys {
    /// Resolves the current epoch key of every class in `schema`.
    ///
    /// A class whose key the keyring cannot produce is skipped: a peer
    /// holding some keys and not others is the normal deployment, and what a
    /// reader sees for the rest is the class's own missing-key policy.
    ///
    /// # Errors
    ///
    /// Returns [`KeyError`] only for a malformed key identity; a key that is
    /// merely absent or unresolvable leaves the class out of the snapshot.
    pub async fn resolve(schema: &Schema, keyring: &dyn Keyring) -> Result<Self, KeyError> {
        let mut keys = BTreeMap::new();
        for (id, class) in schema.classes() {
            let key_id = corium_crypt::KeyId::new(class.key_id.clone())?;
            if let Ok(key) = keyring.key(&key_id, class.current_epoch).await {
                keys.insert((*id, class.current_epoch), Arc::new(key));
            }
        }
        Ok(Self { keys })
    }

    /// Adds one class key, for tests and for epochs resolved on demand.
    pub fn insert(&mut self, class: EntityId, epoch: u32, key: SecretKey) {
        self.keys.insert((class, epoch), Arc::new(key));
    }

    /// Whether this process can open values sealed by `class` at `epoch`.
    #[must_use]
    pub fn holds(&self, class: EntityId, epoch: u32) -> bool {
        self.keys.contains_key(&(class, epoch))
    }

    /// Whether the snapshot resolves no class at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn key(&self, class: EntityId, epoch: u32) -> Result<&SecretKey, ProtectError> {
        self.keys
            .get(&(class, epoch))
            .map(AsRef::as_ref)
            .ok_or(ProtectError::MissingKey { class, epoch })
    }
}

/// Builds the AAD binding a sealed body to its context.
///
/// Every field the design lists is present, so a body cannot be moved to
/// another key, epoch, attribute, subject, or declared type without failing
/// authentication.
fn seal_aad(
    key_id: &str,
    epoch: u32,
    attr: AttrId,
    entity: Option<EntityId>,
    vtype: ValueType,
) -> Vec<u8> {
    let mut aad = Vec::with_capacity(SEAL_AAD_PREFIX.len() + key_id.len() + 21);
    aad.extend_from_slice(SEAL_AAD_PREFIX);
    aad.extend_from_slice(key_id.as_bytes());
    aad.extend_from_slice(&epoch.to_be_bytes());
    aad.extend_from_slice(&attr.raw().to_be_bytes());
    if let Some(entity) = entity {
        aad.extend_from_slice(&entity.raw().to_be_bytes());
    }
    aad.push(value_type_byte(vtype));
    aad
}

/// The declared-type byte the AAD carries, in [`ValueType`] declaration
/// order — the same order the sortable encoding uses.
const fn value_type_byte(vtype: ValueType) -> u8 {
    match vtype {
        ValueType::Bool => 0,
        ValueType::Long => 1,
        ValueType::Double => 2,
        ValueType::Instant => 3,
        ValueType::Uuid => 4,
        ValueType::Keyword => 5,
        ValueType::Str => 6,
        ValueType::Bytes => 7,
        ValueType::Ref => 8,
    }
}

/// The entity an entity-scoped seal binds, or `None` under attribute scope.
///
/// `entity` is unused until entity scope is sealable: doing so needs the
/// entity id before tempid resolution would assign one, which costs an
/// id-reservation round trip the peer does not make yet.
fn scoped_entity(
    class: &ProtectionClass,
    _entity: EntityId,
) -> Result<Option<EntityId>, ProtectError> {
    match class.scope {
        ProtectionScope::Attribute => Ok(None),
        ProtectionScope::Entity => Err(ProtectError::UnsupportedScope(class.id)),
    }
}

/// Rounds plaintext up to the class's padding multiple with zero bytes.
///
/// The true length is not recorded: every value encoding is self-delimiting,
/// so the decoder finds the end of the value and ignores what follows (see
/// `decode_seal_plaintext`). Padding costs storage and removes the length
/// side channel for short, guessable values.
fn pad(plaintext: &mut Vec<u8>, padding: Option<u32>) {
    let Some(multiple) = padding.map(|multiple| multiple as usize).filter(|m| *m > 0) else {
        return;
    };
    let remainder = plaintext.len() % multiple;
    if remainder != 0 {
        plaintext.resize(plaintext.len() + (multiple - remainder), 0);
    }
}

/// Seals `value` for `attr` on entity `e` under the attribute's current class.
///
/// # Errors
///
/// Returns [`ProtectError`] when the attribute is unprotected, its class is
/// missing or entity-scoped, the process holds no key for it, or the value
/// cannot be encoded as seal plaintext.
pub fn seal_value(
    schema: &Schema,
    keys: &ClassKeys,
    attr: AttrId,
    e: EntityId,
    value: &Value,
    interner: &KeywordInterner,
) -> Result<Value, ProtectError> {
    let class_id = schema
        .protection(attr)
        .current()
        .ok_or(ProtectError::NotProtected(attr))?;
    let class = schema
        .class(class_id)
        .ok_or(ProtectError::UnknownClass(class_id))?;
    let entity = scoped_entity(class, e)?;
    let vtype = schema
        .get(attr)
        .map_or(ValueType::Str, |attribute| attribute.value_type);
    let key = keys.key(class_id, class.current_epoch)?;

    let mut plaintext = encode_seal_plaintext(value, interner)?;
    pad(&mut plaintext, class.padding);
    let aad = seal_aad(&class.key_id, class.current_epoch, attr, entity, vtype);
    let body =
        seal_deterministic(key, &aad, &plaintext).map_err(|_| ProtectError::Authentication {
            class: class_id,
            epoch: class.current_epoch,
        })?;
    Ok(Value::Sealed(Sealed {
        class: class_id,
        epoch: class.current_epoch,
        vtype,
        body: Arc::from(body),
    }))
}

/// Opens a sealed value found on `attr` of entity `e`.
///
/// The class and epoch come from the value's own cleartext header, so a
/// mixed attribute — one re-classified or rotated mid-life — decodes without
/// schema archaeology.
///
/// # Errors
///
/// Returns [`ProtectError`] when the header names a class the attribute never
/// used, the process holds no key for it, or the body does not authenticate.
pub fn open_value(
    schema: &Schema,
    keys: &ClassKeys,
    attr: AttrId,
    e: EntityId,
    sealed: &Sealed,
    interner: &KeywordInterner,
) -> Result<Value, ProtectError> {
    if !schema
        .protection(attr)
        .classes()
        .any(|class| class == sealed.class)
    {
        return Err(ProtectError::ForeignClass {
            attr,
            class: sealed.class,
        });
    }
    let class = schema
        .class(sealed.class)
        .ok_or(ProtectError::UnknownClass(sealed.class))?;
    let entity = scoped_entity(class, e)?;
    let key = keys.key(sealed.class, sealed.epoch)?;
    let aad = seal_aad(&class.key_id, sealed.epoch, attr, entity, sealed.vtype);
    let plaintext =
        open_deterministic(key, &aad, &sealed.body).map_err(|_| ProtectError::Authentication {
            class: sealed.class,
            epoch: sealed.epoch,
        })?;
    Ok(decode_seal_plaintext(&plaintext, sealed.vtype, interner)?)
}

/// What a reader should do with one datom on a protected attribute.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Disposition {
    /// The reader holds the key: open the value and bind plaintext.
    Hydrate,
    /// Bind the value as it stands; it renders redacted.
    Redact,
    /// Drop the datom from scan results.
    Hide,
    /// Fail the read.
    Fail,
}

/// Decides what a reader does with a value on a protected attribute.
///
/// `holds_key` is whether this process can open the value in hand — for a
/// sealed value, its own class and epoch; for legacy plaintext, the
/// attribute's current class. The two cases share this decision on purpose:
/// "may this reader have this attribute's values?" is one question, and its
/// answer must not depend on which side of a protection change a datom fell
/// on.
#[must_use]
pub fn disposition(class: &ProtectionClass, holds_key: bool) -> Disposition {
    if holds_key {
        return Disposition::Hydrate;
    }
    match class.on_missing_key {
        MissingKeyPolicy::Redact => Disposition::Redact,
        MissingKeyPolicy::Hide => Disposition::Hide,
        MissingKeyPolicy::Error => Disposition::Fail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::{
        Attribute, Cardinality, Keyword, LegacyPlaintextPolicy, Partition, ProtectionTimeline,
        SealAlgorithm,
    };

    const CLASS: EntityId = EntityId::new(Partition::Db as u32, 100);
    const SSN: AttrId = EntityId::new(Partition::Db as u32, 101);
    const OTHER: AttrId = EntityId::new(Partition::Db as u32, 102);
    const ALICE: EntityId = EntityId::new(Partition::User as u32, 1);

    fn class(padding: Option<u32>, scope: ProtectionScope) -> ProtectionClass {
        ProtectionClass {
            id: CLASS,
            key_id: "file:/etc/corium/pii.key".into(),
            algorithm: SealAlgorithm::Aes256GcmSiv,
            scope,
            padding,
            on_missing_key: MissingKeyPolicy::Redact,
            legacy_plaintext: LegacyPlaintextPolicy::Redact,
            current_epoch: 1,
        }
    }

    fn schema_with(vtype: ValueType, padding: Option<u32>) -> Schema {
        let mut schema = Schema::default();
        schema.insert_class(class(padding, ProtectionScope::Attribute));
        for id in [SSN, OTHER] {
            schema.insert(Attribute {
                id,
                value_type: vtype,
                cardinality: Cardinality::One,
                unique: None,
                is_component: false,
                indexed: false,
                no_history: false,
            });
            schema.set_protection(id, ProtectionTimeline::protected_from(0, CLASS));
        }
        schema
    }

    fn keys() -> ClassKeys {
        let mut keys = ClassKeys::default();
        keys.insert(CLASS, 1, SecretKey::from_slice(&[7_u8; 32]).expect("key"));
        keys
    }

    #[test]
    fn every_value_type_round_trips() {
        let keys = keys();
        let mut interner = KeywordInterner::default();
        let kw = interner.intern(Keyword::new(Some("status"), "active"));
        for (vtype, value) in [
            (ValueType::Bool, Value::Bool(true)),
            (ValueType::Long, Value::Long(-9)),
            (ValueType::Double, Value::Double(corium_core::TotalF64(1.5))),
            (ValueType::Instant, Value::Instant(1_700_000_000_000)),
            (ValueType::Uuid, Value::Uuid(0x1234_5678_9abc_def0)),
            (ValueType::Keyword, Value::Keyword(kw)),
            (ValueType::Str, Value::Str("123-45-6789".into())),
            (ValueType::Bytes, Value::Bytes(Arc::from(vec![0, 1, 0, 0]))),
        ] {
            let schema = schema_with(vtype, None);
            let sealed = seal_value(&schema, &keys, SSN, ALICE, &value, &interner)
                .unwrap_or_else(|error| panic!("{vtype:?} seals: {error}"));
            let Value::Sealed(header) = &sealed else {
                panic!("sealing must produce a sealed value")
            };
            assert_eq!(header.vtype, vtype);
            assert_eq!(header.class, CLASS);
            assert_eq!(
                open_value(&schema, &keys, SSN, ALICE, header, &interner).expect("opens"),
                value
            );
        }
    }

    #[test]
    fn a_reader_that_never_interned_the_keyword_still_gets_it() {
        let schema = schema_with(ValueType::Keyword, None);
        let keys = keys();
        let mut writer = KeywordInterner::default();
        let kw = writer.intern(Keyword::new(Some("status"), "active"));
        let sealed =
            seal_value(&schema, &keys, SSN, ALICE, &Value::Keyword(kw), &writer).expect("seals");
        let Value::Sealed(header) = &sealed else {
            panic!("sealed expected")
        };

        let reader = KeywordInterner::default();
        let opened = open_value(&schema, &keys, SSN, ALICE, header, &reader).expect("opens");
        let Value::Keyword(id) = opened else {
            panic!("keyword expected")
        };
        assert_eq!(
            reader.resolve(id),
            Some(Keyword::new(Some("status"), "active"))
        );
        // The protected vocabulary stayed out of the durable naming table.
        assert_eq!(reader.len(), 0);
    }

    #[test]
    fn sealing_is_deterministic_so_the_transactor_can_pair_facts() {
        let schema = schema_with(ValueType::Str, None);
        let keys = keys();
        let interner = KeywordInterner::default();
        let value = Value::Str("123-45-6789".into());
        let first = seal_value(&schema, &keys, SSN, ALICE, &value, &interner).expect("seals");
        let again = seal_value(&schema, &keys, SSN, ALICE, &value, &interner).expect("seals");
        assert_eq!(first, again);
    }

    #[test]
    fn a_body_cannot_be_moved_to_another_attribute() {
        let schema = schema_with(ValueType::Str, None);
        let keys = keys();
        let interner = KeywordInterner::default();
        let sealed = seal_value(
            &schema,
            &keys,
            SSN,
            ALICE,
            &Value::Str("123-45-6789".into()),
            &interner,
        )
        .expect("seals");
        let Value::Sealed(header) = &sealed else {
            panic!("sealed expected")
        };
        assert_eq!(
            open_value(&schema, &keys, OTHER, ALICE, header, &interner),
            Err(ProtectError::Authentication {
                class: CLASS,
                epoch: 1,
            })
        );
    }

    #[test]
    fn padding_hides_length_without_recording_it() {
        let schema = schema_with(ValueType::Str, Some(64));
        let keys = keys();
        let interner = KeywordInterner::default();
        let mut lengths = std::collections::BTreeSet::new();
        for text in ["a", "abcdefghij", "123-45-6789"] {
            let sealed = seal_value(
                &schema,
                &keys,
                SSN,
                ALICE,
                &Value::Str(text.into()),
                &interner,
            )
            .expect("seals");
            let Value::Sealed(header) = &sealed else {
                panic!("sealed expected")
            };
            lengths.insert(header.body.len());
            assert_eq!(
                open_value(&schema, &keys, SSN, ALICE, header, &interner).expect("opens"),
                Value::Str(text.into())
            );
        }
        assert_eq!(lengths.len(), 1, "padded bodies must share one length");
        // Plaintext padded to 64 bytes plus the 16-byte AEAD tag.
        assert_eq!(lengths.into_iter().next(), Some(80));
    }

    #[test]
    fn a_wrong_key_reports_authentication_not_a_decode_error() {
        let schema = schema_with(ValueType::Str, None);
        let interner = KeywordInterner::default();
        let sealed = seal_value(
            &schema,
            &keys(),
            SSN,
            ALICE,
            &Value::Str("123-45-6789".into()),
            &interner,
        )
        .expect("seals");
        let Value::Sealed(header) = &sealed else {
            panic!("sealed expected")
        };
        let mut wrong = ClassKeys::default();
        wrong.insert(CLASS, 1, SecretKey::from_slice(&[9_u8; 32]).expect("key"));
        assert_eq!(
            open_value(&schema, &wrong, SSN, ALICE, header, &interner),
            Err(ProtectError::Authentication {
                class: CLASS,
                epoch: 1,
            })
        );
    }

    #[test]
    fn a_class_the_attribute_never_used_is_refused_before_any_key_work() {
        let schema = schema_with(ValueType::Str, None);
        let foreign = Sealed {
            class: EntityId::new(Partition::Db as u32, 200),
            epoch: 1,
            vtype: ValueType::Str,
            body: Arc::from(vec![0_u8; 32]),
        };
        assert_eq!(
            open_value(
                &schema,
                &keys(),
                SSN,
                ALICE,
                &foreign,
                &KeywordInterner::default()
            ),
            Err(ProtectError::ForeignClass {
                attr: SSN,
                class: foreign.class,
            })
        );
    }

    #[test]
    fn entity_scope_is_declarable_but_not_yet_sealable() {
        let mut schema = schema_with(ValueType::Str, None);
        schema.insert_class(class(None, ProtectionScope::Entity));
        assert_eq!(
            seal_value(
                &schema,
                &keys(),
                SSN,
                ALICE,
                &Value::Str("x".into()),
                &KeywordInterner::default()
            ),
            Err(ProtectError::UnsupportedScope(CLASS))
        );
    }

    #[test]
    fn a_missing_key_follows_the_class_policy() {
        let mut class = class(None, ProtectionScope::Attribute);
        assert_eq!(disposition(&class, true), Disposition::Hydrate);
        assert_eq!(disposition(&class, false), Disposition::Redact);
        class.on_missing_key = MissingKeyPolicy::Hide;
        assert_eq!(disposition(&class, false), Disposition::Hide);
        class.on_missing_key = MissingKeyPolicy::Error;
        assert_eq!(disposition(&class, false), Disposition::Fail);
    }
}
