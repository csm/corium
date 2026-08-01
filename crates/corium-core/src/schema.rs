//! Schema model shared by transaction validation and peers.

use std::collections::BTreeMap;

use crate::{AttrId, EntityId};

/// Supported value types in v1.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ValueType {
    /// Boolean values.
    Bool,
    /// Signed 64-bit integers.
    Long,
    /// Double precision floating point values.
    Double,
    /// Milliseconds since Unix epoch.
    Instant,
    /// UUID values.
    Uuid,
    /// Interned keywords.
    Keyword,
    /// UTF-8 strings.
    Str,
    /// Byte arrays.
    Bytes,
    /// Entity references.
    Ref,
}

/// Attribute cardinality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cardinality {
    /// One value per entity.
    One,
    /// Many values per entity.
    Many,
}

/// Attribute uniqueness mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unique {
    /// Upsert identity.
    Identity,
    /// Unique value with conflict errors.
    Value,
}

/// Schema attribute metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attribute {
    /// Attribute entity id.
    pub id: AttrId,
    /// Value type.
    pub value_type: ValueType,
    /// Cardinality.
    pub cardinality: Cardinality,
    /// Optional uniqueness.
    pub unique: Option<Unique>,
    /// Component reference flag.
    pub is_component: bool,
    /// AVET coverage flag.
    pub indexed: bool,
    /// Skip history storage.
    pub no_history: bool,
}

/// Sealing algorithm a protection class uses.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SealAlgorithm {
    /// AES-256-GCM-SIV with a fixed nonce and the context in the AAD.
    #[default]
    Aes256GcmSiv,
}

/// What a protection class binds into the sealing context, and therefore what
/// its determinism leaks (`docs/design/encryption.md`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProtectionScope {
    /// Context binds the attribute: a keyless reader can tell that two
    /// entities share a value.
    #[default]
    Attribute,
    /// Context also binds the entity: no cross-entity equality, and a
    /// ciphertext cannot be moved between subjects.
    Entity,
}

/// What a reader without the class key gets in place of a sealed value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MissingKeyPolicy {
    /// The value binds sealed and renders redacted.
    #[default]
    Redact,
    /// The datom is dropped from scan results.
    Hide,
    /// The read fails.
    Error,
}

/// How a plaintext datom asserted before its attribute was protected is
/// treated for a reader that holds no class key.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LegacyPlaintextPolicy {
    /// Treated exactly like a sealed value the reader cannot open.
    #[default]
    Redact,
    /// Returned literally.
    PassThrough,
}

/// Epoch a class seals under until its key is rotated.
///
/// It is 1 to match `corium_crypt::STATIC_KEY_EPOCH`, the epoch a key read
/// from a file or an environment variable always resolves at.
pub const FIRST_KEY_EPOCH: u32 = 1;

/// A protection class: an entity naming a key, never holding one.
///
/// Key material is resolved by each process through its own keyring, so the
/// database records only the identity. See ADR-0018.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectionClass {
    /// Class entity id, as carried in every value sealed under it.
    pub id: EntityId,
    /// Key identity (a `Keyring` URI such as `file:/etc/corium/pii.key`).
    pub key_id: String,
    /// Sealing algorithm.
    pub algorithm: SealAlgorithm,
    /// What the sealing context binds.
    pub scope: ProtectionScope,
    /// Plaintext is padded up to a multiple of this many bytes when set.
    pub padding: Option<u32>,
    /// What a reader without the key sees.
    pub on_missing_key: MissingKeyPolicy,
    /// How pre-protection plaintext is treated.
    pub legacy_plaintext: LegacyPlaintextPolicy,
    /// Epoch new values are sealed under, and the only epoch an assertion may
    /// name. Validation is keyless, so the schema records it rather than the
    /// keyring.
    pub current_epoch: u32,
}

/// An attribute's protection over time, as `(t, class)` pairs.
///
/// Protection changes are forward-only: a datom keeps the form it was
/// asserted in. Assertions must use the form the attribute has *now*;
/// retractions and `:db/cas` old values may use any form it has ever had. The
/// timeline is what makes both questions answerable without decoding
/// anything.
///
/// Protection is declared at database creation today, so a timeline holds at
/// most one entry, at `t = 0`. The shape is the one forward-only alterations
/// will append to.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProtectionTimeline(Vec<(u64, Option<EntityId>)>);

/// The timeline of an attribute that has never been protected.
static UNPROTECTED: ProtectionTimeline = ProtectionTimeline(Vec::new());

impl ProtectionTimeline {
    /// A timeline protecting the attribute under `class` from `t` onward.
    #[must_use]
    pub fn protected_from(t: u64, class: EntityId) -> Self {
        Self(vec![(t, Some(class))])
    }

    /// Builds a timeline from `(t, class)` entries in ascending `t` order.
    #[must_use]
    pub const fn from_entries(entries: Vec<(u64, Option<EntityId>)>) -> Self {
        Self(entries)
    }

    /// The `(t, class)` entries, in ascending `t` order.
    #[must_use]
    pub fn entries(&self) -> &[(u64, Option<EntityId>)] {
        &self.0
    }

    /// The class protecting the attribute now, if any.
    #[must_use]
    pub fn current(&self) -> Option<EntityId> {
        self.0.last().and_then(|(_, class)| *class)
    }

    /// The class protecting the attribute at basis `t`, if any.
    #[must_use]
    pub fn at(&self, t: u64) -> Option<EntityId> {
        self.0
            .iter()
            .take_while(|(entry_t, _)| *entry_t <= t)
            .last()
            .and_then(|(_, class)| *class)
    }

    /// Whether the attribute is or has ever been protected. Such an attribute
    /// may never gain `:db/index` or `:db/unique`.
    #[must_use]
    pub fn ever_protected(&self) -> bool {
        self.0.iter().any(|(_, class)| class.is_some())
    }

    /// Every class this attribute has ever been sealed under, in first-use
    /// order. A retraction may name any of them.
    pub fn classes(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.0.iter().filter_map(|(_, class)| *class)
    }

    /// Whether the timeline records nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Immutable schema cache.
///
/// Protection lives beside the attribute table rather than inside
/// [`Attribute`]: an attribute record mirrors the Datomic attribute map, while
/// a protection timeline is engine state that is always consulted together
/// with the class table.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Schema {
    attrs: BTreeMap<AttrId, Attribute>,
    classes: BTreeMap<EntityId, ProtectionClass>,
    protection: BTreeMap<AttrId, ProtectionTimeline>,
}

impl Schema {
    /// Adds or replaces an attribute.
    pub fn insert(&mut self, attr: Attribute) {
        self.attrs.insert(attr.id, attr);
    }

    /// Looks up an attribute.
    #[must_use]
    pub fn get(&self, id: AttrId) -> Option<&Attribute> {
        self.attrs.get(&id)
    }

    /// Iterates over every installed attribute in id order.
    pub fn iter(&self) -> impl Iterator<Item = (&AttrId, &Attribute)> {
        self.attrs.iter()
    }

    /// Adds or replaces a protection class.
    pub fn insert_class(&mut self, class: ProtectionClass) {
        self.classes.insert(class.id, class);
    }

    /// Looks up a protection class by entity id.
    #[must_use]
    pub fn class(&self, id: EntityId) -> Option<&ProtectionClass> {
        self.classes.get(&id)
    }

    /// Iterates over every protection class in id order.
    pub fn classes(&self) -> impl Iterator<Item = (&EntityId, &ProtectionClass)> {
        self.classes.iter()
    }

    /// Records an attribute's protection timeline.
    pub fn set_protection(&mut self, attr: AttrId, timeline: ProtectionTimeline) {
        if timeline.is_empty() {
            self.protection.remove(&attr);
        } else {
            self.protection.insert(attr, timeline);
        }
    }

    /// An attribute's protection timeline; empty when it has never been
    /// protected.
    #[must_use]
    pub fn protection(&self, attr: AttrId) -> &ProtectionTimeline {
        self.protection.get(&attr).unwrap_or(&UNPROTECTED)
    }

    /// Iterates over every recorded protection timeline in attribute order.
    pub fn protections(&self) -> impl Iterator<Item = (&AttrId, &ProtectionTimeline)> {
        self.protection.iter()
    }

    /// The class currently protecting `attr`, resolved through the class
    /// table.
    #[must_use]
    pub fn protection_class(&self, attr: AttrId) -> Option<&ProtectionClass> {
        self.class(self.protection(attr).current()?)
    }

    /// Whether `attr` is protected now. Protected attributes are absent from
    /// AVET and VAET and support no value-ordered access.
    #[must_use]
    pub fn is_protected(&self, attr: AttrId) -> bool {
        self.protection(attr).current().is_some()
    }
}
