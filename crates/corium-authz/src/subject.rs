//! Mapping a request [`Principal`] onto the subjects a check searches from.
//!
//! A principal carries a subject id, the provider that vouched for it, roles,
//! and free-form claims. Policy tuples are written against *objects*, so the
//! check needs those facts as object references. Some of them are registered
//! in the authz database (a principal entity with roles), and some are
//! ephemeral — an OIDC `groups` claim becomes `group:eng` for the duration of
//! this one request, with no tuple to maintain.

use std::collections::BTreeSet;

use corium_protocol::authz::Principal;

use crate::model::ObjectRef;
use crate::policy::Policy;

/// How a [`Principal`]'s claims become subjects.
#[derive(Clone, Debug)]
pub struct SubjectMapping {
    /// Claim key → object type. A claim's value may list several ids
    /// separated by commas or spaces; each becomes its own subject.
    pub claim_subjects: Vec<(String, String)>,
    /// Require a principal to be registered in the authz database before its
    /// bare `user:<id>` subject is used.
    ///
    /// Off by default, so a policy can name `user:alice` without registering
    /// her first. Turn it on when several providers can mint identities and an
    /// unregistered `alice` from one of them must not inherit tuples written
    /// for another's. Provider-qualified subjects (`user:<provider>/<id>`) are
    /// always derived and are never ambiguous.
    pub require_registered_principal: bool,
}

impl Default for SubjectMapping {
    fn default() -> Self {
        Self {
            claim_subjects: vec![
                ("groups".to_owned(), "group".to_owned()),
                ("group".to_owned(), "group".to_owned()),
                ("tenant".to_owned(), "tenant".to_owned()),
            ],
            require_registered_principal: false,
        }
    }
}

/// Derives the subject set for `principal` under `policy`.
///
/// The set always contains the provider-qualified `user:<provider>/<id>`; the
/// bare `user:<id>` is included unless policy data registers that id against a
/// *different* provider (which would otherwise let one issuer mint an identity
/// another issuer's tuples were written for).
#[must_use]
pub fn subjects_of(
    principal: &Principal,
    policy: &Policy,
    mapping: &SubjectMapping,
) -> BTreeSet<ObjectRef> {
    let mut subjects = BTreeSet::new();
    subjects.insert(ObjectRef::new(
        "user",
        format!("{}/{}", principal.provider, principal.subject),
    ));

    let registered = policy.principal(&principal.subject);
    let provider_matches = registered
        .and_then(|definition| definition.provider.as_deref())
        .is_none_or(|provider| provider == principal.provider);
    let admissible =
        provider_matches && (registered.is_some() || !mapping.require_registered_principal);
    if admissible {
        subjects.insert(ObjectRef::new("user", principal.subject.clone()));
    }

    if !principal.is_anonymous() {
        // Lets a tuple name `authenticated:*` — "any caller who authenticated",
        // whoever they are.
        subjects.insert(ObjectRef::new("authenticated", principal.subject.clone()));
    }

    let mut roles: BTreeSet<&str> = principal.roles.iter().map(String::as_str).collect();
    if admissible && let Some(definition) = registered {
        roles.extend(definition.roles.iter().map(String::as_str));
    }
    for role in roles {
        subjects.insert(ObjectRef::new("role", role));
    }

    for (claim, kind) in &mapping.claim_subjects {
        let Some(value) = principal.claim(claim) else {
            continue;
        };
        for item in value.split([',', ' ']) {
            let item = item.trim();
            if !item.is_empty() {
                subjects.insert(ObjectRef::new(kind.clone(), item));
            }
        }
    }
    subjects
}

/// A stable fingerprint of everything [`subjects_of`] reads, used as the key of
/// the check-result cache. Two principals with the same fingerprint are
/// interchangeable for authorization.
#[must_use]
pub fn fingerprint(principal: &Principal, mapping: &SubjectMapping) -> String {
    let mut text = format!("{}|{}", principal.provider, principal.subject);
    for role in &principal.roles {
        text.push('|');
        text.push_str(role);
    }
    for (claim, _) in &mapping.claim_subjects {
        if let Some(value) = principal.claim(claim) {
            text.push('|');
            text.push_str(claim);
            text.push('=');
            text.push_str(value);
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs(subjects: &BTreeSet<ObjectRef>) -> Vec<String> {
        subjects.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn derives_user_role_and_claim_subjects() {
        let principal = Principal::new("oidc", "alice")
            .with_role("admin")
            .with_claim("groups", "eng, sre")
            .with_claim("tenant", "acme");
        let subjects = subjects_of(&principal, &Policy::empty(), &SubjectMapping::default());
        let rendered = refs(&subjects);
        for expected in [
            "user:alice",
            "user:oidc/alice",
            "authenticated:alice",
            "role:admin",
            "group:eng",
            "group:sre",
            "tenant:acme",
        ] {
            assert!(
                rendered.contains(&expected.to_owned()),
                "missing {expected} in {rendered:?}"
            );
        }
    }

    #[test]
    fn anonymous_gets_no_authenticated_subject() {
        let subjects = subjects_of(
            &Principal::anonymous(),
            &Policy::empty(),
            &SubjectMapping::default(),
        );
        let rendered = refs(&subjects);
        assert!(rendered.contains(&"user:anonymous".to_owned()));
        assert!(
            !rendered
                .iter()
                .any(|subject| subject.starts_with("authenticated:"))
        );
    }

    #[test]
    fn fingerprint_separates_providers_and_roles() {
        let mapping = SubjectMapping::default();
        let alice = Principal::new("oidc", "alice");
        let other = Principal::new("static-token", "alice");
        assert_ne!(fingerprint(&alice, &mapping), fingerprint(&other, &mapping));
        assert_ne!(
            fingerprint(&alice, &mapping),
            fingerprint(&alice.clone().with_role("admin"), &mapping)
        );
    }
}
