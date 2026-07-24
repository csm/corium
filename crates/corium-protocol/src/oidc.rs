//! OIDC / JWT bearer-token authentication.
//!
//! This is the concrete [`TokenVerifier`](crate::authz::TokenVerifier) the
//! [`authz`](crate::authz) spike left as a seam: it verifies a signed JWT
//! against an issuer's JSON Web Key Set (JWKS) and maps its claims onto a
//! [`Principal`](crate::authz::Principal). It is behind the `oidc` cargo
//! feature so the crypto/HTTP dependencies stay out of the default build.
//!
//! The verifier is deliberately shaped the way production middleware is: keys
//! are fetched once (via [`OidcVerifier::from_discovery`], gated on the
//! `oidc-discovery` feature) and cached, and [`OidcVerifier::verify`] is then a
//! cheap synchronous signature-and-claims check with no I/O — which is what lets
//! it run inside the synchronous tonic interceptor
//! ([`IdentityInterceptor`](crate::authz::IdentityInterceptor)). Rotate keys by
//! building a fresh verifier out of band.
//!
//! Supported signature algorithms are the RSA family (`RS256`, `RS384`,
//! `RS512`); other algorithms are rejected. Standard claim checks are applied:
//! `iss` must equal the configured issuer, `exp` must be in the future (within a
//! configurable leeway), `nbf` must not be in the future, and — when audiences
//! are configured — `aud` must contain one of them.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::signature;
use serde::Deserialize;

use crate::authz::{AuthError, Principal, TokenVerifier};

/// How an [`OidcVerifier`] maps verified JWT claims onto a [`Principal`].
#[derive(Clone, Debug)]
pub struct OidcConfig {
    /// Expected `iss` claim; also the discovery base URL.
    pub issuer: String,
    /// Accepted `aud` values. Empty disables audience checking (not advised
    /// on a shared system).
    pub audiences: Vec<String>,
    /// Name recorded in [`Principal::provider`].
    pub provider_name: String,
    /// Claim carrying roles. A JSON array of strings, or a single space- or
    /// comma-delimited string (as OAuth `scope` is), both expand to roles.
    pub role_claim: String,
    /// Claims copied verbatim onto [`Principal::claims`] when present (e.g.
    /// `email`, `tenant`). String, integer, and boolean values are stringified.
    pub copy_claims: Vec<String>,
    /// Clock-skew leeway (seconds) applied to `exp`/`nbf`.
    pub leeway_secs: u64,
}

impl OidcConfig {
    /// A config for `issuer` accepting `audiences`, with sensible defaults:
    /// provider name `oidc`, roles from the `roles` claim, and `email` copied.
    #[must_use]
    pub fn new(
        issuer: impl Into<String>,
        audiences: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            issuer: issuer.into(),
            audiences: audiences.into_iter().map(Into::into).collect(),
            provider_name: "oidc".to_owned(),
            role_claim: "roles".to_owned(),
            copy_claims: vec!["email".to_owned()],
            leeway_secs: 60,
        }
    }

    /// Overrides the [`Principal::provider`] name (builder style).
    #[must_use]
    pub fn with_provider_name(mut self, name: impl Into<String>) -> Self {
        self.provider_name = name.into();
        self
    }

    /// Overrides the roles claim (builder style).
    #[must_use]
    pub fn with_role_claim(mut self, claim: impl Into<String>) -> Self {
        self.role_claim = claim.into();
        self
    }

    /// Sets the claims copied onto the principal (builder style).
    #[must_use]
    pub fn with_copy_claims(mut self, claims: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.copy_claims = claims.into_iter().map(Into::into).collect();
        self
    }
}

/// One RSA verification key from a JWKS.
struct RsaKey {
    kid: Option<String>,
    n: Vec<u8>,
    e: Vec<u8>,
}

/// A JWKS-backed OIDC/JWT verifier. Holds the issuer's public keys and its
/// [`OidcConfig`]; verification is a local, synchronous operation.
pub struct OidcVerifier {
    config: OidcConfig,
    keys: Vec<RsaKey>,
}

/// The subset of a JWK we consume (RSA public keys only).
#[derive(Deserialize)]
struct Jwk {
    kty: String,
    kid: Option<String>,
    n: Option<String>,
    e: Option<String>,
}

/// A JSON Web Key Set document.
#[derive(Deserialize)]
struct JwkSet {
    keys: Vec<Jwk>,
}

impl OidcVerifier {
    /// Builds a verifier from a JWKS JSON document (the body of a `jwks_uri`).
    ///
    /// # Errors
    /// Returns [`AuthError::Unauthenticated`] when the JSON is malformed or
    /// carries no usable RSA key.
    pub fn from_jwks_json(config: OidcConfig, jwks_json: &str) -> Result<Self, AuthError> {
        let set: JwkSet = serde_json::from_str(jwks_json)
            .map_err(|error| AuthError::Unauthenticated(format!("invalid JWKS: {error}")))?;
        let mut keys = Vec::new();
        for jwk in set.keys {
            if jwk.kty != "RSA" {
                continue;
            }
            let (Some(n), Some(e)) = (jwk.n.as_deref(), jwk.e.as_deref()) else {
                continue;
            };
            let n = URL_SAFE_NO_PAD
                .decode(n)
                .map_err(|error| AuthError::Unauthenticated(format!("bad JWK modulus: {error}")))?;
            let e = URL_SAFE_NO_PAD.decode(e).map_err(|error| {
                AuthError::Unauthenticated(format!("bad JWK exponent: {error}"))
            })?;
            keys.push(RsaKey { kid: jwk.kid, n, e });
        }
        if keys.is_empty() {
            return Err(AuthError::Unauthenticated(
                "JWKS contained no usable RSA keys".to_owned(),
            ));
        }
        Ok(Self { config, keys })
    }

    /// Fetches the issuer's OIDC discovery document and JWKS, then builds a
    /// verifier. Gated on the `oidc-discovery` feature (adds an HTTP client).
    ///
    /// # Errors
    /// Returns [`AuthError::Unauthenticated`] when discovery/JWKS cannot be
    /// fetched or parsed, or when the discovery `issuer` disagrees with the
    /// configured one.
    #[cfg(feature = "oidc-discovery")]
    pub async fn from_discovery(config: OidcConfig) -> Result<Self, AuthError> {
        #[derive(Deserialize)]
        struct Discovery {
            issuer: String,
            jwks_uri: String,
        }
        let base = config.issuer.trim_end_matches('/');
        let discovery_url = format!("{base}/.well-known/openid-configuration");
        let client = reqwest::Client::new();
        let discovery: Discovery = client
            .get(&discovery_url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| AuthError::Unauthenticated(format!("OIDC discovery failed: {error}")))?
            .json()
            .await
            .map_err(|error| {
                AuthError::Unauthenticated(format!("bad OIDC discovery document: {error}"))
            })?;
        if discovery.issuer.trim_end_matches('/') != base {
            return Err(AuthError::Unauthenticated(format!(
                "discovery issuer {:?} does not match configured issuer {:?}",
                discovery.issuer, config.issuer
            )));
        }
        let jwks_json = client
            .get(&discovery.jwks_uri)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| AuthError::Unauthenticated(format!("JWKS fetch failed: {error}")))?
            .text()
            .await
            .map_err(|error| AuthError::Unauthenticated(format!("JWKS read failed: {error}")))?;
        Self::from_jwks_json(config, &jwks_json)
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    fn verify_signature(&self, header: &JwtHeader, signing_input: &[u8], sig: &[u8]) -> bool {
        let algorithm: &signature::RsaParameters = match header.alg.as_str() {
            "RS256" => &signature::RSA_PKCS1_2048_8192_SHA256,
            "RS384" => &signature::RSA_PKCS1_2048_8192_SHA384,
            "RS512" => &signature::RSA_PKCS1_2048_8192_SHA512,
            _ => return false,
        };
        // When the token names a `kid`, only that key may sign it; otherwise try
        // every key (a JWKS with a single key needs no `kid`).
        self.keys
            .iter()
            .filter(|key| match (&header.kid, &key.kid) {
                (Some(want), Some(have)) => want == have,
                // Header names no `kid`, or the JWKS key is unlabelled: try it.
                _ => true,
            })
            .any(|key| {
                signature::RsaPublicKeyComponents {
                    n: &key.n,
                    e: &key.e,
                }
                .verify(algorithm, signing_input, sig)
                .is_ok()
            })
    }

    fn check_claims(&self, claims: &Claims) -> Result<(), AuthError> {
        if claims.iss.as_deref() != Some(self.config.issuer.as_str()) {
            return Err(AuthError::Unauthenticated(format!(
                "token issuer {:?} is not {:?}",
                claims.iss, self.config.issuer
            )));
        }
        let now = Self::now_secs();
        if let Some(exp) = claims.exp
            && exp.saturating_add(self.config.leeway_secs) < now
        {
            return Err(AuthError::Unauthenticated("token expired".to_owned()));
        }
        if let Some(nbf) = claims.nbf
            && nbf > now.saturating_add(self.config.leeway_secs)
        {
            return Err(AuthError::Unauthenticated("token not yet valid".to_owned()));
        }
        if !self.config.audiences.is_empty() {
            let audience_ok = claims
                .aud
                .iter()
                .any(|aud| self.config.audiences.contains(aud));
            if !audience_ok {
                return Err(AuthError::Unauthenticated(
                    "token audience is not accepted".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn principal(&self, claims: &Claims) -> Result<Principal, AuthError> {
        let subject = claims.sub.clone().ok_or_else(|| {
            AuthError::Unauthenticated("token has no subject (sub) claim".to_owned())
        })?;
        let mut principal = Principal::new(self.config.provider_name.clone(), subject);
        if let Some(role_value) = claims.extra.get(&self.config.role_claim) {
            for role in roles_from_claim(role_value) {
                principal = principal.with_role(role);
            }
        }
        for name in &self.config.copy_claims {
            if let Some(value) = claims.extra.get(name)
                && let Some(text) = scalar_claim(value)
            {
                principal = principal.with_claim(name.clone(), text);
            }
        }
        Ok(principal)
    }
}

impl TokenVerifier for OidcVerifier {
    fn verify(&self, token: &str) -> Result<Principal, AuthError> {
        let mut parts = token.splitn(3, '.');
        let (Some(header_b64), Some(payload_b64), Some(sig_b64)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Err(AuthError::Unauthenticated("malformed JWT".to_owned()));
        };
        let header: JwtHeader = decode_json(header_b64, "JWT header")?;
        let sig = URL_SAFE_NO_PAD
            .decode(sig_b64)
            .map_err(|error| AuthError::Unauthenticated(format!("bad JWT signature: {error}")))?;
        let signing_input = format!("{header_b64}.{payload_b64}");
        if !self.verify_signature(&header, signing_input.as_bytes(), &sig) {
            return Err(AuthError::Unauthenticated(
                "JWT signature verification failed".to_owned(),
            ));
        }
        let claims: Claims = decode_json(payload_b64, "JWT claims")?;
        self.check_claims(&claims)?;
        self.principal(&claims)
    }
}

fn decode_json<T: for<'de> Deserialize<'de>>(segment: &str, what: &str) -> Result<T, AuthError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(segment)
        .map_err(|error| AuthError::Unauthenticated(format!("bad {what}: {error}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| AuthError::Unauthenticated(format!("bad {what}: {error}")))
}

/// Renders a role claim value (a string array, or a delimited string) as roles.
fn roles_from_claim(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::Array(items) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_owned))
            .collect(),
        serde_json::Value::String(text) => text
            .split([' ', ','])
            .filter(|part| !part.is_empty())
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

/// Stringifies a scalar claim value for [`Principal::claims`].
fn scalar_claim(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => Some(text.clone()),
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

#[derive(Deserialize)]
struct JwtHeader {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

/// The registered JWT claims we inspect, plus a catch-all for the rest.
#[derive(Deserialize)]
struct Claims {
    #[serde(default)]
    iss: Option<String>,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default, deserialize_with = "audience")]
    aud: Vec<String>,
    #[serde(default)]
    exp: Option<u64>,
    #[serde(default)]
    nbf: Option<u64>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

/// `aud` may be a single string or an array of strings; normalize to a `Vec`.
fn audience<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Audience {
        One(String),
        Many(Vec<String>),
    }
    Ok(match Option::<Audience>::deserialize(deserializer)? {
        Some(Audience::One(value)) => vec![value],
        Some(Audience::Many(values)) => values,
        None => Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A 2048-bit RSA test key: the PKCS#8 private key (signs test tokens) and
    // its public modulus/exponent (the JWKS the verifier trusts). Generated
    // once with OpenSSL; used only in these tests.
    const TEST_KEY_PKCS8_B64: &str = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDCjNlYMqERhTY1svp3UnxBTrUEisZnomkHMbWlkMV/8QErAHvknLuar8Gh/3rbHzuGSsW0qTj+QOvr7ZmeTQ1lUP031ssEkjDxJr2DFVC9zhtu0O5VN5OIy3s1DwfeWZfhaTZaEC4Oc0MSFOkxjsTtwvEy5jqgbMZo4xsQ1ug5q7zIxBeQ3WVR3sU4MG3Fw3JJJ/MZW+V8C9tq0bRPSolOdkZg6gOWA+Y9zJVUxCoWLcz9aLaHkIrGNxmY9azWCk55zfCwiZy3USxZC1zKknqV0EYL2OFfOvVfrD4l/vTK1iLpAgSu1dio2+4uxWWvv328rbEFr1x7GuBqtX7fKa67AgMBAAECggEAQI5BLpF6PdiQoOf3UXHG9lq6GTw9UrUjGbaGel5cErSzeQPrmHPjkpQgcfNW3m/yLgEQsn52gXOkdUB9uXgC6mwh4g39htJFuDdtKhqAFMNX+gENHKzY4Ur34qbOqxramXryhJca2UOo7U6QBJhFw0ltBME9ke8WNUaqu/87xqqgUFGs4lcvbDvh5pIYjfQ6WTssE8qnWS5JfHVIvx+acnQS7RLioaWrqtuT5ShGERrL2hagXzDWpEGVGrHujR86RaopL3Fo63MppK6ZXhYdWeLZMoXK3jae/nU6mTyz8KbpvBh1F5AOgCmMoYyKVSip4MjTzlFM5Ec+YhEQNfXIHQKBgQDkU+nUByFFD2+3GdVHgyoadojjt19o/TnD9EHamTUJjByDBJOvRYrlh0MlHEU5QUEIiZUKbQPZLBHbxIRPR3dyIpGAqA3AqlIx2fGtjYFlOwBQiI6S8iC8HVFlKjnkxJW3aIL7lLJyB9M4sKsxhawZHP7McpT9R90Ec6TMlDPddQKBgQDaIPQf4WNwQFKx3ibMQDM3ofYj0EICmSKpx8qeqfjr8vyHvVc+wmkPTdn0t7/RzVnn/NRqFSHJyV9EdEhiWnesq9ki+OuMUhK8meD459Tq25cCzhJzD4Pc/UAkYjNLKa5lNzCOJKqmFV7M0tRSFKCQBBiTNGzlpzrKbVJSd+flbwKBgAURs9xIOD3fRNyszyZiTBoATbO4i366OIEYOCoRQrMukCd8f4bhpV7JLP1y7jqCL15wJ4Xuu6ojp1XYvBNCg+1dxRs1H/EKFv8SVqJCxP+pWq1vCrNKet2STQ9Q664fiy9iO544Q+nyMIdOrM5RqGt6UFHbrWEeKlMB+kOseqZNAoGAaxDRwvQ2gtqPvI52LLs2aJAu6NVIEU5pHTzbz5VOgUH7ggUF1eBHASQNX3jxxmEtSBlpichllU4qXMdW4C/XngGbyvazZ2TBnaFKM+JXOBAgx1eu5psu9kG4QiORWctTtoqoYpzMxkinB5JUdRV62jWoeli5OuAik0mlpqUERjECgYB/7rS+Arvnzj+/YfJaGPiYnysLI8sdTJ9Tzijns6k41CDyLedPA+5l3cGQv6xxRqdDUq6JjnZQceNdnHEZgDMMes+tslaN5tmODoBKGGts+tppplc1SjMg5AkfDkuINkZhd65BIIHh5JDQJGIsvD1bgnPP3+fJIWofMQ1cDolO+w==";
    const TEST_KEY_N_B64URL: &str = "wozZWDKhEYU2NbL6d1J8QU61BIrGZ6JpBzG1pZDFf_EBKwB75Jy7mq_Bof962x87hkrFtKk4_kDr6-2Znk0NZVD9N9bLBJIw8Sa9gxVQvc4bbtDuVTeTiMt7NQ8H3lmX4Wk2WhAuDnNDEhTpMY7E7cLxMuY6oGzGaOMbENboOau8yMQXkN1lUd7FODBtxcNySSfzGVvlfAvbatG0T0qJTnZGYOoDlgPmPcyVVMQqFi3M_Wi2h5CKxjcZmPWs1gpOec3wsImct1EsWQtcypJ6ldBGC9jhXzr1X6w-Jf70ytYi6QIErtXYqNvuLsVlr799vK2xBa9cexrgarV-3ymuuw";
    const TEST_KEY_E_B64URL: &str = "AQAB";

    fn jwks_json() -> String {
        format!(
            r#"{{"keys":[{{"kty":"RSA","kid":"test-key","alg":"RS256","use":"sig","n":"{TEST_KEY_N_B64URL}","e":"{TEST_KEY_E_B64URL}"}}]}}"#
        )
    }

    /// Signs `{header}.{claims}` with the test RSA key, producing a JWT.
    fn sign_jwt(claims: &serde_json::Value) -> String {
        let header = serde_json::json!({"alg": "RS256", "typ": "JWT", "kid": "test-key"});
        let header_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
        let claims_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let signing_input = format!("{header_b64}.{claims_b64}");
        let der = base64::engine::general_purpose::STANDARD
            .decode(TEST_KEY_PKCS8_B64)
            .unwrap();
        let key_pair = signature::RsaKeyPair::from_pkcs8(&der).unwrap();
        let rng = ring::rand::SystemRandom::new();
        let mut sig = vec![0u8; key_pair.public().modulus_len()];
        key_pair
            .sign(
                &signature::RSA_PKCS1_SHA256,
                &rng,
                signing_input.as_bytes(),
                &mut sig,
            )
            .unwrap();
        format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(sig))
    }

    fn future() -> u64 {
        OidcVerifier::now_secs() + 3600
    }

    fn verifier() -> OidcVerifier {
        let config = OidcConfig::new("https://issuer.example", ["corium"])
            .with_role_claim("roles")
            .with_copy_claims(["email", "tenant"]);
        OidcVerifier::from_jwks_json(config, &jwks_json()).unwrap()
    }

    #[test]
    fn verifies_a_well_formed_token_and_maps_claims() {
        let token = sign_jwt(&serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "alice",
            "aud": "corium",
            "exp": future(),
            "roles": ["reader", "writer"],
            "email": "alice@example.com",
            "tenant": "acme",
        }));
        let principal = verifier().verify(&token).unwrap();
        assert_eq!(principal.subject, "alice");
        assert_eq!(principal.provider, "oidc");
        assert!(principal.has_role("reader"));
        assert!(principal.has_role("writer"));
        assert_eq!(principal.claim("email"), Some("alice@example.com"));
        assert_eq!(principal.claim("tenant"), Some("acme"));
    }

    #[test]
    fn scope_style_string_roles_expand() {
        let verifier = {
            let config =
                OidcConfig::new("https://issuer.example", ["corium"]).with_role_claim("scope");
            OidcVerifier::from_jwks_json(config, &jwks_json()).unwrap()
        };
        let token = sign_jwt(&serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "svc",
            "aud": "corium",
            "exp": future(),
            "scope": "reader writer admin",
        }));
        let principal = verifier.verify(&token).unwrap();
        assert!(principal.has_role("reader"));
        assert!(principal.has_role("admin"));
    }

    #[test]
    fn rejects_a_tampered_payload() {
        let token = sign_jwt(&serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "alice",
            "aud": "corium",
            "exp": future(),
        }));
        // Swap the payload segment for a different (unsigned) one.
        let mut parts: Vec<&str> = token.split('.').collect();
        let forged = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({
                "iss": "https://issuer.example",
                "sub": "mallory",
                "aud": "corium",
                "exp": future(),
            }))
            .unwrap(),
        );
        parts[1] = &forged;
        let tampered = parts.join(".");
        assert!(matches!(
            verifier().verify(&tampered),
            Err(AuthError::Unauthenticated(_))
        ));
    }

    #[test]
    fn rejects_expired_wrong_issuer_and_wrong_audience() {
        let expired = sign_jwt(&serde_json::json!({
            "iss": "https://issuer.example", "sub": "a", "aud": "corium",
            "exp": OidcVerifier::now_secs() - 3600,
        }));
        assert!(verifier().verify(&expired).is_err());

        let wrong_issuer = sign_jwt(&serde_json::json!({
            "iss": "https://evil.example", "sub": "a", "aud": "corium", "exp": future(),
        }));
        assert!(verifier().verify(&wrong_issuer).is_err());

        let wrong_audience = sign_jwt(&serde_json::json!({
            "iss": "https://issuer.example", "sub": "a", "aud": "other", "exp": future(),
        }));
        assert!(verifier().verify(&wrong_audience).is_err());
    }

    #[test]
    fn rejects_unsupported_algorithm() {
        // An HS256 token (no RSA signature) must not verify against the JWKS.
        let header_b64 = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT","kid":"test-key"}"#);
        let claims_b64 = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({
                "iss": "https://issuer.example", "sub": "a", "aud": "corium", "exp": future(),
            }))
            .unwrap(),
        );
        let token = format!("{header_b64}.{claims_b64}.{}", URL_SAFE_NO_PAD.encode(b"x"));
        assert!(verifier().verify(&token).is_err());
    }
}
