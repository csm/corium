//! Authentication and TLS configuration shared by Corium servers and clients.
//!
//! v1 ships bearer-token auth behind a pluggable [`Authenticator`] trait and
//! TLS via tonic/rustls (see `docs/design/protocol.md`).

use std::path::Path;
use std::sync::Arc;

use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// The built-in development bearer token.
///
/// Every `corium` CLI program defaults to this token — servers accept it and
/// clients present it — so a local database works with no auth configuration:
/// start a transactor, create a database, and open a console without passing a
/// single credential flag. Override it with `--token` / `--serve-token` or the
/// `CORIUM_TOKEN` environment variable.
///
/// It is deliberately a fixed, well-known string: it exists to make local
/// experimentation frictionless, **not** to secure anything. Never expose a
/// surface that accepts it to a network anyone else can reach; set a real
/// secret (or an OIDC issuer) there instead.
pub const DEFAULT_DEV_TOKEN: &str = "corium-dev-insecure-token";

/// Pluggable per-request authenticator.
pub trait Authenticator: Send + Sync + 'static {
    /// Accepts or rejects a request given its bearer token (if any).
    fn authenticate(&self, bearer: Option<&str>) -> bool;
}

/// Static-token authenticator; `None` accepts every request.
pub struct StaticToken(Option<String>);

impl StaticToken {
    /// Requires `token` on every request, or accepts all when `None`.
    #[must_use]
    pub const fn new(token: Option<String>) -> Self {
        Self(token)
    }
}

impl Authenticator for StaticToken {
    fn authenticate(&self, bearer: Option<&str>) -> bool {
        match &self.0 {
            None => true,
            Some(expected) => bearer == Some(expected.as_str()),
        }
    }
}

fn bearer(metadata: &MetadataMap) -> Option<&str> {
    metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

/// Server interceptor enforcing an [`Authenticator`].
#[derive(Clone)]
pub struct AuthInterceptor(Arc<dyn Authenticator>);

impl AuthInterceptor {
    /// Wraps an authenticator for use with `InterceptedService`.
    #[must_use]
    pub fn new(authenticator: Arc<dyn Authenticator>) -> Self {
        Self(authenticator)
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        if self.0.authenticate(bearer(request.metadata())) {
            Ok(request)
        } else {
            Err(Status::unauthenticated("invalid or missing bearer token"))
        }
    }
}

/// Client interceptor attaching an optional bearer token to every request.
#[derive(Clone, Default)]
pub struct TokenInterceptor(Option<String>);

impl TokenInterceptor {
    /// Attaches `token` when present.
    #[must_use]
    pub const fn new(token: Option<String>) -> Self {
        Self(token)
    }
}

impl Interceptor for TokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = &self.0 {
            let value = format!("Bearer {token}")
                .parse()
                .map_err(|_| Status::internal("token is not valid metadata"))?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

/// Builds a server TLS config from PEM cert/key files.
///
/// # Errors
/// Returns an error when either file cannot be read.
pub fn server_tls(
    cert_pem: &Path,
    key_pem: &Path,
) -> Result<tonic::transport::ServerTlsConfig, std::io::Error> {
    let cert = std::fs::read(cert_pem)?;
    let key = std::fs::read(key_pem)?;
    Ok(tonic::transport::ServerTlsConfig::new()
        .identity(tonic::transport::Identity::from_pem(cert, key)))
}

/// Builds a client TLS config, optionally trusting a custom CA and
/// overriding the domain name expected on the server certificate.
///
/// # Errors
/// Returns an error when the CA file cannot be read.
pub fn client_tls(
    ca_pem: Option<&Path>,
    domain: Option<&str>,
) -> Result<tonic::transport::ClientTlsConfig, std::io::Error> {
    let mut config = tonic::transport::ClientTlsConfig::new().with_enabled_roots();
    if let Some(path) = ca_pem {
        let ca = std::fs::read(path)?;
        config = config.ca_certificate(tonic::transport::Certificate::from_pem(ca));
    }
    if let Some(domain) = domain {
        config = config.domain_name(domain);
    }
    Ok(config)
}
