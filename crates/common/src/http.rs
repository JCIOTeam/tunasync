//! HTTP client construction.
//!
//! Go tunasync uses a custom `http.Client` per component, with optional CA
//! certificate pinning for self-signed manager/worker deployments and (in
//! later configs) client certs for mTLS. We mirror that here on top of
//! `reqwest`'s rustls backend.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::{Certificate, Client, Identity};

/// Builder for tunasync HTTP clients.
///
/// Accumulates optional TLS material before producing a [`reqwest::Client`].
/// Reasonable defaults: 30-second timeout, identifying `User-Agent` header.
#[derive(Default)]
pub struct HttpClientBuilder {
    ca_cert: Option<Vec<u8>>,
    identity_pem: Option<Vec<u8>>,
    timeout: Option<Duration>,
}

impl HttpClientBuilder {
    /// Start a fresh builder with library defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin a custom CA certificate (PEM-encoded) to verify the peer.
    ///
    /// Equivalent to Go tunasync's `Files.CACert` config field.
    pub fn ca_cert_pem_from_path(mut self, path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("read CA cert from {}", path.display()))?;
        self.ca_cert = Some(bytes);
        Ok(self)
    }

    /// Provide a client certificate + key (concatenated PEM) for mTLS.
    pub fn client_identity_pem_from_path(mut self, path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("read client identity from {}", path.display()))?;
        self.identity_pem = Some(bytes);
        Ok(self)
    }

    /// Override the default request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Build the `reqwest::Client`.
    pub fn build(self) -> Result<Client> {
        let mut builder = Client::builder()
            .timeout(self.timeout.unwrap_or(Duration::from_secs(30)))
            .user_agent(concat!("tunasync-rs/", env!("CARGO_PKG_VERSION")));

        if let Some(pem) = self.ca_cert {
            let cert = Certificate::from_pem(&pem).context("parse CA cert as PEM")?;
            builder = builder.add_root_certificate(cert);
        }

        if let Some(pem) = self.identity_pem {
            let identity = Identity::from_pem(&pem).context("parse client identity as PEM")?;
            builder = builder.identity(identity);
        }

        builder.build().context("build HTTP client")
    }
}
