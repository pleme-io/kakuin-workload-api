//! kakuin-workload-api — Rust client for the SPIFFE Workload API.
//!
//! Connects to the spire-agent's gRPC endpoint over a unix domain
//! socket (default `/run/spiffe.io/socket`), fetches the calling
//! workload's X.509 SVID + trust bundle, and exposes a subscription
//! stream that emits a fresh `X509Svid` every time the agent rotates
//! the cert.
//!
//! Sprint M1.2 of `theory/MESH-EXECUTION-PLAN.md`.
//!
//! # Example
//!
//! ```no_run
//! use kakuin_workload_api::WorkloadApiClient;
//!
//! # async fn ex() -> Result<(), Box<dyn std::error::Error>> {
//! // Connects to /run/spiffe.io/socket (or $SPIFFE_ENDPOINT_SOCKET).
//! let mut client = WorkloadApiClient::default().await?;
//! let svid = client.fetch_x509_svid().await?;
//! assert!(svid.spiffe_id.starts_with("spiffe://"));
//! # Ok(())
//! # }
//! ```

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, metadata::MetadataValue};

pub mod proto {
    #![allow(clippy::all, clippy::pedantic)]
    // tonic-build normalizes the proto package name (`SpiffeWorkloadAPI`)
    // to snake_case for the generated file path.
    tonic::include_proto!("spiffe_workload_api");
}

/// Default workload-API socket. Matches the path the upstream
/// `spiffe-csi-driver` mounts in pod containers.
pub const DEFAULT_SOCKET_PATH: &str = "/run/spiffe.io/socket";

/// Env var the SPIFFE spec defines for the socket path. Standard
/// across all Workload API clients.
pub const SPIFFE_ENDPOINT_SOCKET_ENV: &str = "SPIFFE_ENDPOINT_SOCKET";

/// Required gRPC metadata header — without it the agent rejects the
/// stream as "unauthenticated client".
const SECURITY_HEADER: (&str, &str) = ("workload.spiffe.io", "true");

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tonic transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("grpc status: {0}")]
    Status(#[from] tonic::Status),
    #[error("agent returned no SVIDs (workload not registered?)")]
    NoSvids,
    #[error("invalid socket path: {0}")]
    InvalidSocket(PathBuf),
}

pub type Result<T> = std::result::Result<T, Error>;

/// One X.509 SVID — the leaf cert + chain + private key + the CA
/// bundle of the issuing trust domain.
#[derive(Debug, Clone)]
pub struct X509Svid {
    /// e.g. `spiffe://pleme.io/ns/openclaw/sa/openclaw-stack-cartorio`.
    pub spiffe_id: String,
    /// X.509 cert chain (DER, leaf first).
    pub cert_chain_der: Vec<u8>,
    /// PKCS#8 DER private key.
    pub private_key_der: Vec<u8>,
    /// The trust-domain's CA bundle (concatenated DER).
    pub bundle_der: Vec<u8>,
    /// Optional hint emitted by the agent (e.g. `"primary"`).
    pub hint: String,
}

impl X509Svid {
    /// Trust domain extracted from `spiffe_id` (the URI authority).
    /// Returns `None` if the id is malformed.
    #[must_use]
    pub fn trust_domain(&self) -> Option<&str> {
        self.spiffe_id
            .strip_prefix("spiffe://")?
            .split('/')
            .next()
    }

    /// Path component of the SPIFFE-ID (everything after the trust
    /// domain). Returns the leading slash. Useful for SPIFFE-aware
    /// authorization checks against a templated suffix.
    #[must_use]
    pub fn path(&self) -> Option<&str> {
        let after_scheme = self.spiffe_id.strip_prefix("spiffe://")?;
        let slash = after_scheme.find('/')?;
        Some(&after_scheme[slash..])
    }
}

/// Rotation-aware update — emitted on every workload-API stream
/// message. `svids` is in agent-priority order; the first entry is
/// usually the "primary" identity. `federated_bundles` is keyed by
/// trust-domain name.
#[derive(Debug, Clone)]
pub struct X509SvidUpdate {
    pub svids: Vec<X509Svid>,
    pub federated_bundles: HashMap<String, Vec<u8>>,
}

/// SPIFFE Workload API client.
pub struct WorkloadApiClient {
    inner: proto::spiffe_workload_api_client::SpiffeWorkloadApiClient<Channel>,
    socket_path: PathBuf,
}

impl WorkloadApiClient {
    /// Build a client that connects to `$SPIFFE_ENDPOINT_SOCKET` if
    /// set, else `/run/spiffe.io/socket`.
    pub async fn default() -> Result<Self> {
        let path = std::env::var_os(SPIFFE_ENDPOINT_SOCKET_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SOCKET_PATH));
        Self::connect(&path).await
    }

    /// Build a client connected to a specific unix socket.
    pub async fn connect(socket_path: &Path) -> Result<Self> {
        if !socket_path.exists() {
            return Err(Error::InvalidSocket(socket_path.to_path_buf()));
        }

        let socket_path_buf: Arc<PathBuf> = Arc::new(socket_path.to_path_buf());

        // tonic's default Endpoint requires a URI but we're going to
        // override the connector. The URI's authority just has to be
        // a non-empty placeholder.
        let endpoint = Endpoint::try_from("http://[::]:50051")?
            .connect_timeout(std::time::Duration::from_secs(5));

        let socket_path_for_connector = socket_path_buf.clone();
        let channel = endpoint
            .connect_with_connector(tower::service_fn(move |_uri| {
                let path = socket_path_for_connector.clone();
                async move {
                    let stream = UnixStream::connect(&*path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await?;

        let inner = proto::spiffe_workload_api_client::SpiffeWorkloadApiClient::new(channel);
        Ok(Self {
            inner,
            socket_path: socket_path_buf.as_ref().clone(),
        })
    }

    /// Path to the workload-API socket this client is bound to.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Fetch one X509 SVID — blocking on the first stream item.
    /// Convenience for callers that just need an identity at startup;
    /// long-running services should use `subscribe_x509_svid` so the
    /// SVID rotates automatically.
    pub async fn fetch_x509_svid(&mut self) -> Result<X509Svid> {
        let mut req = Request::new(proto::X509svidRequest {});
        attach_security_header(&mut req);
        let mut stream = self.inner.fetch_x509svid(req).await?.into_inner();
        let resp = stream
            .message()
            .await?
            .ok_or(Error::NoSvids)?;
        let svid = resp.svids.into_iter().next().ok_or(Error::NoSvids)?;
        Ok(decode_svid(svid, &resp.federated_bundles))
    }

    /// Subscribe to X.509 SVID updates. The agent re-emits whenever
    /// the SVID rotates; callers can hot-reload mTLS contexts off the
    /// new bytes.
    pub async fn subscribe_x509_svid(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<X509SvidUpdate>> + Send>>> {
        let mut req = Request::new(proto::X509svidRequest {});
        attach_security_header(&mut req);
        let stream = self.inner.fetch_x509svid(req).await?.into_inner();
        let mapped = async_stream::try_stream! {
            let mut stream = stream;
            while let Some(resp) = stream.message().await? {
                let svids = resp
                    .svids
                    .into_iter()
                    .map(|s| decode_svid(s, &resp.federated_bundles))
                    .collect();
                yield X509SvidUpdate {
                    svids,
                    federated_bundles: resp.federated_bundles,
                };
            }
        };
        Ok(Box::pin(mapped))
    }

    /// Fetch the trust bundle for every trust domain the agent knows
    /// about (indexed by trust-domain name → DER-encoded bundle).
    /// Useful when the workload only verifies peer SVIDs and doesn't
    /// need its own identity.
    pub async fn fetch_trust_bundles(&mut self) -> Result<HashMap<String, Vec<u8>>> {
        let mut req = Request::new(proto::X509BundlesRequest {});
        attach_security_header(&mut req);
        let mut stream = self.inner.fetch_x509_bundles(req).await?.into_inner();
        let resp = stream
            .message()
            .await?
            .ok_or(Error::NoSvids)?;
        Ok(resp.bundles)
    }
}

fn decode_svid(s: proto::X509svid, _federated: &HashMap<String, Vec<u8>>) -> X509Svid {
    X509Svid {
        spiffe_id: s.spiffe_id,
        cert_chain_der: s.x509_svid,
        private_key_der: s.x509_svid_key,
        bundle_der: s.bundle,
        hint: s.hint,
    }
}

fn attach_security_header<T>(req: &mut Request<T>) {
    let value: MetadataValue<_> = SECURITY_HEADER
        .1
        .parse()
        .expect("static header value parses");
    req.metadata_mut().insert(SECURITY_HEADER.0, value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn x509svid_trust_domain_extraction() {
        let svid = X509Svid {
            spiffe_id: "spiffe://pleme.io/ns/openclaw/sa/cartorio".into(),
            cert_chain_der: vec![],
            private_key_der: vec![],
            bundle_der: vec![],
            hint: String::new(),
        };
        assert_eq!(svid.trust_domain(), Some("pleme.io"));
        assert_eq!(svid.path(), Some("/ns/openclaw/sa/cartorio"));
    }

    #[test]
    fn x509svid_malformed_id_is_none() {
        let svid = X509Svid {
            spiffe_id: "not-a-spiffe-id".into(),
            cert_chain_der: vec![],
            private_key_der: vec![],
            bundle_der: vec![],
            hint: String::new(),
        };
        assert_eq!(svid.trust_domain(), None);
        assert_eq!(svid.path(), None);
    }
}
