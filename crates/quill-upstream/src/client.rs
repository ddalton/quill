use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use reqwest::header::{ACCEPT, AUTHORIZATION, WWW_AUTHENTICATE};
use thiserror::Error;
use tracing::{debug, instrument};

use quill_storage::Digest;

use crate::auth::{AuthError, AuthMode, BearerCache, TokenRequest};

#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("auth: {0}")]
    Auth(#[from] AuthError),
    #[error("upstream returned {status} for {url}")]
    Status { status: u16, url: String },
    #[error("missing Docker-Content-Digest on HEAD response")]
    MissingDigest,
    #[error("invalid digest from upstream: {0}")]
    InvalidDigest(String),
    #[error("invalid url: {0}")]
    InvalidUrl(String),
}

/// Result of a manifest fetch: bytes + content-type + the resolved digest from
/// `Docker-Content-Digest` (always digest-addressed after a tag→digest HEAD).
#[derive(Debug)]
pub struct ManifestResponse {
    pub bytes: Bytes,
    pub content_type: String,
    pub digest: Digest,
}

/// Subset of the upstream surface area Phase 3 needs.
#[async_trait]
pub trait UpstreamClient: Send + Sync {
    async fn resolve_tag(&self, repo: &str, reference: &str) -> Result<Digest, UpstreamError>;
    async fn get_manifest(
        &self,
        repo: &str,
        reference: &str,
    ) -> Result<ManifestResponse, UpstreamError>;
    async fn stream_blob(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<BlobStream, UpstreamError>;
}

/// Erased blob byte stream returned by the upstream.
pub type BlobStream = std::pin::Pin<
    Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static>,
>;

const MANIFEST_ACCEPT: &str = concat!(
    "application/vnd.oci.image.manifest.v1+json,",
    "application/vnd.oci.image.index.v1+json,",
    "application/vnd.docker.distribution.manifest.v2+json,",
    "application/vnd.docker.distribution.manifest.list.v2+json,",
    "application/vnd.docker.distribution.manifest.v1+json"
);

pub struct HttpUpstream {
    pub name: String,
    base_url: url::Url,
    client: reqwest::Client,
    auth_mode: AuthMode,
    bearer_cache: Arc<BearerCache>,
}

impl HttpUpstream {
    pub fn new(
        name: impl Into<String>,
        base_url: &str,
        auth_mode: AuthMode,
    ) -> Result<Arc<Self>, UpstreamError> {
        let base = url::Url::parse(base_url)
            .map_err(|e| UpstreamError::InvalidUrl(format!("{base_url}: {e}")))?;
        // Tuned defaults per PLAN.md §5.10. These are the *single biggest* knob
        // for first-pull throughput on a high-RTT connection.
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Some(Duration::from_secs(300)))
            .pool_max_idle_per_host(8)
            .http2_initial_stream_window_size(Some(16 * 1024 * 1024))
            .http2_initial_connection_window_size(Some(64 * 1024 * 1024))
            .http2_adaptive_window(false)
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_secs(30))
            .user_agent(concat!("quill/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Arc::new(Self {
            name: name.into(),
            base_url: base,
            client,
            auth_mode,
            bearer_cache: Arc::new(BearerCache::new()),
        }))
    }

    fn url_for(&self, path: &str) -> Result<url::Url, UpstreamError> {
        // Strip a leading slash since base_url paths typically don't end in one.
        // Then join. We want absolute URLs with the upstream host.
        let trimmed = path.strip_prefix('/').unwrap_or(path);
        let mut u = self.base_url.clone();
        // Always overwrite path entirely; upstream base_url is just `https://host`.
        u.set_path(trimmed);
        Ok(u)
    }

    /// Send a request, applying basic-auth if configured. On 401 with a Bearer
    /// challenge, fetch a token and retry exactly once.
    async fn send_with_auth(
        &self,
        method: reqwest::Method,
        url: url::Url,
        accept: Option<&str>,
    ) -> Result<reqwest::Response, UpstreamError> {
        let make = |bearer: Option<&str>| {
            let mut req = self.client.request(method.clone(), url.clone());
            if let Some(a) = accept {
                req = req.header(ACCEPT, a);
            }
            if let Some(t) = bearer {
                req = req.header(AUTHORIZATION, format!("Bearer {t}"));
            } else if let AuthMode::Basic { username, password } = &self.auth_mode {
                req = req.basic_auth(username, Some(password));
            }
            req
        };

        let resp = make(None).send().await?;
        if resp.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(resp);
        }

        let challenge = resp
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .and_then(TokenRequest::parse)
            .ok_or(UpstreamError::Status {
                status: 401,
                url: url.to_string(),
            })?;
        debug!(realm = %challenge.realm, "fetching upstream bearer token");
        let bearer = self
            .bearer_cache
            .fetch(&self.client, &challenge, &self.auth_mode)
            .await?;
        let resp = make(Some(&bearer)).send().await?;
        Ok(resp)
    }
}

#[async_trait]
impl UpstreamClient for HttpUpstream {
    #[instrument(skip(self), fields(upstream = %self.name, repo, reference))]
    async fn resolve_tag(&self, repo: &str, reference: &str) -> Result<Digest, UpstreamError> {
        let url = self.url_for(&format!("/v2/{repo}/manifests/{reference}"))?;
        let resp = self
            .send_with_auth(reqwest::Method::HEAD, url.clone(), Some(MANIFEST_ACCEPT))
            .await?;
        if !resp.status().is_success() {
            return Err(UpstreamError::Status {
                status: resp.status().as_u16(),
                url: url.to_string(),
            });
        }
        let h = resp
            .headers()
            .get("docker-content-digest")
            .and_then(|v| v.to_str().ok())
            .ok_or(UpstreamError::MissingDigest)?;
        Digest::parse(h).map_err(|e| UpstreamError::InvalidDigest(format!("{h}: {e}")))
    }

    #[instrument(skip(self), fields(upstream = %self.name, repo, reference))]
    async fn get_manifest(
        &self,
        repo: &str,
        reference: &str,
    ) -> Result<ManifestResponse, UpstreamError> {
        let url = self.url_for(&format!("/v2/{repo}/manifests/{reference}"))?;
        let resp = self
            .send_with_auth(reqwest::Method::GET, url.clone(), Some(MANIFEST_ACCEPT))
            .await?;
        if !resp.status().is_success() {
            return Err(UpstreamError::Status {
                status: resp.status().as_u16(),
                url: url.to_string(),
            });
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/vnd.oci.image.manifest.v1+json")
            .to_string();
        let digest_hdr = resp
            .headers()
            .get("docker-content-digest")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let bytes = resp.bytes().await?;
        let digest = match digest_hdr {
            Some(h) => Digest::parse(&h).map_err(|e| UpstreamError::InvalidDigest(e.to_string()))?,
            None => digest_from_bytes(&bytes),
        };
        Ok(ManifestResponse {
            bytes,
            content_type,
            digest,
        })
    }

    #[instrument(skip(self), fields(upstream = %self.name, repo, digest = %digest))]
    async fn stream_blob(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<BlobStream, UpstreamError> {
        let url = self.url_for(&format!("/v2/{repo}/blobs/{digest}"))?;
        let resp = self
            .send_with_auth(reqwest::Method::GET, url.clone(), None)
            .await?;
        if !resp.status().is_success() {
            return Err(UpstreamError::Status {
                status: resp.status().as_u16(),
                url: url.to_string(),
            });
        }
        Ok(Box::pin(resp.bytes_stream()))
    }
}

fn digest_from_bytes(bytes: &[u8]) -> Digest {
    use sha2::{Digest as _, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    let hex = hex::encode(h.finalize());
    Digest::parse(&format!("sha256:{hex}")).expect("self-computed sha256 is always valid")
}
