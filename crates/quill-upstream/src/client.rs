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
    async fn list_tags(&self, repo: &str) -> Result<Vec<String>, UpstreamError>;
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

    #[instrument(skip(self), fields(upstream = %self.name, repo))]
    async fn list_tags(&self, repo: &str) -> Result<Vec<String>, UpstreamError> {
        let url = self.url_for(&format!("/v2/{repo}/tags/list"))?;
        let resp = self
            .send_with_auth(reqwest::Method::GET, url.clone(), None)
            .await?;
        if !resp.status().is_success() {
            return Err(UpstreamError::Status {
                status: resp.status().as_u16(),
                url: url.to_string(),
            });
        }
        let body: serde_json::Value = resp.json().await.map_err(UpstreamError::Transport)?;
        let tags = body
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        Ok(tags)
    }
}

/// Whether a primary-upstream error should trigger a fallback attempt.
/// Falls back on not-found (404) and transport/unreachable errors; real 5xx and
/// auth errors from the primary surface as errors instead of masking a problem.
fn should_fallback(e: &UpstreamError) -> bool {
    matches!(
        e,
        UpstreamError::Status { status: 404, .. } | UpstreamError::Transport(_)
    )
}

/// Wraps a primary upstream with a fallback that is tried when the primary
/// returns not-found or is unreachable. The same (already-rewritten) repo path
/// is used for both, so the fallback must lay the repo out identically.
pub struct FallbackUpstream {
    primary: Arc<dyn UpstreamClient>,
    fallback: Arc<dyn UpstreamClient>,
}

impl FallbackUpstream {
    pub fn new(primary: Arc<dyn UpstreamClient>, fallback: Arc<dyn UpstreamClient>) -> Arc<Self> {
        Arc::new(Self { primary, fallback })
    }
}

#[async_trait]
impl UpstreamClient for FallbackUpstream {
    async fn resolve_tag(&self, repo: &str, reference: &str) -> Result<Digest, UpstreamError> {
        match self.primary.resolve_tag(repo, reference).await {
            Err(e) if should_fallback(&e) => {
                debug!(error = %e, "primary resolve_tag failed, trying fallback");
                self.fallback.resolve_tag(repo, reference).await
            }
            other => other,
        }
    }

    async fn get_manifest(
        &self,
        repo: &str,
        reference: &str,
    ) -> Result<ManifestResponse, UpstreamError> {
        match self.primary.get_manifest(repo, reference).await {
            Err(e) if should_fallback(&e) => {
                debug!(error = %e, "primary get_manifest failed, trying fallback");
                self.fallback.get_manifest(repo, reference).await
            }
            other => other,
        }
    }

    async fn stream_blob(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<BlobStream, UpstreamError> {
        match self.primary.stream_blob(repo, digest).await {
            Err(e) if should_fallback(&e) => {
                debug!(error = %e, "primary stream_blob failed, trying fallback");
                self.fallback.stream_blob(repo, digest).await
            }
            other => other,
        }
    }

    async fn list_tags(&self, repo: &str) -> Result<Vec<String>, UpstreamError> {
        // Tags are unioned across primary + fallback (not primary-first): a
        // version may exist on only one registry (e.g. release builds on the
        // fallback, dev builds on the primary), and the resolver needs to see
        // all of them. A later manifest/blob pull for a specific tag still falls
        // back on 404. If the primary hard-fails (non-fallback error), surface it.
        let primary = self.primary.list_tags(repo).await;
        let fallback = self.fallback.list_tags(repo).await;
        match (primary, fallback) {
            (Ok(a), Ok(b)) => {
                let mut set: std::collections::BTreeSet<String> = a.into_iter().collect();
                set.extend(b);
                Ok(set.into_iter().collect())
            }
            (Ok(a), Err(_)) => Ok(a),
            (Err(e), Ok(b)) if should_fallback(&e) => Ok(b),
            (Err(e), _) => Err(e),
        }
    }
}

fn digest_from_bytes(bytes: &[u8]) -> Digest {
    use sha2::{Digest as _, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    let hex = hex::encode(h.finalize());
    Digest::parse(&format!("sha256:{hex}")).expect("self-computed sha256 is always valid")
}

#[cfg(test)]
mod fallback_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn should_fallback_matches_notfound_only() {
        assert!(should_fallback(&UpstreamError::Status {
            status: 404,
            url: "x".into()
        }));
        assert!(!should_fallback(&UpstreamError::Status {
            status: 500,
            url: "x".into()
        }));
        assert!(!should_fallback(&UpstreamError::Status {
            status: 401,
            url: "x".into()
        }));
        assert!(!should_fallback(&UpstreamError::MissingDigest));
    }

    enum Outcome {
        Ok,
        Status(u16),
    }

    struct MockClient {
        tag: String,
        outcome: Outcome,
        calls: Arc<AtomicUsize>,
    }

    impl MockClient {
        fn new(tag: &str, outcome: Outcome) -> (Arc<Self>, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            (
                Arc::new(Self {
                    tag: tag.to_string(),
                    outcome,
                    calls: calls.clone(),
                }),
                calls,
            )
        }
    }

    #[async_trait]
    impl UpstreamClient for MockClient {
        async fn resolve_tag(&self, _repo: &str, _reference: &str) -> Result<Digest, UpstreamError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                Outcome::Ok => Digest::parse(&format!("sha256:{}", "a".repeat(64)))
                    .map_err(|e| UpstreamError::InvalidDigest(e.to_string())),
                Outcome::Status(s) => Err(UpstreamError::Status {
                    status: s,
                    url: "mock".into(),
                }),
            }
        }
        async fn get_manifest(
            &self,
            _repo: &str,
            _reference: &str,
        ) -> Result<ManifestResponse, UpstreamError> {
            unreachable!()
        }
        async fn stream_blob(
            &self,
            _repo: &str,
            _digest: &Digest,
        ) -> Result<BlobStream, UpstreamError> {
            unreachable!()
        }
        async fn list_tags(&self, _repo: &str) -> Result<Vec<String>, UpstreamError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                Outcome::Ok => Ok(vec![self.tag.clone()]),
                Outcome::Status(s) => Err(UpstreamError::Status {
                    status: s,
                    url: "mock".into(),
                }),
            }
        }
    }

    #[tokio::test]
    async fn list_tags_falls_back_on_primary_404() {
        let (primary, pc) = MockClient::new("primary", Outcome::Status(404));
        let (fallback, fc) = MockClient::new("fallback", Outcome::Ok);
        let fb = FallbackUpstream::new(primary, fallback);
        let tags = fb.list_tags("repo").await.unwrap();
        assert_eq!(tags, vec!["fallback".to_string()]);
        assert_eq!(pc.load(Ordering::SeqCst), 1);
        assert_eq!(fc.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn list_tags_unions_primary_and_fallback() {
        // A tag present only on the fallback (e.g. a pinned release build) must
        // still be discoverable even though the primary repo also exists.
        let (primary, pc) = MockClient::new("primary", Outcome::Ok);
        let (fallback, fc) = MockClient::new("fallback", Outcome::Ok);
        let fb = FallbackUpstream::new(primary, fallback);
        let tags = fb.list_tags("repo").await.unwrap();
        // BTreeSet-ordered union of both mock tags.
        assert_eq!(tags, vec!["fallback".to_string(), "primary".to_string()]);
        assert_eq!(pc.load(Ordering::SeqCst), 1);
        assert_eq!(fc.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resolve_tag_does_not_fall_back_when_primary_succeeds() {
        let (primary, pc) = MockClient::new("primary", Outcome::Ok);
        let (fallback, fc) = MockClient::new("fallback", Outcome::Ok);
        let fb = FallbackUpstream::new(primary, fallback);
        let _ = fb.resolve_tag("repo", "tag").await.unwrap();
        assert_eq!(pc.load(Ordering::SeqCst), 1);
        assert_eq!(fc.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn does_not_fall_back_on_primary_5xx() {
        let (primary, pc) = MockClient::new("primary", Outcome::Status(500));
        let (fallback, fc) = MockClient::new("fallback", Outcome::Ok);
        let fb = FallbackUpstream::new(primary, fallback);
        let err = fb.resolve_tag("repo", "tag").await.unwrap_err();
        assert!(matches!(err, UpstreamError::Status { status: 500, .. }));
        assert_eq!(pc.load(Ordering::SeqCst), 1);
        assert_eq!(fc.load(Ordering::SeqCst), 0);
    }
}
