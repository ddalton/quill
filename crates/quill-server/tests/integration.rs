//! End-to-end integration tests for the Quill registry.
//!
//! These tests build a `RegistryState` in-memory (no real network or TLS)
//! and drive it through `tower::ServiceExt::oneshot` against the real axum
//! `Router`. Mock upstream is a `MockUpstream` that implements
//! `quill_upstream::UpstreamClient` from byte-for-byte fixtures.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use futures::stream;
use http_body_util::BodyExt;
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;
use tower::ServiceExt;

use quill_pullthrough::PullThroughTable;
use quill_registry::{router, RegistryState, UpstreamTagCache};
use quill_storage::{
    CasLayout, Digest, GarbageCollector, LocalStorage, LocalTagsStore, UploadStore,
};
use quill_upstream::{
    BlobStream, ManifestResponse, UpstreamClient, UpstreamEntry, UpstreamError, UpstreamRouter,
};

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct Fixture {
    _tmp: TempDir,
    state: RegistryState,
    mock_calls: Arc<AtomicU32>,
    mock: Arc<MockUpstream>,
}

impl Fixture {
    /// Build a fixture with no upstream configured (local-only mode).
    fn new_local_only() -> Self {
        Self::build(None)
    }

    /// Build a fixture with a single mock upstream that intercepts a configured
    /// repo prefix.
    fn new_with_mock_upstream(prefix: &str) -> Self {
        let mock = Arc::new(MockUpstream::new());
        Self::build(Some((prefix.to_string(), mock)))
    }

    fn build(upstream: Option<(String, Arc<MockUpstream>)>) -> Self {
        let tmp = TempDir::new().unwrap();
        let layout = CasLayout::new(tmp.path());
        let storage = Arc::new(LocalStorage::new(layout.clone(), Duration::from_secs(60)));
        let local_tags = Arc::new(LocalTagsStore::new(layout.clone()));
        let uploads = Arc::new(UploadStore::new(layout.clone()));
        let pullthrough = Arc::new(PullThroughTable::new());
        let upstream_tag_cache = Arc::new(UpstreamTagCache::new(Duration::from_millis(150), None));

        let (upstreams, mock_calls, mock) = match upstream {
            Some((prefix, mock)) => {
                let mock_calls = mock.calls.clone();
                let entry = Arc::new(UpstreamEntry {
                    config: quill_config::Upstream {
                        name: "mock".into(),
                        url: "https://mock.invalid".into(),
                        kind: quill_config::UpstreamKind::Generic,
                        repo_prefix: prefix,
                        auth: None,
                    },
                    client: mock.clone() as Arc<dyn UpstreamClient>,
                });
                let router = UpstreamRouter::with_entries(vec![entry]);
                (Arc::new(router), mock_calls, Some(mock))
            }
            None => (
                Arc::new(UpstreamRouter::empty()),
                Arc::new(AtomicU32::new(0)),
                None,
            ),
        };

        let state = RegistryState::new(
            storage,
            local_tags,
            uploads,
            pullthrough,
            upstreams,
            upstream_tag_cache,
        );
        Self {
            _tmp: tmp,
            state,
            mock_calls,
            mock: mock.unwrap_or_else(|| Arc::new(MockUpstream::new())),
        }
    }

    fn router(&self) -> axum::Router {
        router(self.state.clone())
    }

    async fn req(
        &self,
        method: Method,
        path: &str,
        headers: Vec<(&str, &str)>,
        body: Bytes,
    ) -> (StatusCode, axum::http::HeaderMap, Bytes) {
        let mut builder = Request::builder().method(method).uri(path);
        for (k, v) in headers {
            builder = builder.header(k, v);
        }
        let req = builder.body(Body::from(body)).unwrap();
        let resp = self.router().oneshot(req).await.unwrap();
        let (parts, body) = resp.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        (parts.status, parts.headers, bytes)
    }

    async fn push_blob(&self, repo: &str, body: &[u8]) -> Digest {
        let digest = sha256_of(body);

        // POST init
        let (status, headers, _) = self
            .req(
                Method::POST,
                &format!("/v2/{repo}/blobs/uploads/"),
                vec![],
                Bytes::new(),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "blob upload init");
        let location = headers
            .get("location")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // PATCH chunk
        let (status, _, _) = self
            .req(Method::PATCH, &location, vec![], Bytes::copy_from_slice(body))
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "blob upload patch");

        // PUT finalize
        let (status, _, _) = self
            .req(
                Method::PUT,
                &format!("{location}?digest={digest}"),
                vec![],
                Bytes::new(),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "blob upload put");
        digest
    }

    async fn push_manifest(
        &self,
        repo: &str,
        reference: &str,
        manifest: &serde_json::Value,
    ) -> Digest {
        let body = serde_json::to_vec(manifest).unwrap();
        let digest = sha256_of(&body);
        let (status, headers, _resp_body) = self
            .req(
                Method::PUT,
                &format!("/v2/{repo}/manifests/{reference}"),
                vec![("content-type", "application/vnd.oci.image.manifest.v1+json")],
                Bytes::from(body),
            )
            .await;
        if status != StatusCode::CREATED {
            panic!(
                "push_manifest expected 201, got {} headers={:?}",
                status, headers
            );
        }
        digest
    }
}

// `UpstreamRouter::with_entries` doesn't exist publicly; we add it via the mock test path.
// Instead we use `UpstreamRouter::build` against a config — but that requires the full URL
// roundtrip. To keep the mock simple, we add an `empty()` ctor to UpstreamRouter and
// stitch entries in via a helper below.

mod upstream_router_ext {
    use super::*;
    pub trait With {
        fn with_entries(entries: Vec<Arc<UpstreamEntry>>) -> UpstreamRouter;
    }
    impl With for UpstreamRouter {
        fn with_entries(entries: Vec<Arc<UpstreamEntry>>) -> UpstreamRouter {
            UpstreamRouter::__with_entries_for_test(entries)
        }
    }
}
use upstream_router_ext::With as _;

// ---------------------------------------------------------------------------
// MockUpstream
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockUpstreamFixtures {
    /// (repo, tag) -> manifest_digest
    tags: dashmap::DashMap<(String, String), Digest>,
    /// (repo, digest) -> (manifest_bytes, content_type)
    manifests: dashmap::DashMap<(String, Digest), (Bytes, String)>,
    /// (repo, digest) -> blob bytes
    blobs: dashmap::DashMap<(String, Digest), Bytes>,
}

struct MockUpstream {
    calls: Arc<AtomicU32>,
    fixtures: MockUpstreamFixtures,
}

impl MockUpstream {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicU32::new(0)),
            fixtures: MockUpstreamFixtures::default(),
        }
    }

    fn add_blob(&self, repo: &str, body: Bytes) -> Digest {
        let digest = sha256_of(&body);
        self.fixtures
            .blobs
            .insert((repo.to_string(), digest.clone()), body);
        digest
    }

    fn add_manifest(
        &self,
        repo: &str,
        manifest: &serde_json::Value,
        content_type: &str,
    ) -> Digest {
        let body = Bytes::from(serde_json::to_vec(manifest).unwrap());
        let digest = sha256_of(&body);
        self.fixtures.manifests.insert(
            (repo.to_string(), digest.clone()),
            (body, content_type.to_string()),
        );
        digest
    }

    fn tag(&self, repo: &str, tag: &str, digest: &Digest) {
        self.fixtures
            .tags
            .insert((repo.to_string(), tag.to_string()), digest.clone());
    }

    fn retag(&self, repo: &str, tag: &str, digest: &Digest) {
        self.tag(repo, tag, digest);
    }
}

#[async_trait]
impl UpstreamClient for MockUpstream {
    async fn resolve_tag(&self, repo: &str, reference: &str) -> Result<Digest, UpstreamError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Ok(d) = Digest::parse(reference) {
            return Ok(d);
        }
        self.fixtures
            .tags
            .get(&(repo.to_string(), reference.to_string()))
            .map(|e| e.clone())
            .ok_or(UpstreamError::Status {
                status: 404,
                url: format!("/v2/{repo}/manifests/{reference}"),
            })
    }

    async fn get_manifest(
        &self,
        repo: &str,
        reference: &str,
    ) -> Result<ManifestResponse, UpstreamError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let digest = self.resolve_tag(repo, reference).await?;
        let entry = self
            .fixtures
            .manifests
            .get(&(repo.to_string(), digest.clone()))
            .ok_or(UpstreamError::Status {
                status: 404,
                url: format!("/v2/{repo}/manifests/{reference}"),
            })?;
        let (bytes, content_type) = entry.clone();
        Ok(ManifestResponse {
            bytes,
            content_type,
            digest,
        })
    }

    async fn stream_blob(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<BlobStream, UpstreamError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let body = self
            .fixtures
            .blobs
            .get(&(repo.to_string(), digest.clone()))
            .map(|e| e.clone())
            .ok_or(UpstreamError::Status {
                status: 404,
                url: format!("/v2/{repo}/blobs/{digest}"),
            })?;
        let stream = stream::iter(vec![Ok::<_, reqwest::Error>(body)]);
        Ok(Box::pin(stream))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sha256_of(body: &[u8]) -> Digest {
    let mut h = Sha256::new();
    h.update(body);
    Digest::parse(&format!("sha256:{}", hex::encode(h.finalize()))).unwrap()
}

/// Build a tiny image-spec-shaped manifest referencing the supplied digests.
fn build_manifest(config: &Digest, layers: &[(Digest, usize)]) -> serde_json::Value {
    serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 0,
            "digest": config.to_string()
        },
        "layers": layers.iter().map(|(d, sz)| serde_json::json!({
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "size": sz,
            "digest": d.to_string()
        })).collect::<Vec<_>>()
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn v2_check_returns_distribution_api_version() {
    let fx = Fixture::new_local_only();
    let (status, headers, _) = fx
        .req(Method::GET, "/v2/", vec![], Bytes::new())
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("docker-distribution-api-version").unwrap(),
        "registry/2.0"
    );
}

#[tokio::test]
async fn invalid_digest_returns_400() {
    let fx = Fixture::new_local_only();
    let (status, _, body) = fx
        .req(
            Method::GET,
            "/v2/some/repo/blobs/sha256:notvalid",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["errors"][0]["code"], "DIGESTINVALID");
}

#[tokio::test]
async fn missing_blob_with_no_upstream_returns_404() {
    let fx = Fixture::new_local_only();
    let (status, _, body) = fx
        .req(
            Method::GET,
            "/v2/some/repo/blobs/sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["errors"][0]["code"], "BLOBUNKNOWN");
}

#[tokio::test]
async fn push_round_trip_serves_locally() {
    let fx = Fixture::new_local_only();
    let layer_bytes = b"layer-data-payload";
    let config_bytes = br#"{"architecture":"amd64","os":"linux"}"#;

    let layer_d = fx.push_blob("mycorp/myimage", layer_bytes).await;
    let config_d = fx.push_blob("mycorp/myimage", config_bytes).await;
    let manifest = build_manifest(&config_d, &[(layer_d.clone(), layer_bytes.len())]);
    let manifest_d = fx.push_manifest("mycorp/myimage", "v1", &manifest).await;

    // Pull manifest by tag
    let (status, _, body) = fx
        .req(
            Method::GET,
            "/v2/mycorp/myimage/manifests/v1",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(sha256_of(&body), manifest_d);

    // Pull layer by digest
    let (status, _, body) = fx
        .req(
            Method::GET,
            &format!("/v2/mycorp/myimage/blobs/{layer_d}"),
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), layer_bytes);

    // tags/list shows v1
    let (status, _, body) = fx
        .req(
            Method::GET,
            "/v2/mycorp/myimage/tags/list",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["tags"], serde_json::json!(["v1"]));
}

#[tokio::test]
async fn push_manifest_referencing_missing_blob_rejected() {
    let fx = Fixture::new_local_only();
    let bogus_layer = sha256_of(b"never-pushed");
    let bogus_config = sha256_of(b"also-never-pushed");
    let manifest = build_manifest(&bogus_config, &[(bogus_layer, 17)]);
    let body = Bytes::from(serde_json::to_vec(&manifest).unwrap());
    let (status, _, body) = fx
        .req(
            Method::PUT,
            "/v2/mycorp/myimage/manifests/v1",
            vec![("content-type", "application/vnd.oci.image.manifest.v1+json")],
            body,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["errors"][0]["code"], "MANIFESTINVALID");
}

#[tokio::test]
async fn delete_manifest_by_tag_removes_local_tag() {
    let fx = Fixture::new_local_only();
    let layer_d = fx.push_blob("x/y", b"layer").await;
    let config_d = fx.push_blob("x/y", b"{}").await;
    let manifest = build_manifest(&config_d, &[(layer_d, 5)]);
    let _ = fx.push_manifest("x/y", "v1", &manifest).await;

    // Delete the tag.
    let (status, _, _) = fx
        .req(Method::DELETE, "/v2/x/y/manifests/v1", vec![], Bytes::new())
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // Manifest by tag is now unknown.
    let (status, _, _) = fx
        .req(Method::GET, "/v2/x/y/manifests/v1", vec![], Bytes::new())
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn pullthrough_cache_miss_then_warm() {
    let fx = Fixture::new_with_mock_upstream("library/");
    let mock = fx.mock.clone();
    let layer_bytes = Bytes::from(&b"layer-bytes-from-upstream"[..]);
    let config_bytes = Bytes::from(&b"{}"[..]);
    let layer_d = mock.add_blob("library/redis", layer_bytes.clone());
    let _config_d = mock.add_blob("library/redis", config_bytes.clone());

    // Cold pull of the blob: should hit upstream.
    let (status, _, body) = fx
        .req(
            Method::GET,
            &format!("/v2/library/redis/blobs/{layer_d}"),
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, layer_bytes);
    let cold_calls = fx.mock_calls.load(Ordering::SeqCst);
    assert!(cold_calls >= 1, "expected upstream contact on cold pull");

    // Wait for the producer's atomic rename to finish (it spawns; the response
    // body completion races the rename). Poll the metadata cache briefly.
    for _ in 0..50 {
        if fx
            .state
            .storage
            .blob_meta("library/redis", &layer_d)
            .await
            .ok()
            .flatten()
            .is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Warm pull: must not contact upstream again.
    let (status, _, body) = fx
        .req(
            Method::GET,
            &format!("/v2/library/redis/blobs/{layer_d}"),
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, layer_bytes);
    let warm_calls = fx.mock_calls.load(Ordering::SeqCst);
    assert_eq!(
        warm_calls, cold_calls,
        "warm pull must not touch upstream (cold={cold_calls}, warm={warm_calls})"
    );
}

#[tokio::test]
async fn locally_pushed_tag_takes_precedence_over_upstream() {
    let fx = Fixture::new_with_mock_upstream("library/");
    let mock = fx.mock.clone();

    // Upstream has its own version of library/redis:patched.
    let upstream_layer = mock.add_blob("library/redis", Bytes::from(&b"upstream-layer"[..]));
    let upstream_config = mock.add_blob("library/redis", Bytes::from(&b"{}"[..]));
    let upstream_manifest = build_manifest(&upstream_config, &[(upstream_layer, 14)]);
    let upstream_manifest_d =
        mock.add_manifest("library/redis", &upstream_manifest, "application/vnd.oci.image.manifest.v1+json");
    mock.tag("library/redis", "patched", &upstream_manifest_d);

    // We push our own version locally to library/redis:patched.
    let local_layer_d = fx.push_blob("library/redis", b"LOCAL_layer").await;
    let local_config_d = fx.push_blob("library/redis", b"{}").await;
    let local_manifest = build_manifest(&local_config_d, &[(local_layer_d, 11)]);
    let local_manifest_d = fx
        .push_manifest("library/redis", "patched", &local_manifest)
        .await;

    // Pull library/redis:patched. Must serve LOCAL with zero upstream contacts.
    let calls_before = fx.mock_calls.load(Ordering::SeqCst);
    let (status, _, body) = fx
        .req(
            Method::GET,
            "/v2/library/redis/manifests/patched",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let returned = sha256_of(&body);
    assert_eq!(returned, local_manifest_d);
    assert_ne!(returned, upstream_manifest_d);
    let calls_after = fx.mock_calls.load(Ordering::SeqCst);
    assert_eq!(
        calls_after, calls_before,
        "locally-pushed tag should not contact upstream"
    );
}

#[tokio::test]
async fn tags_list_merges_local_and_upstream_cached() {
    let fx = Fixture::new_with_mock_upstream("library/");
    let mock = fx.mock.clone();

    // Upstream has redis:7-alpine.
    let up_layer = mock.add_blob("library/redis", Bytes::from(&b"L"[..]));
    let up_config = mock.add_blob("library/redis", Bytes::from(&b"{}"[..]));
    let up_manifest = build_manifest(&up_config, &[(up_layer, 1)]);
    let up_manifest_d =
        mock.add_manifest("library/redis", &up_manifest, "application/vnd.oci.image.manifest.v1+json");
    mock.tag("library/redis", "7-alpine", &up_manifest_d);

    // Pull through to populate the upstream tag cache.
    let (status, _, _) = fx
        .req(
            Method::GET,
            "/v2/library/redis/manifests/7-alpine",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    // Push a local-only tag.
    let l_layer = fx.push_blob("library/redis", b"local").await;
    let l_config = fx.push_blob("library/redis", b"{}").await;
    let l_manifest = build_manifest(&l_config, &[(l_layer, 5)]);
    let _ = fx.push_manifest("library/redis", "patched", &l_manifest).await;

    let (status, _, body) = fx
        .req(
            Method::GET,
            "/v2/library/redis/tags/list",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let tags: Vec<&str> = v["tags"].as_array().unwrap().iter().map(|t| t.as_str().unwrap()).collect();
    assert!(tags.contains(&"7-alpine"), "tags={tags:?}");
    assert!(tags.contains(&"patched"), "tags={tags:?}");
}

#[tokio::test]
async fn stale_tag_revalidation_serves_local_when_upstream_unchanged() {
    let fx = Fixture::new_with_mock_upstream("library/");
    let mock = fx.mock.clone();

    let up_layer = mock.add_blob("library/redis", Bytes::from(&b"L"[..]));
    let up_config = mock.add_blob("library/redis", Bytes::from(&b"{}"[..]));
    let up_manifest = build_manifest(&up_config, &[(up_layer, 1)]);
    let up_manifest_d =
        mock.add_manifest("library/redis", &up_manifest, "application/vnd.oci.image.manifest.v1+json");
    mock.tag("library/redis", "7-alpine", &up_manifest_d);

    // First pull populates the cache (Fresh).
    let (status, _, _) = fx
        .req(
            Method::GET,
            "/v2/library/redis/manifests/7-alpine",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let calls_after_first = fx.mock_calls.load(Ordering::SeqCst);

    // Wait for TTL to elapse (fixture set to 150ms).
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Second pull is stale → HEAD upstream → digest unchanged → serve local.
    let (status, _, body) = fx
        .req(
            Method::GET,
            "/v2/library/redis/manifests/7-alpine",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(sha256_of(&body), up_manifest_d);
    let calls_after_stale = fx.mock_calls.load(Ordering::SeqCst);
    // Stale-revalidation HEAD costs exactly one upstream call.
    assert_eq!(calls_after_stale - calls_after_first, 1);
}

#[tokio::test]
async fn stale_tag_revalidation_refetches_when_upstream_changed() {
    let fx = Fixture::new_with_mock_upstream("library/");
    let mock = fx.mock.clone();

    // v1 of the manifest in upstream
    let l1 = mock.add_blob("library/x", Bytes::from(&b"v1"[..]));
    let c1 = mock.add_blob("library/x", Bytes::from(&b"{}"[..]));
    let m1 = build_manifest(&c1, &[(l1, 2)]);
    let m1_d = mock.add_manifest("library/x", &m1, "application/vnd.oci.image.manifest.v1+json");
    mock.tag("library/x", "stable", &m1_d);

    // Cache it locally
    let (status, _, _) = fx
        .req(
            Method::GET,
            "/v2/library/x/manifests/stable",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    // Upstream rotates the tag to a new manifest.
    let l2 = mock.add_blob("library/x", Bytes::from(&b"v2-new"[..]));
    let c2 = mock.add_blob("library/x", Bytes::from(&b"{}"[..]));
    let m2 = build_manifest(&c2, &[(l2, 6)]);
    let m2_d = mock.add_manifest("library/x", &m2, "application/vnd.oci.image.manifest.v1+json");
    mock.retag("library/x", "stable", &m2_d);

    tokio::time::sleep(Duration::from_millis(200)).await;

    let (status, _, body) = fx
        .req(
            Method::GET,
            "/v2/library/x/manifests/stable",
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let returned = sha256_of(&body);
    assert_eq!(returned, m2_d, "expected new digest after upstream tag rotation");
}

#[tokio::test]
async fn conditional_get_returns_304_on_match() {
    let fx = Fixture::new_local_only();
    let layer_d = fx.push_blob("a/b", b"layer").await;
    let config_d = fx.push_blob("a/b", b"{}").await;
    let manifest = build_manifest(&config_d, &[(layer_d, 5)]);
    let manifest_d = fx.push_manifest("a/b", "v1", &manifest).await;

    let (status, _, _) = fx
        .req(
            Method::GET,
            "/v2/a/b/manifests/v1",
            vec![("if-none-match", &manifest_d.to_string())],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn conditional_get_returns_200_on_mismatch() {
    let fx = Fixture::new_local_only();
    let layer_d = fx.push_blob("a/b", b"layer").await;
    let config_d = fx.push_blob("a/b", b"{}").await;
    let manifest = build_manifest(&config_d, &[(layer_d, 5)]);
    let _ = fx.push_manifest("a/b", "v1", &manifest).await;

    let (status, _, _) = fx
        .req(
            Method::GET,
            "/v2/a/b/manifests/v1",
            vec![(
                "if-none-match",
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            )],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn gc_removes_orphans_keeps_locally_pushed() {
    let fx = Fixture::new_local_only();
    // Reachable
    let layer_d = fx.push_blob("a/b", b"reachable-layer").await;
    let config_d = fx.push_blob("a/b", b"{}").await;
    let manifest = build_manifest(&config_d, &[(layer_d.clone(), 15)]);
    let manifest_d = fx.push_manifest("a/b", "v1", &manifest).await;
    // Orphan: push a blob, never reference it from a manifest.
    let orphan_d = fx.push_blob("a/b", b"orphan-bytes").await;

    let layout = fx.state.storage.layout().clone();
    let gc = GarbageCollector::new(layout.clone());
    let report = gc.run(std::collections::HashSet::new(), false).await.unwrap();
    assert!(report.deleted >= 1);

    // Reachable blobs still present.
    assert!(layout.blob_path("a/b", &manifest_d).exists());
    assert!(layout.blob_path("a/b", &config_d).exists());
    assert!(layout.blob_path("a/b", &layer_d).exists());
    // Orphan is gone.
    assert!(!layout.blob_path("a/b", &orphan_d).exists());
}

#[tokio::test]
async fn upload_session_404_after_finalize() {
    let fx = Fixture::new_local_only();
    let body = b"hello";
    let digest = sha256_of(body);

    let (status, headers, _) = fx
        .req(Method::POST, "/v2/x/y/blobs/uploads/", vec![], Bytes::new())
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let location = headers.get("location").unwrap().to_str().unwrap().to_string();

    let (status, _, _) = fx
        .req(Method::PATCH, &location, vec![], Bytes::copy_from_slice(body))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, _, _) = fx
        .req(
            Method::PUT,
            &format!("{location}?digest={digest}"),
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // Re-PATCH on the now-finalized session: 404.
    let (status, _, _) = fx
        .req(Method::PATCH, &location, vec![], Bytes::copy_from_slice(b"x"))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn upload_finalize_with_wrong_digest_returns_400() {
    let fx = Fixture::new_local_only();
    let body = b"hello";
    let wrong = sha256_of(b"different");

    let (status, headers, _) = fx
        .req(Method::POST, "/v2/x/y/blobs/uploads/", vec![], Bytes::new())
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let location = headers.get("location").unwrap().to_str().unwrap().to_string();

    let (status, _, _) = fx
        .req(Method::PATCH, &location, vec![], Bytes::copy_from_slice(body))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, _, body) = fx
        .req(
            Method::PUT,
            &format!("{location}?digest={wrong}"),
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["errors"][0]["code"], "DIGESTINVALID");
}

#[tokio::test]
async fn manifest_put_with_mismatched_digest_reference_returns_400() {
    let fx = Fixture::new_local_only();
    let layer_d = fx.push_blob("a/b", b"layer").await;
    let config_d = fx.push_blob("a/b", b"{}").await;
    let manifest = build_manifest(&config_d, &[(layer_d, 5)]);
    let body = serde_json::to_vec(&manifest).unwrap();

    // Push by a digest that doesn't match the body.
    let bogus = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    let (status, _, _) = fx
        .req(
            Method::PUT,
            &format!("/v2/a/b/manifests/{bogus}"),
            vec![("content-type", "application/vnd.oci.image.manifest.v1+json")],
            Bytes::from(body),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn warm_pull_reads_local_only_no_upstream() {
    let fx = Fixture::new_with_mock_upstream("library/");
    let mock = fx.mock.clone();
    let layer_bytes = Bytes::from(&b"L"[..]);
    let layer_d = mock.add_blob("library/x", layer_bytes.clone());

    // First (cold) pull — counts upstream contacts.
    let (status, _, _) = fx
        .req(
            Method::GET,
            &format!("/v2/library/x/blobs/{layer_d}"),
            vec![],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    // Wait for atomic-rename to finish so the warm path hits the local CAS.
    for _ in 0..50 {
        if fx
            .state
            .storage
            .blob_meta("library/x", &layer_d)
            .await
            .ok()
            .flatten()
            .is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let n_before = fx.mock_calls.load(Ordering::SeqCst);
    for _ in 0..5 {
        let (status, _, body) = fx
            .req(
                Method::GET,
                &format!("/v2/library/x/blobs/{layer_d}"),
                vec![],
                Bytes::new(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, layer_bytes);
    }
    let n_after = fx.mock_calls.load(Ordering::SeqCst);
    assert_eq!(n_before, n_after, "warm pulls must not contact upstream");
}
