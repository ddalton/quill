use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use bytes::Bytes;
use tokio_util::io::ReaderStream;
use tracing::{debug, instrument, warn};

use quill_pullthrough::{run_producer, ProducerRole, PullThroughBody, PullThroughEntry};
use quill_storage::{cache::BlobMeta, Digest};
use quill_upstream::UpstreamEntry;

use crate::error::RegistryError;
use crate::state::RegistryState;

pub fn router(state: RegistryState) -> Router {
    Router::new()
        .route("/v2/", get(v2_check))
        .route("/v2", get(v2_check))
        .route("/v2/*rest", any(dispatch))
        .with_state(Arc::new(state))
}

async fn v2_check() -> impl IntoResponse {
    let mut h = HeaderMap::new();
    h.insert(
        "Docker-Distribution-Api-Version",
        HeaderValue::from_static("registry/2.0"),
    );
    (StatusCode::OK, h)
}

enum Action<'a> {
    Blob(&'a str),
    Manifest(&'a str),
    TagsList,
    Unknown,
}

fn split(rest: &str) -> Option<(&str, Action<'_>)> {
    if let Some((repo, digest)) = rest.rsplit_once("/blobs/") {
        if !digest.is_empty() && !digest.contains('/') {
            return Some((repo, Action::Blob(digest)));
        }
    }
    if let Some((repo, reference)) = rest.rsplit_once("/manifests/") {
        if !reference.is_empty() && !reference.contains('/') {
            return Some((repo, Action::Manifest(reference)));
        }
    }
    if let Some(repo) = rest.strip_suffix("/tags/list") {
        return Some((repo, Action::TagsList));
    }
    Some(("", Action::Unknown))
}

#[instrument(skip(state), fields(method = %method, rest = %rest))]
async fn dispatch(
    State(state): State<Arc<RegistryState>>,
    method: Method,
    Path(rest): Path<String>,
) -> Response {
    let (repo, action) = match split(&rest) {
        Some(p) => p,
        None => return RegistryError::name_unknown(rest.clone()).into_response(),
    };
    if repo.is_empty() {
        return RegistryError::name_unknown(rest).into_response();
    }
    match (method, action) {
        (Method::GET, Action::Blob(d)) => get_blob(&state, repo, d, true).await,
        (Method::HEAD, Action::Blob(d)) => get_blob(&state, repo, d, false).await,
        (Method::GET, Action::Manifest(r)) => get_manifest(&state, repo, r, true).await,
        (Method::HEAD, Action::Manifest(r)) => get_manifest(&state, repo, r, false).await,
        (Method::GET, Action::TagsList) => list_tags(&state, repo).await,
        _ => RegistryError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            crate::RegistryErrorCode::Unsupported,
            "method or endpoint not implemented in this phase",
        )
        .into_response(),
    }
}

// ---------- blobs ----------

async fn get_blob(state: &RegistryState, repo: &str, digest: &str, with_body: bool) -> Response {
    let parsed = match Digest::parse(digest) {
        Ok(d) => d,
        Err(_) => return RegistryError::digest_invalid(digest).into_response(),
    };

    // Local hit?
    match state.storage.blob_meta(repo, &parsed).await {
        Ok(Some(meta)) => return serve_local_blob(meta, parsed, with_body, state).await,
        Ok(None) => {}
        Err(e) => return internal_err(e),
    }

    // Local miss — fall through to upstream pull-through if configured.
    let entry_arc = match state.upstreams.route(repo) {
        Some(e) => e.clone(),
        None => return RegistryError::blob_unknown(digest).into_response(),
    };

    serve_blob_via_pullthrough(state, entry_arc.as_ref(), repo, parsed, with_body).await
}

async fn serve_local_blob(
    meta: BlobMeta,
    parsed: Digest,
    with_body: bool,
    state: &RegistryState,
) -> Response {
    debug!(size = meta.size, "blob hit");
    if !with_body {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_LENGTH, HeaderValue::from(meta.size));
        h.insert(
            "Docker-Content-Digest",
            HeaderValue::from_str(&parsed.to_string()).unwrap(),
        );
        return (StatusCode::OK, h).into_response();
    }
    let file = match state.storage.open_blob(&meta).await {
        Ok(f) => f,
        Err(e) => return internal_err(e),
    };
    let stream = ReaderStream::new(file);
    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, meta.size)
        .header("Docker-Content-Digest", parsed.to_string())
        .body(Body::from_stream(stream))
        .expect("static response");
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    resp
}

async fn serve_blob_via_pullthrough(
    state: &RegistryState,
    upstream: &UpstreamEntry,
    repo: &str,
    digest: Digest,
    with_body: bool,
) -> Response {
    // `repo_prefix` is purely a routing selector; the full repo name is sent
    // upstream verbatim. (Path translation is a Phase 4+ knob if anyone needs
    // `mycorp/foo` on Quill → `account-id/foo` on ECR.)
    let upstream_repo = repo;

    // Single-flight: are we producer or subscriber?
    let layout = state.storage.layout().clone();
    let final_path = layout.blob_path(repo, &digest);
    let tempfile_path = layout
        .uploads_dir(repo)
        .join(format!("pt-{}", digest.hex()));

    let (entry, role) = state.pullthrough.get_or_insert(digest.clone(), || {
        Arc::new(PullThroughEntry::new(digest.clone(), tempfile_path.clone()))
    });

    if role == ProducerRole::Producer {
        // Spawn the producer task; it owns the lifecycle of the in-flight entry.
        let upstream_client = upstream.client.clone();
        let entry_for_producer = entry.clone();
        let table = state.pullthrough.clone();
        let storage = state.storage.clone();
        let final_path_for_task = final_path.clone();
        let dig_for_task = digest.clone();
        let repo_owned = repo.to_string();
        let upstream_repo_owned = upstream_repo.to_string();
        let upstream_name = upstream.config.name.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let stream_result = upstream_client
                .stream_blob(&upstream_repo_owned, &dig_for_task)
                .await;
            match stream_result {
                Err(e) => {
                    warn!(upstream = %upstream_name, error = %e, "upstream blob fetch failed");
                    entry_for_producer.finish(quill_pullthrough::ProducerOutcome::Failed(
                        quill_pullthrough::PullThroughError::Upstream(e.to_string()),
                    ));
                    table.finish(&dig_for_task);
                }
                Ok(stream) => {
                    let res = run_producer(
                        entry_for_producer.clone(),
                        final_path_for_task.clone(),
                        stream,
                    )
                    .await;
                    table.finish(&dig_for_task);
                    if let Ok(()) = res {
                        // Populate the metadata cache for the freshly-cached blob.
                        let meta = BlobMeta {
                            path: final_path_for_task,
                            size: entry_for_producer.high_water_mark(),
                            cached_at: Instant::now(),
                        };
                        storage
                            .meta_cache()
                            .insert(&repo_owned, &dig_for_task, meta);
                        debug!(
                            upstream = %upstream_name,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "pull-through complete"
                        );
                    }
                }
            }
        });
    }

    if !with_body {
        // For HEAD we wait for the producer to finish so we know the final size.
        let mut completion = entry.completion();
        if completion.borrow().is_none() {
            let _ = completion.changed().await;
        }
        match entry.outcome() {
            Some(quill_pullthrough::ProducerOutcome::Success { final_size }) => {
                let mut h = HeaderMap::new();
                h.insert(header::CONTENT_LENGTH, HeaderValue::from(final_size));
                h.insert(
                    "Docker-Content-Digest",
                    HeaderValue::from_str(&digest.to_string()).unwrap(),
                );
                (StatusCode::OK, h).into_response()
            }
            Some(quill_pullthrough::ProducerOutcome::Failed(e)) => RegistryError::new(
                StatusCode::BAD_GATEWAY,
                crate::RegistryErrorCode::Unavailable,
                e.to_string(),
            )
            .into_response(),
            None => RegistryError::new(
                StatusCode::BAD_GATEWAY,
                crate::RegistryErrorCode::Unavailable,
                "producer did not complete",
            )
            .into_response(),
        }
    } else {
        let body = PullThroughBody::new(entry.clone());
        let mut resp = Response::builder()
            .status(StatusCode::OK)
            .header("Docker-Content-Digest", digest.to_string())
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::new(body))
            .expect("static response");
        // Content-length unknown ahead of time during streaming; clients accept
        // chunked / EOF-delimited bodies. Phase 4 polish: pre-HEAD the upstream
        // to learn the size, then set Content-Length here.
        resp.headers_mut()
            .insert("Transfer-Encoding", HeaderValue::from_static("chunked"));
        resp
    }
}

// ---------- manifests ----------

async fn get_manifest(
    state: &RegistryState,
    repo: &str,
    reference: &str,
    with_body: bool,
) -> Response {
    // 1. Locally-pushed tag wins (PLAN.md §5.6).
    if Digest::parse(reference).is_err() {
        if let Some(local) = state.local_tags.get(repo, reference) {
            if let Ok(d) = Digest::parse(&local.digest) {
                return serve_local_manifest(state, repo, &d, reference, with_body).await;
            }
        }
    }

    // 2. Reference is a digest? Try local CAS, then fall through to upstream.
    if let Ok(digest) = Digest::parse(reference) {
        match state.storage.blob_meta(repo, &digest).await {
            Ok(Some(_)) => {
                return serve_local_manifest(state, repo, &digest, reference, with_body).await
            }
            Ok(None) => {}
            Err(e) => return internal_err(e),
        }
        // Upstream digest fetch
        if let Some(upstream) = state.upstreams.route(repo) {
            return fetch_and_serve_upstream_manifest(
                state, upstream, repo, reference, with_body,
            )
            .await;
        }
        return RegistryError::manifest_unknown(reference).into_response();
    }

    // 3. Tag-addressed and not local. Check upstream tag cache, then upstream.
    if let Some(digest) = state.upstream_tag_cache.get(repo, reference) {
        match state.storage.blob_meta(repo, &digest).await {
            Ok(Some(_)) => {
                return serve_local_manifest(state, repo, &digest, reference, with_body).await;
            }
            Ok(None) => {}
            Err(e) => return internal_err(e),
        }
    }
    if let Some(upstream) = state.upstreams.route(repo) {
        return fetch_and_serve_upstream_manifest(state, upstream, repo, reference, with_body)
            .await;
    }
    RegistryError::manifest_unknown(reference).into_response()
}

async fn serve_local_manifest(
    state: &RegistryState,
    repo: &str,
    digest: &Digest,
    reference: &str,
    with_body: bool,
) -> Response {
    let meta = match state.storage.blob_meta(repo, digest).await {
        Ok(Some(m)) => m,
        Ok(None) => return RegistryError::manifest_unknown(reference).into_response(),
        Err(e) => return internal_err(e),
    };
    let bytes = match state.storage.read_blob_to_bytes(&meta).await {
        Ok(b) => b,
        Err(e) => return internal_err(e),
    };
    let content_type = sniff_manifest_content_type(&bytes);
    build_manifest_response(bytes, content_type, digest, with_body)
}

async fn fetch_and_serve_upstream_manifest(
    state: &RegistryState,
    upstream: &UpstreamEntry,
    repo: &str,
    reference: &str,
    with_body: bool,
) -> Response {
    // `repo_prefix` is purely a routing selector; the full repo name is sent
    // upstream verbatim. (Path translation is a Phase 4+ knob if anyone needs
    // `mycorp/foo` on Quill → `account-id/foo` on ECR.)
    let upstream_repo = repo;
    let resp = match upstream
        .client
        .get_manifest(&upstream_repo, reference)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "upstream manifest fetch failed");
            return RegistryError::new(
                StatusCode::BAD_GATEWAY,
                crate::RegistryErrorCode::Unavailable,
                e.to_string(),
            )
            .into_response();
        }
    };

    // Persist by digest into local CAS.
    if let Err(e) = state
        .storage
        .put_blob_buffered(repo, &resp.digest, resp.bytes.clone())
        .await
    {
        warn!(error = %e, "manifest persist failed");
    }
    // If the request was tag-addressed, cache the tag→digest resolution.
    if Digest::parse(reference).is_err() {
        state
            .upstream_tag_cache
            .insert(repo, reference, resp.digest.clone());
    }

    build_manifest_response(resp.bytes, resp.content_type, &resp.digest, with_body)
}

fn build_manifest_response(
    bytes: Bytes,
    content_type: String,
    digest: &Digest,
    with_body: bool,
) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, bytes.len())
        .header("Docker-Content-Digest", digest.to_string())
        .header(header::CONTENT_TYPE, content_type);
    if !with_body {
        return builder.body(Body::empty()).expect("static");
    }
    builder = builder;
    builder.body(Body::from(bytes)).expect("static")
}

fn sniff_manifest_content_type(bytes: &[u8]) -> String {
    // Best-effort: read top-level mediaType field if present, else default to OCI.
    let parsed: Option<serde_json::Value> = serde_json::from_slice(bytes).ok();
    if let Some(mt) = parsed
        .as_ref()
        .and_then(|v| v.get("mediaType"))
        .and_then(|v| v.as_str())
    {
        return mt.to_string();
    }
    "application/vnd.oci.image.manifest.v1+json".to_string()
}

// ---------- tags ----------

async fn list_tags(state: &RegistryState, repo: &str) -> Response {
    // Local tags only for now. Phase 4 polish merges with upstream tags/list.
    let tags: Vec<String> = state
        .local_tags
        .list_for_repo(repo)
        .into_iter()
        .map(|(t, _)| t)
        .collect();
    let body = serde_json::json!({
        "name": repo,
        "tags": tags,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

// ---------- helpers ----------

fn internal_err(e: impl std::fmt::Display) -> Response {
    RegistryError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        crate::RegistryErrorCode::Unknown,
        e.to_string(),
    )
    .into_response()
}
