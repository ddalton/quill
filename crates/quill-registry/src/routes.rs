use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt;
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
    /// `POST /v2/<repo>/blobs/uploads/` — open a new upload session.
    UploadInit,
    /// `PATCH|PUT /v2/<repo>/blobs/uploads/<session>` — append or finalize.
    UploadSession(&'a str),
    Unknown,
}

fn split(rest: &str) -> Option<(&str, Action<'_>)> {
    // Push paths must be matched before pull paths because they're more specific
    // (`/blobs/uploads/...` would otherwise be parsed as a blob digest).
    if let Some(repo) = rest.strip_suffix("/blobs/uploads/") {
        return Some((repo, Action::UploadInit));
    }
    if let Some(repo) = rest.strip_suffix("/blobs/uploads") {
        // Some clients omit the trailing slash on POST.
        return Some((repo, Action::UploadInit));
    }
    if let Some((repo, session)) = rest.rsplit_once("/blobs/uploads/") {
        if !session.is_empty() && !session.contains('/') {
            return Some((repo, Action::UploadSession(session)));
        }
    }
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

#[instrument(skip(state, query, body), fields(method = %method, rest = %rest))]
async fn dispatch(
    State(state): State<Arc<RegistryState>>,
    method: Method,
    Path(rest): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if rest == "_catalog" {
        return catalog(&state).await;
    }

    let (repo, action) = match split(&rest) {
        Some(p) => p,
        None => return RegistryError::name_unknown(rest.clone()).into_response(),
    };
    if repo.is_empty() {
        return RegistryError::name_unknown(rest).into_response();
    }
    let repo_owned = repo.to_string();
    match (method, action) {
        (Method::GET, Action::Blob(d)) => get_blob(&state, &repo_owned, d, true).await,
        (Method::HEAD, Action::Blob(d)) => get_blob(&state, &repo_owned, d, false).await,
        (Method::DELETE, Action::Blob(d)) => delete_blob(&state, &repo_owned, d).await,
        (Method::GET, Action::Manifest(r)) => {
            let resp = get_manifest(&state, &repo_owned, r, true).await;
            apply_if_none_match(&headers, resp)
        }
        (Method::HEAD, Action::Manifest(r)) => {
            let resp = get_manifest(&state, &repo_owned, r, false).await;
            apply_if_none_match(&headers, resp)
        }
        (Method::PUT, Action::Manifest(r)) => {
            put_manifest(&state, &repo_owned, r, &headers, body).await
        }
        (Method::DELETE, Action::Manifest(r)) => delete_manifest(&state, &repo_owned, r).await,
        (Method::GET, Action::TagsList) => list_tags(&state, &repo_owned).await,
        (Method::POST, Action::UploadInit) => upload_init(&state, &repo_owned).await,
        (Method::PATCH, Action::UploadSession(s)) => {
            upload_patch(&state, &repo_owned, s, body).await
        }
        (Method::PUT, Action::UploadSession(s)) => {
            upload_put(&state, &repo_owned, s, &query, body).await
        }
        _ => RegistryError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            crate::RegistryErrorCode::Unsupported,
            "method or endpoint not implemented",
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
            Some(quill_pullthrough::ProducerOutcome::Failed(_)) => {
                RegistryError::blob_unknown(&digest.to_string()).into_response()
            }
            None => {
                RegistryError::blob_unknown(&digest.to_string()).into_response()
            }
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

    // 3. Tag-addressed and not local. Check upstream tag cache + freshness.
    use crate::state::TagCacheState;
    let cache_state = state.upstream_tag_cache.lookup(repo, reference);
    let upstream = state.upstreams.route(repo);

    match (&cache_state, upstream) {
        // Fresh cache hit + manifest still in CAS — serve immediately, no upstream.
        (TagCacheState::Fresh(digest), _) => {
            if let Ok(Some(_)) = state.storage.blob_meta(repo, digest).await {
                return serve_local_manifest(state, repo, digest, reference, with_body).await;
            }
            // Fresh entry but blob was GC'd; fall through to a fresh fetch.
        }
        // Stale cache hit + upstream available — try cheap HEAD to revalidate.
        (TagCacheState::Stale(digest), Some(upstream)) => {
            match upstream.client.resolve_tag(repo, reference).await {
                Ok(new_digest) if &new_digest == digest => {
                    // Unchanged upstream — refresh TTL and serve local.
                    state.upstream_tag_cache.touch(repo, reference);
                    if let Ok(Some(_)) = state.storage.blob_meta(repo, digest).await {
                        return serve_local_manifest(
                            state, repo, digest, reference, with_body,
                        )
                        .await;
                    }
                    // Lost the local copy; fall through to full fetch.
                }
                Ok(_new_digest) => {
                    // Tag moved upstream — fall through to full upstream fetch.
                }
                Err(e) => {
                    // Upstream unreachable — serve stale-but-cached.
                    warn!(error = %e, "upstream HEAD failed for stale tag; serving cached");
                    if let Ok(Some(_)) = state.storage.blob_meta(repo, digest).await {
                        return serve_local_manifest(
                            state, repo, digest, reference, with_body,
                        )
                        .await;
                    }
                }
            }
        }
        // Stale + no upstream configured: serve cached (best-effort).
        (TagCacheState::Stale(digest), None) => {
            if let Ok(Some(_)) = state.storage.blob_meta(repo, digest).await {
                return serve_local_manifest(state, repo, digest, reference, with_body).await;
            }
        }
        (TagCacheState::Miss, _) => {}
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
            debug!(error = %e, "upstream manifest fetch failed, treating as not found");
            return RegistryError::manifest_unknown(reference).into_response();
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
    use std::collections::BTreeSet;
    let mut tags: BTreeSet<String> = BTreeSet::new();
    for (t, _) in state.local_tags.list_for_repo(repo) {
        tags.insert(t);
    }
    for t in state.upstream_tag_cache.list_for_repo(repo) {
        tags.insert(t);
    }
    if tags.is_empty() {
        if let Some(upstream) = state.upstreams.route(repo) {
            match upstream.client.list_tags(repo).await {
                Ok(upstream_tags) => {
                    for t in upstream_tags {
                        tags.insert(t);
                    }
                }
                Err(e) => {
                    warn!(error = %e, repo, "upstream tag list failed");
                }
            }
        }
    }
    let body = serde_json::json!({
        "name": repo,
        "tags": tags.into_iter().collect::<Vec<_>>(),
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

// ---------- catalog ----------

async fn catalog(state: &RegistryState) -> Response {
    let repos = match state.storage.layout().list_repos() {
        Ok(r) => r,
        Err(e) => return internal_err(e),
    };
    let repos: Vec<String> = repos
        .into_iter()
        .filter(|r| {
            !state.local_tags.list_for_repo(r).is_empty()
                || !state.upstream_tag_cache.list_for_repo(r).is_empty()
        })
        .collect();
    let body = serde_json::json!({ "repositories": repos });
    (StatusCode::OK, axum::Json(body)).into_response()
}

// ---------- push: blob uploads ----------

async fn upload_init(state: &RegistryState, repo: &str) -> Response {
    if let Err(e) = state.storage.ensure_repo(repo).await {
        return internal_err(e);
    }
    let session = match state.uploads.create_session(repo).await {
        Ok(s) => s,
        Err(e) => return internal_err(e),
    };
    let location = format!("/v2/{repo}/blobs/uploads/{session}");
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(header::LOCATION, location)
        .header(header::RANGE, "0-0")
        .header("Docker-Upload-UUID", session.clone())
        .body(Body::empty())
        .expect("static")
}

async fn upload_patch(state: &RegistryState, repo: &str, session: &str, body: Body) -> Response {
    let bytes = match collect_body(body, 5 * 1024 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return e,
    };
    let total = match state.uploads.append(repo, session, &bytes).await {
        Ok(n) => n,
        Err(quill_storage::UploadError::NotFound(_)) => {
            return RegistryError::new(
                StatusCode::NOT_FOUND,
                crate::RegistryErrorCode::BlobUploadUnknown,
                "upload session not found",
            )
            .into_response();
        }
        Err(e) => return internal_err(e),
    };
    let range = if total == 0 {
        "0-0".to_string()
    } else {
        format!("0-{}", total - 1)
    };
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(
            header::LOCATION,
            format!("/v2/{repo}/blobs/uploads/{session}"),
        )
        .header(header::RANGE, range)
        .header("Docker-Upload-UUID", session.to_string())
        .body(Body::empty())
        .expect("static")
}

async fn upload_put(
    state: &RegistryState,
    repo: &str,
    session: &str,
    query: &HashMap<String, String>,
    body: Body,
) -> Response {
    let digest_str = match query.get("digest") {
        Some(d) => d.as_str(),
        None => {
            return RegistryError::new(
                StatusCode::BAD_REQUEST,
                crate::RegistryErrorCode::DigestInvalid,
                "missing ?digest= query parameter",
            )
            .into_response();
        }
    };
    let digest = match Digest::parse(digest_str) {
        Ok(d) => d,
        Err(_) => return RegistryError::digest_invalid(digest_str).into_response(),
    };
    let bytes = match collect_body(body, 5 * 1024 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return e,
    };
    match state
        .uploads
        .finalize(
            repo,
            session,
            if bytes.is_empty() { None } else { Some(&bytes) },
            &digest,
        )
        .await
    {
        Ok((_path, _total)) => Response::builder()
            .status(StatusCode::CREATED)
            .header(header::LOCATION, format!("/v2/{repo}/blobs/{digest}"))
            .header("Docker-Content-Digest", digest.to_string())
            .body(Body::empty())
            .expect("static"),
        Err(quill_storage::UploadError::DigestMismatch { expected, got }) => RegistryError::new(
            StatusCode::BAD_REQUEST,
            crate::RegistryErrorCode::DigestInvalid,
            format!("digest mismatch: expected {expected}, got {got}"),
        )
        .into_response(),
        Err(quill_storage::UploadError::NotFound(_)) => RegistryError::new(
            StatusCode::NOT_FOUND,
            crate::RegistryErrorCode::BlobUploadUnknown,
            "upload session not found",
        )
        .into_response(),
        Err(e) => internal_err(e),
    }
}

// ---------- push: manifests + deletes ----------

async fn put_manifest(
    state: &RegistryState,
    repo: &str,
    reference: &str,
    headers: &HeaderMap,
    body: Body,
) -> Response {
    if let Err(e) = state.storage.ensure_repo(repo).await {
        return internal_err(e);
    }
    let bytes = match collect_body(body, 32 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return e,
    };

    // Compute digest of the bytes.
    use sha2::{Digest as _, Sha256};
    let mut h = Sha256::new();
    h.update(&bytes);
    let hex = hex::encode(h.finalize());
    let digest = match Digest::parse(&format!("sha256:{hex}")) {
        Ok(d) => d,
        Err(e) => return internal_err(e),
    };

    // If the reference is a digest, it must match the body's hash.
    if let Ok(ref_digest) = Digest::parse(reference) {
        if ref_digest != digest {
            return RegistryError::new(
                StatusCode::BAD_REQUEST,
                crate::RegistryErrorCode::ManifestInvalid,
                format!("manifest body digest {digest} does not match reference {ref_digest}"),
            )
            .into_response();
        }
    }

    // Validate referenced blobs exist locally before persisting (catches
    // misordered pushes where the manifest is PUT before its layers).
    if let Some(missing) = check_manifest_blobs_present(state, repo, &bytes).await {
        return RegistryError::new(
            StatusCode::BAD_REQUEST,
            crate::RegistryErrorCode::ManifestInvalid,
            format!("manifest references missing blob: {missing}"),
        )
        .into_response();
    }

    // Persist manifest by digest.
    if let Err(e) = state.storage.put_blob_buffered(repo, &digest, bytes).await {
        return internal_err(e);
    }

    // If the reference is a tag (not a digest), record it as locally pushed.
    if Digest::parse(reference).is_err() {
        if let Err(e) = state.local_tags.set(repo, reference, &digest) {
            warn!(error = %e, "failed to persist local tag");
            return internal_err(e);
        }
    }

    let mut builder = Response::builder()
        .status(StatusCode::CREATED)
        .header(
            header::LOCATION,
            format!("/v2/{repo}/manifests/{}", digest),
        )
        .header("Docker-Content-Digest", digest.to_string());
    // Echo the client's content-type if they provided one.
    if let Some(ct) = headers.get(header::CONTENT_TYPE) {
        if let Ok(ct_str) = ct.to_str() {
            builder = builder.header(header::CONTENT_TYPE, ct_str);
        }
    }
    builder.body(Body::empty()).expect("static")
}

async fn delete_blob(state: &RegistryState, repo: &str, digest: &str) -> Response {
    let parsed = match Digest::parse(digest) {
        Ok(d) => d,
        Err(_) => return RegistryError::digest_invalid(digest).into_response(),
    };
    match state.storage.delete_blob(repo, &parsed).await {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(quill_storage::StorageError::NotFound(_)) => {
            RegistryError::blob_unknown(digest).into_response()
        }
        Err(e) => internal_err(e),
    }
}

async fn delete_manifest(state: &RegistryState, repo: &str, reference: &str) -> Response {
    // If reference is a tag, remove only the tag mapping; the manifest blob may
    // still be referenced by a digest-addressed pull. Phase 4 GC will reclaim
    // unreferenced manifest blobs.
    if Digest::parse(reference).is_err() {
        match state.local_tags.remove(repo, reference) {
            Ok(true) => return StatusCode::ACCEPTED.into_response(),
            Ok(false) => return RegistryError::manifest_unknown(reference).into_response(),
            Err(e) => return internal_err(e),
        }
    }
    // Digest-addressed: remove the manifest blob plus any local tag entries
    // pointing at it.
    let digest = Digest::parse(reference).unwrap();
    let to_remove: Vec<String> = state
        .local_tags
        .list_for_repo(repo)
        .into_iter()
        .filter(|(_, m)| m.digest == digest.to_string())
        .map(|(t, _)| t)
        .collect();
    for t in &to_remove {
        let _ = state.local_tags.remove(repo, t);
    }
    match state.storage.delete_blob(repo, &digest).await {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(quill_storage::StorageError::NotFound(_)) => {
            RegistryError::manifest_unknown(reference).into_response()
        }
        Err(e) => internal_err(e),
    }
}

/// Check that every blob referenced by a manifest already exists locally.
/// Returns `Some(digest_str)` for the first missing blob, or `None` if all
/// referenced blobs are present. Image-index manifests reference other
/// manifests, which we treat as soft references (not validated here) — they
/// might be pulled-through on demand.
async fn check_manifest_blobs_present(
    state: &RegistryState,
    repo: &str,
    manifest_bytes: &[u8],
) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(manifest_bytes).ok()?;
    let mut required: Vec<String> = Vec::new();
    if let Some(d) = v.get("config").and_then(|c| c.get("digest")).and_then(|d| d.as_str()) {
        required.push(d.to_string());
    }
    if let Some(layers) = v.get("layers").and_then(|l| l.as_array()) {
        for layer in layers {
            if let Some(d) = layer.get("digest").and_then(|d| d.as_str()) {
                required.push(d.to_string());
            }
        }
    }
    for d_str in required {
        let parsed = match Digest::parse(&d_str) {
            Ok(d) => d,
            Err(_) => return Some(d_str),
        };
        match state.storage.blob_meta(repo, &parsed).await {
            Ok(Some(_)) => continue,
            _ => return Some(d_str),
        }
    }
    None
}

// ---------- helpers ----------

async fn collect_body(body: Body, max_bytes: u64) -> Result<Bytes, Response> {
    match body.collect().await {
        Ok(c) => {
            let bytes = c.to_bytes();
            if bytes.len() as u64 > max_bytes {
                return Err(RegistryError::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    crate::RegistryErrorCode::SizeInvalid,
                    "request body too large",
                )
                .into_response());
            }
            Ok(bytes)
        }
        Err(e) => Err(RegistryError::new(
            StatusCode::BAD_REQUEST,
            crate::RegistryErrorCode::Unknown,
            format!("body read failed: {e}"),
        )
        .into_response()),
    }
}

/// If a `GET /manifests/...` response carries a `Docker-Content-Digest` header
/// that matches the request's `If-None-Match`, downgrade it to a 304 Not
/// Modified. Mirrors browser-style conditional GET semantics.
fn apply_if_none_match(req_headers: &HeaderMap, mut resp: Response) -> Response {
    let inm = match req_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        Some(v) => v.trim().trim_matches('"').to_string(),
        None => return resp,
    };
    let digest = match resp
        .headers()
        .get("docker-content-digest")
        .and_then(|v| v.to_str().ok())
    {
        Some(d) => d.to_string(),
        None => return resp,
    };
    if inm != digest {
        return resp;
    }
    let mut new = Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .body(Body::empty())
        .expect("static");
    // Echo the etag-equivalent header so clients can see what was matched.
    if let Ok(v) = HeaderValue::from_str(&digest) {
        new.headers_mut().insert("Docker-Content-Digest", v.clone());
        new.headers_mut().insert(header::ETAG, v);
    }
    // Drop any body the original response may have had.
    let _ = resp.body_mut();
    new
}

fn internal_err(e: impl std::fmt::Display) -> Response {
    RegistryError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        crate::RegistryErrorCode::Unknown,
        e.to_string(),
    )
    .into_response()
}
