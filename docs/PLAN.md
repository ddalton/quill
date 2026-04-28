# Zot Rust Rewrite Plan: Pull-Speed Focus

## Executive Summary

**Quill** is a Rust OCI registry scoped for **single-user, single-machine** deployment (developer laptop). It serves three roles, in this order of importance:

1. **Pull-through cache** in front of upstream registries (initially ECR and Harbor). First-time pulls are streamed-and-cached simultaneously; subsequent pulls are instant from local disk.
2. **Local push target** for patched images and charts. A `docker push localhost:5000/mycorp/redis:7.2-patched` lands on the laptop, and subsequent pulls of that exact tag serve the local copy — *never* re-checked against upstream. This is the "patching" use case: a locally-pushed tag takes precedence over upstream.
3. **Read-through proxy** for everything not pushed locally. Pull a tag that exists only upstream → cache it. Pull a tag you've patched locally → serve the local one. Tags coexist per-repo without conflict.

The performance headline: on a cache miss, Quill streams bytes from upstream to the client and to local disk **simultaneously** — never serializing the two. The current Go zot sync extension downloads the full image to local CAS *then* serves it, roughly doubling first-pull latency. Quill fixes that.

### Scope (deliberately narrow)

This rewrite is **not** a feature-parity replacement for zot. It targets one user, one machine, two upstreams (ECR, Harbor), local filesystem storage, and just enough push support to enable the patching workflow. Many optimizations and features that matter for a multi-tenant production registry are explicitly out of scope — see §3.4.

---

## 1. Bottleneck Analysis of Current Go Implementation

### 1.1 Per-repo RWMutex held during blob open (`imagestore.go:1520-1521`)

`GetBlob` acquires `is.lock.RLock()` before calling `storeDriver.Reader()`. While this is a read lock (allows concurrent reads), it still serializes against any write operation on the same repo. Under mixed read/write workloads (pull while push is in progress), this blocks pull requests.

### 1.2 Extra `Stat()` call per blob (`imagestore.go:1482-1509`)

`originalBlobInfo()` calls `storeDriver.Stat(blobPath)` on every `GetBlob`, `GetBlobPartial`, and `GetBlobContent` request. For local storage this is a syscall; for S3 this is a `HeadObject` round-trip (20-100ms). This happens before the blob is even opened.

### 1.3 Userspace copy loop (`routes.go:2068-2081`)

`WriteDataFromReader` copies data from the blob reader to the HTTP response in a `io.CopyN` loop with 10MB chunks. Data flows: disk -> kernel -> userspace buffer -> kernel -> socket. Go's `net/http` does not use `sendfile(2)`, so every byte of every layer transits userspace.

### 1.4 No HTTP/2 multiplexing

Zot uses `gorilla/mux` on Go's `net/http` which supports HTTP/2 but doesn't optimize for stream multiplexing. Each concurrent layer pull from the same client opens a separate stream but shares the same connection-level flow control.

### 1.5 No blob metadata cache

Every pull request stats the filesystem, even for repeatedly-pulled popular images. There's a dedup cache for digest->path mapping but no in-memory size/existence cache.

---

## 2. Can Layers Be Pulled in Parallel?

**Yes, fully.** This is confirmed at every level:

### 2.1 OCI Distribution Spec

The spec explicitly allows it: blobs may be "retrieved in any order." Each `GET /v2/<name>/blobs/<digest>` is stateless and independent. No ordering constraints exist between layer fetches after the manifest is retrieved.

### 2.2 Client behavior

| Client | Default parallel layers | Config |
|---|---|---|
| containerd | 3 | `max_concurrent_downloads` in config.toml |
| Docker | 3 | `--max-concurrent-downloads` daemon flag |
| skopeo | 6 | `--src-concurrency` flag |
| crane | unlimited | default behavior |

### 2.3 What the server can do

The server's role is passive — it serves concurrent requests as fast as possible. The key insight is that **server-side parallelism is about removing serialization points**, not about the server initiating parallel transfers. Specifically:

1. **Eliminate per-repo locks on the read path** — allow unlimited concurrent blob reads
2. **Minimize syscalls per request** — cache blob metadata to avoid `stat()` on every GET
3. **Use zero-copy I/O** — `sendfile(2)` bypasses userspace entirely
4. **Support HTTP/2 multiplexing** — let clients open many streams on one connection
5. **Support range requests** — allow clients to split large layers into parallel chunk fetches

### 2.4 Range-based parallel chunk pulls

The OCI spec requires registries to support `Range:` headers on blob endpoints. A sufficiently advanced client could:
1. `HEAD /v2/<name>/blobs/<digest>` to get `Content-Length`
2. Split the layer into N range requests: `Range: bytes=0-50MB`, `Range: bytes=50MB-100MB`, etc.
3. Fetch chunks in parallel, reassemble, verify digest

Standard containerd/Docker don't do this today, but tools like `nydus` and `overlaybd` exploit range requests for lazy pulling. The Rust registry should be optimized for this pattern.

---

## 3. Architecture

### 3.1 Technology Stack

| Component | Crate | Why |
|---|---|---|
| Async runtime | `tokio` | Industry standard, required by all async crates below |
| HTTP server | `axum` + `hyper` | Native HTTP/2, tower middleware, streaming responses |
| TLS | `rustls` | Pure Rust, ALPN for HTTP/2, no OpenSSL dependency |
| Storage abstraction | Custom trait | `LocalStorage` only (S3 storage backend is out of scope; see §3.4) |
| AWS SDK | *not used* | ECR is consumed via standard HTTP Basic auth (username `AWS`, password from `aws ecr get-login-password`); no SDK in the binary |
| JSON/OCI types | `oci-spec-rs` + `serde` | OCI image/distribution spec types |
| Server TLS | `rustls` + `tokio-rustls` + `rustls-pemfile` | TLS termination; same trust model as zot |
| Self-signed cert generation | `rcgen` | First-run convenience for laptop/localhost binding |
| Password hashing | `bcrypt` | htpasswd compatibility with zot |
| JWT issuance/verification | `jsonwebtoken` | Bearer-token auth flow compatible with zot's `auth.token` |
| CAS index | `DashMap` | Lock-free concurrent hashmap for blob metadata cache |
| Logging | `tracing` | Structured, async-aware logging |
| Config | `serde` + `toml` | Compatible with existing Zot config format |
| Metrics | `metrics` + `metrics-exporter-prometheus` | Prometheus-compatible |

### 3.2 Project Layout

```
zot-rs/
  Cargo.toml
  src/
    main.rs                  # startup, config loading, server binding
    config.rs                # config parsing (Zot-compatible TOML/JSON)
    server.rs                # axum router setup
    routes/
      mod.rs
      v2.rs                  # GET /v2/ version check
      manifests.rs           # GET/HEAD/PUT/DELETE manifests
      blobs.rs               # GET/HEAD/DELETE blobs (the hot path)
      uploads.rs             # POST/PATCH/PUT blob uploads
      referrers.rs           # GET referrers
      catalog.rs             # GET _catalog
      tags.rs                # GET tags/list
    storage/
      mod.rs                 # StorageDriver trait (single implementation today)
      local.rs               # Local filesystem (sendfile-optimized)
      cas.rs                 # Content-addressable storage layout + path helpers
      cache.rs               # In-memory blob metadata cache (DashMap)
      local_tags.rs          # _local_tags.json sidecar for locally-pushed-tag tracking (§5.6)
    auth/
      mod.rs                 # AuthLayer (tower middleware)
      htpasswd.rs            # bcrypt-backed basic auth, zot-compatible htpasswd file
      token.rs               # Bearer-token / JWT flow at /v2/token
    tls/
      mod.rs                 # rustls server config; PEM loading; self-signed bootstrap
    auth/
      mod.rs                 # Auth middleware
      basic.rs               # Basic auth
      bearer.rs              # Bearer token / OAuth2
    middleware/
      mod.rs
      metrics.rs             # Request timing + Prometheus metrics
      cors.rs                # CORS headers
      logging.rs             # Request/response tracing
    error.rs                 # OCI distribution error types
    digest.rs                # Digest validation + parsing
```

### 3.3 Core Design Principles

1. **No locks on the read path.** Blob serving must be completely lock-free. Use atomic reference counting for metadata cache entries.
2. **Zero-copy by default.** Local blob serving uses `sendfile(2)` (Linux) or `sendfile(2)` (macOS) via `tokio::fs::File` + custom `Body` impl.
3. **Cache blob metadata aggressively.** A `DashMap<Digest, BlobMeta>` caches `{path, size, exists}` to eliminate `stat()` per request.
4. **Stream everything.** Never buffer an entire blob into memory. Manifests (small) can be buffered; layers (large) must stream.
5. **HTTP/2 first.** Default to HTTP/2 with `h2` stream multiplexing so a single client connection can pull all layers concurrently.

### 3.4 In scope and out of scope

This rewrite targets one user, one machine, but with both **pull-through proxy** and **local push** behavior so the patching workflow (§5.6) works.

**Kept:**

- **Pull endpoints**: `GET/HEAD` blobs and manifests, `GET /v2/<name>/tags/list`.
- **Push endpoints**: `POST /v2/<name>/blobs/uploads/`, `PATCH .../<session>`, `PUT .../<session>?digest=...`, `PUT /v2/<name>/manifests/<ref>`, `DELETE /v2/<name>/blobs/<digest>`, `DELETE /v2/<name>/manifests/<ref>`. Resumable upload session tracking.
- **Local-precedence semantics**: a tag that exists locally (pushed by the user) is served from local *unconditionally* — Quill never contacts upstream to revalidate it. This is the moral equivalent of the zot `onlySyncOnMissing` flag, but unconditional rather than configurable. Detail in §5.6.
- **Streaming pull-through with single-flight coalescing.**
- **Tuned HTTP/2 flow-control windows on the upstream client** (16 MB stream window — single biggest perf knob).
- **Auth-token caching** for Harbor (~30min JWT) and any other bearer-token registry. ECR's 12h token is supplied via config (refreshed externally) so no in-binary caching is required for it.
- **Blob metadata cache** (`DashMap`).
- **TLS** via `rustls` — config-supplied cert + key, or self-signed cert auto-generated on first run (§3.5). Same trust model as zot.
- **Server-side credentials**: htpasswd-file basic auth and bearer-token-via-`/v2/token` flow (§3.5). Same on-disk format as zot so existing zot htpasswd files work unchanged.
- **Mark-and-sweep garbage collection** of unreferenced blobs (Phase 4 polish). Necessary because `rm -rf cache/` would now delete locally-pushed content.

**Excluded:**

| Excluded | Why it doesn't matter for one user on a laptop |
|---|---|
| **S3 storage backend** | No multi-replica deployment; local disk is faster, simpler, and durable enough. Tempfile + atomic-rename CAS is dramatically simpler than S3 multipart upload state machines. |
| **`sendfile(2)` zero-copy serving** | Saves CPU, not wall-clock. The bottleneck on a laptop pulling from a remote registry is the network, never userspace memcpy. `tokio::fs::File` streaming is plenty. |
| **Lock-free read path / per-repo `RWMutex` removal** | Go zot's contention only matters under concurrent push+pull from many clients. One user — no real contention. A `parking_lot::RwLock` per repo on the *write* path is sufficient; reads stay lock-free against the metadata cache. |
| **Multiple HTTP/2 connections per upstream (C>1)** | TCP head-of-line blocking mitigation matters under sustained heavy load. One user — one keep-alive connection per upstream is enough. |
| **Parallel chunks per layer (N>1) via HTTP `Range:` requests** | The containerd 2× win was specific to Docker Hub's ~60 MB/s per-stream throttle. ECR (S3-backed) and Harbor don't throttle per stream. |
| **mTLS client-cert auth** | Basic auth + bearer tokens cover the common cases; mTLS is a multi-tenant-deployment feature. |
| **LDAP / OpenID Connect / OAuth2 federated auth** | zot supports these for enterprise SSO. For a laptop, htpasswd basic auth + bearer tokens is enough. |
| **OCI conformance test suite, catalog API, referrers, cosign, search extensions, Prometheus metrics export, distributed tracing** | All targeted at production multi-tenant operation. Observability is `tracing` to stderr. |
| **Elaborate slow-consumer back-pressure / 50-subscriber thundering-herd defenses** | The producer/consumer streaming model is implemented (it's how streaming-while-caching works), but the multi-tenant failure-mode hardening from earlier drafts is over-engineered for one user with at most a couple of concurrent terminals. |
| **HTTP/3 / QUIC support** | Deferred indefinitely. |

A future "make Quill production-grade for multi-tenant deployment" effort would re-introduce most of the excluded items. Not now.

### 3.5 Credentials and TLS

Quill supports server-side TLS and credentials with the same on-disk format and config shape as zot, so existing zot certs and htpasswd files drop in unchanged. Both are implemented in Phase 1 (foundational, not polish) because every subsequent endpoint flows through them.

#### TLS

- **`rustls` only.** No OpenSSL dependency. Pure Rust, ALPN for HTTP/2, modern ciphers by default.
- **Config (TOML):**
  ```toml
  [http.tls]
  cert = "/etc/quill/tls/server.crt"
  key  = "/etc/quill/tls/server.key"
  # Optional; if set, requires client certs signed by this CA (mTLS — out of scope for v1)
  # client_ca = "/etc/quill/tls/ca.crt"
  ```
- **PEM cert + key** (matches zot's format). Loaded once at startup; reload-on-SIGHUP is Phase 4 polish.
- **Self-signed fallback:** if `[http.tls]` is omitted *and* the bind address is `127.0.0.1` / `::1`, Quill generates a self-signed cert at first run (`<root>/_quill/self-signed.{crt,key}`), valid for 10 years, persisted across restarts. Avoids the "configure TLS to use a registry" friction for laptop use; remote bindings *require* an explicit cert.
- **Plaintext HTTP** is allowed only when `[http]` has `tls = false` explicitly *and* binding to localhost. Production deployments without TLS are rejected at config-load time.

#### Credentials

Two auth modes, both compatible with zot's config:

**1. htpasswd basic auth** (simplest, recommended for laptop use)

- **Config:**
  ```toml
  [http.auth.htpasswd]
  path = "/etc/quill/htpasswd"
  ```
- **Same file format as zot:** `username:bcrypt-hash` per line. Use `htpasswd -B -c <file> <user>` to generate.
- **Verification:** `bcrypt` crate. Hashes are CPU-bound (~50–100 ms each); cache successful `Authorization: Basic ...` headers in a small in-memory `DashMap<HeaderHash, (user, expires_at)>` with a short TTL (e.g. 5 min) to avoid bcrypt-on-every-request.
- **Realm:** Quill issues `WWW-Authenticate: Basic realm="quill"` on 401.

**2. Bearer token via `/v2/token`** (compatible with `docker login`-style flows)

- **Config:**
  ```toml
  [http.auth.token]
  realm = "https://localhost:5000/v2/token"
  service = "quill"
  # Issuer key (used to sign the JWT)
  issuer_key = "/etc/quill/tls/issuer.key"
  ```
- **Flow:** unauthenticated request → 401 with `WWW-Authenticate: Bearer realm="...", service="...", scope="..."` → client GETs `/v2/token` with basic auth (against the htpasswd file) → Quill returns a short-lived JWT scoped to the requested repo+actions → client retries with `Authorization: Bearer <jwt>`.
- **JWT signing:** RS256 via `jsonwebtoken` crate. Issuer key is RSA private key in PEM. Public key is embedded in JWT header so clients can verify if they want.
- **Same flow as zot's `auth.token`** — existing `docker login` / `crane auth login` workflows work without change.

**No mTLS, no LDAP, no OIDC, no OAuth2 IdP federation** — those are explicit non-goals (§3.4).

#### Authorization

Per-repo authorization is *not* implemented in v1. A user authenticated via either mechanism above gets full pull *and push* access to all repos. This matches the "single-user laptop" model.

If multi-user-per-laptop ever becomes a requirement, zot's per-repo policy (`accessControl.repositories`) is the model to follow — same TOML shape, same `read`/`create`/`update`/`delete` actions.

#### Wiring

Auth is implemented as `tower::Layer` middleware, applied selectively by the router:

```rust
// pseudocode
let public = axum::Router::new()
    .route("/v2/", get(v2_check))
    .route("/v2/token", get(token_endpoint));

let protected = axum::Router::new()
    .route("/v2/:name/manifests/:ref", any(manifest_handler))
    .route("/v2/:name/blobs/:digest", any(blob_handler))
    .route("/v2/:name/blobs/uploads/", post(upload_init))
    .layer(AuthLayer::new(htpasswd_or_token));

let app = public.merge(protected);
```

This keeps the auth check off of `/v2/` and `/v2/token` (which can't be themselves protected) and applies uniformly elsewhere.

#### Upstream credentials are separate

The credentials in this section are for clients connecting *to* Quill. Credentials Quill uses to reach upstream registries (HTTP Basic for ECR/Harbor, bearer-token discovery for Docker Hub-style anonymous flows) are configured separately under `[upstream.<name>.auth]` and discussed in §5.12. Don't confuse the two.

---

## 4. Pull Path: Cache Hit (Local CAS)

### 4.1 `GET /v2/<name>/blobs/<digest>` — The Hot Path

This is where pull speed is won or lost. The request flow:

```
Request
  |
  v
[axum handler: blobs::get_blob]
  |
  +--> validate digest format (pure computation, no I/O)
  |
  +--> check blob metadata cache (DashMap lookup, lock-free)
  |     |
  |     +-- HIT: have {path, size} -> skip to open
  |     +-- MISS: stat() the file, populate cache, continue
  |
  +--> check Range header
  |     |
  |     +-- No range: serve full blob
  |     +-- Range present: parse range, serve partial
  |
  +--> open file handle (tokio::fs::File::open)
  |
  +--> build response with SendFileBody (zero-copy via sendfile(2))
```

### 4.2 Zero-Copy Blob Serving (Local Storage)

```rust
// Conceptual implementation
async fn get_blob(
    State(store): State<Arc<BlobStore>>,
    Path((name, digest)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let digest = Digest::parse(&digest)?;

    // Lock-free metadata lookup
    let meta = store.blob_meta(&name, &digest).await?;

    // Parse range if present
    if let Some(range) = headers.get(RANGE) {
        let (from, to) = parse_range(range, meta.size)?;
        let file = tokio::fs::File::open(&meta.path).await?;
        // Seek to offset, serve partial with sendfile
        return Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(CONTENT_LENGTH, to - from + 1)
            .header(CONTENT_RANGE, format!("bytes {}-{}/{}", from, to, meta.size))
            .body(SendFileBody::new(file, from, to - from + 1))
    }

    let file = tokio::fs::File::open(&meta.path).await?;
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, meta.size)
        .header("Docker-Content-Digest", digest.to_string())
        .body(SendFileBody::new(file, 0, meta.size))
}
```

The `SendFileBody` type implements `hyper::body::Body` using `sendfile(2)` under the hood, avoiding any userspace buffer. On Linux, this uses `splice()` + `sendfile()`; on macOS, `sendfile(2)` with the BSD calling convention.

### 4.3 Blob Metadata Cache

```rust
struct BlobMeta {
    path: PathBuf,
    size: u64,
    cached_at: Instant,
}

struct BlobCache {
    entries: DashMap<(String, Digest), BlobMeta>,  // (repo, digest) -> meta
    ttl: Duration,  // e.g., 60 seconds
}

impl BlobCache {
    fn get(&self, repo: &str, digest: &Digest) -> Option<BlobMeta> {
        self.entries.get(&(repo.to_string(), digest.clone()))
            .filter(|e| e.cached_at.elapsed() < self.ttl)
            .map(|e| e.clone())
    }

    fn insert(&self, repo: &str, digest: &Digest, meta: BlobMeta) {
        self.entries.insert((repo.to_string(), digest.clone()), meta);
    }
}
```

This eliminates the `stat()` syscall on repeated pulls of the same image. Cache invalidation happens on TTL expiry and on blob deletion (delete handler evicts the cache entry).

---

## 5. Pull Path: Cache Miss (Streaming Pull-Through)

When Quill is configured with an upstream registry (ECR or Harbor) and a blob is not yet in the local CAS, the cache miss must not double end-to-end latency. The bytes traverse the network exactly once from upstream into the proxy; from there they fan out simultaneously to disk and to the requesting client.

> **Single-user note (§3.4):** the producer/consumer/in-flight-table machinery below stays as designed because it's how streaming-while-caching *works at all* — but the elaborate slow-consumer back-pressure, idle-timeout, and many-subscriber-thundering-herd defenses described in earlier drafts are over-engineered for one user. The "N consumers tail one tempfile" pattern is implemented because it's actually simpler than serializing access; the failure modes around 50 simultaneous slow subscribers are not a concern here.

### 5.1 Design

A cold blob fetch goes through the following states:

```
client GET /v2/<name>/blobs/<digest>
        |
        v
[in-flight pull-through table lookup, keyed by digest]
        |
        +-- entry exists --> attach as additional subscriber, skip upstream
        +-- no entry      --> insert entry; this caller becomes the producer
                                  |
                                  v
                  [open tempfile; open upstream GET]
                                  |
                                  v
                  Producer task (1):  read upstream chunk -> append tempfile -> update sha256 -> bump high-water mark -> notify
                  Consumer tasks (N): each tails the tempfile, awaiting notify when it reaches the high-water mark
                                  |
                                  v
                  Producer completes -> verify digest
                                  |
                                  +-- match:    fsync, atomic rename into CAS, populate metadata cache, signal success
                                  +-- mismatch: discard tempfile, surface stream error to all subscribers
```

Key invariants:

1. **Single producer, many consumers.** The in-flight table is a `DashMap<Digest, Arc<PullThroughEntry>>`. Concurrent identical requests join the existing entry instead of starting a second upstream fetch.
2. **Decoupled from client lifecycle.** If every subscribed client disconnects mid-stream, the producer keeps running. The cache fill is the long-term win — the next pull is a hit.
3. **Decoupled from consumer pace.** Consumers tail the tempfile independently. A slow client cannot back-pressure the producer or starve a fast client. The producer runs at upstream network speed regardless.
4. **Tempfile-then-rename.** Tempfile lives at `<repo>/_uploads/<random>`. Atomic rename to `<repo>/blobs/sha256/<digest>` only after digest verification. Unlinked on any failure.
5. **Digest-verified before commit.** A `sha256` is updated as bytes are read from upstream and compared against the requested digest before the rename. A mismatch fails *all* current subscribers — they all see the same wire bytes anyway, so a single verification is correct.

### 5.2 Rust types (sketch)

```rust
/// A single in-flight upstream fetch.
struct PullThroughEntry {
    digest: Digest,
    tempfile_path: PathBuf,
    /// High-water mark: bytes the producer has written to the tempfile and made visible.
    written: AtomicU64,
    /// Notified each time the producer advances `written`.
    progress: Arc<Notify>,
    /// Resolves when the producer terminates: Ok(meta) on success, Err on failure.
    completion: Shared<oneshot::Receiver<Result<BlobMeta, RegistryError>>>,
}

/// Lock-free table of in-flight pull-throughs.
struct PullThroughTable {
    entries: DashMap<Digest, Arc<PullThroughEntry>>,
}

enum ProducerRole { Producer, Subscriber }

impl PullThroughTable {
    /// Returns the entry plus a role: Producer means this caller must drive the upstream fetch;
    /// Subscriber means another caller is already producing and this caller just reads.
    fn get_or_insert(&self, digest: &Digest) -> (Arc<PullThroughEntry>, ProducerRole);
}

/// hyper::Body impl: reads from the tempfile up to entry.written, awaits progress notifications
/// when offset reaches the high-water mark, and resolves on completion.
struct PullThroughBody {
    file: tokio::fs::File,
    offset: u64,
    entry: Arc<PullThroughEntry>,
}
```

### 5.3 Failure handling

| Event | Effect |
|---|---|
| Upstream connection breaks mid-stream | Producer surfaces `Err` via `completion`. Consumers' `PullThroughBody` returns an IO error mid-response (HTTP/2: RST_STREAM with INTERNAL_ERROR; HTTP/1.1: connection close). Tempfile unlinked. Entry removed from table. |
| Digest mismatch on completion | Same as upstream-error path. Critical: must never rename a tempfile whose digest does not match the requested digest. |
| Producer task panics | `oneshot::Sender` drops; consumers see `Err(channel closed)`. Tempfile unlinked by `Drop` impl on `PullThroughEntry`. |
| Client disconnects | Consumer task is dropped. Producer continues. Cache fills. |
| Process crash mid-fetch | Tempfile orphaned in `_uploads/`. Startup sweep removes any tempfile older than the configured TTL (default 24h). |
| Concurrent identical request mid-fetch | Joins existing entry as a subscriber. Reads from offset 0 of the same tempfile. Effective single-flight. |

### 5.4 Range requests on cold blobs

A `Range:` header on a digest that is not yet cached is the awkward case. Three options:

1. **Promote to full fetch (default).** Treat a range request on a cold blob as a full upstream pull. The producer fetches the entire blob; the requesting client's `PullThroughBody` seeks into the tempfile and serves only the requested range. Best of both worlds: the cache is populated even on partial-pull workloads.
2. **Direct passthrough, no cache.** Forward the range request to upstream verbatim. No cache fill. Subsequent full requests trigger a fresh fetch. Simpler but defeats the cache for clients that always pull ranges.
3. **Refuse.** Return `416 Range Not Satisfiable` for cold ranges. Hostile to clients; not recommended.

Default to option 1. Make the policy configurable.

### 5.5 Manifests

Manifests are typically <1MB; streaming gain is negligible and digest verification needs the whole blob anyway. Manifests use a simpler full-buffer fetch-verify-persist-serve path, not the streaming pull-through machinery.

Tag-based manifest requests are mutable upstream and require an extra step: a HEAD upstream to learn the current digest, then either serve the local copy (hit) or full-buffer fetch the new manifest (miss). The existing `Check local cache before upstream sync for tag-based manifest requests` logic continues to apply.

The streaming pull-through model is restricted to **layer blobs**, where transfer size dominates pull latency.

### 5.6 Local-pushed tags take precedence (the patching model)

Quill is both a pull-through cache and a push target. The two interact at tag granularity: a user might push `mycorp/redis:7.2-patched` locally while still expecting `library/redis:7.2` to proxy through to Docker Hub. The rule:

> **A tag that was pushed locally is served from local, unconditionally. Quill never contacts upstream to revalidate it.**

This is the moral equivalent of zot's `onlySyncOnMissing=true` mode, but applied per-tag rather than per-deployment, and unconditional rather than configurable — it's the only sensible behavior given the patching use case.

#### Tracking which tags are local

Each repo has a sidecar file `<root>/<repo>/_local_tags.json`:

```json
{
  "7.2-patched": {
    "digest": "sha256:abcd...",
    "pushed_at": "2026-04-28T10:15:00Z"
  }
}
```

Written atomically (write-temp + rename) on every successful `PUT /v2/<name>/manifests/<tag>`. Loaded into an in-memory `DashMap<(Repo, Tag), LocalTagMeta>` at startup and on file change.

A tag is "locally pushed" iff it appears in this map. There is no implicit conversion (e.g. an upstream pull of `library/redis:7.2` does *not* mark the tag local). The only way for a tag to become local is an explicit `PUT /v2/<name>/manifests/<tag>` from a client.

#### Resolution logic per request type

| Request | Local-tag map says... | Behavior |
|---|---|---|
| `GET/HEAD /v2/<name>/manifests/<sha256:...>` | (digest-addressed, ignored) | Local hit → serve. Local miss → pull-through fetch. Digests are immutable; never revalidate. |
| `GET/HEAD /v2/<name>/manifests/<tag>` | tag is local | Resolve to local digest, serve from local CAS. **Never contact upstream.** |
| `GET/HEAD /v2/<name>/manifests/<tag>` | tag is not local, not yet cached | Pull-through fetch from upstream, cache, serve. Do *not* mark as local. |
| `GET/HEAD /v2/<name>/manifests/<tag>` | tag is not local, cached previously from upstream | Serve cached manifest if within freshness TTL (default 5 min); past TTL, HEAD upstream and revalidate via `Docker-Content-Digest`; on digest mismatch, fetch new manifest. |
| `GET /v2/<name>/blobs/<digest>` | (digest-addressed) | Local hit → serve. Local miss → pull-through fetch from upstream. Same digest may serve both locally-pushed and upstream-pulled manifests; no special-casing needed (CAS deduplicates). |
| `PUT /v2/<name>/manifests/<tag>` | (push) | Verify referenced blobs are present, write manifest to CAS, atomic update of `_local_tags.json` to add/overwrite the tag. |
| `DELETE /v2/<name>/manifests/<tag>` | (push) | Remove from `_local_tags.json`. The next request for that tag falls back to upstream (if upstream has it). |
| `GET /v2/<name>/tags/list` | (combined view) | Merge local-pushed tags with upstream tags; local-pushed shadow same-named upstream tags. |

#### Why "unconditional, not configurable"

I considered an `auto_revalidate_local_tags` flag for the case where a user pushes a tag that *also* exists upstream and wants both to track upstream. Rejected: the only reason to push that tag locally is to have it differ from upstream (otherwise, why push?). If the user truly wants upstream's version back, they can `docker pull <upstream>/<image>:<tag>` from upstream directly with a different repo prefix, or `quill cache forget <repo>:<tag>` to remove the local entry. No flag needed.

#### `tags/list` merge behavior

```
LocalTags(<repo>):     {"7.2-patched", "v1-mycorp"}
CachedUpstream(<repo>): {"7.2", "7.2.1", "latest"}
UpstreamLive(<repo>):   {"7.2", "7.2.1", "7.2.2", "latest"}  (refreshed past TTL)

GET /v2/<repo>/tags/list returns: {"7.2-patched", "v1-mycorp", "7.2", "7.2.1", "7.2.2", "latest"}
```

Local-pushed tags shadow same-named upstream tags in the merge — but in practice users push tags with distinct names (e.g. `7.2-patched`) precisely *because* they want local and upstream to coexist visibly.

### 5.7 Performance model

| Scenario | Sequential cache (current Go) | Streaming pull-through |
|---|---|---|
| Cold pull, single client | `t_upstream + t_local_write + t_local_read + t_send` (~2× upstream) | `max(t_upstream, t_local_write)` (~1× upstream on NVMe) |
| Cold pull, N simultaneous clients on same blob | N upstream fetches or fully serialized | 1 upstream fetch, N tempfile-tail readers |
| Warm pull | `sendfile(2)` from CAS | `sendfile(2)` from CAS (identical) |
| Client disconnects mid-pull | cache may be discarded | cache fills regardless |
| Slow client + fast client on same cold blob | slow client throttles fast | independent; producer runs at upstream speed |

### 5.8 Observability

`tracing` to stderr with structured spans on the request path: `request_id`, `repo`, `digest`, `cache_hit/miss`, `upstream_duration_ms`, `bytes`. No Prometheus exporter (§3.4); a developer reads the logs.

### 5.9 High-performance micro-optimizations for single-user

The structural wins (streaming pull-through, HTTP/2 window tuning, auth-token caching, keep-alive) account for >95% of the perceived speedup. The optimizations below are the marginal gains worth taking on top because they're cheap to implement and add up over a developer's daily pull volume:

| Optimization | Why it matters for one user | Cost |
|---|---|---|
| **Aggressive in-memory manifest+config cache** (e.g. 128 MB budget keyed by digest, parsed `oci_spec` types) | Eliminates JSON parse cost on warm pulls; manifests are tiny but re-read on every `docker pull`. | `moka` or `quick_cache` crate, ~50 lines |
| **`mimalloc` or `jemallocator` as global allocator** | macOS default and glibc allocator are the slow path for `bytes::Bytes` churn; ~5–15% on hot paths. | One `#[global_allocator]` line |
| **Release profile: `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`** | ~10–20% improvement on hot-path microbenchmarks. | Cargo.toml |
| **`parking_lot::Mutex` instead of `tokio::sync::Mutex` where the critical section doesn't await** | Tokio mutexes do an `.await` even on uncontended acquires. | Drop-in |
| **`Arc<str>` and `bytes::Bytes` instead of `String::clone` on the request path** | Allocator pressure shows up in `criterion` benches even at single-user load. | Discipline |
| **Speculative config-blob prefetch on manifest fetch** | A manifest fetch always means a config-blob fetch is next; issuing them in parallel saves ~1 RTT per pull. | Phase 3 polish |
| **`oci-spec-rs` types deserialized once, kept hot** | Avoids repeated `serde_json` work on warm path. | Combined with manifest cache |
| **`tokio::io::copy_buf` with 64 KB `BufReader` over the upstream stream** | Better than default 8 KB syscall granularity on tempfile writes. | One line |
| **`fs::write` `O_TMPFILE` on Linux for tempfiles** | Avoids namespace pollution and a separate `unlink` on cleanup. | Phase 2 |
| **No `tokio_util::io::ReaderStream` if avoidable** | `hyper::Body::from_stream` allocates per-chunk; a custom `Body` impl reuses buffers. | Phase 2 if profiling justifies |

Skip on purpose:

- **`O_DIRECT`** — hurts warm-pull latency by bypassing page cache. NVMe + page cache is the right combination.
- **`io_uring`** — Linux-only, and on macOS the user's primary OS the equivalent (`kqueue`) is what Tokio already uses. Marginal at best.
- **Custom HTTP parser** — `hyper` is already the fastest HTTP/2 client in Rust.

### 5.10 Optimizing the upstream-fetch leg

The streaming model ensures the cache fill doesn't *add* latency, but the upstream GET itself still bounds first-pull speed. Go zot's sync extension uses `containers/image` with mostly default HTTP behavior; the Rust rewrite can do better:

**Connection-level:**

1. **Per-upstream `hyper` client with HTTP/2 + ALPN.** Most large registries (GHCR, Docker Hub, ECR, GAR, Quay) speak HTTP/2. A persistent `hyper::Client` with HTTP/2 multiplexes all in-flight blob/manifest fetches over one TCP+TLS connection — no per-request TCP/TLS handshake.
2. **Long-lived connection pool, idle keep-alive 5–10 min.** Avoid re-handshaking against upstreams that throttle aggressively (Docker Hub).
3. **TLS session resumption (rustls).** When a new connection is unavoidable, resume — saves a round-trip.
4. **Pre-warm.** On startup or config reload, open one connection per configured upstream so the first user request doesn't pay handshake cost.
5. **DNS caching with TTL respect.** Cache resolved upstream addresses; refresh on TTL expiry. Avoids per-request resolver hits.

**Request-level:**

1. **Skip the upstream `HEAD` before `GET`.** Go zot tends to HEAD-then-GET; for digest-addressed blobs that's a wasted round-trip. Issue `GET` directly and rely on response status.
2. **Authentication token caching.** Bearer-token auth (Docker Hub, GHCR) requires a separate token-server round-trip. Cache tokens per `(upstream, repo, scope)` until expiry; refresh ~30 s early. Coalesce concurrent token refreshes (single-flight, like blob pulls).
3. **Larger `hyper` HTTP/2 flow-control windows.** Defaults are conservative (~64 KB initial window). Bump initial stream window to 4–16 MB and connection window proportionally — for large layers over high-RTT links this is the single biggest knob. Match server-side window expansion if upstream allows.
4. **Disable response decompression.** OCI blobs are already gzipped/zstd-compressed — never re-compress and don't let the HTTP client try to decode `Content-Encoding`. Pass through raw bytes.

**Parallelism:**

1. **Parallel range fetches for large layers.** For layers above a threshold (e.g. 100 MB), split into N parallel `Range:` GETs. Each chunk writes into the same tempfile at its byte offset; the producer task coordinates completion. Empirically gives 2–4× speedup on long-RTT links where a single TCP stream can't fill the pipe. Make N configurable per upstream; default 4 for layers >100 MB.
2. **Manifest + config + layers in flight together.** When syncing a full image (not just a single blob request), fetch the manifest, config, and all unique layers concurrently from the same HTTP/2 connection. Bounded by a per-upstream concurrency cap to avoid being rate-limited.

**Correctness/efficiency:**

1. **Honor upstream `Cache-Control` and `ETag` for manifests.** Conditional GETs (`If-None-Match`) on tag-based manifest revalidation save full-body transfers when the tag hasn't moved.
2. **Retries with jittered exponential backoff** on 5xx/429, but never on 4xx other than 429. Respect `Retry-After`.
3. **Per-upstream concurrency limits and rate-limit awareness.** Track `RateLimit-*` headers (Docker Hub, GHCR) and proactively slow down before hitting a hard 429.

### 5.11 Concurrency model: parallel layers × per-layer chunks over HTTP/2

Three independent dimensions of concurrency apply on the upstream-fetch leg, and they compose multiplicatively:

1. **Parallel layers per image (M).** All unique layers from a manifest are fetched in flight simultaneously. Reference points: containerd defaults to 3, Docker daemon to 3, skopeo to 6, crane to GOMAXPROCS.
2. **Parallel chunks per layer (N).** Each large layer is split into N HTTP `Range:` requests fetched concurrently. containerd PR #10177 (`ConcurrentLayerFetchBuffer`) shipped this and observed ~2× speedup on an 8.6 GB image because Docker Hub throttles each stream at ~60 MB/s — the per-stream cap, not bandwidth, was the real bottleneck.
3. **Connections per upstream host (C).** The HTTP/2 client may open multiple TCP+TLS connections to the same upstream to mitigate TCP-level head-of-line blocking and per-connection rate limits at the CDN.

Total in-flight upstream requests on a cold pull ≈ `M × N`, balanced across `C` connections via least-loaded scheduling.

#### HTTP/2 multiplexing within a single connection

Empirically verified — Docker Hub (`registry-1.docker.io`), GHCR (`ghcr.io`), Quay (`quay.io`), ECR Public (`public.ecr.aws`), and GCR (`gcr.io`) all speak HTTP/2 directly today (ALPN-probed via `curl --http2`). The protocol composes cleanly with parallel layers and parallel chunks:

- Default `SETTINGS_MAX_CONCURRENT_STREAMS` is 100 (RFC 7540 §6.5.2). Image pulls never hit this — even a 30-layer image at N=4 chunks needs at most 120 streams, easily spread across 2 connections.
- All `M × N` requests share a single TCP+TLS handshake per connection. First-pull handshake cost amortizes across the entire image.
- Multiplexed streams are independent at the application layer: a slow upstream response on layer 5 does not block layer 6 from streaming.
- HTTP/2 stream prioritization (weight + dependency tree) was effectively **deprecated** in RFC 9113 due to interop problems and is not implemented uniformly across CDNs. Don't rely on it; treat all in-flight blob streams as equal-priority.

#### TCP-level head-of-line blocking — the trade-off

HTTP/2 application-layer streams share one TCP connection. One dropped TCP packet stalls *every* stream on that connection until retransmit. On lossy or congested links, `N` streams on 1 HTTP/2 connection can underperform `N` HTTP/1.1 connections.

Mitigation: open a small pool of HTTP/2 connections per upstream (default `C=2..4`) and spread `M × N` requests across them. This preserves most of the multiplexing benefit (handshake amortization, header compression, stream independence within each connection) while bounding HoL impact and sidestepping any per-connection CDN rate caps.

HTTP/3 over QUIC eliminates this category entirely — each QUIC stream has independent loss recovery. The `h3` crate is mature enough that we should plumb it through behind a config flag and switch on by default for upstreams that advertise `h3` ALPN.

#### HTTP/2 flow control — the silent killer

HTTP/2 has both connection-level and stream-level flow control. The default `SETTINGS_INITIAL_WINDOW_SIZE` is **65,535 bytes** (RFC 7540 §5.2.1). For a single stream over a 100 ms RTT link, that caps throughput at ~640 KB/s regardless of available bandwidth — and it's `hyper`'s default.

Required tuning:

- Bump `SETTINGS_INITIAL_WINDOW_SIZE` to ≥16 MB (max is 2^31−1) via `hyper::client::conn::http2::Builder::initial_stream_window_size`.
- Bump connection-level window proportionally via `initial_connection_window_size` (recommend ≥4× stream window so M×N parallel streams can saturate together).
- Without these knobs, parallel layer pulls *will not fill the pipe* even with perfect multiplexing. This is one of the most common reasons HTTP/2-everywhere migrations under-deliver.

#### Two-hop reality: registry host vs. CDN host

Docker Hub blob `GET` requests return a 307 redirect to a Cloudflare-backed CDN at a different hostname. GCR/AR redirect to Google's blob service. This splits the upstream into two separate clients:

- **Registry client** (`registry-1.docker.io`): manifests, auth tokens, blob *redirects*. Few RTTs; small pool (`C=1..2`).
- **CDN client** (Cloudflare/GCS/etc.): the actual blob bytes. This is where HTTP/2 multiplexing pays off; recommend `C=2..4` and tuned windows.
- **Cache the redirect target by digest** for the lifetime of the in-flight pull-through entry. Otherwise every chunk in a parallel range fetch eats an extra RTT on the registry host before the CDN GET.

#### Recommended defaults

| Knob | Default | Rationale |
|---|---|---|
| Parallel layers per image (M) | 6 | Matches skopeo; saturates most upstreams without provoking 429 |
| Chunks per layer (N) | 4 for layers >100 MB, else 1 | containerd PR #10177 sweet spot; small layers don't benefit |
| Connections per upstream (C) | 2 | Cuts TCP HoL impact while preserving HTTP/2 multiplexing benefit |
| HTTP/2 initial stream window | 16 MB | Required for high-BDP links to fill the pipe |
| HTTP/2 initial connection window | 64 MB | ≥4× stream window so M×N concurrent streams can saturate together |
| Concurrency cap per upstream | 32 in-flight requests | Prevents 429 on rate-limit-aggressive upstreams |
| HTTP/3 (QUIC) | off, opt-in via config | Promote to default once `h3` ALPN is widely advertised |

All values are per-upstream-configurable so a Docker Hub free-tier configuration can throttle down (M=2, N=1) while a mirror configuration pegs the limits (M=12, N=8).

#### What this looks like end-to-end on a cold image pull

For a 1.5 GB image with 8 layers averaging 200 MB across a 50 ms RTT link to Docker Hub from a fresh proxy with no cached connections:

1. **t=0**: Manifest GET on registry host (HTTP/2 handshake + 1 RTT) → manifest in hand.
2. **t≈100ms**: Issue 6 parallel layer GETs (M=6) on the registry host. Each gets a 307 redirect to CDN. Cache the resolved CDN host.
3. **t≈150ms**: Open 2 HTTP/2 connections (C=2) to the CDN, handshake in parallel.
4. **t≈250ms**: Issue 6 layers × 4 chunks each = 24 streams across 2 connections (12 streams/conn, well under 100). Each stream uses the tuned 16 MB window.
5. **t≈250ms+**: Producer task tees bytes from 24 streams into 8 tempfiles at offsets, sha256-hashes each layer, and consumer client streams the bytes out to the requesting Docker pull at the same time.
6. **t≈8s** (assuming ~200 MB/s aggregate after windows tuned): All layers complete, digests verified, atomic renames into CAS, metadata cache populated.

Compare to current Go zot's sync extension, which would: pull each layer sequentially (or with limited goroutine parallelism via `containers/image`), with default 64 KB window, single connection, and full disk write before serving — typically 20-30 s for the same image.

### 5.12 Upstream-specific considerations: ECR and Harbor

The defaults in §5.9–5.10 are tuned against the Docker Hub adversarial case (per-stream throttling, aggressive 429s). The two upstreams this deployment actually targets — **AWS ECR** and **Harbor** — are friendlier, and the configuration shifts accordingly.

#### ECR (Elastic Container Registry, private)

**Auth flow:**

- ECR's authorization token is fetched out-of-band via the AWS CLI: `aws ecr get-login-password --region <region>` returns a 12-hour token. ECR accepts that token as the password in standard HTTP Basic auth with the literal username `AWS`.
- Quill consumes ECR via the standard HTTP Basic auth code path — no `aws-sdk-rust` dependency, no SigV4 implementation, no IAM credential provider chain inside the binary. A small external refresher (cron, systemd timer, or shell script) re-runs `aws ecr get-login-password` every ~12 hours and updates Quill's config + reloads.
- This is the single biggest scope reduction in the plan. The cost is one extra moving part outside the binary; the simplification inside it is substantial.

**Two-hop topology:**

- Manifest, auth, and blob *redirect* requests hit `<account>.dkr.ecr.<region>.amazonaws.com`.
- Blob `GET`s return 307 redirects to `prod-<region>-starport-layer-bucket.s3.<region>.amazonaws.com`. The actual bytes come from S3.
- Critical: **S3 does not impose per-stream throttling** the way Docker Hub's CDN does. A single S3 GET can saturate a connection.

**Practical knobs:**

- **Chunks per layer (N) is mostly unnecessary.** S3 single-stream throughput is already high. Default `N=1`. Bump to `N=4` only for layers >500 MB or when zot runs in a different region than the ECR registry.
- **Parallel layers (M) is the dominant win.** Default `M=8`. AWS does not 429 individual ECR pulls at reasonable concurrency.
- **VPC endpoints flatten the network path.** If zot runs in a VPC, configure interface endpoints for `com.amazonaws.<region>.ecr.api` and `com.amazonaws.<region>.ecr.dkr`, plus the gateway endpoint for `com.amazonaws.<region>.s3`. Eliminates internet egress, drops latency to single-digit ms, and saves on data-transfer costs.
- **Same-region deployment dwarfs every HTTP-level optimization.** Zot in `us-east-1` pulling from `us-east-1` ECR will outperform one in `eu-west-1` by 5–10× regardless of window tuning. Document this explicitly in operator guidance.
- **Rate limits are account-level, not per-stream.** ECR throttles at thousands of pulls/sec/region (per published service quotas). Single-tenant proxies are nowhere near; multi-tenant ones may need a configurable per-upstream concurrency cap.
- `ThrottlingException` (HTTP 400) responses indicate API throttling on the *manifest* path, not the S3 blob path. Back off only on the manifest request.

#### Harbor

**Auth flow:**

- Harbor implements the standard Docker registry token-auth flow: `WWW-Authenticate: Bearer realm="...", service="...", scope="..."` → fetch a JWT from `/service/token` → use as `Authorization: Bearer ...` on subsequent requests.
- Tokens are short-lived (typically 30 min). Cache per `(harbor host, repo, scope)` until expiry. Single-flight refresh as in §5.9.
- For mTLS-protected Harbor instances, use `rustls`-based client certs configured per upstream.

**Topology depends on Harbor's storage backend:**

- **Filesystem storage (default for small deployments):** blob `GET`s stream directly through Harbor's nginx → registry → filesystem. No redirect. The Rust client just streams the response.
- **S3 / OSS / Azure Blob backend:** behavior depends on Harbor's `redirect.disable` setting. By default Harbor returns a 307 to a presigned object-store URL (same shape as ECR). With redirects disabled, Harbor proxies bytes through itself.
- The pull-through producer must transparently follow redirects on blob `GET` regardless of mode. Don't assume.

**Practical knobs:**

- **No per-stream throttling.** Same as ECR: parallel layers (M) is the dominant lever; chunked range fetches (N>1) rarely help.
- **Connection pooling matters most.** A typical Harbor sits behind a single nginx — keep-alive on 2 HTTP/2 connections gets you near link-rate.
- **Network-local deployment.** Harbor is usually inside the corporate network or VPC, so RTT is already small. The flow-control-window tuning matters less here than for cross-region ECR.

**Harbor-specific gotchas:**

- Harbor sometimes returns the Docker `manifest.v2+json` content-type even when the client `Accept`s OCI variants. The Rust client must accept all manifest media types — non-issue for digest-addressed sync but worth handling.
- Harbor with vulnerability scanners (Trivy/Clair) can delay first-pull of a freshly pushed image while the image is being scanned. Surface in the upstream-duration metric so operators can correlate.
- Harbor's replication can itself be a pull-through cache from another upstream — when zot proxies a Harbor that proxies Docker Hub, total cold-pull latency includes Harbor's replication delay. Visible only via timing metrics.

#### Recommended defaults — ECR vs. Harbor vs. Docker Hub

| Knob | ECR | Harbor | Docker Hub (ref) |
|---|---|---|---|
| Parallel layers (M) | 8 | 6 | 6 |
| Chunks per layer (N) | 1 (4 cross-region or >500 MB) | 1 | 4 for >100 MB |
| Connections per upstream (C) | 2 (3-4 cross-region) | 2 | 2 |
| HTTP/2 initial stream window | 16 MB | 16 MB | 16 MB |
| HTTP/2 initial connection window | 64 MB | 64 MB | 64 MB |
| Concurrency cap per upstream | 32 | 32 | 16 |
| Auth-token refresh-before | 30 min (12 h TTL) | 5 min (~30 min TTL) | 5 min |
| Cache 307 redirect target | yes (S3 host) | yes if redirect-mode | yes (CDN host) |

ECR's dominant cost driver is **region locality + auth-token caching**, not stream-level concurrency tricks. Harbor's is **connection keep-alive + token caching**. Both want a smaller `N` and larger `M` than the Docker Hub baseline.

#### Phase-5 deliverable adjustments for ECR + Harbor

- ECR is consumed via the standard HTTP Basic upstream-auth code path; no AWS SDK dependency. Document the `aws ecr get-login-password` flow in the operator guide.
- Per-upstream config struct includes `kind: "ecr" | "harbor" | "generic"` so connection/auth/redirect behavior can be selected without operator-tuned knobs in the common case.
- Document: same-region zot deployment, VPC endpoints, and IRSA/instance-profile auth in the operator guide.

### 5.13 Expected upstream-leg speedup over Go zot

| Optimization | Effect |
|---|---|
| HTTP/2 multiplexed client + keep-alive | Eliminates per-request handshake (~50–200 ms over TLS); biggest win on Docker Hub |
| Pre-warmed connection + TLS resumption | First-pull no longer pays handshake cost |
| Token caching + single-flight refresh | Eliminates token-server RTT on every blob (~20–100 ms saved per blob) |
| Skip HEAD-before-GET | Saves one RTT per blob |
| Larger HTTP/2 windows | Single-stream throughput on high-BDP links goes from ~window/RTT to link-rate |
| Parallel range fetches | 2–4× single-layer throughput on long-RTT or rate-limited per-stream paths |
| Conditional manifest GET | Tag-revalidation transfers go from ~few KB to ~0 bytes |

---

## 6. Phased Implementation Plan

Four small phases targeting a runnable laptop registry in roughly four weeks. Each phase ships something usable.

### Phase 1: Local CAS server with TLS + auth (week 1)

Goal: a binary that `docker pull` against `localhost:5000` can fetch images from, given the blobs already exist on local disk. No upstream interaction yet, no push yet — but TLS and credentials are wired in from day one because they affect every subsequent endpoint.

- `axum` + `tokio` workspace; `quill serve` binary
- Endpoints: `GET /v2/`, `GET/HEAD /v2/<name>/manifests/<ref>`, `GET/HEAD /v2/<name>/blobs/<digest>`, `GET /v2/<name>/tags/list`
- `LocalStorage` driver over zot-compatible CAS layout (`<root>/<repo>/blobs/sha256/<digest>`)
- `tokio::fs::File` streaming response bodies
- `DashMap` blob metadata cache
- **TLS via `rustls`** — config-supplied cert + key, or self-signed cert auto-generated on first run (§3.5)
- **Server-side credentials** — htpasswd-file basic auth + bearer-token-via-`/v2/token` flow, both compatible with zot's auth model (§3.5)
- TOML config: bind addr, cache root, TLS, htpasswd path, upstreams (parsed but unused this phase)

**Validation:** pre-populate cache root via `skopeo copy --src ECR --dst dir:./cache/...`; `docker login localhost:5000`, `docker pull localhost:5000/<image>`, succeeds over TLS.

### Phase 2: Push path (week 2)

Local push enables the patching workflow (§5.6). No upstream involvement; pushes land on the laptop.

- `POST /v2/<name>/blobs/uploads/` — initiate session, return `Location: /v2/<name>/blobs/uploads/<session>`
- `PATCH /v2/<name>/blobs/uploads/<session>` — append chunk to session tempfile; verify `Content-Range`
- `PUT /v2/<name>/blobs/uploads/<session>?digest=<sha256:...>` — finalize: verify digest, atomic rename into CAS
- `PUT /v2/<name>/manifests/<ref>` — verify referenced blobs exist, write manifest to CAS, atomic update of `_local_tags.json` if `<ref>` is a tag
- `DELETE /v2/<name>/blobs/<digest>` and `DELETE /v2/<name>/manifests/<ref>` — remove from CAS / `_local_tags.json`
- Resumable upload session state in `<root>/<repo>/_uploads/<session>` with metadata sidecar
- `parking_lot::RwLock` per repo on the write path; reads stay lock-free
- Local-tag tracking: `<root>/<repo>/_local_tags.json` sidecar; in-memory `DashMap<(Repo, Tag), LocalTagMeta>` (§5.6)

**Validation:** `docker push localhost:5000/mycorp/myimage:v1` succeeds; pull of same tag returns the locally-pushed content.

### Phase 3: Streaming pull-through cache (week 3)

The performance headline. Cache miss → simultaneous stream-and-cache from upstream.

- `PullThroughTable: DashMap<Digest, Arc<PullThroughEntry>>` with single-flight coalescing
- Producer task: upstream `GET` → tempfile append + sha256 hash, `tokio::sync::Notify` high-water-mark progress
- `PullThroughBody` consumer: tails tempfile up to high-water mark, awaits notify
- Atomic tempfile-to-CAS rename on digest match; unlink on mismatch
- Manifests: full-buffer fetch, verify, persist, serve (no streaming)
- Startup sweep of orphaned `_uploads/` tempfiles older than 24h
- Upstream HTTP/2 client (`hyper`) per upstream:
  - keep-alive, TLS session resumption
  - **`initial_stream_window_size = 16 MB`, `initial_connection_window_size = 64 MB`** (single biggest perf knob)
  - skip HEAD-before-GET
  - retries with jittered backoff on 5xx/429
- ECR auth: standard HTTP Basic with username `AWS` and password supplied via config (refreshed externally via `aws ecr get-login-password` every ~12h)
- Harbor auth: standard bearer-token flow against `/service/token`, cached per `(host, repo, scope)` until expiry
- Local-precedence tag resolution (§5.6): tags in `_local_tags.json` are served from local, *never* revalidated against upstream

**Validation:** `docker pull localhost:5000/<image-not-yet-cached>` completes in roughly the same wall-clock as `docker pull <upstream>/<image>`; pulled-then-patched workflow round-trips correctly (pull `library/redis:7.2`, push `mycorp/redis:7.2-patched`, both pullable, patched tag never re-checks upstream).

### Phase 4: Polish + GC (week 4, optional)

- Tag-based manifest revalidation for non-local tags: HEAD upstream past TTL, refresh-or-fetch (§5.6)
- Conditional manifest GET (`If-None-Match`)
- `GET /v2/<name>/tags/list` merge view — local + cached-upstream + (past TTL) live upstream
- Range requests on cache-hit path (HTTP 206)
- Manifest-and-config prefetch: when a tag resolves, speculatively GET the config blob in parallel
- **Mark-and-sweep garbage collection:** mark = walk all manifests in `index.json` files plus all entries in `_local_tags.json`; sweep = delete unreferenced blobs. Necessary because `rm -rf cache/` would now delete locally-pushed patches. Triggered manually via `quill gc` or automatically on disk-pressure.
- `quill cache rm <repo>`, `quill cache du`, `quill gc` CLI subcommands
- `tracing` to stderr with structured fields
- README with config samples for Docker Desktop, colima, containerd registry-mirror config

---

## 7. Expected Performance Gains (single-user laptop)

The original benchmark targets in this section assumed a multi-tenant production deployment on NVMe + 10 GbE. For a laptop pulling from ECR/Harbor over residential or office WiFi, the bottleneck is almost always the network, and the user-visible wins look different.

| Scenario | Go zot (proxy mode) | Quill | What the user feels |
|---|---|---|---|
| **First pull of an uncached image** | `~2× upstream pull time` (sequential cache-then-serve) | `~1× upstream pull time` (streaming pull-through) | Halves the worst-case wait |
| **First pull, default HTTP/2 windows on high-RTT link** | ~640 KB/s ceiling per stream regardless of bandwidth | Link-rate after window tuning (§5.10) | Order-of-magnitude difference on a 100 ms RTT link |
| **Repeat pull of cached image** | Local FS read | Local FS read with `DashMap` metadata cache | Both fast; Quill saves ~one syscall per blob |
| **`docker pull` of an image with N layers, none cached** | Sequential cache fill, then concurrent serve | M parallel upstream layer fetches, simultaneous fan-out to client | First pull is dramatically faster |
| **`docker pull` again while previous pull still streaming** | New upstream fetch (no coalescing) | Joins existing in-flight entry (single-flight) | Avoids redundant upstream traffic |
| **ECR auth round-trip cost** | Per-pull SigV4 token mint via `containers/image` | Token supplied via config (refreshed externally every 12h); no per-request token mint | Eliminates ~50–200 ms per pull |

The honest summary: on a fast home connection, repeat pulls are local-disk-fast (microseconds) and first pulls are bounded by upstream bandwidth × RTT × HTTP/2 window — Quill's job is to never *add* latency on top of that.

---

## 8. Compatibility

### 8.1 Storage layout

Quill uses zot's CAS directory layout so an existing zot cache directory is readable as-is, and so a future migration in either direction is trivial:

```
<root>/
  <repo>/
    blobs/
      sha256/
        <digest>
    index.json
  _uploads/        # in-flight pull-through tempfiles (Quill-specific, swept on startup)
```

### 8.2 Config

Quill uses a small TOML config — not zot's JSON. The two configs solve different scopes (laptop cache vs. production registry); cross-compatibility is not a goal.

### 8.3 API

The pull subset of the OCI Distribution Spec v1.1 (`GET /v2/`, `GET/HEAD /v2/<name>/manifests/<ref>`, `GET/HEAD /v2/<name>/blobs/<digest>`, `GET /v2/<name>/tags/list`). Sufficient for `docker pull`, `containerd`, `skopeo copy --src docker://...`, `crane pull`. Push, catalog, referrers, and other endpoints are out of scope (§3.4).

---

## 9. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Pull-through tempfile orphans on crash | Startup sweep removes any `_uploads/` tempfile older than 24h |
| Digest mismatch from upstream surfaces mid-stream | HTTP/2 RST_STREAM (clients retry); HTTP/1.1 connection close. Acceptable because committing a corrupt blob is worse. |
| Cache disk usage grows unbounded | Manual `quill cache rm <repo>` and `quill cache du`; an automated GC is out of scope (§3.4) |
| Upstream auth-token expiry mid-pull | Background refresh ~5–30 min before expiry; on 401 mid-stream, retry once with a fresh token |
| ECR cross-region latency dominates pull time | Operator guidance: deploy Quill on a laptop in the same continent as the AWS region, prefer `nearest` ECR replica, accept that physics is physics |
| Manifests change upstream (mutable tags) | Tag-based requests revalidate via HEAD when in Phase 3 polish; until then, manifest cache TTL bounds staleness |
