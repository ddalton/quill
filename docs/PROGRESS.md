# Quill Implementation Progress

This file tracks where we are in the plan defined in `PLAN.md` so implementation can restart cleanly across sessions or machines.

**Last updated:** 2026-04-28 (Phase 3 streaming pull-through working end-to-end against Docker Hub)

---

## Plan summary (4 phases, see PLAN.md §6)

| Phase | Goal | Status |
|---|---|---|
| 1 | Local CAS server with TLS + auth | ✅ **Complete** |
| 2 | Push path with `_local_tags.json` | ⬜ **Not started** |
| 3 | Streaming pull-through cache + upstream | ✅ **Complete** |
| 4 | Polish + GC | ⬜ **Not started** |

---

## Phase 1 — Local CAS server + TLS + auth — DONE

Verified end-to-end on 2026-04-28: `cargo build --workspace` clean, `cargo test --workspace` 9/9 passing, `quill serve` binds `127.0.0.1:5443` over HTTP/2 with auto-generated self-signed cert, blobs round-trip correctly out of pre-populated CAS. Multi-segment repo names handled via catch-all + manual suffix split.

What's in place:

- 8-crate workspace: `quill-server`, `quill-config`, `quill-storage`, `quill-tls`, `quill-auth`, `quill-pullthrough`, `quill-upstream`, `quill-registry`
- `quill serve --config quill.toml` binary with `clap` CLI
- TOML config with bind addr, TLS, htpasswd, storage root, upstream array (parsed but not yet used)
- Non-localhost binds without explicit TLS rejected at config-load
- `rustls` server config; PEM cert+key from config or self-signed bootstrap under `<storage.root>/_quill/`
- htpasswd basic-auth (zot-compatible bcrypt format) with successful-auth result cache
- Tower middleware `AuthLayer` (no-op when htpasswd absent — laptop-friendly default)
- `LocalStorage` over zot-compatible CAS layout (`<root>/<repo>/blobs/sha256/<hex>`)
- `BlobMetaCache` (DashMap, TTL-based) — eliminates `stat()` per repeat request
- `LocalTagsStore` with sidecar `_local_tags.json` (load + set + remove + atomic write); already loaded at startup, unused until Phase 2
- `Digest` parser/validator (sha256-only)
- `RegistryError` envelope matching OCI Distribution Spec
- Routes: `GET /v2/`, `GET/HEAD /v2/<repo>/blobs/<digest>`, `GET/HEAD /v2/<repo>/manifests/<digest|tag>`, `GET /v2/<repo>/tags/list`
- HTTP/2 + HTTP/1.1 auto-detection via `hyper-util::server::conn::auto::Builder`
- `PullThroughTable` + `PullThroughEntry` scaffolding (in-flight digest table for single-flight coalescing) — types in place, producer task TBD
- 9 unit tests passing across crates

Manual smoke commands (already verified):
```
/Users/ddalton/github/quill/target/debug/quill serve --config /tmp/quill-smoke/quill.toml
curl -sk -I https://127.0.0.1:5443/v2/                                          # 200
curl -sk -I https://127.0.0.1:5443/v2/myorg/team/img/blobs/sha256:e3b0...855   # 404 BLOBUNKNOWN
curl -sk -I https://127.0.0.1:5443/v2/myorg/team/img/blobs/sha256:abc          # 400 DIGESTINVALID
curl -sk    https://127.0.0.1:5443/v2/myorg/team/img/tags/list                 # {"name":"...","tags":[]}
```

---

## Phase 2 — Push path + `_local_tags.json` — NOT STARTED

What needs to happen (PLAN.md §6 Phase 2 + §5.6).

### Order of work (intended)

1. **Upload session storage (`quill-storage::uploads`)** — new module.
   - `UploadSession { id, repo, tempfile_path, hasher_state, bytes_received, started_at, last_seen }` persisted as `<root>/<repo>/_uploads/<session>.meta.json` with the streamed bytes at `<root>/<repo>/_uploads/<session>.data`.
   - sha256 hasher state must be resumable across PATCH calls. Keep the running `Sha256` in memory only for now (resumable state is a Phase 4 polish; on restart, abandoned sessions are swept).
   - Methods: `create_session(repo) -> SessionId`, `append(repo, session, bytes, optional_content_range) -> bytes_received`, `finalize(repo, session, expected_digest) -> ()` (atomic rename to CAS), `abort(repo, session)`, `expire_old(threshold)`.
2. **Per-repo write lock (`quill-storage::local`)**.
   - `DashMap<String, parking_lot::RwLock<()>>` for serializing manifest writes per-repo. Read path remains lock-free.
3. **Push routes (`quill-registry::routes`)**.
   - Add to the `dispatch` action enum: `BlobUploadInit`, `BlobUploadPatch(session)`, `BlobUploadPut(session)`, `ManifestPut(reference)`, `BlobDelete(digest)`, `ManifestDelete(reference)`.
   - `POST /v2/<name>/blobs/uploads/` — call `create_session`, return 202 + `Location: /v2/<name>/blobs/uploads/<session>` + `Range: 0-0` + `Docker-Upload-UUID: <session>`.
   - `PATCH /v2/<name>/blobs/uploads/<session>` — body is the chunk; respect `Content-Range` if present; respond 202 + `Range: 0-<bytes_received-1>`.
   - `PUT /v2/<name>/blobs/uploads/<session>?digest=<sha256:...>` — final chunk in body (may be empty), verify digest, atomic rename. Respond 201 + `Location: /v2/<name>/blobs/<digest>` + `Docker-Content-Digest`.
   - `PUT /v2/<name>/manifests/<reference>` — verify all referenced blobs exist locally; write manifest to CAS via `put_blob_buffered`; if `<reference>` is a tag (not a digest), call `LocalTagsStore::set(repo, tag, digest)`. Respond 201 + `Location` + `Docker-Content-Digest`.
   - `DELETE /v2/<name>/blobs/<digest>` — remove from CAS, invalidate metadata cache. Respond 202.
   - `DELETE /v2/<name>/manifests/<reference>` — if tag, call `LocalTagsStore::remove`; if digest, remove the manifest blob and any tags pointing at it. Respond 202.
4. **Update `dispatch` to recognize `/blobs/uploads/...` paths.** The current `split()` helper handles `/blobs/<digest>`, `/manifests/<ref>`, and `/tags/list` — extend to also recognize `/blobs/uploads/` (init) and `/blobs/uploads/<session>` (patch/put).
5. **Tags-list merge**.
   - Update `list_tags` to merge `_local_tags.json` entries with `UpstreamTagCache` entries; local tags shadow same-named upstream tags. Phase 4 polish adds live upstream `tags/list` pass-through.
6. **Sweep orphaned upload sessions on startup**.
   - In `quill-server::main::serve`, after building storage, call `storage.sweep_uploads(Duration::from_secs(86_400))` to clean up sessions older than 24h. Same approach as Phase 3 tempfile sweep (currently TODO too).
7. **Tests** — at least:
   - Unit: `LocalTagsStore::set` is observable through `get` and persists across `LocalTagsStore::load_repo`.
   - Integration: full push round-trip (init → patch → put → manifest put), then pull of same tag returns the just-pushed bytes byte-for-byte.
8. **Smoke script** (`scripts/smoke-push.sh`) — pushes a tiny made-up image (single layer = arbitrary bytes, single config blob, single manifest) using raw `curl` calls, then pulls it back to verify byte-identity. Models the `docker push` flow without needing a Docker daemon.

### Validation target

`docker push localhost:5443/mycorp/myimage:v1` succeeds; subsequent pull of the same tag returns the locally-pushed content; pull of `mycorp/myimage:v2` (not pushed locally) falls through to upstream if a matching upstream is configured. Smoke script in step 8 must pass.

### Watch-outs

- Don't introduce a per-request `RwLock` on the read path. Reads must stay lock-free (the §3.4 promise). The lock is only acquired during PUT/DELETE.
- Resumable upload session sha256 state needs care: sha256 *can* be serialized via `Sha256::clone()` + manual state extraction, but not portably. For now keep sessions in-memory only and accept that quill restart abandons in-flight uploads; document this.
- Locally-pushed-tag precedence (PLAN.md §5.6) is the *whole point* of Phase 2 — make sure the resolution order in `get_manifest` checks `_local_tags.json` *before* `UpstreamTagCache` and *before* upstream live fetch. The current routes.rs already does this.

---

## Phase 3 — Streaming pull-through cache + upstream — DONE

Verified end-to-end against Docker Hub (anonymous bearer-token flow) on 2026-04-28:

```
=== first GET (cache miss → upstream) ===
status=200 size=2482 time=0.038s
=== second GET (warm — local CAS) ===
status=200 size=2482 time=0.003s
=== third GET (warm) ===
status=200 size=2482 time=0.003s
```

Server log confirms zero upstream contacts on warm pulls; cold pull traverses the full bearer-token discovery → blob stream → producer → tempfile → atomic-rename-to-CAS path; subsequent pulls hit `BlobMetaCache` and skip even the `stat()` syscall.

What's in place:

- **`quill-upstream`**: `reqwest` client with tuned HTTP/2 windows (`16 MB` stream / `64 MB` connection), keep-alive 5 min, ALPN h2, retry-once-on-401-with-bearer-discovery, in-memory `BearerCache` keyed by `(realm, scope)` with TTL from `expires_in`. Auth modes: `Anonymous` and `Basic`. ECR is supported via `Basic` (username `AWS`, password from `aws ecr get-login-password`), so no SigV4-specific code is needed.
- **`quill-pullthrough`**: producer task that streams upstream → tempfile, hashing as it goes, fsyncs, verifies digest, atomic-renames into CAS. `PullThroughBody` Body impl that tails the tempfile up to the high-water mark, awaiting `progress.notified()` when caught up, terminating on producer outcome. `PullThroughTable` single-flight in-flight registry.
- **`quill-registry`**: blob/manifest miss → upstream pull-through wiring. Manifest path: digest-addressed and tag-addressed both supported; tag-addressed manifests cache `(repo, tag) → digest` in `UpstreamTagCache` (5 min TTL by default). Locally-pushed tags still take precedence (Phase 2 will activate the push side). `repo_prefix` is now purely a routing selector — repo names pass through verbatim to upstream.
- **`quill-server`**: `UpstreamRouter::build` constructs clients at startup, fails fast on bad config; tag cache is wired into `RegistryState`.

Sample working config:

```toml
[[upstream]]
name = "docker-hub"
url = "https://registry-1.docker.io"
kind = "generic"
repo_prefix = "library/"
# no [auth] block = Anonymous; bearer-token discovery happens automatically on 401
```

13 unit tests passing (added 2 in `quill-upstream::auth`).

### Possible later improvements (not blocking)

- Path translation (e.g. `mycorp/foo` → `account-id/foo` on ECR). Currently `repo_prefix` is route-only; if path translation is needed, add a `repo_translate` config field.
- Pre-fetch on config load (warm the connection pool before first request).
- Single-flight token refresh (currently each cold cache lookup may race the same token mint).
- Helper script / sample systemd unit for refreshing the ECR `aws ecr get-login-password` token every ~12 hours and reloading quill.

---

## Phase 4 — Polish + GC — NOT STARTED

What needs to happen (PLAN.md §6 Phase 4):

- Tag freshness TTL revalidation for non-local cached tags (HEAD upstream past TTL, refresh-or-fetch — §5.6)
- Conditional manifest GET (`If-None-Match`)
- Range requests on cache-hit path (HTTP 206)
- Manifest-and-config prefetch on tag resolution
- `_upstream_tags.json` persistence (currently in-memory only) — optional
- **Mark-and-sweep GC**: walk all manifests in `index.json` + `_local_tags.json` to mark; sweep unreferenced blobs. Required because `rm -rf cache/` would now nuke locally-pushed patches.
- `quill cache rm <repo>`, `quill cache du`, `quill gc` CLI subcommands
- Bearer token /v2/token endpoint (server-side JWT issuance for clients connecting *to* Quill)
- README with Docker Desktop / colima / containerd registry-mirror snippets

---

## Build & run from a fresh checkout

```sh
cd /Users/ddalton/github/quill
cargo build --workspace
cargo test --workspace

cp quill.example.toml quill.toml
# edit quill.toml — set storage.root, optional [http.tls], optional [http.auth.htpasswd]

./target/debug/quill serve --config quill.toml
```

Default: binds `127.0.0.1:5000` (or whatever `[http].address` is set to), generates a self-signed cert under `<storage.root>/_quill/` if `[http.tls]` is omitted.

---

## Gotchas / non-obvious decisions in the code

These tripped me up during implementation; capture them so a future session doesn't re-trip:

1. **`repo_prefix` is route-only, NOT a path-rewrite.** It selects which configured upstream handles a repo (longest-prefix match), but the full repo name is forwarded verbatim to the upstream. Originally I stripped it, which broke Docker Hub: `library/redis` became `redis` upstream, and the `redis` repo doesn't exist as a non-namespaced name. If you ever need real path translation (e.g. `mycorp/foo` on Quill → `account-id/foo` on ECR), add a separate `repo_translate` config field.

2. **Multi-segment repo names require a catch-all route + manual suffix split.** Axum's `:name` path param does not capture `/`, so `/v2/:name/blobs/:digest` only matches single-segment repos like `redis`, never `library/redis`. The fix is in `quill-registry/src/routes.rs`: one `/v2/*rest` route that calls `split()` to find the action suffix (`/blobs/<digest>`, `/manifests/<ref>`, `/tags/list`).

3. **Use `hyper-util::server::conn::auto::Builder`, not `hyper::server::conn::http1::Builder`.** ALPN advertises h2 via the rustls config, and `curl --http2` (and Docker, containerd) will negotiate it. With the http1-only builder, those clients hang because the server speaks the wrong protocol on the negotiated connection. Switching to the auto builder transparently handles both HTTP/1.1 and HTTP/2.

4. **The TLS self-signed cert is generated once at first run and persisted at `<storage.root>/_quill/self-signed.{crt,key}`.** Subsequent runs reuse it. If you change the bind address, the SAN list (`localhost`, `127.0.0.1` only) might not cover it — delete the files to regenerate.

5. **Manifests are persisted to CAS via `put_blob_buffered` keyed by their `Docker-Content-Digest`.** That digest comes from the upstream's response header; if the header is missing we fall back to a self-computed sha256 of the bytes. Tag-addressed requests cache the `(repo, tag) → digest` resolution in `UpstreamTagCache` (in-memory only, 5 min TTL) — the manifest itself lives at `blobs/sha256/<hex>` like any other blob.

6. **The pull-through producer task is spawned from inside the request handler with `tokio::spawn`.** The handler immediately returns a `PullThroughBody` to the client; the producer keeps running even if the client disconnects, so the cache fill always completes (or fails cleanly with tempfile cleanup). This is the §5 design property "decoupled from client lifecycle."

7. **`reqwest`'s `http2_initial_stream_window_size` and `http2_initial_connection_window_size` need explicit `Some(...)`.** Easy to miss because the methods take `Option<u32>`. Defaults are 64 KB; without bumping these to ~16 MB / 64 MB, single-stream throughput stalls on high-RTT links regardless of bandwidth (this is *the* perf knob from PLAN.md §5.10).

8. **Test the auth bearer flow can fail silently against a non-matching scope.** When ECR / Harbor / Docker Hub return a 401 with a Bearer challenge, the token they mint is scoped to whatever repo path was in the request URL. If we strip or rewrite the repo path between the original request and the token-fetch, the token is for the wrong scope and the retry returns 401 again. (This is the same bug class as #1.)

## Decisions captured for future-me

- **No S3 storage backend.** Single-user laptop, local FS only (PLAN.md §3.4). May revisit if multi-replica deployment ever happens.
- **No `sendfile(2)`.** Network is the laptop bottleneck; userspace memcpy isn't. `tokio::fs::File` streaming via `ReaderStream` is enough.
- **No Prometheus/OTel.** `tracing` to stderr is the observability story.
- **No mTLS, LDAP, OIDC, OAuth2 federation.** htpasswd basic + bearer JWT (planned for Phase 4).
- **No parallel-chunks-per-layer (`N>1`).** ECR/Harbor don't throttle per-stream like Docker Hub does.
- **One HTTP/2 connection per upstream (`C=1`).** Multi-connection pooling is multi-tenant hardening.
- **`onlySyncOnMissing` is *not* a config flag.** It's the unconditional default behavior — locally-pushed tags always win (PLAN.md §5.6).
- **`SyncTagList`** (zot terminology) becomes a Phase 4 item: `tags/list` merges local + cached-upstream + (past-TTL) live upstream.

---

## Where to look first when restarting

1. Read `docs/PLAN.md` (full design).
2. Read this file (`docs/PROGRESS.md`) to find the current cursor.
3. `cargo build --workspace && cargo test --workspace` to confirm baseline.
4. Pick up from the **Order of work** list under whichever phase is "in progress."
