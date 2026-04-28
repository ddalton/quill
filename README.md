# quill

Single-user, high-performance OCI registry for a developer laptop.

Quill is a pull-through cache in front of upstream registries (initially ECR
and Harbor) plus a local push target for patched images and charts. A
locally-pushed tag takes precedence over upstream — pulls of that exact tag
are never re-checked against upstream. See [docs/rust-rewrite-plan.md](../zot/docs/rust-rewrite-plan.md)
in the sibling zot repo for the full design.

## Status

**All 4 phases of the plan complete.** Local CAS server with TLS, htpasswd
auth, full push path with locally-pushed-tag precedence, streaming pull-through
cache against any standard OCI registry, tag-revalidation TTL, conditional
manifest GET, mark-and-sweep GC, and CLI subcommands for cache management.

33 automated tests + 2 smoke scripts, all passing.

Verified end-to-end:
- Cold pull from Docker Hub: stream-while-cache, ~390 ms.
- Warm pull: ~3 ms (local CAS, zero upstream traffic).
- Push: full init → patch → put → manifest-put round-trip; pull-back is
  byte-identical; manifests referencing missing blobs are rejected.
- Local-tag precedence: a tag pushed to `library/redis:7-patched` is served
  locally with zero upstream contact, while `library/redis:7-alpine` falls
  through to Docker Hub as expected.
- Stale tag revalidation: past TTL, HEAD upstream; if digest matches, refresh
  TTL and serve local; if it differs, fetch new manifest.
- Conditional GET: `If-None-Match: <digest>` returns `304 Not Modified` on match.
- GC: `quill gc --dry-run` reports orphans; `quill gc` deletes them.

See [docs/PROGRESS.md](docs/PROGRESS.md) for detailed status.

## Build and run

```sh
cargo build --release
cp quill.example.toml quill.toml  # edit cache root, address, etc.
./target/release/quill serve --config quill.toml
```

By default, quill binds `127.0.0.1:5000` and auto-generates a self-signed cert
under `<storage.root>/_quill/` on first run. Non-localhost binds require an
explicit `[http.tls]` config block.

## Smoke tests

After `cargo build`, run either smoke script:

```sh
./scripts/smoke-pullthrough.sh   # Phase 3: cold + warm pull through Docker Hub
./scripts/smoke-push.sh          # Phase 2: full push round-trip + pull-back
```

Expected: cold pull ~30–400 ms (network), warm pull ~3 ms (local CAS, zero
upstream traffic). Push smoke verifies byte-identical round-trip and
`_local_tags.json` correctness.

## Workspace layout

```
crates/
  quill-server/        # binary; wires everything together
  quill-config/        # TOML config, validation
  quill-storage/       # local FS CAS, blob meta cache, _local_tags.json
  quill-tls/           # rustls config, self-signed bootstrap
  quill-auth/          # htpasswd basic auth + tower middleware
  quill-pullthrough/   # in-flight table for streaming cache fills
  quill-upstream/      # upstream client trait + repo→upstream router
  quill-registry/      # axum routes, OCI error envelope
```

## What works today (Phase 1)

- `GET /v2/` — version check
- `GET / HEAD /v2/<repo>/manifests/<digest-or-tag>` — serves from local CAS;
  tag names look up `_local_tags.json` first
- `GET / HEAD /v2/<repo>/blobs/<digest>` — streams from local CAS
- `GET /v2/<repo>/tags/list` — local-pushed tags only
- TLS via `rustls`; PEM cert+key from config or self-signed fallback
- Basic auth via zot-compatible htpasswd file (bcrypt)
- Multi-segment repo names (e.g. `myorg/team/img`) handled correctly

## Working today

**Pull-through cache** against any OCI registry (Docker Hub, Harbor, generic).
- Cold pull = stream-while-cache (PLAN.md §5).
- Warm pull = local CAS read with `DashMap` metadata cache (no `stat()` syscall).
- Bearer-token discovery on 401 (Docker Hub anonymous works out of the box).
- HTTP basic auth for Harbor and ECR (ECR uses username `AWS` + the 12-hour token from `aws ecr get-login-password`).
- Tuned HTTP/2 flow-control windows (16 MB stream / 64 MB connection).

**Push path** (the patching workflow).
- `POST → PATCH → PUT` blob upload sessions; `PUT` manifest by tag/digest.
- Locally-pushed tags take precedence over upstream — never re-checked against upstream while local copy exists.
- Manifest validation: rejects pushes that reference missing blobs.

**TLS + auth**.
- TLS via `rustls`, self-signed cert auto-generated for localhost.
- Optional htpasswd basic auth on the client-facing side (zot-compatible bcrypt).

**Cache management.**
- Tag freshness TTL with HEAD-revalidation (5 min default).
- Conditional GET (`If-None-Match` → 304).
- `quill du`, `quill gc [--dry-run]`, `quill cache-rm <repo>` CLI subcommands.
- Mark-and-sweep GC walks `_local_tags.json` roots, follows manifests, sweeps unreferenced blobs.
