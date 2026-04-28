# quill

Single-user, high-performance OCI registry for a developer laptop.

Quill is a pull-through cache in front of upstream registries (initially ECR
and Harbor) plus a local push target for patched images and charts. A
locally-pushed tag takes precedence over upstream — pulls of that exact tag
are never re-checked against upstream. See [docs/rust-rewrite-plan.md](../zot/docs/rust-rewrite-plan.md)
in the sibling zot repo for the full design.

## Status

**Phases 1 + 3 complete.** Local CAS server with TLS, htpasswd auth, and
streaming pull-through cache against any standard OCI registry (anonymous +
bearer + HTTP basic). Verified end-to-end against Docker Hub; cold pulls
stream-while-cache, warm pulls serve from local CAS in ~3 ms.

Phase 2 (push path) and Phase 4 (GC + tag-revalidation TTL) are next. See
[docs/PROGRESS.md](docs/PROGRESS.md) for detailed status.

## Build and run

```sh
cargo build --release
cp quill.example.toml quill.toml  # edit cache root, address, etc.
./target/release/quill serve --config quill.toml
```

By default, quill binds `127.0.0.1:5000` and auto-generates a self-signed cert
under `<storage.root>/_quill/` on first run. Non-localhost binds require an
explicit `[http.tls]` config block.

## Smoke test

After `cargo build`, run the pull-through smoke script:

```sh
./scripts/smoke-pullthrough.sh
```

Expected: cold pull from Docker Hub takes ~30–400 ms (network bound),
subsequent warm pulls take ~3 ms (local CAS hit, zero upstream traffic).

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

- Pull-through cache against any OCI registry (Docker Hub, Harbor, generic).
- Cold pull = stream-while-cache (PLAN.md §5).
- Warm pull = local CAS read with `DashMap` metadata cache (no `stat()` syscall).
- Bearer-token discovery on 401 (Docker Hub anonymous works out of the box).
- HTTP basic auth for Harbor and ECR (ECR uses username `AWS` + the 12-hour token from `aws ecr get-login-password`).
- Tuned HTTP/2 flow-control windows (16 MB stream / 64 MB connection).
- TLS via `rustls`, self-signed cert auto-generated for localhost.
- Optional htpasswd basic auth on the client-facing side.

## What's coming

- Phase 2: push path (`POST/PATCH/PUT /v2/.../blobs/uploads/`, manifest PUT,
  `_local_tags.json` updates on tag push) — required for the patching workflow.
- Phase 4: tag revalidation TTL, conditional manifest GET, mark-and-sweep GC,
  CLI subcommands (`quill cache du`, `quill gc`).
