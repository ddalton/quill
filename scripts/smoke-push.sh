#!/usr/bin/env bash
# Smoke-test for Phase 2 push path.
#
# Pushes a minimal hand-rolled image (one config blob + one layer + one
# manifest) into quill, then pulls it back to verify byte identity. Exercises
# the full OCI Distribution push flow without needing Docker:
#
#   POST   /v2/<repo>/blobs/uploads/         (init)
#   PATCH  /v2/<repo>/blobs/uploads/<id>     (chunk)
#   PUT    /v2/<repo>/blobs/uploads/<id>?digest=<sha256>  (finalize)
#   PUT    /v2/<repo>/manifests/<tag>        (push manifest)
#   GET    /v2/<repo>/manifests/<tag>        (pull-back-by-tag)
#   GET    /v2/<repo>/blobs/<digest>         (pull-back-by-digest)
#   GET    /v2/<repo>/tags/list              (verify tag is listed)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
QUILL_BIN="${QUILL_BIN:-${REPO_ROOT}/target/debug/quill}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/quill-push-smoke}"
PORT="${PORT:-5443}"
REPO="${REPO:-mycorp/myimage}"
TAG="${TAG:-v1-patched}"
BASE="https://127.0.0.1:${PORT}"

if [ ! -x "$QUILL_BIN" ]; then
  echo "quill binary not found at $QUILL_BIN — run \`cargo build\` first" >&2
  exit 1
fi

echo "==> setting up $SMOKE_DIR"
rm -rf "$SMOKE_DIR"
mkdir -p "$SMOKE_DIR/cache" "$SMOKE_DIR/payload"

cat > "$SMOKE_DIR/quill.toml" <<EOF
[http]
address = "127.0.0.1:${PORT}"

[storage]
root = "${SMOKE_DIR}/cache"
EOF

echo "==> starting quill"
"$QUILL_BIN" serve --config "$SMOKE_DIR/quill.toml" > "$SMOKE_DIR/quill.log" 2>&1 &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT
sleep 1

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "==> quill failed to start; log:" >&2; cat "$SMOKE_DIR/quill.log" >&2; exit 1
fi

# Helper to compute a sha256 digest in the form "sha256:<hex>".
digest_of() { printf 'sha256:%s' "$(shasum -a 256 "$1" | awk '{print $1}')"; }

# --- 1. Build a tiny "image" payload ---
echo "==> building tiny image payload"
echo '{"layerData":"hello quill push smoke"}' > "$SMOKE_DIR/payload/layer.bin"
LAYER_DIGEST=$(digest_of "$SMOKE_DIR/payload/layer.bin")
LAYER_SIZE=$(wc -c < "$SMOKE_DIR/payload/layer.bin" | tr -d ' ')

cat > "$SMOKE_DIR/payload/config.json" <<EOF
{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":["${LAYER_DIGEST}"]}}
EOF
CONFIG_DIGEST=$(digest_of "$SMOKE_DIR/payload/config.json")
CONFIG_SIZE=$(wc -c < "$SMOKE_DIR/payload/config.json" | tr -d ' ')

cat > "$SMOKE_DIR/payload/manifest.json" <<EOF
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":${CONFIG_SIZE},"digest":"${CONFIG_DIGEST}"},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","size":${LAYER_SIZE},"digest":"${LAYER_DIGEST}"}]}
EOF
MANIFEST_DIGEST=$(digest_of "$SMOKE_DIR/payload/manifest.json")

echo "  layer:    $LAYER_DIGEST  ($LAYER_SIZE bytes)"
echo "  config:   $CONFIG_DIGEST  ($CONFIG_SIZE bytes)"
echo "  manifest: $MANIFEST_DIGEST"

# --- 2. Push each blob ---
push_blob() {
  local digest="$1" path="$2"
  # Init
  local init_resp
  init_resp=$(curl -sk -i -X POST "$BASE/v2/${REPO}/blobs/uploads/" -o "$SMOKE_DIR/init.headers")
  local location
  location=$(grep -i '^location:' "$SMOKE_DIR/init.headers" | head -1 | tr -d '\r' | awk '{print $2}')
  if [ -z "$location" ]; then
    echo "    upload init failed for $digest" >&2
    cat "$SMOKE_DIR/init.headers" >&2
    return 1
  fi
  # Single PATCH+PUT in two requests for clarity. Real clients can use a
  # monolithic PUT with body in the same request, which we exercise via the
  # PATCH-then-empty-PUT path.
  curl -sk -X PATCH --data-binary "@${path}" "${BASE}${location}" -o /dev/null
  curl -sk -X PUT "${BASE}${location}?digest=${digest}" -o /dev/null -w "    PUT status=%{http_code}\n"
}

echo "==> pushing layer"
push_blob "$LAYER_DIGEST" "$SMOKE_DIR/payload/layer.bin"
echo "==> pushing config"
push_blob "$CONFIG_DIGEST" "$SMOKE_DIR/payload/config.json"

# --- 3. Push manifest by tag ---
echo "==> pushing manifest by tag"
curl -sk -X PUT \
  -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
  --data-binary "@${SMOKE_DIR}/payload/manifest.json" \
  "${BASE}/v2/${REPO}/manifests/${TAG}" \
  -o /dev/null -w "    status=%{http_code}\n"

# --- 4. Pull back ---
echo "==> pulling manifest by tag"
curl -sk "${BASE}/v2/${REPO}/manifests/${TAG}" -o "$SMOKE_DIR/pulled.manifest.json"
diff -q "$SMOKE_DIR/payload/manifest.json" "$SMOKE_DIR/pulled.manifest.json" \
  && echo "    manifest round-trips byte-identical"

echo "==> pulling layer by digest"
curl -sk "${BASE}/v2/${REPO}/blobs/${LAYER_DIGEST}" -o "$SMOKE_DIR/pulled.layer.bin"
diff -q "$SMOKE_DIR/payload/layer.bin" "$SMOKE_DIR/pulled.layer.bin" \
  && echo "    layer round-trips byte-identical"

echo "==> tags/list"
curl -sk "${BASE}/v2/${REPO}/tags/list"; echo

echo "==> _local_tags.json on disk:"
cat "${SMOKE_DIR}/cache/${REPO}/_local_tags.json"; echo

echo "==> CAS contents:"
find "${SMOKE_DIR}/cache/${REPO}" -type f | sed "s|${SMOKE_DIR}/cache/||"
