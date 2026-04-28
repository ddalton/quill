#!/usr/bin/env bash
# Smoke-test for Phase 3 streaming pull-through cache.
#
# Spins up quill bound to 127.0.0.1:5443 with Docker Hub configured as an
# anonymous upstream proxy for `library/*`, fetches a known small blob from
# `library/redis:7-alpine` cold, then twice warm, and reports timings.
#
# Expected end state (verified 2026-04-28):
#   first GET (cold)  ~ 30-200 ms (network bound)
#   second GET (warm) ~ 1-10 ms   (local CAS, BlobMetaCache hit)
#   third GET (warm)  ~ 1-10 ms
#
# Requires: a built quill binary at ./target/debug/quill (run `cargo build` first).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
QUILL_BIN="${QUILL_BIN:-${REPO_ROOT}/target/debug/quill}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/quill-smoke}"
PORT="${PORT:-5443}"

if [ ! -x "$QUILL_BIN" ]; then
  echo "quill binary not found at $QUILL_BIN — run \`cargo build\` first" >&2
  exit 1
fi

echo "==> setting up $SMOKE_DIR"
rm -rf "$SMOKE_DIR"
mkdir -p "$SMOKE_DIR/cache"

cat > "$SMOKE_DIR/quill.toml" <<EOF
[http]
address = "127.0.0.1:${PORT}"

[storage]
root = "${SMOKE_DIR}/cache"

[[upstream]]
name = "docker-hub"
url = "https://registry-1.docker.io"
kind = "generic"
repo_prefix = "library/"
EOF

echo "==> starting quill"
"$QUILL_BIN" serve --config "$SMOKE_DIR/quill.toml" > "$SMOKE_DIR/quill.log" 2>&1 &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT
sleep 2

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "==> quill failed to start; log:" >&2
  cat "$SMOKE_DIR/quill.log" >&2
  exit 1
fi

echo
echo "==> resolving manifest by tag (cold; triggers bearer-token discovery + manifest fetch)"
curl -sk --http1.1 -i "https://127.0.0.1:${PORT}/v2/library/redis/manifests/7-alpine" \
  > "$SMOKE_DIR/manifest.http" 2>&1
head -5 "$SMOKE_DIR/manifest.http"

# Pull a small blob from inside the manifest. The redis 7-alpine manifest is an
# image-index; the first listed manifest's `digest` is the platform-specific
# image manifest, which is small enough to round-trip cheaply for a smoke test.
DIGEST=$(python3 -c '
import json,sys
body = open("'"$SMOKE_DIR"'/manifest.http", "rb").read().split(b"\r\n\r\n", 1)[1]
m = json.loads(body)
if "manifests" in m: print(m["manifests"][0]["digest"])
elif "config" in m: print(m["config"]["digest"])
')

echo
echo "==> blob digest under test: $DIGEST"
echo
echo "==> first GET (cache miss → upstream stream-and-cache)"
curl -sk --http1.1 -o /dev/null -w "  status=%{http_code} size=%{size_download} time=%{time_total}s\n" \
  "https://127.0.0.1:${PORT}/v2/library/redis/blobs/${DIGEST}"
echo "==> second GET (warm — local CAS hit, no upstream contact)"
curl -sk --http1.1 -o /dev/null -w "  status=%{http_code} size=%{size_download} time=%{time_total}s\n" \
  "https://127.0.0.1:${PORT}/v2/library/redis/blobs/${DIGEST}"
echo "==> third GET (warm)"
curl -sk --http1.1 -o /dev/null -w "  status=%{http_code} size=%{size_download} time=%{time_total}s\n" \
  "https://127.0.0.1:${PORT}/v2/library/redis/blobs/${DIGEST}"

echo
echo "==> upstream contacts during this run (should be 2: manifest fetch + blob stream, both on cold pull only):"
grep -cE "stream_blob|fetching upstream bearer token" "$SMOKE_DIR/quill.log" || echo 0

echo
echo "==> local CAS contents:"
find "$SMOKE_DIR/cache/library" -type f | sed "s|${SMOKE_DIR}/cache/||"

echo
echo "==> log: $SMOKE_DIR/quill.log"
echo "==> stopping quill (cleaning up via EXIT trap)"
