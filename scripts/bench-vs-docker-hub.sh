#!/usr/bin/env bash
# Benchmark Quill in proxy mode against direct Docker Hub.
#
# Three regimes, each measured with N samples:
#   1. Cold first-pull through Quill (cache deleted between samples)
#   2. Warm-cache pull through Quill (cache populated; many samples cheaply)
#   3. Direct pull from Docker Hub's CDN (no Quill in the path)
#
# A reporting helper computes mean and stddev in python3.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
QUILL_BIN="${QUILL_BIN:-${REPO_ROOT}/target/release/quill}"
BENCH_DIR="${BENCH_DIR:-/tmp/quill-bench}"
PORT="${PORT:-5443}"
COLD_RUNS="${COLD_RUNS:-10}"
WARM_RUNS="${WARM_RUNS:-50}"
DIRECT_RUNS="${DIRECT_RUNS:-10}"
REPO="library/alpine"
TAG="3.19"

if [ ! -x "$QUILL_BIN" ]; then
  echo "quill release binary not found at $QUILL_BIN — run \`cargo build --release\` first" >&2
  exit 1
fi

mkdir -p "$BENCH_DIR/cache"
cat > "$BENCH_DIR/quill.toml" <<EOF
[http]
address = "127.0.0.1:${PORT}"

[storage]
root = "${BENCH_DIR}/cache"

[[upstream]]
name = "docker-hub"
url = "https://registry-1.docker.io"
kind = "generic"
repo_prefix = "library/"
EOF

start_quill() {
  "$QUILL_BIN" serve --config "$BENCH_DIR/quill.toml" > "$BENCH_DIR/quill.log" 2>&1 &
  QUILL_PID=$!
  sleep 1
  if ! kill -0 "$QUILL_PID" 2>/dev/null; then
    echo "quill failed to start; log:" >&2
    cat "$BENCH_DIR/quill.log" >&2
    exit 1
  fi
}

stop_quill() {
  kill "$QUILL_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}

# --- Resolve the test image's blob digests. Use the *amd64* manifest's actual
# layer + config so we're benchmarking realistic-size data, not the index. ---
echo "==> resolving $REPO:$TAG via Quill"
start_quill
INDEX=$(curl -sk --http1.1 "https://127.0.0.1:${PORT}/v2/${REPO}/manifests/${TAG}")
AMD64_MANIFEST_DIGEST=$(printf '%s' "$INDEX" | python3 -c '
import json,sys
m=json.loads(sys.stdin.read())
for x in m.get("manifests", []):
    if x.get("platform", {}).get("architecture") == "amd64" and x.get("platform", {}).get("os") == "linux":
        print(x["digest"]); break
')
AMD64_MANIFEST=$(curl -sk --http1.1 \
  -H "Accept: application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json" \
  "https://127.0.0.1:${PORT}/v2/${REPO}/manifests/${AMD64_MANIFEST_DIGEST}")
LAYER_DIGEST=$(printf '%s' "$AMD64_MANIFEST" | python3 -c '
import json,sys
m=json.loads(sys.stdin.read())
print(m["layers"][0]["digest"])')
LAYER_SIZE=$(printf '%s' "$AMD64_MANIFEST" | python3 -c '
import json,sys
m=json.loads(sys.stdin.read())
print(m["layers"][0]["size"])')
CONFIG_DIGEST=$(printf '%s' "$AMD64_MANIFEST" | python3 -c '
import json,sys
m=json.loads(sys.stdin.read())
print(m["config"]["digest"])')
echo "    amd64 manifest: $AMD64_MANIFEST_DIGEST"
echo "    layer digest:   $LAYER_DIGEST  ($LAYER_SIZE bytes)"
echo "    config digest:  $CONFIG_DIGEST"
stop_quill

# Pre-resolve the direct Docker Hub URL we'll hit later, since /v2/... requires
# a fresh bearer token per pull. We'll mint one outside the benchmark loop.
echo
echo "==> minting an anonymous Docker Hub bearer token (used for direct comparison)"
DH_TOKEN=$(curl -fsS \
  "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${REPO}:pull" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')

# ---------------- Run the three benchmarks ----------------
report() {
  local label="$1"; shift
  printf '%s' "$@" | python3 -c '
import sys, statistics
xs = [float(x) for x in sys.stdin.read().split() if x.strip()]
mean = statistics.mean(xs)
sd = statistics.stdev(xs) if len(xs) > 1 else 0.0
mn, mx = min(xs), max(xs)
print(f"    n={len(xs)}  mean={mean*1000:7.1f} ms  stddev={sd*1000:6.1f} ms  min={mn*1000:7.1f}  max={mx*1000:7.1f}")
'
}

echo
echo "==> [1/3] COLD pull-through Quill (cache deleted between samples), n=$COLD_RUNS"
COLD_TIMES=""
for i in $(seq 1 $COLD_RUNS); do
  rm -rf "$BENCH_DIR/cache"; mkdir -p "$BENCH_DIR/cache"
  start_quill
  T=$(curl -sk --http1.1 -o /dev/null -w "%{time_total}" \
    "https://127.0.0.1:${PORT}/v2/${REPO}/blobs/${LAYER_DIGEST}")
  COLD_TIMES="$COLD_TIMES $T"
  stop_quill
done
report "cold" "$COLD_TIMES"

echo
echo "==> [2/3] WARM Quill cache hit, n=$WARM_RUNS"
# Re-prime once so the cache is populated for all warm samples.
rm -rf "$BENCH_DIR/cache"; mkdir -p "$BENCH_DIR/cache"
start_quill
curl -sk --http1.1 -o /dev/null \
  "https://127.0.0.1:${PORT}/v2/${REPO}/blobs/${LAYER_DIGEST}"
WARM_TIMES=""
for i in $(seq 1 $WARM_RUNS); do
  T=$(curl -sk --http1.1 -o /dev/null -w "%{time_total}" \
    "https://127.0.0.1:${PORT}/v2/${REPO}/blobs/${LAYER_DIGEST}")
  WARM_TIMES="$WARM_TIMES $T"
done
stop_quill
report "warm" "$WARM_TIMES"

echo
echo "==> [3/3] DIRECT pull from Docker Hub CDN (no Quill), n=$DIRECT_RUNS"
DIRECT_TIMES=""
for i in $(seq 1 $DIRECT_RUNS); do
  T=$(curl -fsS -L -o /dev/null -w "%{time_total}" \
    -H "Authorization: Bearer ${DH_TOKEN}" \
    "https://registry-1.docker.io/v2/${REPO}/blobs/${LAYER_DIGEST}")
  DIRECT_TIMES="$DIRECT_TIMES $T"
done
report "direct" "$DIRECT_TIMES"

echo
echo "==> done"
