#!/usr/bin/env bash
set -euo pipefail

BIN="./build/rx_only"
OUT="bench/results.jsonl"

mkdir -p bench
rm -f "$OUT"

if [[ ! -x "$BIN" ]]; then
  echo "Binary not found/executable: $BIN"
  echo "Build first: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j"
  exit 1
fi

run() {
  echo "== $*"
  sudo $*
}

echo "Writing results to: $OUT"
echo

# Scaling (per-worker pool)
run $BIN -l 0-1 -n 4 -- --mode synth --workers 1 --pool per_worker --duration 3 --burst 32 --mbufs 8192 --mbuf-cache 256 --latency --json-out "$OUT"
run $BIN -l 0-3 -n 4 -- --mode synth --workers 3 --pool per_worker --duration 3 --burst 32 --mbufs 8192 --mbuf-cache 256 --latency --json-out "$OUT"
run $BIN -l 0-5 -n 4 -- --mode synth --workers 5 --pool per_worker --duration 3 --burst 32 --mbufs 8192 --mbuf-cache 256 --latency --json-out "$OUT"

# Pool comparison (5 workers)
run $BIN -l 0-5 -n 4 -- --mode synth --workers 5 --pool shared --duration 3 --burst 32 --mbufs 8192 --mbuf-cache 256 --latency --json-out "$OUT"

# Workload stress (5 workers)
run $BIN -l 0-5 -n 4 -- --mode synth --workers 5 --pool per_worker --duration 3 --burst 32 --mbufs 8192 --mbuf-cache 256 --latency --work-nops 200 --json-out "$OUT"
run $BIN -l 0-5 -n 4 -- --mode synth --workers 5 --pool shared --duration 3 --burst 32 --mbufs 8192 --mbuf-cache 256 --latency --work-nops 200 --json-out "$OUT"

echo
echo "=== RESULTS (JSONL) ==="
cat "$OUT"
