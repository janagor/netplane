#!/usr/bin/env bash
# Open-loop multi-rate comparison of buffered vs backend-only with latency breakdown.
# Uses the current HEAD probe only (schema v3 open_loop fields).
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
cd "$repo"
runs=5
samples=262144
warmup=1024
workers=4
callers=32
routes=4
output=target/bench-routing-openloop

if [[ -e $output ]]; then
  if [[ ! -d $output ]] || [[ -n $(find "$output" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    echo "output directory must be empty: $output" >&2
    exit 1
  fi
else
  mkdir -p "$output"
fi
output=$(cd "$output" && pwd)

echo "building head probe"
cargo build --release -p netplane --example single_handoff_probe
probe=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release/examples/single_handoff_probe
# Prefer isolated copy so CARGO_TARGET_DIR sharing cannot overwrite mid-run.
mkdir -p "$output/bins"
cp -f "$probe" "$output/bins/probe-head"
probe=$output/bins/probe-head

# Stay below closed-loop saturation (~0.7M rps): open-loop at/above that fills
# max_outstanding and the probe aborts on AtCapacity. Top rate is ~½–⅔ of sustained.
rates=(50000 100000 200000 400000)
max_outstanding=16384
for ((rep=1; rep<=runs; rep++)); do
  if ((rep % 2)); then order=(graph-builder backend-only); else order=(backend-only graph-builder); fi
  for rate in "${rates[@]}"; do
    for impl in "${order[@]}"; do
      out="$output/${impl}_open_immediate_r${routes}_rate${rate}_rep${rep}.json"
      echo "impl=$impl rate=$rate rep=$rep"
      "$probe" \
        --implementation "$impl" \
        --tokio-workers "$workers" \
        --workload immediate \
        --offered-rps "$rate" \
        --skew-percent 0 \
        --callers "$callers" \
        --routes "$routes" \
        --samples "$samples" \
        --warmup "$warmup" \
        --max-outstanding "$max_outstanding" \
        --repetition "$rep" \
        --output "$out"
    done
  done
done

python3 "$repo/scripts/bench-routing-openloop-report.py" "$output" "$runs"
echo "wrote $output/comparison.md"
