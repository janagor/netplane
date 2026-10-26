#!/usr/bin/env bash
# Run from repository root. Compares the two production GraphBuilder policies.
set -euo pipefail

output=${1:-target/bench-routing-policy-matrix}
mkdir -p "$output"
cargo build --release -p netplane --example single_handoff_probe
probe=target/release/examples/single_handoff_probe
base_dir=$(mktemp -d)
cleanup() {
  git worktree remove --force "$base_dir/base" 2>/dev/null || true
  rm -rf -- "$base_dir"
}
trap cleanup EXIT
git worktree add --detach "$base_dir/base" 1e06bd1
CARGO_TARGET_DIR="$base_dir/target" cargo build --release --manifest-path "$base_dir/base/Cargo.toml" -p netplane --example single_handoff_probe
base_probe="$base_dir/target/release/examples/single_handoff_probe"
for workload in immediate delayed; do
  samples=4096
  if [[ $workload == delayed ]]; then samples=2048; fi
  for repetition in 1 2 3; do
    "$base_probe" --implementation graph-builder --workload "$workload" --routes 4 \
      --callers 32 --samples "$samples" --warmup 256 --repetition "$repetition" \
      --output "$output/base_closed_${workload}_w4_s0_r${repetition}.json"
  done
done

for workers in 1 2 4 8; do
  for workload in immediate delayed; do
    samples=4096
    rate=20000
    if [[ $workload == delayed ]]; then
      samples=2048
      rate=2000
    fi
    for skew in 0 90; do
      for mode in closed open; do
        extra=()
        if [[ $mode == open ]]; then
          extra=(--offered-rps "$rate")
        fi
        for implementation in graph-builder backend-only; do
          for repetition in 1 2 3; do
            "$probe" \
              --implementation "$implementation" \
              --tokio-workers "$workers" \
              --workload "$workload" \
              --routes 4 \
              --skew-percent "$skew" \
              --callers 32 \
              --samples "$samples" \
              --warmup 256 \
              --repetition "$repetition" \
              --output "$output/${implementation}_${mode}_${workload}_w${workers}_s${skew}_r${repetition}.json" \
              "${extra[@]}"
          done
        done
      done
    done
  done
done
python3 scripts/bench-routing-policy-matrix-report.py "$output"
