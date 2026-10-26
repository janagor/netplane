#!/usr/bin/env bash
# Long paired comparison: 1e06bd1 graph-builder vs current buffered GraphBuilder default.
# Identical workload/selector (no skew). ~15s closed-loop per cell, 5 repetitions.
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
cd "$repo"
base_sha=$(git rev-parse --verify "1e06bd1^{commit}")
runs=5
# ~15s at ~850k rps with 32 closed-loop callers.
samples=12800000
warmup=4096
workers=4
routes=4
output=target/bench-buffered-baseline

if [[ -e $output ]]; then
  if [[ ! -d $output ]] || [[ -n $(find "$output" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    echo "output directory must be empty: $output" >&2
    exit 1
  fi
else
  mkdir -p "$output"
fi
output=$(cd "$output" && pwd)
mkdir -p "$output/bins"

work=$(mktemp -d)
cleanup() {
  git -C "$repo" worktree remove --force "$work/base" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

echo "worktree base=$base_sha"
git worktree add --detach "$work/base" "$base_sha" >/dev/null

# PR1 probe capped samples at 2e6; raise only in this disposable worktree.
python3 - <<PY
from pathlib import Path
path = Path("$work/base/crates/netplane/examples/single_handoff_probe/config.rs")
text = path.read_text()
old = "if !(1..=2_000_000).contains(&config.samples)"
new = "if !(1..=20_000_000).contains(&config.samples)"
if old not in text:
    raise SystemExit("base sample limit pattern not found")
path.write_text(text.replace(old, new, 1).replace(
    'return Err("samples must be 1..=2000000".into());',
    'return Err("samples must be 1..=20000000".into());',
    1,
))
print("patched base sample limit for long equivalence run")
PY

echo "building base probe (4 Tokio workers hard-coded in 1e06bd1)"
CARGO_TARGET_DIR="$work/target-base" cargo build --release \
  --manifest-path "$work/base/Cargo.toml" -p netplane --example single_handoff_probe
echo "building head probe"
CARGO_TARGET_DIR="$work/target-head" cargo build --release -p netplane --example single_handoff_probe

base_bin=$work/target-base/release/examples/single_handoff_probe
head_bin=$work/target-head/release/examples/single_handoff_probe
cp -f "$base_bin" "$output/bins/probe-base"
cp -f "$head_bin" "$output/bins/probe-head"
base_bin=$output/bins/probe-base
head_bin=$output/bins/probe-head
if cmp -s "$base_bin" "$head_bin"; then
  echo "base and head binaries identical; refusing" >&2
  exit 1
fi
md5sum "$base_bin" "$head_bin"

run_one() {
  local side=$1 bin=$2 callers_n=$3 rep=$4
  local out="$output/${side}_graph-builder_immediate_r${routes}_c${callers_n}_w${workers}_rep${rep}.json"
  echo "side=$side callers=$callers_n workers=$workers rep=$rep samples=$samples"
  if [[ $side == head ]]; then
    "$bin" \
      --implementation graph-builder \
      --tokio-workers "$workers" \
      --workload immediate \
      --callers "$callers_n" \
      --routes "$routes" \
      --samples "$samples" \
      --warmup "$warmup" \
      --repetition "$rep" \
      --output "$out"
  else
    "$bin" \
      --implementation graph-builder \
      --workload immediate \
      --callers "$callers_n" \
      --routes "$routes" \
      --samples "$samples" \
      --warmup "$warmup" \
      --repetition "$rep" \
      --output "$out"
  fi
}

for side_bin in "base:$base_bin" "head:$head_bin"; do
  side=${side_bin%%:*}
  bin=${side_bin#*:}
  if [[ $side == head ]]; then
    "$bin" --implementation graph-builder --tokio-workers "$workers" --workload immediate \
      --callers 1 --routes 1 --samples 4096 --warmup 256 --repetition 1 \
      --output "$output/warmup-$side.json"
  else
    "$bin" --implementation graph-builder --workload immediate \
      --callers 1 --routes 1 --samples 4096 --warmup 256 --repetition 1 \
      --output "$output/warmup-$side.json"
  fi
done

for ((rep=1; rep<=runs; rep++)); do
  if ((rep % 2)); then order=(base head); else order=(head base); fi
  for callers_n in 4 32; do
    for side in "${order[@]}"; do
      bin=$base_bin
      [[ $side == head ]] && bin=$head_bin
      run_one "$side" "$bin" "$callers_n" "$rep"
    done
  done
done

python3 "$repo/scripts/bench-buffered-baseline-report.py" "$output" "$base_sha" "$runs"
echo "wrote $output/comparison.md"
