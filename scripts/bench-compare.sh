#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo 'usage: bench-compare.sh --base REF --head REF [--profile standard] [--scenario four-balanced] [--runs N] [--output DIR] [-- extra probe args]' >&2
  exit 2
}

base= head= profile=standard scenario=four-balanced runs=5 output=target/bench-comparison
extra=()
while (($#)); do
  case "$1" in
    --base) base=${2:?}; shift 2 ;;
    --head) head=${2:?}; shift 2 ;;
    --profile) profile=${2:?}; shift 2 ;;
    --scenario) scenario=${2:?}; shift 2 ;;
    --runs) runs=${2:?}; shift 2 ;;
    --output) output=${2:?}; shift 2 ;;
    --) shift; extra=("$@"); break ;;
    *) usage ;;
  esac
done
[[ -n $base && -n $head && $runs =~ ^[0-9]+$ && $runs -ge 5 ]] || usage
repo=$(git rev-parse --show-toplevel)
base_sha=$(git -C "$repo" rev-parse --verify "${base}^{commit}")
head_sha=$(git -C "$repo" rev-parse --verify "${head}^{commit}")
mkdir -p "$output"
output=$(cd "$output" && pwd)
work=$(mktemp -d)
cleanup() {
  git -C "$repo" worktree remove --force "$work/base" 2>/dev/null || true
  git -C "$repo" worktree remove --force "$work/head" 2>/dev/null || true
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT
for side in base head; do
  sha=$base_sha
  [[ $side == head ]] && sha=$head_sha
  git -C "$repo" worktree add --detach "$work/$side" "$sha" >/dev/null
  [[ -f "$work/$side/crates/netplane/examples/operational_probe/main.rs" ]] || {
    echo "$side ($sha) predates the operational runner; use the shared Criterion benchmarks for this historical comparison" >&2
    exit 1
  }
  (cd "$work/$side" && cargo build --release -p netplane --example operational_probe)
  cp "$work/$side/Cargo.lock" "$output/$side-Cargo.lock"
done
base_deps=$(sha256sum "$output/base-Cargo.lock" | cut -d' ' -f1)
head_deps=$(sha256sum "$output/head-Cargo.lock" | cut -d' ' -f1)
[[ $base_deps == "$head_deps" ]] || {
  echo 'dependency resolution differs; strict paired comparison refused' >&2
  exit 1
}
base_bin="$work/base/target/release/examples/operational_probe"
head_bin="$work/head/target/release/examples/operational_probe"
probe_args=(--profile "$profile" --scenario "$scenario" --implementation netplane "${extra[@]}")
for side in base head; do
  bin=$base_bin
  [[ $side == head ]] && bin=$head_bin
  (cd "$work/$side" && "$bin" --profile smoke --scenario "$scenario" --implementation netplane --output "$output/$side-warmup.json")
done
for ((pair=1; pair<=runs; pair++)); do
  if ((pair % 2)); then order=(base head); else order=(head base); fi
  for side in "${order[@]}"; do
    bin=$base_bin
    [[ $side == head ]] && bin=$head_bin
    (cd "$work/$side" && "$bin" "${probe_args[@]}" --output "$output/$side-$pair.json")
  done
done
python3 "$repo/scripts/bench-report.py" "$output" "$base_sha" "$head_sha" "$runs"
