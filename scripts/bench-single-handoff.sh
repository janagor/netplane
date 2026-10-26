#!/usr/bin/env bash
# Paired single-handoff experiment: five topologies × workloads × caller counts.
set -euo pipefail

usage() {
  echo "usage: bench-single-handoff.sh [--runs N] [--samples N] [--output DIR] [--quick]" >&2
  exit 2
}

runs=5
samples=32768
warmup=1024
output=target/bench-single-handoff
quick=0
while (($#)); do
  case "$1" in
    --runs) runs=${2:?}; shift 2 ;;
    --samples) samples=${2:?}; shift 2 ;;
    --warmup) warmup=${2:?}; shift 2 ;;
    --output) output=${2:?}; shift 2 ;;
    --quick) quick=1; shift ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done
[[ $runs =~ ^[0-9]+$ && $runs -ge 5 ]] || usage

repo=$(git rev-parse --show-toplevel)
cd "$repo"

if [[ -e $output ]]; then
  if [[ ! -d $output ]]; then
    echo "output path exists and is not a directory: $output" >&2
    exit 1
  fi
  if [[ -n $(find "$output" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    echo "output directory must be empty to avoid mixing runs: $output" >&2
    echo "choose a fresh path, for example target/bench-single-handoff-$(date +%Y%m%d-%H%M%S)" >&2
    exit 1
  fi
else
  mkdir -p "$output"
fi
output=$(cd "$output" && pwd)

if ((quick)); then
  samples=4096
  warmup=256
fi

implementations=(
  tower-bounded
  tower-double-buffered
  graph-builder
  single-handoff
  graph-draft
)
workloads=(immediate delayed)
callers=(1 4 8 16 32)

echo "building single_handoff_probe (release)"
cargo build --release -p netplane --example single_handoff_probe
target_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
bin="$target_dir/release/examples/single_handoff_probe"
[[ -x $bin ]] || {
  echo "missing release probe binary at $bin" >&2
  exit 1
}

# Warm each binary path once before paired collection.
for impl in "${implementations[@]}"; do
  "$bin" \
    --implementation "$impl" \
    --workload immediate \
    --callers 1 \
    --samples "$samples" \
    --warmup "$warmup" \
    --repetition 1 \
    --output "$output/warmup-$impl.json" >/dev/null
done

matrix=()
for workload in "${workloads[@]}"; do
  for n in "${callers[@]}"; do
    matrix+=("$workload:$n")
  done
done

for ((pair=1; pair<=runs; pair++)); do
  # Alternate implementation order across pairs to reduce temporal bias.
  if ((pair % 2)); then
    order=("${implementations[@]}")
  else
    mapfile -t order < <(printf '%s\n' "${implementations[@]}" | tac)
  fi
  for cell in "${matrix[@]}"; do
    workload=${cell%%:*}
    n=${cell##*:}
    # Delayed sleeps dominate wall time; keep enough samples for percentiles
    # without making a full matrix multi-hour on a laptop.
    cell_samples=$samples
    if [[ $workload == delayed && $quick -eq 0 && $samples -gt 8192 ]]; then
      cell_samples=8192
    fi
    for impl in "${order[@]}"; do
      out="$output/${impl}_${workload}_c${n}_r${pair}.json"
      echo "run pair=$pair impl=$impl workload=$workload callers=$n samples=$cell_samples"
      "$bin" \
        --implementation "$impl" \
        --workload "$workload" \
        --callers "$n" \
        --samples "$cell_samples" \
        --warmup "$warmup" \
        --work-us 100 \
        --repetition "$pair" \
        --output "$out"
    done
  done
done

python3 "$repo/scripts/bench-single-handoff-report.py" "$output" "$runs"
echo "wrote $output/comparison.json and $output/comparison.md"
