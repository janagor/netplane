#!/usr/bin/env bash
set -euo pipefail
profile=${1:-smoke}
output=${2:-target/bench-operational}
mkdir -p "$output"
for scenario in single two-balanced two-skewed four-balanced eight-balanced; do
  for implementation in netplane tower; do
    cargo run --release -p netplane --example operational_probe -- \
      --profile "$profile" --scenario "$scenario" --implementation "$implementation" \
      --output "$output/$scenario-$implementation.json"
  done
done
python3 "$(dirname "$0")/bench-operational-report.py" "$output"
