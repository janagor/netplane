#!/usr/bin/env bash
set -euo pipefail
for tool in oha curl python3 openssl; do command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }; done
output=${1:-target/bench-http}
requests=${2:-1000}
connections=${3:-16}
duration=${4:-5}
total_qps=${5:-1000}
[[ $requests =~ ^[0-9]+$ && $requests -ge 4 && $connections =~ ^[0-9]+$ && $connections -ge 4 ]] || exit 2
[[ $duration =~ ^[0-9]+$ && $duration -ge 1 && $total_qps =~ ^[0-9]+$ && $total_qps -ge 4 ]] || exit 2
mkdir -p "$output"
output=$(cd "$output" && pwd)
repo=$(git rev-parse --show-toplevel)

port_free() {
  local port=$1
  if command -v ss >/dev/null; then
    ! ss -ltn "sport = :$port" | tail -n +2 | grep -q .
  else
    ! (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null
  fi
}
for port in 3000 3001; do
  port_free "$port" || { echo "port $port is already in use" >&2; exit 1; }
done

(cd "$repo" && cargo build --release -p netplane-http --example policy_gateway)
token=$(openssl rand -hex 16)
NETPLANE_ADMIN_TOKEN=$token "$repo/target/release/examples/policy_gateway" >"$output/gateway.log" 2>&1 &
gateway_pid=$!
cleanup() { kill "$gateway_pid" 2>/dev/null || true; wait "$gateway_pid" 2>/dev/null || true; }
trap cleanup EXIT
ready=0
for _ in {1..50}; do
  if curl -fsS --max-time 1 -X POST -H 'x-request-key: bench-0' -d x http://127.0.0.1:3000/process >/dev/null; then ready=1; break; fi
  sleep 0.1
done
[[ $ready == 1 ]] || { echo 'gateway did not become ready' >&2; exit 1; }

# Controlled keys under the gateway FNV-1a bucket hash:
# bench-0 -> 84 (stable at 25% and 50%)
# bench-2 -> 6  (canary at 25% and 50%)
# bench-4 -> 40 (stable at 25%, canary at 50%)
stable_key=bench-0
canary_key=bench-2
switch_key=bench-4

version=$(oha --version)
python3 - "$output/metadata.json" "$version" "$requests" "$connections" "$duration" "$total_qps" \
  "$stable_key" "$canary_key" "$switch_key" <<'PY'
import json, subprocess, sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps({
    "oha_version": sys.argv[2],
    "git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "requests_per_run": int(sys.argv[3]),
    "connections": int(sys.argv[4]),
    "mixed_duration_secs": int(sys.argv[5]),
    "mixed_total_qps": int(sys.argv[6]),
    "stable_key": sys.argv[7],
    "canary_key": sys.argv[8],
    "switch_key": sys.argv[9],
}, indent=2) + "\n")
PY

backend_for() {
  local key=$1
  curl -fsS -D - -o /dev/null -X POST -H "x-request-key: $key" -d x http://127.0.0.1:3000/process \
    | awk 'tolower($1)=="x-backend:" {print $2}' | tr -d '\r'
}

run_count() {
  local label=$1 key=$2 count=$3 concurrency=$4
  oha --no-tui --output-format json -n "$count" -c "$concurrency" -m POST -d x -H "x-request-key: $key" \
    -o "$output/$label.json" http://127.0.0.1:3000/process
}

run_rate() {
  local label=$1 key=$2 qps=$3 concurrency=$4
  oha --no-tui --output-format json -z "${duration}s" -q "$qps" -c "$concurrency" -m POST -d x \
    -H "x-request-key: $key" -o "$output/$label.json" http://127.0.0.1:3000/process
}

run_count stable "$stable_key" "$requests" "$connections"
run_count canary "$canary_key" "$requests" "$connections"

# Sustained mixed load over a shared duration: 75% stable / 25% canary.
stable_qps=$(( total_qps * 3 / 4 ))
canary_qps=$(( total_qps - stable_qps ))
group_stable_qps=$(( stable_qps / 3 ))
group_stable_rem=$(( stable_qps % 3 ))
pids=()
for group in 0 1 2; do
  qps=$((group_stable_qps + (group < group_stable_rem)))
  conn=$((connections / 4 + (group < connections % 4)))
  [[ $qps -ge 1 && $conn -ge 1 ]] || continue
  run_rate "mixed-$group" "$stable_key" "$qps" "$conn" & pids+=("$!")
done
canary_conn=$((connections / 4 + (3 < connections % 4)))
run_rate mixed-3 "$canary_key" "$canary_qps" "$canary_conn" & pids+=("$!")
for pid in "${pids[@]}"; do wait "$pid"; done

before=$(backend_for "$switch_key")
[[ $before == stable ]] || { echo "switch key $switch_key expected stable before reconfigure, got $before" >&2; exit 1; }

oha --no-tui --output-format json -z 3s -q 1000 -c "$connections" -m POST -d x \
  -H "x-request-key: $switch_key" -o "$output/reconfigure-traffic.json" http://127.0.0.1:3000/process &
load_pid=$!
sleep 0.2
status=$(curl -fsS -H "Authorization: Bearer $token" http://127.0.0.1:3001/status)
expected=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["active"]["version"])' <<<"$status")
curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  -d "{\"expected_version\":$expected,\"policy\":{\"canary_percent\":50}}" \
  http://127.0.0.1:3001/reconfigure >"$output/reconfigure.json"
python3 - "$output/reconfigure.json" "$expected" <<'PY'
import json, sys
from pathlib import Path
body = json.loads(Path(sys.argv[1]).read_text())
expected = int(sys.argv[2])
published = body.get("published_version")
previous = body.get("previous_version")
if previous != expected:
    raise SystemExit(f"reconfigure previous_version mismatch: {body}")
if published is None or published <= expected:
    raise SystemExit(f"reconfigure did not advance version: {body}")
PY
status_after=$(curl -fsS -H "Authorization: Bearer $token" http://127.0.0.1:3001/status)
python3 -c 'import json,sys; s=json.load(sys.stdin); assert s["active"]["policy"]["canary_percent"]==50, s' <<<"$status_after"
wait "$load_pid"
after=$(backend_for "$switch_key")
[[ $after == canary ]] || { echo "switch key $switch_key expected canary after reconfigure, got $after" >&2; exit 1; }
