#!/usr/bin/env bash
# Verify orchestration failure status, diagnostics and cleanup without Docker.
set -euo pipefail
cd "$(dirname "$0")/.."
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin"
cat > "$scratch/bin/mock" <<'MOCK'
#!/usr/bin/env bash
set -eu
tool=${0##*/}
printf '%s %s\n' "$tool" "$*" >> "$MOCK_LOG"
case "$tool" in
  kind)
    if [[ $1 == version ]]; then echo 'kind v0.31.0 go1.26 linux/amd64'; fi
    if [[ $1 == create && ${MOCK_CREATE_FAIL:-false} == true ]]; then exit 19; fi
    if [[ $1 == delete && ${MOCK_DELETE_FAIL:-false} == true ]]; then exit 23; fi
    if [[ $1 == export && ${MOCK_DIAGNOSTIC_FAIL:-false} == true ]]; then exit 29; fi
    ;;
  kubectl|curl)
    if [[ ${MOCK_DIAGNOSTIC_FAIL:-false} == true ]]; then exit 29; fi
    ;;
  cargo)
    if [[ " $* " == *' --list '* ]]; then
      printf 'first: test\nsecond: test\n'
    elif [[ ${MOCK_FAIL:-false} == true ]]; then exit 17
    fi
    ;;
esac
MOCK
chmod +x "$scratch/bin/mock"
for tool in docker kind kubectl cargo curl; do ln -s mock "$scratch/bin/$tool"; done
export PATH="$scratch/bin:$PATH"
export MOCK_LOG="$scratch/log"
export NETPLANE_E2E_ARTIFACTS="$scratch/artifacts"
./scripts/kubernetes-e2e.sh > "$scratch/success" 2>&1
rg -q 'kind delete cluster' "$MOCK_LOG"
rg -q 'kind export logs' "$MOCK_LOG"
rg -q 'cargo test .*second.*--exact' "$MOCK_LOG"
: > "$MOCK_LOG"
export MOCK_FAIL=true
set +e
./scripts/kubernetes-e2e.sh > "$scratch/failure" 2>&1
result=$?
set -e
[[ $result == 17 ]]
rg -q 'kind delete cluster' "$MOCK_LOG"
rg -q 'kind export logs' "$MOCK_LOG"
if rg -q 'cargo test .*second.*--exact' "$MOCK_LOG"; then exit 1; fi
: > "$MOCK_LOG"
export MOCK_FAIL=false
./scripts/kubernetes-e2e.sh --keep-cluster > "$scratch/keep" 2>&1
if rg -q 'kind delete cluster' "$MOCK_LOG"; then exit 1; fi
rg -q 'Retained cluster:' "$scratch/keep"
: > "$MOCK_LOG"
export MOCK_CREATE_FAIL=true
set +e
./scripts/kubernetes-e2e.sh > "$scratch/create-failure" 2>&1
result=$?
set -e
[[ $result == 19 ]]
rg -q 'kind delete cluster' "$MOCK_LOG"
export MOCK_CREATE_FAIL=false MOCK_DELETE_FAIL=true
set +e
./scripts/kubernetes-e2e.sh > "$scratch/delete-failure" 2>&1
result=$?
set -e
[[ $result == 1 ]]
rg -q 'Failed to delete cluster' "$scratch/delete-failure"
export MOCK_FAIL=true MOCK_DIAGNOSTIC_FAIL=true
set +e
./scripts/kubernetes-e2e.sh > "$scratch/all-failures" 2>&1
result=$?
set -e
[[ $result == 17 ]]
export MOCK_DELETE_FAIL=false MOCK_FAIL=false
: > "$MOCK_LOG"
./scripts/kubernetes-e2e.sh > "$scratch/diagnostic-failure" 2>&1
rg -q 'kind delete cluster' "$MOCK_LOG"
rg -q 'Failed to export cluster diagnostics' "$scratch/diagnostic-failure"
# Reject divergent host dependencies before Docker or cluster creation.
mkdir -p "$scratch/repo/scripts" "$scratch/repo/tests/kubernetes"
cp scripts/kubernetes-e2e.sh "$scratch/repo/scripts/"
cp tests/kubernetes/Cargo.lock "$scratch/repo/tests/kubernetes/"
printf 'deliberately different lockfile\n' > "$scratch/repo/Cargo.lock"
: > "$MOCK_LOG"
set +e
bash "$scratch/repo/scripts/kubernetes-e2e.sh" > "$scratch/lock-mismatch" 2>&1
result=$?
set -e
[[ $result == 2 ]]
rg -q 'Cargo.lock differs' "$scratch/lock-mismatch"
if rg -q 'docker build|kind create' "$MOCK_LOG"; then exit 1; fi
rg -q 'deliberately different lockfile' "$scratch/repo/Cargo.lock"
echo 'Kubernetes orchestration checks passed'
