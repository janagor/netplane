#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
keep=false
filter=""
while (($#)); do
  case "$1" in
    --keep-cluster) keep=true; shift ;;
    --test) filter="${2:?missing test filter}"; shift 2 ;;
    *) echo "usage: $0 [--keep-cluster] [--test FILTER]" >&2; exit 2 ;;
  esac
done
for tool in docker kind kubectl cargo; do command -v "$tool" >/dev/null; done
[[ $(kind version) == 'kind v0.31.0 '* ]] || { echo 'Kind v0.31.0 is required' >&2; exit 2; }
cluster="netplane-e2e-$$"
artifacts="${NETPLANE_E2E_ARTIFACTS:-$(pwd)/artifacts/kubernetes}/$cluster"
mkdir -p "$artifacts"
export KUBECONFIG="$artifacts/kubeconfig"
created=false
finish() {
  result=$?
  trap - EXIT INT TERM
  set +e
  if $created; then
    kubectl --request-timeout=5s get events -A > "$artifacts/kubernetes-events.txt" 2>&1
    for resource in pods deployments services endpointslices; do
      kubectl --request-timeout=5s get "$resource" -A -o yaml > "$artifacts/$resource.yaml" 2>&1
    done
    for namespace in $(kubectl --request-timeout=5s get namespaces -l netplane-e2e=true -o jsonpath='{.items[*].metadata.name}'); do
      mkdir -p "$artifacts/$namespace"
      kubectl --request-timeout=5s logs -n "$namespace" -l app=gateway --all-containers --prefix --tail=-1 > "$artifacts/$namespace/gateway.log" 2>&1
      kubectl --request-timeout=5s logs -n "$namespace" -l app=backend --all-containers --prefix --tail=-1 > "$artifacts/$namespace/backend-pods.log" 2>&1
    done
    curl --max-time 5 -fsS http://127.0.0.1:18080/status > "$artifacts/reconciliation-status.json"
    if ! kind export logs --name "$cluster" "$artifacts/kind-logs"; then
      echo "Failed to export cluster diagnostics: $cluster" >&2
    fi
    if $keep; then
      echo "Retained cluster: $cluster; export KUBECONFIG=$KUBECONFIG"
    else
      if ! kind delete cluster --name "$cluster"; then
        echo "Failed to delete cluster: $cluster" >&2
        if [[ "$result" == 0 ]]; then result=1; fi
      fi
      # Do not upload cluster administrative credentials.
      rm -f "$KUBECONFIG"
    fi
  fi
  exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
[[ -f Cargo.lock ]] || cp tests/kubernetes/Cargo.lock Cargo.lock
if ! cmp -s Cargo.lock tests/kubernetes/Cargo.lock; then
  echo 'Cargo.lock differs from tests/kubernetes/Cargo.lock; synchronize the fixture lockfile before running.' >&2
  exit 2
fi
docker info >/dev/null
docker build -f tests/kubernetes/Dockerfile -t netplane-e2e:local .
created=true
kind create cluster --name "$cluster" --config tests/kubernetes/kind.yaml --kubeconfig "$KUBECONFIG" --wait 120s
kind load docker-image --name "$cluster" netplane-e2e:local
# Run each scenario in its own process and stop at the first failure, retaining
# that namespace for diagnostics without colliding over the shared NodePort.
listing=$(cargo test --locked -p netplane-kubernetes-e2e --test cluster "$filter" -- --ignored --list)
mapfile -t scenarios < <(awk '/: test$/ { sub(/: test$/, ""); print }' <<< "$listing")
((${#scenarios[@]})) || { echo "No scenarios match: $filter" >&2; exit 2; }
for scenario in "${scenarios[@]}"; do
  cargo test --locked -p netplane-kubernetes-e2e --test cluster "$scenario" -- --ignored --exact --test-threads=1 --nocapture
done
