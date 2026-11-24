# Kubernetes integration testing

Run `./scripts/kubernetes-e2e.sh` with Docker, Kind **v0.31.0**, kubectl and Rust
installed. The suite creates a single-node Kubernetes **v1.32.11** cluster pinned by
image digest, builds both Rust fixtures using Cargo.lock, and loads their shared
image directly into Kind. `tests/kubernetes/Cargo.lock` pins the fixture dependency
graph. The runner copies it only when the workspace lockfile is absent and rejects
an existing lockfile that differs, before building images or creating a cluster.
This preserves the library's untracked workspace lockfile policy while ensuring
host and container dependency resolution agree. When deliberately updating
dependencies, synchronize the fixture lockfile with the workspace lockfile.
No registry or external application is required.
Port 18080 on localhost must be available. Scenarios run sequentially, stopping
at the first failure, in separate namespaces; ordinary `cargo test --workspace` compiles but ignores cluster tests.

Use `--test readiness`, `--test recovery`, or another Rust test-name substring to
select a scenario. Use `--keep-cluster` to retain the cluster and kubeconfig for
debugging. The script prints its name and kubeconfig path. Delete it afterwards
with `kind delete cluster --name NAME`. Do not upload the retained kubeconfig.

The gateway runs the actual Discovery → KubernetesIntegration →
ReconciliationDriver → HttpGraphPreparer pipeline. Its namespace Role permits only
list/watch of Services and EndpointSlices. The host harness uses a separate Kind
administrative client to create workloads and check authorization. Neither fixture
is production deployment configuration. Test controls must never be exposed in a
production gateway.

`/observation` and `/status` return a combined sample of discovery endpoints,
discovery revision, submitted sequence, desired/applied revisions and publication.
The fixture checks the discovery observation identity, integration status,
reconciliation transition sequence and controller inspection before and after
collection, retrying unstable reads. Processed-resource acknowledgements are read
before discovery and reconciliation state, and assertion-sensitive diagnostics
are collected before the final validation. Readiness uses the same correlation. The
harness returns the exact observation that satisfied its bounded wait. `/topology`
remains a convenience view of discovery identities; assertions use `/observation`.
Endpoint checks compare Pod UIDs against real EndpointSlice eligibility conditions.

The fixture distinguishes the following signals:

| Signal | Meaning |
| --- | --- |
| Liveness (`/healthz`) | The application HTTP server is running. |
| Runtime readiness | A usable published graph exists and runtime admission is open; this is a separate production policy. |
| Discovery health (`/status.discovery_state`) | Both Kubernetes watchers are operational. |
| Convergence (`/status.converged`) | The latest observed desired configuration is applied and both watchers are healthy. |

The fixture's `/readyz` deliberately requires convergence and nonempty endpoints.
It becomes unready during API disconnection even when the retained graph can still
serve requests. Its NodePort Service sets `publishNotReadyAddresses: true` so tests
can continue routing during that outage. This synchronization policy is not a
production readiness recommendation: production applications can keep runtime
availability separate from discovery health and convergence.

The metadata test waits for `discovery.resource_processed` to report the final
Service resource version before asserting unchanged preparation and publication.
That diagnostic event is emitted after cache processing and observation delivery;
resource versions are opaque strings, never compared numerically. Marker identity
strings are constructed only when DEBUG tracing is enabled. The preparation
gate holds one real candidate while distinct endpoint sets are observed and
submitted. After release, the test proves obsolete queued states were coalesced
and the final state was prepared and applied, with at most one active preparation.
These diagnostics and controls belong only to the fixture; no public runtime API
was added. The first preparation history entry is the intentional empty bootstrap,
separate from the complete discovery snapshots checked afterwards.

The readiness scenario blocks a request in a known Pod, makes that Pod unready,
waits for a replacement publication and proves new traffic excludes it before
releasing the old request. Rollout tests record successes and failures during the acknowledged rollout window; Kubernetes
termination can break transport connections and is not a graph lifetime guarantee.
The API fault control closes existing TLS tunnels and refuses new ones, including
watches. Recovery relists quiet collections so watcher health recovers without
requiring unrelated Service changes. TLS authentication and namespace RBAC remain
enforced. Discovery owns at most eight API tunnels in the fixture; graph preparation permits at most sixteen
backends, eight requests per backend and thirty-two outstanding runtime requests.
The reconciler uses its single worker and latest-value mailbox.

The non-ignored `expired_watch` test serves a deterministic Kubernetes HTTP API:
it emits a watch ERROR with status 410, then a complete replacement list with a
deleted endpoint and duplicate slices. It verifies kube-rs actually relists and
the observed replacement snapshot is deduplicated. A latest-value receiver cannot
prove that no transient snapshot was overwritten; acknowledged cache-level tests
cover that invariant, interrupted relists and independently recovering collections.
Additional paused-time tests cover retry minimum delays after successful relists,
initial-list failures, and cancellation during backoff or a pending relist. These tests run in dedicated CI separately
from Kind reconnection, without depending on API-server history compaction.

On exit, including failures, the script collects namespace fixture logs, resources,
events, reconciliation status and Kind logs under `artifacts/kubernetes`, then
deletes the cluster unless retained. Successful scenarios delete their namespace;
failed scenarios leave it available to diagnostic collection. The exit status of
the suite is preserved even if diagnostics or cleanup fail. If the suite succeeds
but cluster deletion fails, the runner reports failure. Diagnostic export is best
effort and never prevents deletion. The orchestration tests inject creation,
deletion, diagnostic and combined failures, and reject divergent lockfiles.
Logs and object dumps may contain environment information;
CI uploads only this dedicated directory and excludes kubeconfig credentials.

These tests validate execution against running Pods; active probing, retries,
Netplane-managed rollout and automatic rollback policies remain separate features.
