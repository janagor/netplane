# netplane-kubernetes

Optional namespace-scoped Kubernetes Service and EndpointSlice discovery. This
crate depends on neither `netplane` nor `netplane-control`; it observes topology
without creating HTTP upstreams or publishing runtime graphs.

```sh
cargo run -p netplane-kubernetes --example discover -- \
  --namespace default --service backend-stable --service backend-canary
```

The example uses the ordinary kube-rs client configuration and exits on Ctrl-C.
`manifests/rbac.yaml` grants only namespace-scoped Service and EndpointSlice read
permissions. Change the namespace consistently in all objects before applying.
No Pod, Secret, ConfigMap, or cluster-wide permissions are needed.

Construct `Discovery` with a client and `DiscoveryConfig`, then poll `run` in a
caller-owned task with a `CancellationToken`. Constructors spawn no tasks. Handles
provide `snapshot`, `subscribe`, `status`, and `subscribe_status`. Snapshot channels
coalesce obsolete updates and continue progressing with no subscribers. Consumers
can retain immutable `Arc` snapshots; that caller-owned retention is not bounded by
the discovery channel. Clone snapshots and release watch read guards before
processing or awaiting; a retained guard can block the sender:

```rust,ignore
let snapshot = { receiver.borrow_and_update().clone() };
if let Some(snapshot) = snapshot {
    process_snapshot(snapshot).await;
}
```

The same guard-lifetime rule applies to status receivers. Status and snapshots are
independent observations, not an atomic pair: a snapshot can briefly have a newer
revision than `status.latest_revision`. Reconciliation must use the revision on the
snapshot it processes.

Dropping or cancelling the runner stops both watch streams.

`None` means initial synchronization is incomplete; revision one with an empty
map is a successfully synchronized empty topology. Revisions advance only when
normalized topology changes. These local revisions are distinct from Kubernetes
resource versions, desired-state revisions, and runtime publication generations.

Each collection stages complete relists before replacing its committed cache.
Publication waits for both initial listings and pauses during either relist,
retaining the last complete snapshot. Independent listings are eventually
consistent, not a transactional cross-resource view. Once initialized, updates
from either watcher may publish using the other collection's committed cache even
when that watcher reports `Recovering`. This combines fresh and potentially stale
observations; recovery alone does not freeze publication. Relisting does freeze
publication until both caches are synchronized. A watch error itself never changes
topology or advances a revision. Consumers such as reconciliation decide whether
degraded discovery is acceptable. kube-rs supplies reconnection,
resource-version expiration handling, and retry backoff. Watch failures expose
`Recovering`; unexpected stream termination returns a typed error and marks that
watcher `Failed`. Cancellation interrupts even pending backoff. Status reports the
latest observed event, not an active connectivity probe; a quiet watch can remain
`Recovering` until its next event. Authentication and permission errors are retried
with backoff and reported through status and structured tracing.

Endpoints use concrete slice ports, matched by Service port name and transport.
Missing and empty port names both normalize to `None` for matching, endpoint
deduplication, and snapshots; representation-only changes do not advance revisions.
IPv4, IPv6, TCP, UDP, SCTP, headless Services, and multiple slices are supported.
Orphan slices and mismatched Service owner UIDs are excluded. ExternalName Services,
non-IP address types, malformed addresses, unknown protocols, and nonconcrete ports
are skipped with diagnostics. Unready and terminating endpoints remain observable.
Default eligibility treats unspecified readiness as ready and excludes termination;
`publishNotReadyAddresses` is preserved as a policy hint without overriding it.
Duplicate destinations merge readiness/serving toward false and termination toward
true. Unknown conditions remain unknown unless another duplicate specifies them.
Output order and duplicate merging are deterministic.

Tests use constructed resources and synthetic watcher streams; no cluster, sockets,
Docker, or Netplane runtime is required. Real-cluster end-to-end tests and translation
to desired runtime configurations belong to subsequent integration work.
