# netplane-kubernetes

Namespace-scoped Kubernetes Service and EndpointSlice discovery with explicit
translation into bounded Netplane HTTP graphs. Discovery remains independently
usable. The optional integration uses the existing `netplane-control` reconciler
and `netplane-http` forwarding implementation; request execution never contacts
the Kubernetes API.

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

The same guard-lifetime rule applies to status receivers. Legacy status and snapshot
subscriptions remain independent observations. Use `subscribe_observations()` for
one `Arc<DiscoveryObservation>` containing both topology and watcher health captured
after the same watcher event. Its `status.latest_revision` matches its snapshot.
This makes the local observation coherent; Kubernetes collections remain eventually
consistent across independent resource watches.

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

Discovery tests use synthetic watcher streams. Translation, reconciliation and
request-retention tests use synthetic snapshots and local HTTP servers, without
Docker, Kind or a Kubernetes installation. Real-cluster verification remains future work.

## Discovered gateway

```sh
cargo run -p netplane-kubernetes --example discovered_gateway -- \
  --namespace default --service payments --port-name http \
  --listen 127.0.0.1:8080 --backend-capacity 32
```

Use `--port-number 80` instead of `--port-name http` to select a virtual Service
port numerically. Actual upstream ports always come from EndpointSlices, including
different target ports across slices. Headless Services and IPv4/IPv6 IP endpoints
work identically. Only the explicitly bound Service becomes a routing destination;
ExternalName and non-TCP forwarding are unsupported. No ClusterIP is used.

The listener initially returns HTTP 503 until complete discovery becomes available.
`x-request-key` supplies a sticky routing key. When it is absent, the example assigns
a unique counter key to each request before admission, so repeated requests to one
path distribute across the eligible endpoints. The key stays fixed for that request;
there is no retry or fallback to another backend. INFO-level tracing is initialized
by default. Applications can supply their own borrowed-key callback to
`HttpGraphPreparer`. Uniform routing hashes key bytes modulo the sorted endpoint
count, performs no retries, and uses an immutable backend set per graph version.
Changing the set can remap keys; this is not consistent hashing.

## Translation and reconciliation

Create one `ServiceBinding` and call `translate` on a complete `DiscoverySnapshot`.
The result is an immutable `DesiredState` with sorted, deduplicated destinations and
positive backend capacities. Its `Eq` implementation supplies the existing control
layer's semantic deduplication; no Kubernetes configuration fingerprint is added.
Service and known Pod UIDs distinguish recreation. EndpointSlice names, revisions,
resource versions, ordering and unrelated metadata do not enter desired equality.
Duplicate destinations merge readiness toward false and termination toward true.
Missing target UIDs do not erase a known UID; duplicates with one distinct known UID
retain it. If every UID is missing, identity remains unknown. Two different known
UIDs for the same address/port make the destination ineligible, regardless of
ordering. Any retained UID on that excluded destination is diagnostic only.

Eligibility accepts missing readiness, rejects explicit unready or terminating
endpoints, and ignores serving on terminating endpoints. Invalid addresses and
unresolved EndpointSlice ports are excluded with discovery diagnostics. A selected
Service port must exist and be unambiguous TCP. `publishNotReadyAddresses` is rejected
because Kubernetes can override its readiness signals. Service deletion and zero
eligible endpoints are valid configurations and publish typed `Unavailable` errors,
mapped to HTTP 503 by `map_http_error`. They never retain obsolete routes for new
requests. A later eligible endpoint set recovers through ordinary reconciliation.

Construct `Reconciler<DesiredState, HttpGraphPreparer, _, _>` and its
`ReconciliationDriver`, then connect `KubernetesIntegration::new` to the discovery
handle and driver's `UpdateSender`. Poll all runners in caller-owned tasks. The
bridge waits for initialization, gates updates on both watchers being healthy, and
uses a single coherent discovery observation. When it processes API recovery,
incomplete relists or translation rejection, the bridge calls `invalidate_pending()` to withdraw earlier
candidates and freeze the active graph. Health reflects watcher events, not an
independent connectivity probe. A quiet watch may remain in its last observed state.

`IntegrationStatus` reports submitted discovery revision, control sequence and
backend count, or a typed translation rejection. The driver's own status reports
validation/preparation/publication outcomes and retry decisions. Translation failures
do not close the bridge. Invalidation is serialized with the atomic swap: after it
returns, an earlier candidate cannot publish. A swap that completed first stays
active; accepted requests retain it. Preparation may finish before its rejected
candidate releases resources. Recovery submits the latest valid observation, even
when its topology revision is unchanged. The driver coalesces
updates and reconsiders publication conflicts using its existing policies. No task
is spawned per Kubernetes event. Equal topology does not publish a new version.

Observation delivery is latest-value, not an event log. If discovery disconnects and
recovers before the bridge next polls, the intermediate unhealthy observation can
be overwritten. If the recovered topology has the same revision, the bridge neither
invalidates nor resubmits its existing candidate; that candidate may publish. There
is no recovery epoch or synchronization from every discovery event to publication.
The freeze guarantee begins when the bridge processes a degraded or rejected
observation and calls invalidation. It does not begin when discovery produces an
observation, and it does not revoke a swap that already completed.

## Resource ownership and shutdown

Each prepared version owns a new HTTP client, shared among only that version's
backends. Clients, queues and workers are not reused across versions. This trades
connection churn for safe lifetime isolation, including when a new known Pod UID
appears at an old IP. Idle pooling retains at most eight connections per destination
and expires after 30 seconds. `UpstreamLimits::max_backends` defaults to 256 and
rejects larger candidates before allocating clients/workers. Client construction or
graph validation failure releases the candidate and retains the active graph.

`backend_capacity` bounds both concurrent exchanges and the Tower queue per backend;
`RuntimeLimits::max_outstanding` bounds all accepted unfinished requests across
versions and handles. Execution uses backend-only GraphBuilder routing. Saturation
waits on the selected backend and never reroutes. The HTTP adapter bounds request
bodies before admission; outbound response collection and exchange duration are
bounded by `UpstreamLimits`.

Requests accepted before publication retain their version, including unpolled
futures. New requests use the replacement set; removed endpoints receive no new
requests under that version. Old clients remain alive until their workers, accepted
requests and optional rollback history release them. Cancellation releases Netplane
ownership but cannot undo work already received by a Pod, and retaining a version
cannot keep a physically removed Pod alive.

Cancel discovery and integration when shutting down the runtime. Dropping or
cancelling the bridge closes shared control update admission. Await the driver to
confirm its last preparation/publication has finished; independently spawned tasks
remain application-owned. The example closes runtime admission, drains accepted
requests, stops the listener and joins runners, with a bounded forced-cleanup path.

## Real cluster validation

See [local Kubernetes integration tests](../../docs/kubernetes-integration-testing.md)
for the pinned Kind environment, fixture controls, namespace-scoped permissions,
API fault injection and diagnostic artifacts. Watch errors trigger a complete
relist before reporting recovery, including collections with no intervening updates.
The last published topology remains available throughout recovery.
