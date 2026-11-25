# Backend health and eligibility

Health is optional operational state composed with ordinary Tower services and
existing routing policies. Discovery determines graph membership and readiness;
health only removes candidates from that membership. Health observations never
publish a graph, mutate configured weights, retry, or move dispatched requests.

## Identity and ownership

`HealthRegistry` registers a `BackendIdentity` containing service, instance, and
incarnation strings. Use a stable endpoint identity and a physical replacement
identifier, such as a Pod UID. Never identify every endpoint by service name alone.
Compatible repeated registrations share state. A different policy or recovery mode
for a still-live identity is rejected; it cannot reset a retained graph's state.
Replacing an incarnation creates independent state. An unknown incarnation cannot
be inferred from an IP address; applications must supply one when discovery cannot.

The registry indexes weak references. Handles in graph selectors, wrapped services,
probe registrations, and observation tokens retain records. Publication, rollback,
and candidate cleanup therefore follow ordinary ownership. Old graphs retain their
own membership and health handles. Final reference release reclaims the record.

## Recovery and observations

`HealthPolicy` uses nonzero consecutive failure and recovery thresholds and a
monotonic cooldown. Passive backends start healthy. Failure threshold exhaustion
marks a backend unhealthy. After cooldown it becomes recovering and can receive
ordinary weighted traffic to collect recovery evidence. This is not a circuit
breaker: there is no dedicated half-open request budget.

Active backends start recovering and remain ineligible until enough successful
active probes arrive. `begin_observation()` marks passive execution and
`begin_probe()` marks a probe; each token retains its source. Passive successes
never advance active-mode recovery, including calls selected earlier that begin
execution after cooldown. Those calls finish on their original backend. Valid
failures from either source return a recovering backend to unhealthy. Failed recovery returns to unhealthy and restarts cooldown.
Active and passive observations share the same state machine. Successful checks
while unhealthy cannot bypass cooldown. Cooldown transitions are advanced lazily
by observation or inspection; Tokio's monotonic clock supports deterministic tests.
Ignored observations change neither counters nor observation timestamps. Reporting
samples monotonic time after acquiring the per-record mutex, matching serialized
update order; observation timestamps and cooldown transitions cannot be backdated
by a reporter that waited for the lock.

Each execution captures an owning observation token at `Service::call`. Tokens
identify the record and health generation. State transitions invalidate earlier
tokens, preventing delayed successes from restoring a backend after exclusion.
A token cannot report against another incarnation. Counters saturate rather than
wrap. `HealthLayer` forwards readiness to the exact wrapped instance, preserves
result types, and records each completion once. Dropping its future records
nothing. Apply it directly to backend execution, inside Netplane's queues and
capacity limits, so local admission and saturation are never backend failures.
For deadlines, wrap an upstream execution timeout inside the health layer;
a deadline on the outer runtime future is caller cancellation, not evidence.

## Routing

`HealthAwareRouting` binds one health handle per `WeightedRouting` label. It samples
each positive-weight candidate once into a stack mask, then selects with the
existing FNV-1a hash modulo remaining total weight. Selection is O(N), allocates
nothing, and preserves existing bucket assignments when all candidates are
eligible. Weights remain immutable. `UniformRouting::select_eligible` supports
discovered endpoint sets; sets up to 128 use stack storage, larger sets use one
bounded-by-membership mask allocation.

An eligibility mask samples independent records sequentially, not a simultaneous
transaction across the registry. Once sampled, it is fixed for that routing
decision. Concurrent updates affect later decisions. Stronger cross-backend
snapshot consistency is not promised.

Use `GraphBuilder::try_router` to propagate `RoutingError::NoEligibleBackends`.
Errors dispatch no backend, including the declared fallback. Successful unknown
labels retain ordinary fallback behavior. Both buffered and backend-only graphs
use current health when route selection runs inside the pinned graph. A previously
selected destination remains fixed while waiting for its backend or executing.

Removing a stable backend can shift all selectable traffic to a canary, regardless
of configured canary percentage. Future rollout policies must inspect operational
eligibility as well as configured weights.

## Active checks

`HealthScheduler` is caller-owned; create it, obtain its registration handle, and
run its `run` future on Tokio. Configuration requires positive interval and timeout,
nonzero global concurrency, and an explicit maximum registration count. Hold each
`ProbeRegistration` alongside the graph or configuration that needs it. Unchanged
registrations share one check only when the health record and `ProbeIdentity`
configuration key both match. Incompatible probe configuration returns typed
`IncompatibleProbeConfiguration`; the old registration remains unchanged. A key
must cover all behavior-affecting settings. Probe configuration is distinct from
physical incarnation: do not change a backend incarnation to change a probe path.

The scheduler starts no permanent endpoint tasks. It keeps bounded registration
metadata and at most the global limit of check tasks. Each backend has at most one
running check. The next deadline follows completion; missed intervals do not queue.
Oldest due registrations run first. Probe timeouts report failure, registration
cancellation reports nothing, and shutdown aborts and joins managed checks.
`HealthSchedulerHandle::status()` exposes registered and running counts alongside
both configured limits. The registration budget covers the **union** of identities
held by active, historical, in-flight, and candidate graphs. It is not just an
active-topology limit. A full active topology needs replacement headroom: with a
budget of 100 and 100 active endpoints, replacing one requires 101 live identities
during preparation. Retrying cannot converge while every old lease remains held.
Retained history and unpolled admitted futures can prolong that requirement.
Capacity becomes available only after the last relevant lease drops; snapshots
help operators distinguish that condition from probe concurrency saturation.

Probe completion is accepted while holding the same registration mutex used by
removal. If acceptance wins, the health state update completes before removal can
return. If removal wins, the obsolete completion is discarded. Tracing callbacks
and final record destruction run after unlocking, so subscribers may inspect the
scheduler. Delaying callbacks cannot apply an old result to a replacement probe.

Checks consume an independent budget, never runtime admission. Shared HTTP client
pools still require sensible concurrency limits. Dropping `run` aborts managed
work; await shutdown completion when synchronous resource reclamation matters.

## Adapter boundaries

`netplane-http::health` supplies a configurable status classifier and a bounded
GET probe. Its compatibility identity includes origin, path, response bound,
classifier overrides and transport identity. An opaque caller-provided reqwest
client cannot be introspected: the default conservatively shares only upstream
clones. `with_transport_key` explicitly declares compatibility of separately built
clients; callers must include timeout, proxy, retry, redirect, protocol, and TLS
behavior in that key. Kubernetes supplies a key for its controlled client settings,
including exchange timeout. The passive default treats 2xx as success, 5xx as
failure, and other statuses as ignored. The active probe default requires 2xx and
treats all other statuses as failure; a missing or redirected health endpoint therefore fails the health
contract. Explicit overrides remain available through `with_classifier`.
Upstream transport errors and exchange timeouts fail; local runtime errors do not.
The HTTP error mapper maps no eligible backend to 503.

Kubernetes translation first filters readiness and termination. The optional
health configuration on `HttpGraphPreparer` reuses endpoint identities including
Service and known Pod UIDs. A healthy local record cannot re-add a discovery-excluded
endpoint. Readiness changes do not reset unchanged local state. Netplane writes no
Kubernetes resource to communicate local observations.

## Diagnostics and measurement

Registry snapshots expose owned per-backend state and aggregate counts. These are
diagnostic samples across records, not a global transaction. State transitions emit
structured tracing events after releasing state locks. Successful requests and
probes do not emit INFO events. Active check counts cover executing probe futures.

Run `cargo bench -p netplane --bench health` for disabled, all-healthy,
partially-unhealthy, concurrent-observation, and observation-update measurements.
The 128-label weighted fixture has 100 positive weights and 28 zero weights because
the existing integer percentage policy cannot express 128 positive targets.
Performance results depend on host and contention; a five-percent target is an
aspiration, not an API guarantee. See the [measured benchmark results](../health-performance.md).
