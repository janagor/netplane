# Execution semantics

This is the canonical contract for the implemented runtime. `crates/netplane` executes a Tower service root, not an arbitrary graph schema.

## Validation and publication

`GraphDraft` accepts a Tower service root, including one built with `ServiceBuilder` and layers. Validation checks positive concurrency and buffer capacities and starts a Tower buffer worker. It does not parse a graph schema, validate topology, inspect routes, or prove that an application policy is correct. The [live reconfiguration example](../../crates/netplane/examples/live_reconfiguration.rs) implements a simulated authorization filter and routing policy as ordinary Tower services.

`Handle::execute()` admits a request and selects an immutable version synchronously when called, before its returned future is polled. `Netplane::publish()` atomically replaces a complete version using `ArcSwapOption`. An accepted request keeps its selected version through completion or future drop, so an old request can finish after a new composition is published. The Tower buffer worker polls readiness and calls the same owned service instance. Requests selected after publication use the new version.

## Capacity and cancellation

Each version has its own concurrency limit and bounded Tower buffer. Capacity is not shared across retained versions; a transition can therefore have work executing in both versions. The buffer bounds queued requests, while callers waiting for queue capacity remain pending. There is no global budget for accepted request futures or queue waiters; the outstanding counter only rejects overflow. The example's router chooses between two always-ready in-memory backends, and its filter checks a caller-provided Boolean before routing. This is only a simulated authorization decision, not identity authentication.

Dropping a queued request cancels its wait. Cancellation of a dispatched call depends on the underlying service. Netplane owns each Tower buffer worker and aborts it when the last owning endpoint and request future reference disappears, including when the worker is stuck in readiness.

## Shutdown

Calling `Netplane::shutdown(grace_period)` closes admission synchronously across handles, even before its returned future is polled. Publication is rejected from that point. Shutdown waits for accepted request futures to complete or be dropped, including futures never polled. A timeout reports the outstanding count and leaves admission closed and draining, so shutdown can be retried. A successful drain clears the active graph even if inactive handles remain. Shutdown cannot forcibly reclaim resources held by an uncooperative user-owned future or tasks spawned independently inside a service.

## Outside the current runtime

General graph construction, topology and policy validation, global admission budgeting, observability, a reconfiguration control API, and HTTP integration remain future work.
