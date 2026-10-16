# ADR 0004: Internal graph composition prototype

**Status:** Accepted for the internal prototype

**Date:** 2026-10-17

## Context

Tower readiness has no request argument. A router that selects a backend from the request cannot promise that backend's readiness in its own `poll_ready`. Netplane already owns bounded endpoints, immutable publication, and global admission. The integration prototype composes these pieces without adding a public graph API.

## Decision

Use typed, acyclic Tower composition for sequential middleware and one selected branch per request. The example and integration test use a `FilterLayer` before an internal request-aware router. The router owns cloneable `Endpoint` handles for two named, independently owned backend services. It chooses synchronously from `&Request`, then transfers the request to exactly one endpoint. Cloning a handle does not clone the raw service.

The selected endpoint's worker polls readiness and calls its own service instance. Each backend has explicit concurrency and buffer capacities. The complete root is validated through `GraphDraft` and published as one immutable version. Accepted requests retain their selected version; endpoint execution futures retain worker ownership. An unmatched route must be rejected explicitly unless a fallback was deliberately configured. The two-branch prototype has an exhaustive parity route.

This model does not define a graph schema or a public `GraphBuilder`, `Graph`, `Node`, or `Router`. Cycles, fan-out and joins, mutable nodes, shared service instances, and serialized declarations are outside this decision.

## Readiness alternatives

Per-backend buffering is the prototype choice. It lets each endpoint own readiness and dispatch, works with stateful non-`Clone` services, and isolates an unready A worker from B. It adds one worker and bounded queue per backend. The router may itself report readiness while the selected endpoint is saturated; backpressure then appears as a pending execution future. `RuntimeLimits::max_outstanding` bounds accepted callers, including unpolled futures and callers waiting at either queue.

Direct readiness-aware routing might reduce queues and worker overhead, but the route is unknown during the router's `poll_ready`. Selecting after readiness would require synchronization or reservations that preserve the same service instance and version, with possible head-of-line blocking. These are design considerations, not measured performance or complexity results: the prototype implements only per-backend buffering.

The outer graph endpoint also has a concurrency limit. A waiting A request consumes one outer execution slot, so that limit must leave capacity for B to progress. If all outer slots are occupied by A, B can wait even though B is ready. The integration test demonstrates B progress with one A request and two outer slots; it does not claim isolation under arbitrary A saturation.

## Ownership and cancellation

Netplane guarantees its accepted-request budget is released when an execution future completes or is dropped. It retains selected versions and endpoint worker references for accepted requests, and aborts a worker after its last Netplane owner disappears. Queued waits can be canceled. A dispatched call's cancellation behavior, any work spawned by a service, and the duration of that work belong to the user-supplied Tower service. Shutdown closes admission immediately and waits for accepted futures to drain; it cannot force a service's independent work to stop.

## Findings and follow-up

The prototype tests demonstrate that `call` follows successful backend readiness, B completes before A becomes ready when an outer slot is available, and a non-`Clone` stateful service works through its endpoint. Publication lets old work complete on old backend instances, and obsolete workers release after their last owners disappear. Backend errors propagate without rerouting. The configured Tower buffer supplies the queue bound; the prototype's dispatch test asserts the concurrency limit rather than measuring queue occupancy. Independent backend readiness does not guarantee independent progress when all outer slots are occupied. These findings support a later `GraphBuilder` design, but topology validation and policy validation remain open for PR 10.
