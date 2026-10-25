# Netplane architecture

## Status and purpose

Netplane is a Rust library for dynamically reconfigurable Tower service compositions. The workspace has a bounded Tower endpoint, immutable version publication, shutdown admission control, and `xtask`. [Execution semantics](architecture/execution-semantics.md) is the canonical contract for implemented runtime behavior.

## Design constraints

1. **Tower interoperability.** Compose with `tower::Service` and preserve its readiness contract. Introduce a Netplane-specific abstraction only when a working prototype shows why it is needed.
2. **Graph composition.** Use the implemented single-router `GraphBuilder` for named backend composition and ordinary Tower layers for middleware. Broader graph representations require a concrete use case.
3. **Immutable publication.** Build and validate a replacement configuration before publishing it. Active graph versions must not be modified in place.
4. **Request lifetime.** An in-flight request must retain the graph version and resources it needs until completion or cancellation. Readiness and dispatch for an individual request must operate on a consistent service instance and graph version. Graph replacement must not invalidate an outstanding readiness decision or prematurely release resources required by pending requests.
5. **Backpressure.** Bound concurrency and queues. Do not bypass readiness or spawn unbounded tasks to hide load.
6. **Cancellation and shutdown.** Define ownership and cleanup so cancellation, timeouts, and shutdown release resources without disrupting unrelated requests.
7. **Transport independence.** Core graph execution must be testable without sockets and must not depend on Hyper, Kubernetes, or a specific deployment system.
8. **Observability.** Instrument lifecycle and failures with `tracing` when the corresponding runtime behavior exists, without leaking sensitive request data.

## Workspace boundaries

| Path | Responsibility |
| --- | --- |
| `crates/netplane` | Public, transport-independent runtime library. Keep its public dependency surface small. |
| `crates/netplane-http` | Optional bounded HTTP ingress and Axum listener integration; depends on `netplane`. |
| `crates/xtask` | Build and quality automation. Never a runtime dependency of `netplane`. |
| `docs/adr` | Accepted architectural decisions and their consequences. |

The HTTP adapter is a separate crate because body buffering, status mapping, and listener integration require protocol dependencies. The core does not depend on it.

## Implemented runtime

The in-memory runtime accepts a Tower service root, bounds its concurrency and buffer per version, enforces a global accepted-request budget, and publishes validated immutable versions atomically. Requests retain the version selected when `Handle::execute()` is called. Cancellation, worker ownership, and graceful shutdown follow the [execution contract](architecture/execution-semantics.md). Integration tests exercise publication, readiness, resource lifetime, and shutdown without sockets. `GraphBuilder` exposes the [composition prototype](adr/0004-graph-composition.md) as a validated single-router composition over named, independently bounded backend endpoints. Ordinary Tower layers provide middleware.

## Current limits and future work

A positive runtime admission limit bounds accepted, unfinished requests across all versions and handles; excess requests are rejected immediately. `GraphDraft` validates capacities only. `GraphBuilder` validates its declared single-router topology and capacities, but cannot prove application policy or selector behavior. Arbitrary directed graphs, cycles, shared nodes, and dynamic node editing require a concrete use case before implementation. Structured tracing events are documented in [Runtime observability](observability.md). `ReconfigurationController` provides conditional publication, bounded rollback history, and inspection. The optional HTTP adapter reads and decodes bounded bodies before selecting a version through `try_execute`.

## Decisions

- [ADR 0001: Tower service model](adr/0001-tower-service-model.md)
- [ADR 0002: Runtime reconfiguration](adr/0002-runtime-reconfiguration.md)
- [ADR 0003: Crate boundaries](adr/0003-crate-boundaries.md)
- [ADR 0004: Internal graph composition prototype](adr/0004-graph-composition.md)
- [ADR 0005: Reconfiguration control](adr/0005-reconfiguration-control.md)

Inherited tooling ADRs in `docs/adr/tooling/` are not Netplane runtime design decisions.
