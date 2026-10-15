# Netplane architecture

## Status and purpose

Netplane is a Rust library for dynamically reconfigurable Tower service graphs. The workspace has a bounded Tower endpoint, immutable version publication, and `xtask`. Shutdown admission control is not implemented yet.

## Design constraints

1. **Tower interoperability.** Compose with `tower::Service` and preserve its readiness contract. Introduce a Netplane-specific abstraction only when a working prototype shows why it is needed.
2. **Graph composition.** Model services and policies as validated executable compositions. Decide the graph representation through a small in-memory prototype.
3. **Immutable publication.** Build and validate a replacement configuration before publishing it. Active graph versions must not be modified in place.
4. **Request lifetime.** An in-flight request must retain the graph version and resources it needs until completion or cancellation. Readiness and dispatch for an individual request must operate on a consistent service instance and graph version. Graph replacement must not invalidate an outstanding readiness decision or prematurely release resources required by pending requests.
5. **Backpressure.** Bound concurrency and queues. Do not bypass readiness or spawn unbounded tasks to hide load.
6. **Cancellation and shutdown.** Define ownership and cleanup so cancellation, timeouts, and shutdown release resources without disrupting unrelated requests.
7. **Transport independence.** Core graph execution must be testable without sockets and must not depend on Hyper, Kubernetes, or a specific deployment system.
8. **Observability.** Instrument lifecycle and failures with `tracing` when the corresponding runtime behavior exists, without leaking sensitive request data.

## Workspace boundaries

| Path | Responsibility |
| --- | --- |
| `crates/netplane` | Public library and future graph runtime. Keep its public dependency surface small. |
| `crates/xtask` | Build and quality automation. Never a runtime dependency of `netplane`. |
| `docs/adr` | Accepted architectural decisions and their consequences. |

Create another crate only when a concrete dependency or API boundary justifies it. A future transport adapter may depend on the core; the core must not depend on that adapter.

## First implementation milestone

Build an in-memory, transport-independent prototype that executes a composition of Tower services and publishes a new graph version. Its integration tests should show that requests already using the old version complete with the correct resources while new requests use the published version. Add focused tests for readiness, bounded concurrency, cancellation, and shutdown as the corresponding mechanisms are introduced.

The initial prototype uses a Tower service root as its composition, validates positive concurrency and queue capacities, and publishes complete versions with `ArcSwap`. `execute()` selects a version synchronously when invoked and retains it through completion or future drop. Capacity is per version. A dropped future cancels its queue wait, while cancellation of dispatched work depends on the service. There is no HTTP listener or deployment integration. Arbitrary directed graphs, cycles, shared nodes, and dynamic node editing are not initial requirements; add them only when a concrete use case justifies them.

## Decisions

- [ADR 0001: Tower service model](adr/0001-tower-service-model.md)
- [ADR 0002: Runtime reconfiguration](adr/0002-runtime-reconfiguration.md)
- [ADR 0003: Crate boundaries](adr/0003-crate-boundaries.md)

Inherited tooling ADRs in `docs/adr/tooling/` are not Netplane runtime design decisions.
