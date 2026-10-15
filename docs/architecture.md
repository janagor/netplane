# Netplane architecture

## Status and purpose

Netplane is a Rust library for dynamically reconfigurable Tower service compositions. The workspace has a bounded Tower endpoint, immutable version publication, shutdown admission control, and `xtask`. [Execution semantics](architecture/execution-semantics.md) is the canonical contract for implemented runtime behavior.

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
| `crates/netplane` | Public, transport-independent runtime library. Keep its public dependency surface small. |
| `crates/xtask` | Build and quality automation. Never a runtime dependency of `netplane`. |
| `docs/adr` | Accepted architectural decisions and their consequences. |

Create another crate only when a concrete dependency or API boundary justifies it. A future transport adapter may depend on the core; the core must not depend on that adapter.

## Implemented runtime

The in-memory runtime accepts a Tower service root, bounds its concurrency and buffer per version, and publishes validated immutable versions atomically. Requests retain the version selected when `Handle::execute()` is called. Cancellation, worker ownership, and graceful shutdown follow the [execution contract](architecture/execution-semantics.md). Integration tests exercise publication, readiness, resource lifetime, and shutdown without sockets.

## Current limits and future work

There is no global admission budget across versions or callers waiting for queue capacity. Validation checks positive capacities, not graph topology or application policy. There is no general-purpose graph construction API. Global admission budgeting, observability, a reconfiguration control API, HTTP integration, and broader graph composition are planned work; none are current guarantees. Arbitrary directed graphs, cycles, shared nodes, and dynamic node editing require a concrete use case before implementation.

## Decisions

- [ADR 0001: Tower service model](adr/0001-tower-service-model.md)
- [ADR 0002: Runtime reconfiguration](adr/0002-runtime-reconfiguration.md)
- [ADR 0003: Crate boundaries](adr/0003-crate-boundaries.md)

Inherited tooling ADRs in `docs/adr/tooling/` are not Netplane runtime design decisions.
