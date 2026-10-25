# Architecture

Netplane is an early stage library for dynamically reconfigurable Tower service compositions. Bounded execution, immutable version publication, and graceful shutdown are implemented.

The core interoperates with `tower::Service`, preserves readiness and request lifetime across publication, bounds per-version execution and global accepted requests, and stays independent of concrete transports. `GraphBuilder` compiles a validated single-router composition; arbitrary DAGs, nested graphs, and dynamic node editing are unsupported. `ReconfigurationController` provides token-checked publication and bounded rollback history.

The canonical [architecture contract](https://github.com/janagor/netplane/blob/main/docs/architecture.md), [execution semantics](execution-semantics.md), and decisions are maintained in the repository:

- [ADR 0001: Tower service model](https://github.com/janagor/netplane/blob/main/docs/adr/0001-tower-service-model.md)
- [ADR 0002: Runtime reconfiguration](https://github.com/janagor/netplane/blob/main/docs/adr/0002-runtime-reconfiguration.md)
- [ADR 0003: Crate boundaries](https://github.com/janagor/netplane/blob/main/docs/adr/0003-crate-boundaries.md)
- [ADR 0004: Graph composition](https://github.com/janagor/netplane/blob/main/docs/adr/0004-graph-composition.md)
- [ADR 0005: Reconfiguration control](https://github.com/janagor/netplane/blob/main/docs/adr/0005-reconfiguration-control.md)

The workspace has `crates/netplane` for the transport-independent runtime, `crates/netplane-http` for the optional HTTP adapter, and `crates/xtask` for development automation.
