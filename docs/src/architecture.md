# Architecture

Netplane is an early stage library for dynamically reconfigurable Tower service compositions. Bounded execution, immutable version publication, and graceful shutdown are implemented.

The core interoperates with `tower::Service`, preserves readiness and request lifetime across publication, bounds per-version execution and global accepted requests, and stays independent of concrete transports. General graph construction is not implemented.

The canonical [architecture contract](https://github.com/janagor/netplane/blob/main/docs/architecture.md), [execution semantics](execution-semantics.md), and decisions are maintained in the repository:

- [ADR 0001: Tower service model](https://github.com/janagor/netplane/blob/main/docs/adr/0001-tower-service-model.md)
- [ADR 0002: Runtime reconfiguration](https://github.com/janagor/netplane/blob/main/docs/adr/0002-runtime-reconfiguration.md)
- [ADR 0003: Crate boundaries](https://github.com/janagor/netplane/blob/main/docs/adr/0003-crate-boundaries.md)

The workspace currently has `crates/netplane` for the library and `crates/xtask` for development automation.
