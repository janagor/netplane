# Architecture

Netplane is an early stage library for dynamically reconfigurable Tower service graphs. The runtime and public API have not yet been implemented.

The core is intended to interoperate with `tower::Service`, preserve readiness and request lifetime across graph publication, bound resources, and stay independent of concrete transports. The first prototype may use a service composition or routing tree; it does not need a general directed graph.

The canonical [architecture contract](https://github.com/janagor/netplane/blob/main/docs/architecture.md) and initial decisions are maintained in the repository:

- [ADR 0001: Tower service model](https://github.com/janagor/netplane/blob/main/docs/adr/0001-tower-service-model.md)
- [ADR 0002: Runtime reconfiguration](https://github.com/janagor/netplane/blob/main/docs/adr/0002-runtime-reconfiguration.md)
- [ADR 0003: Crate boundaries](https://github.com/janagor/netplane/blob/main/docs/adr/0003-crate-boundaries.md)

The workspace currently has `crates/netplane` for the library and `crates/xtask` for development automation.
