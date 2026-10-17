# Netplane

The optional `netplane-http` workspace crate serves buffered HTTP requests through a Netplane handle. It bounds body size, body read concurrency, and read time, then rejects oversized encoded responses. See `crates/netplane-http/examples/reconfigure.rs` for a persistent listener with process-local graph publication.

Netplane is an early stage Rust library for constructing and managing dynamically reconfigurable Tower service graphs. It aims to make graph publication, request lifetime, readiness, backpressure, and cancellation behavior explicit.

The repository provides bounded Tower service endpoints, atomic publication of validated execution versions, graceful shutdown, and development tooling. Accepted requests retain the version selected at invocation. Successful shutdown releases the active graph, even when handles remain. Version `0.0.0` marks this pre-release stage; the crate is not published.

## Scope

Netplane builds on `tower::Service`. The core is transport independent and testable with in-memory services. The optional HTTP adapter has its own crate; deployment integrations can be added behind real dependency boundaries. Netplane is not a standalone HTTP proxy or a replacement async runtime.

The design contract is in [docs/architecture.md](docs/architecture.md), with decisions recorded in [docs/adr](docs/adr). They distinguish current behavior from future requirements.

## Get started

The repository pins Rust 1.88 in `rust-toolchain.toml`. See [QUICKSTART.md](QUICKSTART.md) for setup and verification commands.

```bash
cargo build --workspace
cargo test --workspace
cargo run -p netplane --example live_reconfiguration
```

The workspace contains `crates/netplane` (the public library), `crates/netplane-http` (the optional HTTP adapter), and `crates/xtask` (development tooling). Dependencies are added only when used. `Cargo.lock` is ignored for this library workspace.

## Contribute

Read [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) before changing the architecture or public API. Report vulnerabilities through [the security policy](SECURITY.md). Netplane is licensed under [MIT](LICENSE).
