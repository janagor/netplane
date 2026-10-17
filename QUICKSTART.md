# Quick start

HTTP example: `cargo run -p netplane-http --example reconfigure`. Start `curl http://127.0.0.1:3000/a` in another terminal; it waits on v1. Press Enter in the example terminal to publish v2. A second `curl http://127.0.0.1:3000/a` returns v2 while the first still waits. Press Enter again to release the v1 request, then Ctrl-C to stop the listener and drain Netplane.

Netplane is a Rust library workspace. Its library crate provides bounded Tower service endpoints, atomic publication of immutable execution versions, and bounded graceful draining.

## Prerequisites

- Rust via [rustup](https://rustup.rs/); `rust-toolchain.toml` selects 1.88 with rustfmt and Clippy.
- Git for cloning the repository.
- Optional: `cargo-nextest` and the tools used by the full quality gate script.

## Build and verify

```bash
git clone https://github.com/janagor/netplane.git
cd netplane
cargo metadata --no-deps
cargo fmt --all -- --check
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

`cargo test --workspace` includes documentation tests. If `cargo-nextest` is installed, use `cargo nextest run --workspace` for the test suite. Run `./scripts/quality-gates.sh` for the repository's broader local checks; it may require additional tools reported by the script.

Nix users can enter `nix develop`, then run the same Cargo commands. Toolchain consistency between the Nix shell and `rust-toolchain.toml` is part of the separate environment and CI work.

## Where to work

- `crates/netplane`: public library code with bounded execution, version publication, and shutdown admission.
- `crates/xtask`: repository automation, not a runtime dependency.
- `docs/architecture.md` and `docs/adr/`: design contract and decisions.
- `AGENTS.md`: coding and review instructions.

Read [the architecture](docs/architecture.md) before changing graph types or adding transport adapters. Add a dependency only with its first consumer. Keep the core independent of sockets, Hyper, Kubernetes, and specific deployment environments.

`GraphDraft::validate()` checks capacity settings and prepares an endpoint with a Tower buffer worker, so call it inside a Tokio runtime. `Netplane::publish()` swaps the active version atomically while running; requests already selected keep their previous version. `Netplane::shutdown(grace_period)` closes admission when called and returns a future that waits for accepted request futures to finish or be dropped. Successful draining releases the active graph, including when handles remain. A timeout leaves admission closed and can be retried; publication remains closed during and after shutdown.

Run the in-memory transition example with `cargo run -p netplane --example live_reconfiguration`. It changes a direct Backend A service into a router with a simulated authorization filter for Backends A and B while an old request is in progress, then drains accepted work during shutdown. See [execution semantics](docs/architecture/execution-semantics.md) for the guarantees and limits.
