# Quick start

Netplane is a Rust library workspace. Its library crate provides bounded Tower service endpoints and atomic publication of immutable execution versions.

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

- `crates/netplane`: public library code with bounded execution and version publication.
- `crates/xtask`: repository automation, not a runtime dependency.
- `docs/architecture.md` and `docs/adr/`: design contract and decisions.
- `AGENTS.md`: coding and review instructions.

Read [the architecture](docs/architecture.md) before changing graph types or adding transport adapters. Add a dependency only with its first consumer. Keep the core independent of sockets, Hyper, Kubernetes, and specific deployment environments.

`GraphDraft::validate()` checks capacity settings and prepares an endpoint with a Tower buffer worker, so call it inside a Tokio runtime. `Netplane::publish()` swaps the active version atomically; requests already selected keep their previous version. The next milestone is shutdown admission control and bounded graceful draining.
