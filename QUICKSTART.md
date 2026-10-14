# Quick start

Netplane is already initialized as a Rust library workspace. Its current library crate is a documented scaffold without a graph runtime or public API.

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

- `crates/netplane`: public library code, currently a scaffold.
- `crates/xtask`: repository automation, not a runtime dependency.
- `docs/architecture.md` and `docs/adr/`: design contract and decisions.
- `AGENTS.md`: coding and review instructions.

Read [the architecture](docs/architecture.md) before introducing graph types or transport adapters. Add a dependency only with its first consumer. Keep the core independent of sockets, Hyper, Kubernetes, and specific deployment environments.

The first planned technical milestone is an in-memory graph of Tower services. Its tests should cover publishing a new version while requests use the old one, readiness, bounded concurrency, cancellation, and resource lifetime.
