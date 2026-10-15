# Contributing to Netplane

Netplane is an early stage Rust library with bounded Tower execution, atomic version publication, cancellation, and graceful shutdown. A general graph construction API is not established. Before proposing one, read [the architecture](docs/architecture.md), [execution semantics](docs/architecture/execution-semantics.md), and [ADRs](docs/adr/), and explain how the change preserves Tower readiness, bounded resources, request lifetime, and transport independence.

## Set up

Follow [QUICKSTART.md](QUICKSTART.md) for the pinned toolchain and local commands. The workspace contains `crates/netplane` and development tooling in `crates/xtask`.

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace
cargo test --workspace
```

Run `./scripts/quality-gates.sh` before submitting when the required local tools are installed. Runtime changes need focused tests with in-memory Tower services.

## Changes and review

Open an [issue](https://github.com/janagor/netplane/issues) for a substantial API or architecture change, or submit a draft pull request with a small prototype. Keep changes focused and document ownership, cancellation, readiness, and error behavior for new public APIs. Changes to execution, lifecycle, or publication need appropriate tests for version selection and lifetime, concurrent publication, cancellation and resource cleanup, shutdown and worker teardown, Tower readiness, and bounded execution. Update the canonical [execution semantics](docs/architecture/execution-semantics.md) whenever runtime behavior changes. Keep transport adapters separate from the core.

Use lowercase [Conventional Commit](https://www.conventionalcommits.org/) subjects, such as `docs: establish netplane architecture`. Update documentation for changed behavior and record durable design decisions in `docs/adr/`. Pull requests should include the verification commands run and any relevant limitations.

Netplane remains at version `0.0.0` and `crates/netplane` has `publish = false`. Publishing and compatibility policy will be decided before the first usable release. Do not trigger a release as part of an ordinary contribution.

Report vulnerabilities privately through [the security policy](SECURITY.md), not a public issue.
