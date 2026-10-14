# Contributing to Netplane

Netplane is an early stage Rust library. The public graph API is not yet established. Before proposing one, read [the architecture](docs/architecture.md) and [ADRs](docs/adr/), and explain how the change preserves Tower readiness, bounded resources, request lifetime, and transport independence.

## Set up

Follow [QUICKSTART.md](QUICKSTART.md) for the pinned toolchain and local commands. The workspace contains `crates/netplane` and development tooling in `crates/xtask`.

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace
cargo test --workspace
```

Run `./scripts/quality-gates.sh` before submitting when the required local tools are installed. The current library is a scaffold, so tests for future runtime behavior should be added alongside the implementation, not as claims of existing behavior.

## Changes and review

Open an [issue](https://github.com/janagor/netplane/issues) for a substantial API or architecture change, or submit a draft pull request with a small prototype. Keep changes focused and document ownership, cancellation, readiness, and error behavior for new public APIs. Add tests for graph version lifetime, concurrent publication, and resource bounds when those mechanisms are introduced. Keep transport adapters separate from the core.

Use lowercase [Conventional Commit](https://www.conventionalcommits.org/) subjects, such as `docs: establish netplane architecture`. Update documentation for changed behavior and record durable design decisions in `docs/adr/`. Pull requests should include the verification commands run and any relevant limitations.

Netplane remains at version `0.0.0` and `crates/netplane` has `publish = false`. Publishing and compatibility policy will be decided before the first usable release. Do not trigger a release as part of an ordinary contribution.

Report vulnerabilities privately through [the security policy](SECURITY.md), not a public issue.
