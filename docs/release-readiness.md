# Pre-release verification status

This is a source-tree audit for a possible future `0.1.0` decision, not release authorization. The workspace and both library crates remain at `0.0.0` with `publish = false`; no date, tag, or publication is planned here.

## Scope and decisions

| Item | Status | Evidence or next decision |
| --- | --- | --- |
| Core and HTTP boundary | Complete | `Cargo.toml` lists `netplane`, `netplane-http`, and `xtask`; the adapter depends on the core, not the reverse. |
| Runtime contract and API review | Complete for current source | [Execution semantics](architecture/execution-semantics.md) and [API review](api-readiness.md) map admission, publication, rollback, shutdown, and HTTP behavior to tests. A frozen `0.1.0` API review remains incomplete. |
| Performance evidence | Complete for current source | [The baseline](performance-baseline.md) preserves exploratory numbers and records the full longer Criterion run, two task-mode latency runs, and two paced and burst lifetime runs. Further runs would be needed for a regression claim. |
| Pre-1.0 compatibility policy | Proposed, awaiting decision | [Migration guide](../MIGRATION.md) proposes documenting breaking changes for each minor version and compatible fixes for patch versions. It is not yet a release commitment. |
| HTTP publication | Open decision | Choose whether `netplane-http` publishes alongside `netplane` or remains private; if published, define its version relationship and verify its package independently. |
| External consumer | Complete for current core package; release candidate incomplete | Workspace external-consumer tests cover the current APIs. A fresh `/tmp` binary depending on `target/package/netplane-0.0.0` passed `cargo check --offline` and imported public `Capacity` and `RuntimeLimits`. Repeat a full API consumer build against the frozen release candidate and check the HTTP package if publication is approved. |
| Changelog and migration notes | Complete for unreleased source | [Changelog](../CHANGELOG.md) and [migration guide](../MIGRATION.md) describe current source behavior. Release-specific notes remain for the release decision. |

## Verification record

Run on 2026-10-26 from source commit `e44258e6ff5b610db5cb1a8129f457284aeff4fd` plus documentation-only working-tree changes, with rustc 1.88.0 on NixOS Linux x86_64. A passing local check verifies this source tree and local dependency resolution, not a future release candidate. `Cargo.lock` is ignored in this library workspace.

| Check | Command | Result |
| --- | --- | --- |
| Format | `cargo fmt --all -- --check` | Passed. |
| Clippy | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed. |
| Build and tests | `cargo build --workspace`; `cargo test --workspace`; `cargo test --workspace --doc` | Passed. The initial sandboxed workspace test run could not bind local HTTP sockets; the identical command passed with local socket access. |
| Bench compilation and smoke test | `cargo bench -p netplane --no-run`; `cargo test -p netplane --benches` | Passed, including all four benchmark targets. |
| Rustdoc | `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` | Passed. |
| mdBook | `mdbook build docs` | Passed with mdBook 0.5.4. |
| Package contents and verification | `cargo package -p netplane --list`; `cargo package -p netplane --allow-dirty --offline` | Passed. The file list includes README, license, source, tests, benches, and examples; Cargo packaged 47 files and verified a build. Packaging does not publish. |
| Standalone consumer | `cargo check --offline` in a fresh binary with a path dependency on `target/package/netplane-0.0.0` | Passed; the consumer imported and used public capacity and runtime-limit types. This is a smoke build, not full API compatibility certification. |
| Agent context | `cargo run -p xtask -- agents validate`; `cargo run -p xtask -- agents check-context` | Passed; generated `llms.txt` and `llms-full.txt` match their sources. |

Before a release decision, repeat this matrix on the chosen commit, review the packaged file list and generated rustdoc, resolve the two open publication and compatibility decisions, and leave version changes for a separate PR.
