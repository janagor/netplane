# Initial release readiness

Netplane remains at `0.0.0` with `publish = false`. A separate release decision must choose versioning and publication policy.

- [x] Core and optional HTTP adapter are workspace members; HTTP remains outside the core dependency graph.
- [x] Core benchmark targets compile on the configured Rust 1.88 toolchain.
- [x] Core benchmarks smoke-test via `cargo test -p netplane --benches` on Rust 1.88.
- [x] Execution contract documents synchronous version selection, admission ownership, timeouts, rollback and shutdown.
- [x] Benchmark modes cover batched and multi-task callers, Tower double-buffered routing baselines, paced and burst resource probes, and yield-once execution.
- [ ] Collect refreshed tail latency and resource-retention results on a controlled host, including multi-task and burst modes, with longer Criterion sampling than the exploratory baseline.
- [ ] Review public API docs and external-consumer examples with the intended `0.1.0` surface frozen.
- [ ] Run format, Clippy, workspace build and tests, documentation tests, package verification and documentation build for the release candidate. The Cargo checks and warning-free rustdoc passed on this working tree; `mdbook` is not installed locally.
- [x] Inspect `cargo package -p netplane --list` for README, license and manifest metadata. Packaging and verification pass on the working tree with `--allow-dirty --offline`.
- [ ] Decide whether `netplane-http` is published with the core crate or stays private, and document its version relationship.
- [x] Replace template-era migration and changelog text with Netplane pre-release notes.
- [ ] Decide and document the compatibility policy for pre-1.0 versions, then authorize a version change explicitly.
