# Changelog

Netplane has not been released. Version and workspace manifests remain at `0.0.0`, and the crates are private to this repository until an explicit release decision.

## Unreleased

### Added

- Criterion benchmarks for direct and bounded Tower execution, Netplane execution, concurrency, routing, admission, publication and selected lifecycle operations.
- Concurrent caller benchmarks for both single-task batched in-flight work and independently scheduled multi-task callers.
- Tower double-buffered router baseline, route-label lookup isolation and a yield-once serial execution path.
- Latency and resource probes with explicit `batched`/`tasks` and `paced`/`burst` modes.
- Performance methodology and a first local execution baseline.
- External-consumer API tests for the core crate and optional HTTP adapter, plus benchmark compilation in CI on Rust 1.88.

### Documentation

- Release-readiness checklist and updated core crate guidance for routing, controller publication, deadlines and rollback.
- Reconciled contributor and agent documentation with the implemented HTTP adapter, single-router builder, and reconfiguration controller.
- Added a follow-up performance run and evidence-backed pre-release audit without changing runtime behavior or publication settings.
