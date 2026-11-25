# Changelog

Netplane has not been released. Version and workspace manifests remain at `0.0.0`, and the crates are private to this repository until an explicit release decision.

## Unreleased

### Added

- Guarded progressive traffic rollouts in `netplane-control`, with validated stages, attributed observations, lifecycle control, bounded execution and real HTTP scenarios.

- Optional per-instance health registry, generation-safe observations, passive Tower middleware, and bounded active probes.
- Health-aware weighted and discovered-endpoint routing with typed unavailability, HTTP classification, a gateway example, and health benchmarks.

- Criterion benchmarks for direct and bounded Tower execution, Netplane execution, concurrency, routing, admission, publication and selected lifecycle operations.
- Concurrent caller benchmarks for both single-task batched in-flight work and independently scheduled multi-task callers.
- Tower double-buffered router baseline, route-label lookup isolation and a yield-once serial execution path.
- Latency and resource probes with explicit `batched`/`tasks` and `paced`/`burst` modes.
- Performance methodology and a first local execution baseline.
- External-consumer API tests for the core crate and optional HTTP adapter, plus benchmark compilation in CI on Rust 1.88.

### Changed

- Stage submission retains one validated revision/publication snapshot and reobserves the previous stage after stale authorization. Eligibility and configuration restarts respect the rollout polling interval.

- Rollout cancellation preserves committed desired state; terminal decisions validate revision and publication atomically. Administrative halts persist across updates, eligibility resets retain the stage deadline, and first-stage exposure cannot decrease. HTTP rollout observations now exercise latency conditions.

- `netplane-control` reconciliation status initialization makes `Reconciler::new()`
  an ordinary function instead of a `const fn`; construct reconcilers at runtime.
- Both reconciliation status channels now report `Stopped` on driver drop or
  forced cancellation. Failure audit events carry the failed attempt's category
  independently of the current status snapshot.

### Documentation

- Release-readiness checklist and updated core crate guidance for routing, controller publication, deadlines and rollback.
- Reconciled contributor and agent documentation with the implemented HTTP adapter, single-router builder, and reconfiguration controller.
- Added a follow-up performance run and evidence-backed pre-release audit without changing runtime behavior or publication settings.
