# Backend health implementation

The approved design keeps health as shared runtime state, independent of graph
publication and discovery membership. Existing Tower execution owns dispatch.

1. Implement domain, generations, weak registry ownership and passive Tower layer.
2. Implement shared bounded probe scheduler with cancellation and timeouts.
3. Extend weighted and uniform selectors and both graph execution modes with typed selection failure.
4. Compose optional HTTP and Kubernetes health using stable incarnation identities.
5. Verify state, cancellation, publication, admission, scheduler and distribution behavior.
6. Add example, benchmarks and public contracts; run workspace quality gates.

Work is split by file ownership between core health, scheduler, adapters, and
routing/verification. No graph reconstruction or retries follow health changes.

## Verification

All phases completed. Full quality gates passed on Rust 1.88, including format,
strict workspace Clippy, all-target build, 463 tests (9 environment-dependent tests
skipped), doc tests, security and dependency checks, and repository scans.
Criterion measurements are recorded in `docs/health-performance.md`; isolated
and end-to-end execution runs do not establish the suggested five-percent throughput goal.
Allocation behavior is source-inspected rather than allocator-profiled.

## Review corrections

- Separate passive observations from active recovery evidence; preserve queued destinations.
- Reject incompatible probe configuration with an explicit compatibility identity.
- Sample observation time under the state mutex and remove registry entries by key.
- Expose scheduler capacity status and document retained-graph union/headroom requirements.
- Targeted regressions and end-to-end execution measurements completed; workspace quality gates repeated.

## Final concurrency review

Probe completion acceptance now updates health under the registration mutex,
with tracing and discarded-token destruction deferred until after unlock.
Deterministic tests cover acceptance-first, removal-first and reentrant record
cleanup. Active HTTP probes now default to requiring 2xx; passive defaults remain
unchanged. Longer isolated performance runs remain a future follow-up.
