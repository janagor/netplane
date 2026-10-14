# ADR 0003: Crate boundaries

**Status:** Accepted

**Date:** 2026-10-14

## Context

Transport dependencies can constrain reuse and make graph behavior harder to test. The workspace currently contains one library crate and `xtask`.

## Decision

Keep graph composition and execution in `crates/netplane`, independent of concrete transports and deployment systems. `xtask` remains development-only. Add another crate only when a real dependency or public API boundary is demonstrated. A future transport adapter may depend on the core; the core must not depend on the adapter.

## Consequences

Core behavior should be testable with mock Tower services and no sockets. Hyper, Kubernetes, and deployment integrations are deferred. The decision does not prohibit optional features or later crates when a prototype justifies them.
