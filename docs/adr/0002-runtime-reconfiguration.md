# ADR 0002: Runtime reconfiguration

**Status:** Accepted

**Date:** 2026-10-14

## Context

Changing a running service composition must not make in-flight requests observe a partially updated graph or lose resources they still need.

## Decision

Build and validate a complete replacement graph before publication. Publish graph versions immutably. A request retains the version and resources it needs until completion or cancellation; new requests may use a newer version after publication. Define shutdown behavior alongside the first implementation. Readiness and `call()` for a request must use the same service instance and graph version, even when publication occurs between those operations. Publication must preserve resources needed by an outstanding readiness decision.

## Consequences

Tests must exercise publication during active requests and between readiness and dispatch, version isolation, and cleanup after cancellation. The publication mechanism, atomicity details, reclamation strategy, and version identifier type remain open until an in-memory prototype establishes their requirements.

The first prototype uses `ArcSwapOption` for publication. Shutdown closes admission before draining accepted futures, rejects later publication, and clears the active snapshot after draining succeeds. A timed-out drain remains retryable.

Each endpoint retains the handle for its Tower buffer worker. The worker is aborted after its last endpoint and request future owner drops, so pending readiness cannot keep an obsolete version's service alive indefinitely.
