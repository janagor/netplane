# ADR 0005: Reconfiguration control

**Status:** Accepted

**Date:** 2026-10-17

## Context

Applications need concurrent administrative callers to replace and inspect a running graph, and sometimes restore a prior validated executable graph. A graph object is not a publication generation. Tower services cannot generally be reconstructed or cloned.

## Decision

`ReconfigurationController` exclusively owns one `Netplane` publisher. A short-lived mutex serializes control operations; execution handles continue to select versions through the existing atomic active store. Callers capture a snapshot and its runtime-scoped `VersionToken`, validate candidate graphs outside the lock, and submit both for publication. Foreign and stale tokens fail without a swap or automatic retry. The controller uses the same publication primitive as direct `Netplane::publish()`.

Published wrappers share an `Arc<GraphVersion>`. A rollback takes the executable graph from retained history and publishes a new wrapper with a fresh generation. It therefore preserves Tower service state, readiness ownership, and request lifetimes. Controller history retains at most the configured number of displaced publications in publication order. A zero limit disables rollback. In-flight requests may keep evicted versions alive.

Shutdown closes admission while holding the control mutex, clears history, and returns the runtime's drain future. A timeout remains retryable, but rollback history is never restored. Inspection reads active ID, graph metadata, and the matching token from the same loaded publication. Controller publication and shutdown events are emitted after releasing the control mutex so subscriber callbacks may inspect the controller.

## Consequences

Control callers must inspect after conflicts and decide whether their candidate still applies. A historical publication may be evicted even while request futures still use it. `version.released` describes publication wrapper ownership, not necessarily final executable graph reclamation. Persistent history, health decisions, transport administration, and graph diffing remain outside this API.
