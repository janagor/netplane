# ADR 0006: Backend health outside immutable graph publication

**Status:** Accepted

## Context

Discovery membership does not establish operational availability. Rebuilding a
graph for each failed request would entangle live observations with desired-state
reconciliation and reset service ownership unnecessarily.

## Decision

Compose transport-independent health handles with existing routing policies and
ordinary Tower layers. Registries retain weak records keyed by endpoint incarnation;
graphs, observations and probe registrations own strong handles. Backend-local state
transitions invalidate observation generations. Passive recovery admits traffic
after cooldown; active recovery requires successful active probes before eligibility.
Observation tokens distinguish passive calls from probes. Duplicate probe registrations
share ownership only with a matching configuration identity; changes are rejected
while an old registration is retained. Registration capacity covers the union of
active, historical, in-flight, and candidate backend identities.
A shared caller-owned scheduler bounds registrations and simultaneous checks.
Fallible graph selectors return a typed error when no positive eligible weight
remains. Health never changes immutable graph membership or an issued destination.

## Consequences

Health survives compatible publication and rollback through handle ownership.
Known replacement identities start fresh. Transport and discovery dependencies stay
outside core. Health decisions sample backend-local eligibility; they are not a
global transaction. Passive recovery allows ordinary configured traffic and is not
a full circuit breaker. Eligibility can redistribute all traffic to a remaining
canary, so future rollout control must account for effective traffic weights.

The contract and lifecycle are described in [backend health](../architecture/health.md).
