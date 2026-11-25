# ADR 0007: Guarded progressive traffic rollouts

**Status:** Accepted

## Context

Gradual canary traffic needs explicit evidence attributed to a published version
and logical backend group. Runtime-wide counters cannot establish that exposure
occurred. Configuration reconciliation already owns bounded construction, desired
revision invalidation and optimistic publication.

## Decision

Keep rollout policy inside optional `netplane-control`. An immutable specification,
pure condition evaluator and deterministic machine authorize desired weight
changes. A caller-owned driver submits them to the existing reconciler using a
mailbox revision comparison and a publication guard retained through every retry
and the atomic swap. A single validated snapshot binds next-stage submission to
its observed revision and publication; rejection discards that authorization and
reobserves the previous stage. Eligibility-triggered resets preserve the polling
cadence, including when reconciliation interrupts a wait. No new executable
publisher or request-path mechanism exists.
At most one rollout owns a reconciler. Observation and control use bounded latest
snapshots, one outstanding provider future and finite deadlines. Evidence resets
preserve the original stage deadline; only explicit resume may reset it. The first
stage cannot reduce exposure below the applied baseline.

Applications resolve groups, preserve unrelated configuration and provide complete
publication-attributed evidence with authoritative N10 eligibility identities.
Missing evidence blocks; the final stage requires successful evaluation. Material
membership or eligibility changes invalidate the window. Exact integer basis
points avoid floating point error-rate decisions.

## Consequences

Commit acknowledgement is written under the controller lock while the mailbox
freshness guard still excludes withdrawal. Cancellation withdraws only owned
pending intent; it preserves committed desired state and cannot undo traffic.
Evaluation acceptance holds lifecycle and mailbox guards and validates publication
under that same controller mutex. Thus final success and definite failure have an
explicit linearization point, independent of event delivery. Timeout and lost
ownership remain halted until explicit resume, regardless of subsequent updates.
Pause may allow an already requested publication to finish. Failure does not roll
back. Status retains identifiers and decisions for N12 without retaining executable
graphs or promising history retention. Integrations own telemetry, source freshness,
response classification, shutdown coordination and endpoint membership identities.
See the [rollout contract](../progressive-rollouts.md).
