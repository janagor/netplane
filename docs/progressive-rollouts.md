# Guarded progressive rollouts

`netplane_control::rollout` changes desired traffic policy through the existing
reconciliation worker. It does not execute requests, manage deployments, promote
instances, or automatically restore earlier traffic. The application supplies
`TrafficConfiguration` for its typed desired state and an `ObservationProvider`
for publication-attributed, backend-group-specific evidence. Core runtime status
and N10 health counts alone cannot establish canary success.

## Starting and owning a worker

First submit the baseline desired configuration through `UpdateSender` and await
convergence. Call `RolloutDriver::start(updates, provider, spec, poll_interval)` to
receive `(driver, handle)`, then poll `driver.run()` alongside the existing
`ReconciliationDriver::run()`. Construction spawns nothing. The application owns,
cancels and awaits both workers alongside runtime shutdown. Reconciliation worker
shutdown also terminates a paused or blocked rollout. Dropping the rollout future
cancels its observation and withdraws any owned uncommitted intent. Independently
spawned work inside application providers or preparers remains their responsibility.

One permit, shared by all producers of a reconciler, permits at most one pending,
active, paused or blocked rollout. Terminal completion or worker drop releases it.
Status is a bounded watch snapshot; intermediate transitions may coalesce.

`TrafficConfiguration::group_policy` resolves logical stable and canary groups,
which may contain many discovered endpoints. The two group weights must sum to
100. `set_weights` changes only those groups and must preserve unrelated desired
state. The preparer remains responsible for compiling those weights into existing
`WeightedRouting` or `HealthAwareRouting` and validating application policy.
`membership_revision` identifies material membership changes, including replacement
of an endpoint at a reused address. This identity is supplied by the integration.

## Specification and evaluation

`RolloutSpec` owns distinct group names and a nonempty stage sequence ending at
100% canary. Integer percentages are checked in 0..=100 and cannot decrease.
The first stage must not be below the currently applied canary allocation.
Every stage has explicit conditions and a finite positive observation deadline
strictly longer than its minimum observation duration. A 0% preflight stage may
check eligibility but cannot require canary traffic. Every positive canary stage
must require positive canary samples. Stable traffic conditions are invalid at
100% canary. No timing, error-rate, latency or sample thresholds are library defaults.

Conditions use AND semantics. Error-rate thresholds are exact integer basis
points in 0..=10,000; comparison uses widened integer products, without rounding
or floating point. Statistical conditions carry their own positive minimum sample
count. A definite violation takes precedence over another condition's missing
evidence. Eligibility is an explicit nonstatistical condition and may fail
immediately. The minimum duration gates success; elapsed time never replaces
samples. The final 100% stage must independently pass before completion.

The evaluator rejects incomplete windows, duplicate or missing groups, failure
counts above completed counts, mismatched publications, and mismatched epochs.
Missing latency, zero actual canary requests and source unavailability cannot pass.
A provider operation is bounded by both the polling interval and stage deadline.
Provider calls remain at least one polling interval apart across eligibility and
configuration restarts. Lifecycle and reconciliation changes interrupt waits to
revalidate or cancel work, but do not move the next provider-call deadline earlier.
Timeout and lost ownership remain halted until explicit resume, even if unrelated
configuration or membership updates subsequently converge. Pending compatible
configuration and insufficient samples, duration, latency or source availability
may recover automatically within the original stage deadline. The stage deadline
is exclusive: an evaluation at or after it cannot pass. The driver does not retry
reconciliation independently of the reconciler's existing retry policy.

## Observation contract

`ObservationRequest` specifies an exact publication, increasing epoch and cumulative
`since..until` window. The beginning is recorded only after matching publication
confirmation, including deduplicated unchanged state. Counts cover the whole
window with no losses; `complete = false` explicitly prevents evaluation.

Requests belong to the execution publication selected at admission and the logical
group selected inside that pinned version, including requests finishing after
publication changes. Completion-time active publication must never be used as
attribution. Count completed responses and service errors. Exclude admission
rejection and cancellation from both the numerator and denominator. The application
documents which unsuccessful responses count as failures. A reported p95 covers
accepted-to-terminal latency, including queues, over every counted completion;
report `None` when the latency population is incomplete.

Read N10 eligibility instead of recreating health tracking. Supply a semantic
`eligibility_revision` and set `eligibility_changed` if membership or eligibility
changed anywhere during the requested window, even before the first provider call.
A material change restarts the observation epoch and minimum duration without
extending the overall stage deadline. This restart requires no unchanged-weight
reconciliation. Explicit administrative resume resets both the evidence window
and overall stage deadline; pausing alone does not reset either. Stable
metrics cannot substitute for canary metrics. Configured weights allocate sticky
key buckets and do not guarantee measured exposure or effective weights after
health filtering. The integration must bound its telemetry storage; the driver
retains only the current decision and at most two group eligibility identities.

## Ownership, conflicts and lifecycle

Stage submission consumes one validated configuration snapshot: desired revision,
publication, desired state and group policies. It never rereads newer values to
replace the authorization identities during submission. The mailbox revision
comparison and publication check execute under the existing control locks before
accepting the desired update; the same publication remains its preparation guard.
A stale snapshot invalidates the unsubmitted next-stage intent. Compatible updates
restart observation of the previously applied stage before any additional traffic
can be requested, including when membership changed without changing weights. The existing worker checks that publication guard through
the atomic swap, including every retry. A superseded candidate cannot overwrite
an administrator's newer desired state or external executable publication. The
rollout preserves unrelated updates by reading the latest converged configuration
and starting a fresh observation epoch. Changed targeted weights or unresolved
groups block with a configuration conflict. All producers should use the same
`UpdateSender`; competing external publishers are detected and block progress.

`pause()` prevents additional progression and interrupts observation; a previously
requested stage may still commit. `resume()` revalidates desired state and starts
a fresh epoch, including after an observation timeout. `cancel()` synchronously
withdraws only this rollout's uncommitted pending update. An update accepted from
another producer is never withdrawn. A publication already across its atomic swap
remains active. An owned intent carries a once-only publication acknowledgement,
written under the controller lock before the mailbox freshness guard is released.
Withdrawal checks this acknowledgement under the mailbox write lock: pending
intent can be withdrawn, committed desired state remains intact, and superseded
intent cannot affect its replacement. Status reports confirmed applied weight
separately from desired weight.

Cancellation, failure and pause never restore earlier weights. Once terminal,
lifecycle methods return `RolloutError::Terminal`. Permanent or exhausted preparation/publication failures and definite condition
violations enter `Failed`; publication conflicts block for explicit intervention.
The machine rejects stale and duplicate events and cannot reactivate terminal
states. Resume is an explicit request to revalidate, not permission to overwrite
conflicting administrator policy.

The baseline publication, baseline desired revision, failed stage, applied
publication, last successful stage, terminal state and condition violation remain
in `RolloutStatus` for a future rollback policy. They retain no executable graphs.
The controller's bounded history can evict the baseline; no recovery persistence
or distributed ownership guarantee is provided.

## Decision linearization

Evidence carries an `EvaluationIdentity` containing desired revision, publication,
epoch, control generation and stage deadline. Acceptance holds the lifecycle and
mailbox guards while the controller validates the publication and accepts the
bounded state transition under its existing publication mutex. The order is
control, mailbox, controller, then rollout status. All guards are released before
tracing or application-owned cleanup. No lock spans an await, and request execution
uses no additional lock. Success, failure and final-stage completion use the same
linearization point, so stale decisions cannot become terminal rollout events.
A later configuration change can supersede a valid decision; future rollback
policy must still guard its own action against the recorded revision/publication.

## Advanced synchronization APIs

`ReconfigurationController::with_current_publication` and
`publish_if_current_guarded_with` are advanced control-plane integration primitives,
not ordinary application callbacks. They execute under the controller mutex.
Calling even `inspect()` recursively can deadlock; blocking or fallible callbacks
are prohibited. A panic in a commit acknowledgement occurs after the swap and
cannot undo the committed graph. Ordinary callers should use `inspect()` with
conditional publication, or `publish_if_current_guarded` when holding an external
freshness guard. Reassess this unconstrained callback surface before the first
public release; a constrained acknowledgement or transaction API may replace it.

## Audit and example

Structured tracing uses target `netplane_control` and events `rollout.started`,
`rollout.stage.requested`, `rollout.stage.applied`, `rollout.stage.observing`,
`rollout.stage.blocked`, `rollout.stage.passed`, `rollout.stage.failed`,
`rollout.configuration_conflict`, `rollout.paused`, `rollout.resumed`,
`rollout.cancelled`, and `rollout.completed`. Each identifies the rollout, stage,
epoch, confirmed publication and the decision or blocked reason belonging to
that transition. Event payloads are captured before a later transition changes
current status; passed records name the evaluated stage, even when advancement
has already selected the next stage. Identical blocked decisions do
not repeatedly emit polling events. Rollout IDs increase independently of desired revisions and are scoped to their
reconciler; audit records also include runtime and reconciler identifiers.

The [progressive HTTP example](../crates/netplane-http/examples/progressive_rollout/README.md)
uses real HTTP upstreams and a bounded sample provider for successful progression,
canary errors and insufficient evidence. No mandatory HTTP, Kubernetes, metrics
exporter or request instrumentation dependency was added to the control library.
