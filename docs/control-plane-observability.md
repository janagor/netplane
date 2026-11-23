# Reconciliation status and audit events

`netplane-control` exposes `Reconciler::status()` and `subscribe_status()`.
After moving a reconciler into a driver, use `UpdateSender::status()` and
`subscribe_status()` to observe the same state. The existing `driver.status()`
continues to expose typed worker outcomes and retry delays. Both channels finish
at `Stopped` after graceful completion, forced cancellation, or unused driver
drop. They are updated separately; readings taken during a transition need not
reflect the same instant. Await worker completion or drop before comparing their
terminal values. `Stopped` is terminal for a reconciler. Moving a stopped
reconciler into `ReconciliationDriver::new` creates a stopped driver with closed
update admission and both channels already reporting `Stopped`. Its `run` future
exits immediately. Submissions reject with `UpdateError::Stopped` without changing
the desired revision, failure, or status sequence; admission checks the terminal
observation inside the synchronized mailbox decision.

## Revisions and freshness

`Revision` reuses the driver's local `u64` update sequence. It identifies accepted
submissions, not an upstream resource version. Equal resubmissions receive new
revisions. Explicit `apply` assigns a revision when first polled after obtaining
the single-flight permit; a busy call does not replace the desired revision.
An unpolled call is not yet accepted. Revision exhaustion rejects new work.
Moving a previously used reconciler into a driver preserves revision numbering.

`desired_revision` identifies the latest accepted submission, even during
coalescing or another candidate's preparation. `applied_revision` identifies the
last successful publication or equivalent-state deduplication. Coalesced or
superseded updates do not acquire fabricated applied records.
`active_publication` uses the runtime's `VersionId`, independently of revisions.
It starts with the controller's active publication and records successful
reconciliation and observed stale-token conflicts.

These are reconciliation observations. External publication and runtime shutdown
are not continuously monitored. `is_converged()` assumes exclusive publication
through this reconciler; it does not guarantee backend health or application
correctness. Use controller inspection for a separate current runtime sample.
Runtime admission counts and lifecycle remain in `RuntimeStatus`.

## Transitions and failures

Accepted input enters `Pending`, clearing its previous failure and retry count.
Validation and construction enter `Preparing`. Publication or deduplication enters
`Idle`, updates the applied revision, and clears the failure. Deduplication keeps
the publication ID. A retryable failure enters `RetryScheduled`; `retry_attempt`
counts retries scheduled after the initial attempt and saturates at `u32::MAX`.
The failure remains visible during the next preparation. Permanent or exhausted
failures enter `Blocked` until resubmission, preserving the last applied revision
and executable graph. Failure categories contain no application error strings.
The existing `ApplyError` and `DriverStatus` retain typed errors for applications.

A new submission during preparation immediately becomes the desired revision.
The older completion cannot change that revision or make it idle. Publication can
race after the driver's existing final freshness check: a successful older commit
is recorded as applied while the newest desired revision remains pending.
Shutdown closes update admission immediately; `Stopped` means worker cleanup has
completed. Await the worker to rule out further publication, including a commit
that already passed its freshness check. Dropping an unused driver or aborting
and joining its run task also reports `Stopped`. Dropping an explicit application
future during preparation records `Blocked` with `Cancelled` and releases its
permit; subsequent explicit application can recover.

Each status value is committed atomically under a short observation lock. No lock
crosses an await. Tokio watch stores one latest value; unread notifications
coalesce and slow polling does not delay reconciliation or retain an event queue.
As with any Tokio watch API, clone the borrowed value and release the borrow
promptly; never hold a watch borrow across an await or a reconciliation call.

## Audit events

Tracing target `netplane_control` emits these typed transitions:

| Event | Level |
| --- | --- |
| `reconcile.desired_updated` | INFO |
| `reconcile.started` | DEBUG |
| `reconcile.applied` | INFO |
| `reconcile.unchanged` | DEBUG |
| `reconcile.superseded` | DEBUG |
| `reconcile.retry_scheduled` | WARN |
| `reconcile.failed` | WARN |
| `reconcile.stopped` | INFO |

Fields include `runtime_id` (shared with core tracing), process-local
`reconciler_id`, `event_sequence`, candidate `revision`, `desired_revision`,
`applied_revision`, `publication_id`, `retry_attempt`, and bounded `error_kind`.
`error_kind` describes the attempt identified by `revision`, independently of
`last_failure` in the status snapshot. Obsolete or post-stop failures retain their
own event category without overwriting the latest desired revision's failure.
Non-failure events carry no failure category.
Optional fields are absent when unknown. Logical event sequences increase per
reconciler, saturating at `u64::MAX`, and are also exposed in status snapshots.
Multiple reconcilers can share a runtime, so correlate sequence numbers using
`reconciler_id` rather than runtime ID alone.

Events capture the transition snapshot and emit after releasing observation,
mailbox, and controller locks. Subscriber callbacks can inspect status. Concurrent
or reentrant callbacks may deliver events out of logical order; use the sequence
rather than arrival order. A status read from a callback can already be newer
than the event snapshot. Subscriber execution is synchronous, so expensive
subscribers should offload their own work.

Events contain no configuration values, credentials, request contents, or arbitrary
upstream error text. Tracing is best-effort observability, not a guaranteed
persistent audit log. Filtering or subscriber failure can lose events. There is
no durable audit store, unbounded event queue, exporter, or administrative endpoint.

## Constructor compatibility

`Reconciler::new()` is now an ordinary function rather than a `const fn`: it
inspects the controller to initialize publication status and allocates the watch
channel. Consumers using constant initialization must construct the reconciler at
runtime. This is an intentional API change while the crate remains experimental
and unpublished.
