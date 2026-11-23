# Typed desired-state reconciliation

The optional `netplane-control` crate depends on `netplane`; the runtime does not
depend on it. It translates application configuration into conditional graph
publications through explicit `Reconciler::apply` calls or an optional caller-owned
`ReconciliationDriver`. It includes no file reader or discovery integration.

## Application contract

Implement `GraphPreparer<D, Request, Response>` and supply a shared
`Arc<ReconfigurationController<Request, Response>>` to `Reconciler::new`.
Desired state implements `Clone + Eq + Send + Sync`; the preparer is `Sync`.
Normalize configurations before submission so equality means equivalent policy.
The control crate imposes no universal configuration schema and does not compare
Tower service objects.

`validate(&desired)` checks application policy synchronously, before deduplication
or service construction. `prepare(desired)` asynchronously constructs services and
returns a validated `GraphVersion` using `GraphDraft` or `GraphBuilder`. Preparation
runs on the caller's executor outside the controller lock. Bound preparer resource
use and offload blocking work explicitly. Runtime graph validation still checks
capacities and supported topology; application policy belongs to the preparer.

## Publication and concurrency

Each reconciler admits one polled application at a time using a preparation permit.
A concurrent call returns `ApplyError::Busy`, without validation or preparation.
After validation, the reconciler inspects the active publication and its token.
If the last successful desired state compares equal and its publication is still
active, the result is `ApplyOutcome::Unchanged` with no construction or generation
change. This is a successful no-op at the observed publication generation, not a
guarantee of current convergence. Another publisher can replace that generation
before the caller receives the result.

Otherwise, the reconciler prepares one candidate and calls `publish_if_current`
with the original token. Success returns `ApplyOutcome::Applied` and records
`LastApplied { desired, published }` before any further await. `last_applied()`
returns a shared historical record; it does not claim that publication remains
active. Publication generations are distinct from application configuration revisions.
An external replacement or rollback invalidates deduplication even when the desired
configuration is equal, because rollback creates a fresh publication generation.

Validation and preparation errors preserve both the active graph and the last
successful record. Publication errors expose the existing `ControllerError`,
including `Stale` and `ShuttingDown`. Rejected candidates are dropped. Conflicts
never retry automatically: the caller decides whether explicit resubmission is
appropriate. Separate reconcilers have separate permits and can conflict through
the shared controller. Their last-applied records remain independent.

For integrations, prefer one authoritative reconciler per managed runtime, with
multiple configuration producers feeding it. Coordinate discovery, file updates,
and administrative changes through that owner to avoid competing sources
continually replacing each other. This is an integration recommendation, not an
API restriction; PR4 permits multiple reconcilers sharing a controller.

Dropping an application future during preparation drops its candidate resources
and releases its permit. Resources or tasks independently retained by an application
preparer remain its responsibility. There is no await between successful publication
and updating the local record. Accepted runtime requests keep their selected graph
through publication and can finish afterward. Shutdown visible at inspection rejects
before preparation; shutdown racing with preparation rejects at conditional publication.

## Example and verification

Run `cargo run -p netplane-control --example typed_reconciliation`. The example
applies A, accepts a request on A, publishes B, skips duplicate B, rejects invalid C,
and verifies that A completes while B remains active. Deterministic in-memory tests
also cover dispatched request lifetime, stale tokens, external replacements,
preparation failure, single-flight admission, candidate cleanup, and shutdown races.

## Bounded update driver

`ReconciliationDriver::new(reconciler, policy)` returns a worker and cloneable
`UpdateSender`. Construction spawns nothing; run `driver.run(is_transient)` on a
caller-owned executor, retaining its join handle. Producers call `submit(desired)`
to overwrite one latest-value slot. Each submission receives a checked, locally
increasing sequence independent of application revisions. Input retained during
startup, preparation, and backoff has constant slot count. Configuration sizes
and resources retained independently by applications remain application limits.

Only one preparation runs. A newer input does not cancel an active preparation;
the worker awaits completion and discards an obsolete candidate before conditional
publication. The last valid runtime graph remains available on failure. Submission
and publication are separate operations: an update racing after the final freshness
check can arrive as the previous candidate publishes, then gets reconciled next.
Driver shutdown can race in that same interval: it closes update admission but
cannot prevent publication that has already passed the freshness check. Awaiting
worker completion guarantees that no further driver publications can occur.
No watch borrow or controller lock crosses an await or publication callback.

Defaults are a 100 ms quiet period, a one-second maximum coalescing window, and
exponential retry delays from 100 ms up to five seconds, with deterministic local
jitter of up to 20 percent of the capped exponential base, with fractional
nanoseconds truncated. The result is then clamped to at least one nanosecond and
at most the retry maximum; the five-second cap includes jitter. Zero debounce
bypasses the quiet period; other scheduling durations must be positive and ordered.
Continuous input cannot indefinitely postpone starting preparation, though a
candidate superseded during preparation is still discarded. Convergence assumes
updates eventually stabilize, preparation completes, and dependencies recover.

Validation failures are permanent. `run`'s classifier decides whether preparation
errors are transient. Stale publication tokens cause a fresh inspection and rebuild
after backoff; publication always remains conditional. Runtime shutdown exits the
worker; foreign-runtime and exhausted-generation failures wait for new input.
New input interrupts backoff and resets retry accounting. `max_retry_attempts`
counts retries after the initial attempt; `None` retries indefinitely. Exhausted
or permanent inputs remain in the latest slot and can be explicitly resubmitted.
After success, external publications require resubmission to detect drift.

Subscribe through `driver.status()` before moving the worker. This bounded watch
reports the latest preparation, success, typed failure and optional retry delay,
or graceful stop; it is not an audit stream. `UpdateSender::shutdown()` closes
input immediately. Await the worker to confirm cleanup: active preparation is
allowed to finish before the worker exits, so uncooperative preparation can delay
graceful shutdown indefinitely. Candidates that have not passed the final freshness
check are discarded. Dropping all producers also stops the worker. For forced
cancellation, drop the worker future, or abort and join its task. This drops
preparation according to the preparer's cancellation contract. There are no independently spawned worker tasks or retry timers.
Driver shutdown does not shut down the runtime or disrupt accepted requests.

Run `cargo run -p netplane-control --example burst_updates` for a complete producer,
status subscription, and joined shutdown example. Paused-time tests cover bursts,
superseded preparation, retry interruption and exhaustion, stale tokens, bounded
coalescing windows, and shutdown during preparation or retry.

Discovery integrations and a full desired-versus-actual audit stream remain future
extensions.
