# Typed desired-state reconciliation

The optional `netplane-control` crate depends on `netplane`; the runtime does not
depend on it. It translates application configuration into conditional graph
publications through explicit `Reconciler::apply` calls. It creates no controller
loop, pending queue, retry scheduler, file reader, or discovery integration.

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

Stream coalescing, automatic retries, discovery, and desired-versus-actual status
remain future extensions.
