# Runtime observability

Netplane emits structured `tracing` events with target `netplane`. The library does not install a subscriber. Applications can enable events with `netplane=debug`.

`runtime_id` identifies a runtime within one process. `version_id` is a publication generation within that runtime, starting at 1. `request_id` is allocated on admission and shared by cloned handles. Correlate requests with `(runtime_id, request_id)` and publications with `(runtime_id, version_id)`. Identifiers are process local.

| Event | Level | Fields | Meaning |
| --- | --- | --- | --- |
| `request.admitted` | DEBUG | `runtime_id`, `version_id`, `request_id` | Synchronous admission and version selection. |
| `request.rejected` | WARN or DEBUG | `runtime_id`, `reason`, optional `outstanding_limit` | `capacity_exceeded` is WARN; `admission_closed` is DEBUG. |
| `request.completed` | DEBUG | IDs, `duration_us` | Accepted future succeeded. |
| `request.failed` | WARN | IDs, `duration_us`, `error_kind` | Accepted future failed; `service_error` and `buffer_closed` hide error text. |
| `request.cancelled` | DEBUG | IDs, `duration_us` | Accepted future was dropped, including before its first poll. |
| `graph.validated` | DEBUG | `node_count`, `route_count` | Validation succeeded. |
| `graph.validation_failed` | WARN | `reason` | Fixed validation category. |
| `version.published` | INFO | `runtime_id`, `old_version`, `new_version` | Atomic publication; initial old version is 0. |
| `version.replaced` | DEBUG | `runtime_id`, `old_version`, `new_version` | Old version is no longer active. |
| `version.endpoint` | DEBUG | `runtime_id`, `version_id`, `endpoint_id` | Associates a published version with each of its endpoint workers. |
| `version.released` | DEBUG | `runtime_id`, `version_id` | Final published-version reference dropped. |
| `version.publish_rejected` | DEBUG or WARN | `runtime_id`, `reason` | `shutting_down` is DEBUG; `generation_exhausted` is WARN. |
| `shutdown.started` | INFO | `runtime_id`, `shutdown_attempt_id`, `grace_period_ms`, `outstanding` | Admission closed. |
| `shutdown.completed` | INFO | `runtime_id`, `shutdown_attempt_id`, `duration_ms` | Drain completed. |
| `shutdown.timed_out` | WARN | `runtime_id`, `shutdown_attempt_id`, `duration_ms`, `remaining` | Deadline expired; remaining count sampled at resolution. |
| `worker.terminated` | ERROR or DEBUG | `endpoint_id`, `reason` | `panicked` is ERROR; `completed` and `aborted` are DEBUG. |

`duration_us` measures from synchronous admission to future completion or drop. It includes time before the first poll, queue waits, readiness waits, and execution. It is not queue latency. `duration_ms` measures a shutdown attempt from method call to resolution using Tokio's clock. Each shutdown call receives a distinct attempt ID, including when its returned future is never polled. Subscriber filters may suppress any event.

Request terminal events and execution polling use the subscriber active when admission occurs. Version release uses the subscriber active when that version was published, worker termination uses the subscriber active when the endpoint was constructed, and a shutdown result uses the subscriber active when shutdown was invoked. This preserves scoped subscribers when futures move between threads. A later subscriber in the polling thread does not replace the captured subscriber for that work.

The `netplane.request` span covers polling of the caller's execution future. Tower executes the service in a separate buffer worker, so application spans inside that service are not guaranteed to be children of `netplane.request`. Constructing a graph and publishing it under different subscribers also sends worker and publication events to different subscribers; correlate their numeric endpoint IDs only when both event streams are available.

Runtime, request, endpoint, and shutdown attempt IDs use process-local `u64` counters. Exhaustion of those counters is outside the supported operational lifetime. Publication generations are checked and reject publication before reuse.

The publisher emits `version.published`, `version.replaced`, and `version.endpoint` before it releases its old-version reference. Requests and other owners can release references concurrently, so there is no total ordering guarantee across threads. A worker may terminate for a validated candidate that was never published; in that case no `version.endpoint` event associates its `endpoint_id` with a publication.

Default events contain no request, response, header, selector output, graph name, or arbitrary error text. Netplane's worker event omits panic payloads, but Rust's panic hook or an application-installed hook may independently print them to stderr. Dropping a dispatched request does not guarantee termination of independently spawned service work.

See the [example](../crates/netplane/examples/observability.rs) for subscriber setup.
