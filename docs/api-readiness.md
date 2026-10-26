# Public API review

This review covers the current `0.0.0` source surface. Existing builder calls remain valid. Adding `GraphMetadata::backend_capacities` is source-breaking for consumers constructing `GraphMetadata` with struct literals; those consumers must supply the new field. The external-consumer tests in `crates/netplane/tests/public_api.rs` and `crates/netplane-http/tests/public_api.rs` exercise the supported entry points from outside each library crate.

| Area | Review outcome |
| --- | --- |
| `Capacity`, `RuntimeLimits` | Units are request counts. Both endpoint capacities and the global outstanding budget require positive values; defaults are 32 in flight and 64 buffered per endpoint. |
| `GraphDraft`, `GraphBuilder` | The draft validates capacity and prepares one Tower root. The builder validates one named router with independently bounded backends, declared labels and an explicit fallback. `service_with_capacity` overrides the default backend capacity for its service, independent of call order. All declared capacities must be positive. Neither validates application policy or arbitrary topology. |
| `GraphVersion`, `GraphMetadata` | Versions are immutable after validation. `metadata()` returns a shared read-only reference; controller snapshots clone the metadata and can be inspected independently. `backend_capacity` is the configured default; `backend_capacities` lists effective per-service capacities in name order and is empty for a single-root draft. |
| `Handle` | Admission and version selection occur at method invocation, including for futures that are never polled. `try_execute` and `try_execute_until` return typed synchronous admission errors. `execute` and `execute_until` box those errors into the returned future. All four paths return service and worker errors as Tower `BoxError`. |
| `Netplane` | Publication accepts validated versions and rejects after shutdown begins. Shutdown closes admission when called, drains accepted futures, and can be retried after a timeout. |
| `ReconfigurationController` | Tokens are runtime-scoped and optimistic; stale and foreign tokens fail. Bounded history retains executable graph state, and rollback assigns a fresh publication ID. |
| HTTP adapter | `HttpLimits` validates body and response limits. `HttpAdapter` maps transport failures to HTTP responses and remains in a separate optional workspace crate. |

The current typed error boundary is deliberate: configuration, admission, publication, controller and drain failures have dedicated types; Tower service and buffer errors remain boxed for interoperability. The execution contract documents cancellation after dispatch and deadline expiration. The source remains pre-release, so a release decision still needs to declare its compatibility policy and decide whether the optional HTTP crate will be published.

## Contract evidence and gaps

| Contract | Current evidence | Limit of this review |
| --- | --- | --- |
| Admission | `netplane/tests/public_api.rs` holds one unpolled request and observes synchronous `AtCapacity`; `netplane-http/tests/public_api.rs` observes HTTP 503 at capacity. | The full workspace suite covers additional concurrent and cross-version cases. |
| Publication and rollback | The core external-consumer test publishes a `GraphBuilder` version, completes the old selected request, then rolls back and observes the original result. | Stale and foreign token rejection, history eviction, and metadata behavior rely on controller integration tests. |
| Shutdown | The core external-consumer test drains and observes closed admission afterward. | Timeout retry and outstanding-future ownership rely on lifecycle and admission tests. |
| HTTP | The adapter external-consumer test checks successful body echo, overload mapping, and router construction. | Body limits, timeout, response limits, and listener behavior rely on HTTP integration tests. |

These tests compile as external consumers inside the workspace and use workspace-resolved dependencies. A fresh standalone binary also passed `cargo check --offline` against the verified `target/package/netplane-0.0.0` package and imported `Capacity` and `RuntimeLimits`. This smoke build does not exercise the full public API or the HTTP package. The public API must be frozen and a broader consumer build repeated before a `0.1.0` decision.
