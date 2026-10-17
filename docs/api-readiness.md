# Public API review

This review covers the current `0.0.0` source surface. PR 15 keeps public signatures intact; no migration is required for existing source consumers. The external-consumer test in `crates/netplane/tests/public_api.rs` compiles and runs the main routing, publication, rollback, admission and drain paths. The optional HTTP adapter has a separate smoke test in `crates/netplane-http/tests/public_api.rs` that stays outside the core dependency graph.

| Area | Review outcome |
| --- | --- |
| `Capacity`, `RuntimeLimits` | Units are request counts. Both endpoint capacities and the global outstanding budget require positive values; defaults are 32 in flight and 64 buffered per endpoint. |
| `GraphDraft`, `GraphBuilder` | The draft validates capacity and prepares one Tower root. The builder validates one named router with independently bounded backends, declared labels and an explicit fallback. Neither validates application policy or arbitrary topology. |
| `GraphVersion`, `GraphMetadata` | Versions are immutable after validation. `metadata()` returns a shared read-only reference; controller snapshots clone the metadata and can be inspected independently. |
| `Handle` | Admission and version selection occur at method invocation, including for futures that are never polled. `try_execute` and `try_execute_until` return typed synchronous admission errors. `execute` and `execute_until` box those errors into the returned future. All four paths return service and worker errors as Tower `BoxError`. |
| `Netplane` | Publication accepts validated versions and rejects after shutdown begins. Shutdown closes admission when called, drains accepted futures, and can be retried after a timeout. |
| `ReconfigurationController` | Tokens are runtime-scoped and optimistic; stale and foreign tokens fail. Bounded history retains executable graph state, and rollback assigns a fresh publication ID. |
| HTTP adapter | `HttpLimits` validates body and response limits. `HttpAdapter` maps transport failures to HTTP responses and remains in a separate optional workspace crate. |

The current typed error boundary is deliberate: configuration, admission, publication, controller and drain failures have dedicated types; Tower service and buffer errors remain boxed for interoperability. The execution contract documents cancellation after dispatch and deadline expiration. The source remains pre-release, so a release decision still needs to declare its compatibility policy and decide whether the optional HTTP crate will be published.
