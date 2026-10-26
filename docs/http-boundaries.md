# HTTP resource boundaries

`netplane-http` buffers a request body before decoding and calling `Handle::try_execute()`. Axum and Hyper parse headers before the adapter runs. The body-read semaphore is shared by clones of one adapter, uses immediate rejection when full, and does not count open sockets. The body permit is released after decoding or when collection is canceled. A request selects its graph when `try_execute()` accepts it, so publication during an upload selects the newly active graph.

| Resource | Owner | Boundary |
| --- | --- | --- |
| Buffered request body | Netplane HTTP | Per-request `max_body_bytes`; `Content-Length` can reject early, streamed bodies are checked while collecting. |
| Concurrent body readers | Netplane HTTP | `max_concurrent_body_reads` counts body collection and decoding, not connections. Exhaustion immediately rejects. |
| Body collection time | Netplane HTTP | `body_read_timeout` covers asynchronous collection after headers, not header parsing or synchronous decoder CPU time. |
| Outstanding execution | Netplane core | Global admission starts after decoding and includes accepted, unfinished requests, even unpolled futures. |
| Selected graph | Netplane core | Selected at `try_execute()`, retained through accepted execution or future drop. |
| Encoded response size | Netplane HTTP | `max_response_bytes` is checked after encoding, including mapped service errors. It does not bound temporary encoder allocations. |
| Slow response writer | HTTP transport | A response can retain its bytes after Netplane admission is released. Socket writes are outside the execution budget. |
| Incomplete HTTP headers | HTTP transport | The adapter has not been called. Wire behavior is determined by Axum and Hyper. |
| Open HTTP connections | HTTP transport | No connection-count limit is supplied by the adapter. Apply server-level policy where needed. |
| Detached service work | Tower service | Dropping execution releases Netplane admission; service-owned work may continue according to that service's cancellation semantics. |

| Adapter condition | HTTP status |
| --- | --- |
| Body-read permits exhausted | 503 |
| Body too large | 413 |
| Body collection timeout | 408 |
| Failed body collection | 400 |
| Request decoder failure | 400 |
| Runtime admission exhausted or closed | 503 |
| Response encoder failure | 500 |
| Encoded response over limit | 500 |

These are adapter-level mappings. Hyper may reject malformed requests before the adapter and produce a different wire response.

`Netplane::shutdown()` closes core admission immediately and drains accepted executions. It does not cancel an upload that has not reached execution. `axum::serve(...).with_graceful_shutdown(...)` separately manages the listener and connections. The two shutdown deadlines are independent. A pending upload may finish after core shutdown and then receive 503 at admission; a stalled connection may require transport-level termination.
