# HTTP upstream gateway

This example forwards buffered HTTP requests over TCP to two independent servers.
The client and its typed errors are private to the example; the core library has
no HTTP client dependency. Rust 1.88 was verified with reqwest 0.12.28.

Start each command in a separate terminal:

```sh
cargo run -p netplane-http --example upstream_server -- stable 127.0.0.1:4001
cargo run -p netplane-http --example upstream_server -- canary 127.0.0.1:4002
cargo run -p netplane-http --example upstream_gateway
```

Then send traffic:

```sh
curl -i -H 'X-Request-Key: example' --data hello http://127.0.0.1:3000/process
curl -i -H 'X-Request-Key: example' http://127.0.0.1:3000/failure
curl -i -H 'X-Request-Key: example' http://127.0.0.1:3000/health
```

A valid key contains 1–128 ASCII letters, digits, dots, underscores or hyphens.
The same FNV-1a bucket calculation as policy_gateway sends 20% of key buckets to
canary; other keys go to stable. Missing or invalid keys get 400 from the local
fallback. Valid keys forward any origin-form method/path/query to the selected
server; upstream 404 and 503 responses remain ordinary HTTP responses.

POST /process returns `backend=stable;processed_bytes=5` for the request above
(or canary, depending on the key), with an X-Backend header. GET /health returns
200. GET /failure returns 503. GET /slow waits for POST /release sent directly to
the selected server. Release stores one permit, so release-before-wait also works.
These demo listeners bind loopback and have no authentication.

## Publication during traffic

Start a slow request in a fourth terminal:

```sh
curl -i -H 'X-Request-Key: example' http://127.0.0.1:3000/slow
```

Press Enter in the gateway terminal to publish a graph swapping stable and canary
origins. Send another /process request with the same key: its X-Backend changes.
Release the first request by posting to its original upstream:

```sh
curl -X POST http://127.0.0.1:4001/release
curl -X POST http://127.0.0.1:4002/release
```

The slow request completes from its original server. Release within ten seconds
or it returns 504. Each Enter publishes another immutable version; clients share
the connection pool, while origins stay fixed within each version. Set
NETPLANE_STABLE and NETPLANE_CANARY to override the initial HTTP origins.
Ctrl-C closes runtime admission and drains accepted requests for fifteen seconds.
On drain timeout the example exits with an error; the library supports retrying
shutdown, but this process does not provide an administrative retry interface.

## Bounds and failure behavior

Each backend has eight concurrent calls and sixteen buffered queue slots. The
graph has no outer capacity, so a saturated stable backend does not consume an
outer slot needed by canary. A runtime budget of 64 accepted unfinished futures
still applies across versions, including unpolled requests and queue waiters.
The inbound adapter uses its explicit default limits (1 MiB request and response,
64 concurrent body reads, 30 second body read timeout).

The outbound client uses a two second connect timeout, ten second whole-exchange
timeout, HTTP/1.1 pooling with eight idle connections per origin and a thirty
second idle timeout. Proxies, redirects and retries are disabled. TLS is outside
this local HTTP demo. Responses are rejected while collecting chunks once their
body would exceed 1 MiB; a known oversized Content-Length is rejected immediately.
This bounds the collected body, not the transport's individual chunk allocation.
Connection errors and oversized responses become empty 502 responses; timeouts
become empty 504 responses. Inbound overload or closed admission returns 503.

Connection-nominated and standard hop-by-hop headers are removed in both
directions. The inbound Host and Content-Length are replaced by client framing.
Absolute-form request targets return an empty 400 response; path and query cannot replace the
configured destination origin. No failover or replay occurs. Dropping a Netplane
future releases admission; remote work may continue after a disconnect, and no
claim is made that cancellation undoes upstream side effects.

## Verification

```sh
cargo test -p netplane-http --example upstream_gateway
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace --all-features
```

Tests bind ephemeral loopback ports and use Notify gates. A separate ingress
client exercises the complete TCP gateway-to-upstream path and error mapping. They cover real TCP
routing, HTTP status preservation, refusal, timeout, fixed and chunked response
limits, header removal, connection reuse on one accepted socket, publication
while an upstream is blocked, global admission, cancellation and shutdown.
