# Policy routing gateway

Run the persistent example:

```bash
cargo run -p netplane-http --example policy_gateway
```

The listener uses `127.0.0.1:3001`. Its typed initial policy assigns 25% of valid keys to canary using fixed FNV-1a hashing and returns the same branch for the same key. Send a canary request:

```bash
curl -i -X POST http://127.0.0.1:3001/process \
  -H 'X-Request-Key: customer-1' \
  --data 'hello'
```

This returns `200`, `x-backend: canary`, `x-policy-revision: 1`, and `backend=canary;processed_bytes=5`. Send a stable request:

```bash
curl -i -X POST http://127.0.0.1:3001/process \
  -H 'X-Request-Key: customer-2' \
  --data 'hello'
```

This returns `200`, `x-backend: stable`, `x-policy-revision: 1`, and `backend=stable;processed_bytes=5`. A missing or invalid key receives `400`; `GET /process` receives `405`; an unknown path receives `404`. All use the named fallback backend. Valid keys contain 1 to 128 ASCII letters, digits, `.`, `_`, or `-`.

Change `canary_percent` in [`config.rs`](config.rs) to set the initial percentage, then rebuild and restart the example. The `revision` field identifies the immutable application policy; it is separate from Netplane's publication ID. Live policy changes are outside this example. The stable and canary delay and failure fields in the same file can be changed to explore congestion or explicit `503` responses. Press Ctrl-C to stop the process; it closes Netplane admission and drains admitted requests while Axum stops accepting connections. The Netplane drain timeout does not impose a hard deadline on every HTTP connection.
