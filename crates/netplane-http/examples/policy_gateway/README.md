# Policy routing gateway

Run the public listener on `127.0.0.1:3000` and enable the separate loopback administrative listener on `127.0.0.1:3001`:

```bash
export NETPLANE_ADMIN_TOKEN="replace-with-a-long-random-token"
cargo run -p netplane-http --example policy_gateway
```

The token must be at least 16 bytes. Without it, the administrative listener is disabled. This is local example authentication: it has no TLS, identity management, or network exposure beyond loopback. Every administrative route requires `Authorization: Bearer <token>`.

Send public traffic:

```bash
curl -i -X POST http://127.0.0.1:3000/process \
  -H 'X-Request-Key: customer-1' --data 'hello'
```

Valid keys contain 1 to 128 ASCII letters, digits, `.`, `_`, or `-`. The initial policy assigns 25% of keys to canary. Responses identify the backend and application policy revision. Missing or invalid keys use the fallback backend.

Inspect the active publication and rollback history:

```bash
curl -H "Authorization: Bearer $NETPLANE_ADMIN_TOKEN" http://127.0.0.1:3001/status
```

Change the canary percentage:

```bash
curl -X POST http://127.0.0.1:3001/reconfigure \
  -H "Authorization: Bearer $NETPLANE_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"expected_version":1,"policy":{"canary_percent":25}}'
```

The response contains `previous_version`, `published_version`, and `kind`. Use the current numeric publication ID as `expected_version`; stale mutations return `409`. A percentage above 100 returns `422`. Replacement builds a complete new graph, then publishes it atomically. Accepted requests keep their selected graph.

Restore a retained executable graph:

```bash
curl -X POST http://127.0.0.1:3001/rollback \
  -H "Authorization: Bearer $NETPLANE_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"expected_version":2,"target_version":1}'
```

Rollback returns a fresh publication ID and `source_version`. It reuses the retained Tower graph, including service state. The two most recently displaced publications are rollback eligible by default. `/status` shows only that controller history; older requests can still finish on an evicted graph. The application policy revision in public responses can reappear after rollback because it belongs to that graph, while the publication ID always advances.

Shut down through the admin listener or press Ctrl-C:

```bash
curl -X POST http://127.0.0.1:3001/shutdown \
  -H "Authorization: Bearer $NETPLANE_ADMIN_TOKEN"
```

Shutdown closes Netplane admission immediately and drains accepted work within the configured grace period. The Netplane drain timeout does not impose a hard deadline on every HTTP connection. Administrative JSON bodies are limited to 4 KiB. Read `/status` or the previous mutation response before submitting another mutation if other administrators may be active.
