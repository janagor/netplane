# Progressive rollout over real HTTP

Run `cargo run -p netplane-http --example progressive_rollout -- success`,
`failure`, `insufficient`, `latency-success`, or `latency-failure`. Each run binds three ephemeral loopback listeners:
stable v1, canary v2 and a Netplane HTTP gateway. A bounded sequential client
sends actual HTTP requests through the gateway. Every task is awaited on exit.

The success scenario observes 5%, 25%, 50% and 100% stages before completing.
The failure scenario receives actual HTTP 500 responses from canary and stops
at 5%, retaining those weights. Insufficient traffic cannot meet the canary
sample floor: the 5% stage blocks until its deadline, then the demonstration
explicitly cancels without restoring traffic. Timings and thresholds are demo
configuration, not library defaults.

The provider reads at most 4,096 completed outcomes. Eviction marks the window
incomplete. Graph-specific router names associate backend samples with the
confirmed immutable publication, including old-version completions. Only samples
accepted after the observation window begins and completed by its requested end
count. The ingress decode timestamp is a conservative approximation immediately
before admission. There are no retries or concurrent administrative publications
in this example. Transport errors and HTTP 5xx are failures; admission rejection
and cancellation are excluded. The provider sorts the bounded, complete population
of decode-to-backend-terminal durations and reports nearest-rank p95, including
queueing and the actual upstream exchange. The decode timestamp approximates
admission, so this is demonstration instrumentation rather than a general runtime
telemetry engine. `latency-success` verifies its configured p95 limit; in
`latency-failure` the real canary waits 40 ms and violates a 10 ms threshold,
retaining the 5% allocation. A missing latency population still reports `None`.

Eligibility comes from N10 health handles. This example registers healthy
instances and leaves passive classification disabled so the failure scenario
can demonstrate its error-rate condition directly; it does not claim that
registration is an operational readiness check. Production integrations should
supply authoritative health transitions and publication-attributed telemetry.
The library adds no request-path instrumentation for this demonstration.
