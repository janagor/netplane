# Health-aware HTTP gateway

Run `cargo run -p netplane-http --example health_aware_gateway`. The example starts
three independent HTTP upstreams at ports 4101–4103 and a gateway at port 4100.
Each upstream serves ordinary requests and `/health`. POST `/fail` simulates an
unavailable upstream, including its health endpoint; POST `/recover` restores it.
These controls belong only to the demonstration.

Active checks run every second, with two concurrent checks, a 500 ms timeout,
three failures before exclusion, a three-second cooldown and two successes before
recovery. Backends start recovering, so initial gateway requests may return 503
until successful checks complete. Ordinary executions also report passive outcomes.

Inspect `curl localhost:4100/admin/health`. Its `publication_id` stays unchanged
through every health transition: the example publishes exactly one graph.

Use distinct `x-routing-key` headers to sample the configured 60/30/10 weights:

```sh
for key in $(seq 1 1000); do
  curl -s -H "x-routing-key: $key" localhost:4100/
  echo
done | sort | uniq -c
curl -X POST localhost:4102/fail
curl localhost:4100/admin/health
```

After backend B becomes unhealthy, repeat the sample: A/C receive approximately
85.7/14.3 percent. Restore B with `curl -X POST localhost:4102/recover`, wait for
cooldown and recovery checks, and repeat to see 60/30/10 again. Sticky key hashing
allocates buckets, so repeated identical keys do not sample traffic proportions.

Fail all three backends to see explicit HTTP 503 from the gateway:

```sh
for port in 4101 4102 4103; do curl -X POST localhost:$port/fail; done
curl localhost:4100/admin/health
curl -i localhost:4100/
```

Ctrl-C closes runtime admission, gracefully stops listeners, drains accepted work
and awaits the probe scheduler. Ordinary request and probe budgets are independent.
