# netplane

Netplane provides a bounded Tower endpoint and atomic publication of immutable
execution versions. A request selects its version when `execute()` is called.

Build a Tower composition with `ServiceBuilder` and `Layer`, then pass it to
`Endpoint::new` inside a Tokio runtime. Each endpoint has its own concurrency
limit and bounded buffer. `execute` returns a future that waits for capacity
and for the service result. The buffer worker polls readiness on the same
service instance it calls.

```rust,no_run
use netplane::{Capacity, Endpoint};
use tower::{ServiceBuilder, service_fn};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let service = ServiceBuilder::new().service(service_fn(|value: usize| async move {
    Ok::<_, std::convert::Infallible>(value + 1)
}));
let endpoint = Endpoint::new(service, Capacity {
    max_in_flight: 32,
    buffer_capacity: 64,
})?;
let result = endpoint.execute(1).await?;
assert_eq!(result, 2);
# Ok(())
# }
```

To reconfigure execution, validate a `GraphDraft` and give it to `Netplane`:

```rust,no_run
use netplane::{Capacity, GraphDraft, Netplane, RuntimeLimits};
use tower::service_fn;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let capacity = Capacity { max_in_flight: 32, buffer_capacity: 64 };
let first = GraphDraft::new(service_fn(|value: usize| async move {
    Ok::<_, std::convert::Infallible>(value + 1)
})).capacity(capacity).validate()?;
let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 1024 })?;
let handle = runtime.handle();
let old_request = handle.execute(1);
let second = GraphDraft::new(service_fn(|value: usize| async move {
    Ok::<_, std::convert::Infallible>(value + 2)
})).capacity(capacity).validate()?;
runtime.publish(second)?;
assert_eq!(old_request.await?, 2);
assert_eq!(handle.execute(1).await?, 3);
# Ok(())
# }
```

Construct endpoints and validate drafts inside a Tokio runtime. Each version
has its own execution and buffer capacity limits. The explicit global limit bounds accepted, unfinished requests across every handle and retained version, including unpolled futures. At the limit, `try_execute()` returns `AdmissionError::AtCapacity` immediately. Dropping an execution future cancels its queue
wait; cancellation of work already dispatched depends on the service.

Call `runtime.shutdown(grace_period)` to close admission immediately and wait for
accepted request futures to finish or be dropped. A timeout reports the number
still outstanding; admission stays closed, and the drain can be retried. A
successful drain releases the active graph even while handles remain, and
publication is rejected once shutdown starts. An unpolled request future
accepted before shutdown counts as outstanding until it
is polled to completion or dropped. Draining cannot forcibly reclaim work
retained by an underlying service after its caller drops the future.

Netplane owns each Tower buffer worker. When a replaced version or stopped
runtime loses its final endpoint and request future owner, Netplane aborts that
worker. This also releases a service stuck in `poll_ready()`. Tasks spawned
independently by a user service remain the service's responsibility.
