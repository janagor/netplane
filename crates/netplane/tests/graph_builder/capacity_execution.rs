use super::*;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

fn gated_backend(
    name: &'static str,
    started: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    signal: Arc<Notify>,
    gate: Arc<Semaphore>,
) -> BoxService<Request, Response, BoxError> {
    BoxService::new(
        service_fn(move |request: Request| {
            let started = started.clone();
            let active = active.clone();
            let peak = peak.clone();
            let signal = signal.clone();
            let gate = gate.clone();
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                started.fetch_add(1, Ordering::SeqCst);
                signal.notify_waiters();
                gate.acquire().await.map_err(io::Error::other)?.forget();
                active.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, io::Error>(format!("{name}:{request}"))
            }
        })
        .map_err(Into::into),
    )
}
async fn wait_for(count: &AtomicUsize, signal: &Notify, expected: usize) -> Result<(), BoxError> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let notified = signal.notified();
            if count.load(Ordering::SeqCst) >= expected {
                break;
            }
            notified.await;
        }
    })
    .await?;
    Ok(())
}

async fn assert_responses(
    jobs: Vec<(Request, tokio::task::JoinHandle<Result<Response, BoxError>>)>,
) -> Result<(), BoxError> {
    for (request, job) in jobs {
        let result = tokio::time::timeout(Duration::from_secs(5), job).await??;
        let response = result?;
        let expected = if request == 0 {
            format!("slow:{request}")
        } else {
            format!("fast:{request}")
        };
        assert_eq!(response, expected);
    }
    Ok(())
}

#[tokio::test]
async fn backend_limits_control_underlying_calls() -> Result<(), BoxError> {
    let slow_started = Arc::new(AtomicUsize::new(0));
    let fast_started = Arc::new(AtomicUsize::new(0));
    let slow_active = Arc::new(AtomicUsize::new(0));
    let fast_active = Arc::new(AtomicUsize::new(0));
    let slow_peak = Arc::new(AtomicUsize::new(0));
    let fast_peak = Arc::new(AtomicUsize::new(0));
    let fast_routed = Arc::new(AtomicUsize::new(0));
    let fast_routed_signal = Arc::new(Notify::new());
    let selected_fast = fast_routed.clone();
    let selected_fast_signal = fast_routed_signal.clone();
    let slow_signal = Arc::new(Notify::new());
    let fast_signal = Arc::new(Notify::new());
    let slow_gate = Arc::new(Semaphore::new(0));
    let fast_gate = Arc::new(Semaphore::new(0));
    let graph = GraphBuilder::new()
        .service_with_capacity(
            "slow",
            gated_backend(
                "slow",
                slow_started.clone(),
                slow_active.clone(),
                slow_peak.clone(),
                slow_signal.clone(),
                slow_gate.clone(),
            ),
            Capacity {
                max_in_flight: 1,
                buffer_capacity: 4,
            },
        )
        .service_with_capacity(
            "fast",
            gated_backend(
                "fast",
                fast_started.clone(),
                fast_active.clone(),
                fast_peak.clone(),
                fast_signal.clone(),
                fast_gate.clone(),
            ),
            Capacity {
                max_in_flight: 8,
                buffer_capacity: 16,
            },
        )
        .router("dispatch", move |request: &Request| {
            if *request == 0 {
                "slow"
            } else {
                selected_fast.fetch_add(1, Ordering::SeqCst);
                selected_fast_signal.notify_waiters();
                "fast"
            }
        })
        .route("dispatch", "slow", "slow")
        .route("dispatch", "fast", "fast")
        .fallback("dispatch", "fast")
        .entry("dispatch")
        .capacity(Capacity {
            max_in_flight: 16,
            buffer_capacity: 32,
        })
        .validate()?;
    let runtime = Netplane::new(
        graph,
        RuntimeLimits {
            max_outstanding: 32,
        },
    )?;
    let handle = runtime.handle();
    let mut jobs = Vec::new();
    for request in [0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9] {
        let handle = handle.clone();
        jobs.push((
            request,
            tokio::spawn(async move { handle.execute(request).await }),
        ));
    }
    wait_for(&slow_started, &slow_signal, 1).await?;
    wait_for(&fast_started, &fast_signal, 8).await?;
    wait_for(&fast_routed, &fast_routed_signal, 9).await?;
    assert_eq!(slow_started.load(Ordering::SeqCst), 1);
    assert_eq!(fast_started.load(Ordering::SeqCst), 8);
    assert_eq!(slow_peak.load(Ordering::SeqCst), 1);
    assert_eq!(fast_peak.load(Ordering::SeqCst), 8);
    slow_gate.add_permits(1);
    wait_for(&slow_started, &slow_signal, 2).await?;
    assert_eq!(fast_started.load(Ordering::SeqCst), 8);
    fast_gate.add_permits(1);
    wait_for(&fast_started, &fast_signal, 9).await?;
    slow_gate.add_permits(1);
    fast_gate.add_permits(8);
    assert_responses(jobs).await?;
    assert_eq!(slow_active.load(Ordering::SeqCst), 0);
    assert_eq!(fast_active.load(Ordering::SeqCst), 0);
    assert_eq!(slow_peak.load(Ordering::SeqCst), 1);
    assert_eq!(fast_peak.load(Ordering::SeqCst), 8);
    Ok(())
}
