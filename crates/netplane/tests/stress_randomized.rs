//! Seeded concurrent publication, admission, cancellation, and failure runs.

use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use netplane::{AdmissionError, GraphDraft, GraphVersion, Handle, Netplane, RuntimeLimits};
use tokio::{
    sync::{Barrier, Semaphore},
    time::timeout,
};
use tower::{BoxError, service_fn};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

const PRODUCERS: usize = 8;
const PHASES: usize = 8;
const LIMIT: usize = 32;

fn graph(
    version: usize,
    permits: Arc<Semaphore>,
    calls: Arc<Vec<AtomicUsize>>,
) -> Result<GraphVersion<usize, (usize, usize)>, netplane::ConfigError> {
    GraphDraft::new(service_fn(move |id: usize| {
        calls[id].fetch_add(1, Ordering::SeqCst);
        let permits = Arc::clone(&permits);
        async move {
            permits
                .acquire_owned()
                .await
                .map_err(io::Error::other)?
                .forget();
            if id % 20 == 0 {
                Err(io::Error::other("injected failure"))
            } else {
                Ok((version, id))
            }
        }
    }))
    .validate()
}

const fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn check_result(
    result: Result<(usize, usize), BoxError>,
    expected_version: usize,
    id: usize,
    seed: u64,
) -> TestResult {
    if id % 20 == 0 {
        let error = result
            .err()
            .ok_or_else(|| format!("seed {seed}, request {id}: expected failure"))?;
        assert!(
            error.downcast_ref::<io::Error>().is_some(),
            "seed {seed}, request {id}: {error}"
        );
    } else {
        assert_eq!(result?, (expected_version, id), "seed {seed}, request {id}");
    }
    Ok(())
}

#[derive(Clone)]
struct SyncPoints {
    start: Arc<Barrier>,
    filled: Arc<Barrier>,
    cancel: Arc<Barrier>,
    cancelled: Arc<Barrier>,
    completed: Arc<Barrier>,
}

impl SyncPoints {
    fn new() -> Self {
        let barrier = || Arc::new(Barrier::new(PRODUCERS + 1));
        Self {
            start: barrier(),
            filled: barrier(),
            cancel: barrier(),
            cancelled: barrier(),
            completed: barrier(),
        }
    }
}

async fn produce(
    producer: usize,
    seed: u64,
    handle: Handle<usize, (usize, usize)>,
    sync: SyncPoints,
    admitted: Arc<AtomicUsize>,
    settled: Arc<AtomicUsize>,
) -> TestResult {
    let mut rng = seed ^ ((producer as u64 + 1) * 0x9e37_79b9);
    for phase in 0..PHASES {
        sync.start.wait().await;
        let mut requests = Vec::with_capacity(4);
        for offset in 0..4 {
            let id = phase * LIMIT + producer * 4 + offset;
            let future = handle.try_execute(id)?;
            admitted.fetch_add(1, Ordering::SeqCst);
            requests.push((id, tokio::spawn(future)));
        }
        sync.filled.wait().await;
        sync.cancel.wait().await;
        let cancelled = (next(&mut rng) % 4) as usize;
        let (_, cancelled_request) = requests.swap_remove(cancelled);
        cancelled_request.abort();
        assert!(
            matches!(cancelled_request.await, Err(error) if error.is_cancelled()),
            "seed {seed}"
        );
        settled.fetch_add(1, Ordering::SeqCst);
        sync.cancelled.wait().await;
        for (id, request) in requests {
            check_result(request.await?, phase * 2, id, seed)?;
            settled.fetch_add(1, Ordering::SeqCst);
        }
        sync.completed.wait().await;
    }
    Ok(())
}

fn verify_accounting(
    seed: u64,
    calls: &[AtomicUsize],
    admitted: &AtomicUsize,
    settled: &AtomicUsize,
) {
    assert_eq!(
        admitted.load(Ordering::SeqCst),
        PHASES * (LIMIT + PRODUCERS)
    );
    assert_eq!(
        settled.load(Ordering::SeqCst),
        admitted.load(Ordering::SeqCst)
    );
    for (id, count) in calls.iter().enumerate() {
        assert!(
            count.load(Ordering::SeqCst) <= 1,
            "seed {seed}, request {id} executed twice"
        );
    }
}

async fn run(seed: u64) -> TestResult {
    let calls = Arc::new((0..322).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let admitted = Arc::new(AtomicUsize::new(0));
    let settled = Arc::new(AtomicUsize::new(0));
    let sync = SyncPoints::new();
    let first_gate = Arc::new(Semaphore::new(0));
    let mut runtime = Netplane::new(
        graph(0, Arc::clone(&first_gate), Arc::clone(&calls))?,
        RuntimeLimits {
            max_outstanding: LIMIT,
        },
    )?;
    let handle = runtime.handle();
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|producer| {
            tokio::spawn(produce(
                producer,
                seed,
                handle.clone(),
                sync.clone(),
                Arc::clone(&admitted),
                Arc::clone(&settled),
            ))
        })
        .collect();

    for phase in 0..PHASES {
        let gate = if phase == 0 {
            Arc::clone(&first_gate)
        } else {
            Arc::new(Semaphore::new(0))
        };
        if phase > 0 {
            runtime.publish(graph(phase * 2, Arc::clone(&gate), Arc::clone(&calls))?)?;
        }
        sync.start.wait().await;
        sync.filled.wait().await;
        assert!(
            matches!(
                handle.try_execute(320),
                Err(AdmissionError::AtCapacity { limit: LIMIT })
            ),
            "seed {seed}, phase {phase}"
        );
        runtime.publish(graph(phase * 2 + 1, Arc::clone(&gate), Arc::clone(&calls))?)?;
        sync.cancel.wait().await;
        sync.cancelled.wait().await;
        let mut replacements = Vec::with_capacity(PRODUCERS);
        for offset in 0..PRODUCERS {
            let id = 256 + phase * PRODUCERS + offset;
            replacements.push(handle.try_execute(id)?);
            admitted.fetch_add(1, Ordering::SeqCst);
        }
        assert!(
            matches!(
                handle.try_execute(320),
                Err(AdmissionError::AtCapacity { limit: LIMIT })
            ),
            "seed {seed}, phase {phase}: admission did not refill"
        );
        let replacements: Vec<_> = replacements.into_iter().map(tokio::spawn).collect();
        gate.add_permits(LIMIT + PRODUCERS);
        for (offset, replacement) in replacements.into_iter().enumerate() {
            let id = 256 + phase * PRODUCERS + offset;
            check_result(replacement.await?, phase * 2 + 1, id, seed)?;
            settled.fetch_add(1, Ordering::SeqCst);
        }
        sync.completed.wait().await;
        assert_eq!(
            admitted.load(Ordering::SeqCst),
            (phase + 1) * (LIMIT + PRODUCERS),
            "seed {seed}"
        );
        assert_eq!(
            settled.load(Ordering::SeqCst),
            admitted.load(Ordering::SeqCst),
            "seed {seed}"
        );
    }

    for producer in producers {
        producer.await??;
    }
    let final_gate = Arc::new(Semaphore::new(1));
    runtime.publish(graph(16, final_gate, Arc::clone(&calls))?)?;
    check_result(handle.execute(321).await, 16, 321, seed)?;
    runtime.shutdown(Duration::from_secs(1)).await?;
    verify_accounting(seed, &calls, &admitted, &settled);
    Ok(())
}

#[test]
fn seeded_concurrent_runs() -> TestResult {
    let repetitions = std::env::var("NETPLANE_STRESS_RUNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100);
    let base = std::env::var("NETPLANE_STRESS_SEED")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0x5eed_cafe);
    let threads = std::env::var("NETPLANE_STRESS_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for repetition in 0..repetitions {
            let seed = base.wrapping_add(repetition as u64);
            let result = timeout(Duration::from_secs(10), run(seed))
                .await
                .map_err(|error| format!("seed {seed}: {error}"))?;
            result.map_err(|error| format!("seed {seed}: {error}"))?;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })?;
    Ok(())
}
