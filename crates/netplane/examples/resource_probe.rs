//! Count live service instances across repeated publication and old-version release.
//!
//! Modes:
//! - `paced`: wait for cleanup after each publication (deterministic correctness)
//! - `burst`: publish without waiting, sample peak retention, then release and drain

use std::{
    convert::Infallible,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use netplane::{GraphDraft, Netplane, RuntimeLimits};
use tower::Service;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Paced,
    Burst,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match value {
            "paced" => Ok(Self::Paced),
            "burst" => Ok(Self::Burst),
            _ => Err("mode must be paced or burst".into()),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Paced => "paced",
            Self::Burst => "burst",
        }
    }
}

struct CountedService(Arc<AtomicUsize>);

impl CountedService {
    fn new(live: &Arc<AtomicUsize>) -> Self {
        live.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(live))
    }
}

impl Drop for CountedService {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Service<u64> for CountedService {
    type Response = u64;
    type Error = Infallible;
    type Future = std::future::Ready<Result<u64, Infallible>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: u64) -> Self::Future {
        std::future::ready(Ok(request + 1))
    }
}

async fn wait_for_count(
    live: &AtomicUsize,
    expected: usize,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while live.load(Ordering::Relaxed) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
}

fn sample_peak(live: &AtomicUsize, peak: &mut usize) {
    *peak = (*peak).max(live.load(Ordering::Relaxed));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let publications = std::env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(1000);
    let mode = std::env::args()
        .nth(2)
        .as_deref()
        .map(Mode::parse)
        .transpose()?
        .unwrap_or(Mode::Paced);
    if publications == 0 {
        return Err("publication count must be positive".into());
    }
    let live = Arc::new(AtomicUsize::new(0));
    let initial = GraphDraft::new(CountedService::new(&live)).validate()?;
    let outstanding = match mode {
        Mode::Paced => 1,
        Mode::Burst => publications
            .checked_add(1)
            .ok_or("publication count is too large for admission budget")?,
    };
    let mut runtime = Netplane::new(
        initial,
        RuntimeLimits {
            max_outstanding: outstanding,
        },
    )?;
    let handle = runtime.handle();
    let mut peak = live.load(Ordering::Relaxed);

    match mode {
        Mode::Paced => {
            for _ in 0..publications {
                let old = handle.try_execute(7)?;
                let candidate = GraphDraft::new(CountedService::new(&live)).validate()?;
                runtime.publish(candidate)?;
                sample_peak(&live, &mut peak);
                drop(old);
                wait_for_count(&live, 1).await?;
            }
        }
        Mode::Burst => {
            let mut retained = Vec::with_capacity(publications);
            for index in 0..publications {
                let old = handle.try_execute(7)?;
                let candidate = GraphDraft::new(CountedService::new(&live)).validate()?;
                runtime.publish(candidate)?;
                retained.push(old);
                if index % 32 == 0 || index + 1 == publications {
                    sample_peak(&live, &mut peak);
                }
            }
            sample_peak(&live, &mut peak);
            drop(retained);
            wait_for_count(&live, 1).await?;
            sample_peak(&live, &mut peak);
        }
    }

    runtime.shutdown(Duration::from_secs(5)).await?;
    wait_for_count(&live, 0).await?;
    println!(
        "mode={} publications={publications} peak_live_services={peak} final_live_services={}",
        mode.as_str(),
        live.load(Ordering::Relaxed)
    );
    Ok(())
}
