//! Verifies the runnable live reconfiguration example.

#[path = "../examples/live_reconfiguration.rs"]
mod example;

use std::error::Error;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn direct_backend_becomes_authorized_router() -> Result<(), Box<dyn Error + Send + Sync>> {
    let results = timeout(Duration::from_secs(5), example::run_demo()).await??;
    assert_eq!(results.old_request, "A:1");
    assert_eq!(results.routed_a, "A:2");
    assert_eq!(results.routed_b, "B:3");
    assert!(results.rejected);
    Ok(())
}
