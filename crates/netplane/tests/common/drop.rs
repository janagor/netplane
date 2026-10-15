use std::sync::atomic::{AtomicUsize, Ordering};

pub async fn wait_for_drop(counter: &AtomicUsize, expected: usize) {
    loop {
        let observed = counter.load(Ordering::SeqCst);
        assert!(
            observed <= expected,
            "service dropped {observed} times, expected {expected}"
        );
        if observed == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
}
