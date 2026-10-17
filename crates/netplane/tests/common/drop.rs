use std::sync::atomic::{AtomicUsize, Ordering};

pub async fn wait_for_drop(counter: &AtomicUsize, expected: usize) {
    for _ in 0..10_000 {
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
    assert_eq!(
        counter.load(Ordering::SeqCst),
        expected,
        "resource was not released after bounded scheduling opportunities"
    );
}
