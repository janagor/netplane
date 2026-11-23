use std::time::Duration;

/// Scheduling limits for one reconciliation worker.
#[derive(Clone, Debug)]
pub struct UpdatePolicy {
    /// Quiet period before preparation; zero disables debounce.
    pub debounce: Duration,
    /// Maximum time a burst may postpone starting preparation.
    pub max_coalesce_window: Duration,
    /// First retry delay.
    pub retry_initial: Duration,
    /// Upper bound on retry delays, applied after jitter.
    /// Jitter scales the capped exponential base by 80–120 percent, truncating
    /// less than one nanosecond, then clamps to `[1 ns, retry_max]`.
    pub retry_max: Duration,
    /// Retries after the initial attempt; `None` retries indefinitely.
    pub max_retry_attempts: Option<usize>,
}

/// Invalid scheduling durations or ordering.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error(
    "require nonzero coalescing window and retry delays, debounce <= window, initial <= maximum"
)]
pub struct InvalidUpdatePolicy;

impl Default for UpdatePolicy {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(100),
            max_coalesce_window: Duration::from_secs(1),
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(5),
            max_retry_attempts: None,
        }
    }
}

impl UpdatePolicy {
    pub(crate) fn validate(&self) -> Result<(), InvalidUpdatePolicy> {
        if self.max_coalesce_window.is_zero()
            || self.retry_initial.is_zero()
            || self.retry_initial > self.retry_max
            || self.debounce > self.max_coalesce_window
            || tokio::time::Instant::now()
                .checked_add(self.max_coalesce_window)
                .is_none()
            || tokio::time::Instant::now()
                .checked_add(self.retry_max)
                .is_none()
        {
            return Err(InvalidUpdatePolicy);
        }
        Ok(())
    }

    pub(crate) fn delay(&self, retries: usize, sequence: u64) -> Duration {
        let base = (0..retries.min(128)).fold(self.retry_initial, |delay, _| {
            delay.saturating_mul(2).min(self.retry_max)
        });
        // A local deterministic mixer spreads different sequences and attempts.
        let seed = sequence.wrapping_add(u64::try_from(retries).unwrap_or(u64::MAX));
        let mixed = seed.wrapping_mul(6_364_136_223_846_793_005).rotate_left(17);
        let percent = 80 + u32::try_from(mixed % 41).unwrap_or(0);
        // Scale the remainder too, preserving sub-100 ns inputs without an
        // overflowing multiplication of the full duration.
        let quotient = base / 100;
        let remainder = base - quotient * 100;
        quotient
            .saturating_mul(percent)
            .saturating_add(remainder * percent / 100)
            .min(self.retry_max)
            .max(Duration::from_nanos(1))
    }
}

#[cfg(test)]
mod tests {
    use super::UpdatePolicy;
    use std::time::Duration;

    #[test]
    fn exponential_delay_is_jittered_and_capped() {
        let policy = UpdatePolicy::default();
        for retry in 0_usize..30 {
            let base = policy
                .retry_initial
                .saturating_mul(1_u32 << retry)
                .min(policy.retry_max);
            for sequence in 1..100 {
                let delay = policy.delay(retry, sequence);
                assert!(delay >= base.mul_f64(0.8));
                assert!(delay <= base.mul_f64(1.2).min(policy.retry_max));
            }
        }
        assert!(policy.delay(usize::MAX, u64::MAX) <= Duration::from_secs(5));
    }
}

#[cfg(test)]
mod precision_tests {
    use super::UpdatePolicy;
    use std::time::Duration;

    #[test]
    fn small_non_round_delays_preserve_jitter_before_capping() {
        for nanos in [1, 7, 99, 101, 999, 1001] {
            let policy = UpdatePolicy {
                retry_initial: Duration::from_nanos(nanos),
                retry_max: Duration::from_nanos(nanos * 4),
                ..UpdatePolicy::default()
            };
            for sequence in 1..=100 {
                for retry in 0..=3 {
                    let base = u128::from(nanos * (1 << retry)).min(policy.retry_max.as_nanos());
                    let lower = (base * 80 / 100).max(1);
                    let upper = (base * 120 / 100).min(policy.retry_max.as_nanos()).max(1);
                    let delay = policy.delay(retry, sequence).as_nanos();
                    assert!(
                        (lower..=upper).contains(&delay),
                        "base={base}, delay={delay}"
                    );
                }
            }
        }
    }
}
