use std::{fmt, net::SocketAddr, time::Duration};

use netplane::{Capacity, RuntimeLimits};
use netplane_http::HttpLimits;

#[derive(Clone, Debug)]
pub struct BackendBehavior {
    pub delay: Duration,
    pub fail: bool,
}

#[derive(Clone, Debug)]
pub struct PolicyConfig {
    pub revision: u64,
    pub canary_percent: u8,
    pub stable: BackendBehavior,
    pub canary: BackendBehavior,
}

#[derive(Debug, PartialEq, Eq)]
pub enum InvalidPolicy {
    ZeroRevision,
    CanaryPercent(u8),
}

impl fmt::Display for InvalidPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRevision => f.write_str("policy revision must be positive"),
            Self::CanaryPercent(value) => write!(f, "canary percentage {value} exceeds 100"),
        }
    }
}

impl std::error::Error for InvalidPolicy {}

impl PolicyConfig {
    pub const fn validate(&self) -> Result<(), InvalidPolicy> {
        if self.revision == 0 {
            return Err(InvalidPolicy::ZeroRevision);
        }
        if self.canary_percent > 100 {
            return Err(InvalidPolicy::CanaryPercent(self.canary_percent));
        }
        Ok(())
    }
}

pub struct GatewayConfig {
    pub listen: SocketAddr,
    pub policy: PolicyConfig,
    pub outer_capacity: Capacity,
    pub backend_capacity: Capacity,
    pub runtime_limits: RuntimeLimits,
    pub http_limits: HttpLimits,
    pub shutdown_grace_period: Duration,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        let capacity = Capacity {
            max_in_flight: 32,
            buffer_capacity: 64,
        };
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 3001)),
            policy: PolicyConfig {
                revision: 1,
                canary_percent: 25,
                stable: BackendBehavior {
                    delay: Duration::ZERO,
                    fail: false,
                },
                canary: BackendBehavior {
                    delay: Duration::from_millis(25),
                    fail: false,
                },
            },
            outer_capacity: capacity,
            backend_capacity: capacity,
            runtime_limits: RuntimeLimits {
                max_outstanding: 128,
            },
            http_limits: HttpLimits::default(),
            shutdown_grace_period: Duration::from_secs(10),
        }
    }
}
