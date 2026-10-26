use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::{error::Error, path::PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Implementation {
    Netplane,
    Tower,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    Single,
    TwoBalanced,
    TwoSkewed,
    FourBalanced,
    EightBalanced,
    Publication,
    Retention,
}

impl Scenario {
    pub const fn backends(self) -> usize {
        match self {
            Self::Single | Self::Retention => 1,
            Self::TwoBalanced | Self::TwoSkewed | Self::Publication => 2,
            Self::FourBalanced => 4,
            Self::EightBalanced => 8,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Smoke,
    Standard,
    Soak,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Load {
    Closed,
    Open,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum RetentionMode {
    Paced,
    BurstShort,
    BurstLong,
}

#[derive(Debug, Parser)]
pub struct Cli {
    #[arg(long, value_enum, default_value_t = Profile::Smoke)]
    pub profile: Profile,
    #[arg(long, value_enum, default_value_t = Scenario::Single)]
    pub scenario: Scenario,
    #[arg(long, value_enum, default_value_t = Implementation::Netplane)]
    pub implementation: Implementation,
    #[arg(long, value_enum, default_value_t = Load::Closed)]
    pub load: Load,
    #[arg(long)]
    pub clients: Option<usize>,
    #[arg(long)]
    pub max_outstanding: Option<usize>,
    #[arg(long)]
    pub generator_capacity: Option<usize>,
    #[arg(long)]
    pub outer_in_flight: Option<usize>,
    #[arg(long)]
    pub outer_buffer: Option<usize>,
    #[arg(long)]
    pub backend_in_flight: Option<usize>,
    #[arg(long)]
    pub backend_buffer: Option<usize>,
    #[arg(long)]
    pub slow_in_flight: Option<usize>,
    #[arg(long, default_value_t = 0)]
    pub slow_ms: u64,
    #[arg(long)]
    pub offered_rps: Option<u64>,
    #[arg(long)]
    pub warmup_secs: Option<u64>,
    #[arg(long)]
    pub duration_secs: Option<u64>,
    #[arg(long)]
    pub publications: Option<usize>,
    #[arg(long, value_enum, default_value_t = RetentionMode::Paced)]
    pub retention_mode: RetentionMode,
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
    #[arg(long)]
    pub output: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Config {
    pub profile: Profile,
    pub scenario: Scenario,
    pub implementation: Implementation,
    pub load: Load,
    pub clients: usize,
    pub max_outstanding: usize,
    pub generator_capacity: usize,
    pub outer_in_flight: usize,
    pub outer_buffer: usize,
    pub backend_in_flight: usize,
    pub backend_buffer: usize,
    pub slow_in_flight: usize,
    pub slow_ms: u64,
    pub offered_rps: Option<u64>,
    pub warmup_secs: u64,
    pub duration_secs: u64,
    pub publications: usize,
    pub retention_mode: RetentionMode,
    pub seed: u64,
}

impl Cli {
    pub fn validate(&self) -> Result<Config, Box<dyn Error + Send + Sync>> {
        let (clients, warmup, duration, publications) = match self.profile {
            Profile::Smoke => (2, 0, 1, 10),
            Profile::Standard => (16, 3, 15, 100),
            Profile::Soak => (32, 10, 120, 1000),
        };
        let max_outstanding = self.max_outstanding.unwrap_or(128);
        let config = Config {
            profile: self.profile,
            scenario: self.scenario,
            implementation: self.implementation,
            load: self.load,
            clients: self.clients.unwrap_or(clients),
            max_outstanding,
            generator_capacity: self
                .generator_capacity
                .unwrap_or_else(|| max_outstanding.saturating_mul(2)),
            outer_in_flight: self.outer_in_flight.unwrap_or(32),
            outer_buffer: self.outer_buffer.unwrap_or(64),
            backend_in_flight: self.backend_in_flight.unwrap_or(8),
            backend_buffer: self.backend_buffer.unwrap_or(16),
            slow_in_flight: self.slow_in_flight.unwrap_or(1),
            slow_ms: self.slow_ms,
            offered_rps: self.offered_rps,
            warmup_secs: self.warmup_secs.unwrap_or(warmup),
            duration_secs: self.duration_secs.unwrap_or(duration),
            publications: self.publications.unwrap_or(publications),
            retention_mode: self.retention_mode,
            seed: self.seed,
        };
        if config.clients == 0
            || config.max_outstanding == 0
            || config.generator_capacity == 0
            || config.outer_in_flight == 0
            || config.outer_buffer == 0
            || config.backend_in_flight == 0
            || config.backend_buffer == 0
            || config.slow_in_flight == 0
            || config.duration_secs == 0
            || config.publications > 1_000_000
            || config.duration_secs > 3600
            || config.clients > 4096
            || config.max_outstanding > 1_000_000
            || config.generator_capacity > 1_000_000
        {
            return Err("capacities, clients, and duration must be positive and bounded".into());
        }
        if config.load == Load::Open && !matches!(config.offered_rps, Some(1..=1_000_000)) {
            return Err("open load requires --offered-rps in 1..=1000000".into());
        }
        if config.scenario == Scenario::Retention {
            if config.implementation != Implementation::Netplane {
                return Err("retention requires netplane implementation".into());
            }
            if config.publications == 0 {
                return Err("retention requires a positive publication count".into());
            }
        }
        if config.scenario == Scenario::Publication
            && config.implementation != Implementation::Netplane
        {
            return Err("publication requires netplane implementation".into());
        }
        Ok(config)
    }
}

pub fn route(sequence: u64, seed: u64, scenario: Scenario) -> usize {
    let mut value = sequence
        .wrapping_add(seed)
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    if scenario == Scenario::TwoSkewed {
        usize::from(value % 100 >= 95)
    } else {
        usize::try_from(value % scenario.backends() as u64).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_is_reproducible_and_skewed() {
        let a: Vec<_> = (0..10_000)
            .map(|i| route(i, 42, Scenario::TwoSkewed))
            .collect();
        let b: Vec<_> = (0..10_000)
            .map(|i| route(i, 42, Scenario::TwoSkewed))
            .collect();
        assert_eq!(a, b);
        assert!((400..600).contains(&a.iter().filter(|&&i| i == 1).count()));
    }
}
