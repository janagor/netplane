//! CLI and run configuration for the single-handoff experiment.

use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::{error::Error, path::PathBuf};

/// Compared execution topologies. Production runtime code is unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Implementation {
    /// One Tower buffer + concurrency limit, no routing.
    TowerBounded,
    /// Outer + backend Tower buffers (mirrors `GraphBuilder` topology).
    TowerDoubleBuffered,
    /// Current Netplane `GraphBuilder` path (admission + two handoffs).
    GraphBuilder,
    /// Experimental direct route into backend `Endpoint`s (one handoff).
    SingleHandoff,
    /// Current Netplane `GraphDraft` path (admission + one handoff).
    GraphDraft,
}

/// Service cost model applied inside each backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Workload {
    /// Immediately ready `u64` increment.
    Immediate,
    /// Sleep for `--work-us` microseconds before completing.
    Delayed,
}

#[derive(Debug, Parser)]
pub struct Cli {
    #[arg(long, value_enum)]
    pub implementation: Implementation,
    #[arg(long, value_enum, default_value_t = Workload::Immediate)]
    pub workload: Workload,
    #[arg(long, default_value_t = 1)]
    pub callers: usize,
    #[arg(long, default_value_t = 32_768)]
    pub samples: usize,
    #[arg(long, default_value_t = 1_024)]
    pub warmup: usize,
    /// Simulated service delay for `delayed` workload, in microseconds.
    #[arg(long, default_value_t = 100)]
    pub work_us: u64,
    #[arg(long, default_value_t = 64)]
    pub max_in_flight: usize,
    #[arg(long, default_value_t = 128)]
    pub buffer_capacity: usize,
    #[arg(long, default_value_t = 256)]
    pub max_outstanding: usize,
    #[arg(long, default_value_t = 1)]
    pub routes: usize,
    /// Explicit paired-run identifier written into the JSON report.
    #[arg(long, default_value_t = 1)]
    pub repetition: u32,
    #[arg(long)]
    pub output: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Config {
    pub implementation: Implementation,
    pub workload: Workload,
    pub callers: usize,
    pub samples: usize,
    pub warmup: usize,
    pub work_us: u64,
    pub max_in_flight: usize,
    pub buffer_capacity: usize,
    pub max_outstanding: usize,
    pub routes: usize,
    pub repetition: u32,
    #[serde(skip)]
    pub output: Option<PathBuf>,
}

impl Cli {
    pub fn validate(self) -> Result<Config, Box<dyn Error + Send + Sync>> {
        let config = Config {
            implementation: self.implementation,
            workload: self.workload,
            callers: self.callers,
            samples: self.samples,
            warmup: self.warmup,
            work_us: self.work_us,
            max_in_flight: self.max_in_flight,
            buffer_capacity: self.buffer_capacity,
            max_outstanding: self.max_outstanding,
            routes: self.routes,
            repetition: self.repetition,
            output: self.output,
        };
        if !(1..=32).contains(&config.callers) {
            return Err("callers must be 1..=32".into());
        }
        if !(1..=2_000_000).contains(&config.samples) {
            return Err("samples must be 1..=2000000".into());
        }
        if !config.samples.is_multiple_of(config.callers) {
            return Err("samples must be divisible by callers".into());
        }
        if config.warmup > 1_000_000
            || config.max_in_flight == 0
            || config.buffer_capacity == 0
            || config.max_outstanding == 0
            || config.routes == 0
            || config.routes > 64
            || config.repetition == 0
            || (config.workload == Workload::Delayed && config.work_us == 0)
        {
            return Err(
                "capacities, routes, repetition, and delayed work_us must be positive and bounded"
                    .into(),
            );
        }
        Ok(config)
    }
}
