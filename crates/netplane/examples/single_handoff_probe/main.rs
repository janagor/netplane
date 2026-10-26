//! Paired comparison probe for the single-handoff router experiment.
//!
//! Compares production buffered and backend-only `GraphBuilder` policies with
//! Tower, `GraphDraft`, and the earlier single-handoff fixture.
//!
//! See `docs/architecture/routed-execution.md` for capacity differences.

mod config;
mod fixtures;
mod measure;

use clap::Parser;
use config::Cli;
use fixtures::ProbeError;
use measure::RunMetrics;
use serde::Serialize;
use std::fs;

#[derive(Serialize)]
struct Environment {
    git_sha: Option<String>,
    rustc: Option<String>,
    target: String,
    os: String,
    cpu_model: Option<String>,
    tokio_workers: usize,
}

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    experiment: &'static str,
    config: config::Config,
    environment: Environment,
    capacity_notes: &'static str,
    metrics: RunMetrics,
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn environment(tokio_workers: usize) -> Environment {
    let cpu_model = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("model name\t:")
                    .map(str::trim)
                    .map(str::to_owned)
            })
        });
    Environment {
        git_sha: command_output("git", &["rev-parse", "HEAD"]),
        rustc: command_output("rustc", &["--version"]),
        target: command_output("rustc", &["-vV"])
            .and_then(|details| {
                details
                    .lines()
                    .find_map(|line| line.strip_prefix("host: ").map(str::to_owned))
            })
            .unwrap_or_else(|| std::env::consts::ARCH.to_owned()),
        os: std::env::consts::OS.to_owned(),
        cpu_model,
        tokio_workers,
    }
}

fn main() -> Result<(), ProbeError> {
    let config = Cli::parse().validate()?;
    let output = config.output.clone();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.tokio_workers)
        .enable_all()
        .build()?;
    let report = runtime.block_on(async {
        let call = fixtures::build(&config)?;
        measure::warm(&call, config.warmup).await?;
        let metrics = measure::measure(&config, &call).await?;
        Ok::<_, ProbeError>(Report {
            schema_version: 3,
            experiment: "single_handoff_router",
            environment: environment(config.tokio_workers),
            config,
            capacity_notes: "graph_builder is double-buffered; backend_only and single_handoff omit aggregate per-version capacity while preserving backend and global outstanding limits",
            metrics,
        })
    })?;
    let body = serde_json::to_string_pretty(&report)?;
    match output {
        Some(path) => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(path, body)?;
        }
        None => println!("{body}"),
    }
    Ok(())
}
