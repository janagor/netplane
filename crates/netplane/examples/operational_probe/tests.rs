use super::*;
use metrics::{HISTOGRAM_HIGH_NS, HISTOGRAM_SIG_FIGS, Latency, Metrics};
use workload::DRAIN_SECS;

fn config(args: &[&str]) -> Result<Config, ProbeError> {
    Ok(Cli::try_parse_from(args)?.validate()?)
}

#[tokio::test]
async fn fixtures_route_to_same_backend() -> Result<(), ProbeError> {
    for implementation in [Implementation::Netplane, Implementation::Tower] {
        let option = match implementation {
            Implementation::Netplane => "netplane",
            Implementation::Tower => "tower",
        };
        let config = config(&[
            "probe",
            "--scenario",
            "two-balanced",
            "--implementation",
            option,
        ])?;
        let (execute, controller) = call(&config)?;
        for sequence in 0..100 {
            let request = fixture::request(sequence, &config);
            match execute(request) {
                workload::Admit::Ready(future) => {
                    assert_eq!(future.await?.0, request.backend);
                }
                other => panic!("unexpected admit result: {other:?}"),
            }
        }
        if let Some(controller) = controller {
            controller.shutdown(Duration::from_secs(1)).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn admission_is_nonblocking_for_both_fixtures() -> Result<(), ProbeError> {
    for implementation in ["netplane", "tower"] {
        let config = config(&[
            "probe",
            "--max-outstanding",
            "1",
            "--implementation",
            implementation,
        ])?;
        let (execute, controller) = call(&config)?;
        let held = match execute(fixture::request(0, &config)) {
            workload::Admit::Ready(future) => future,
            other => panic!("expected ready admit, got {other:?}"),
        };
        assert!(matches!(
            execute(fixture::request(1, &config)),
            workload::Admit::AtCapacity
        ));
        drop(held);
        match execute(fixture::request(2, &config)) {
            workload::Admit::Ready(future) => assert!(future.await.is_ok()),
            other => panic!("expected ready admit, got {other:?}"),
        }
        if let Some(controller) = controller {
            controller.shutdown(Duration::from_secs(1)).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn open_overload_accounts_for_rejections() -> Result<(), ProbeError> {
    let config = config(&[
        "probe",
        "--scenario",
        "two-balanced",
        "--load",
        "open",
        "--offered-rps",
        "1000",
        "--max-outstanding",
        "1",
        "--generator-capacity",
        "2",
        "--slow-ms",
        "5",
    ])?;
    let report = run(config).await?;
    assert!(report.metrics.rejected_at_capacity > 0);
    assert_eq!(
        report.metrics.attempted,
        report.metrics.admitted
            + report.metrics.rejected_at_capacity
            + report.metrics.admission_other_errors
    );
    assert_eq!(
        report.metrics.admitted,
        report.metrics.completed + report.metrics.runtime_errors + report.metrics.drain_cancelled
    );
    assert!(report.metrics.nominal_arrivals.is_some());
    assert!(report.metrics.scheduled_arrivals.is_some());
    assert!(report.metrics.generated_attempts.is_some());
    assert_eq!(
        report.metrics.latency_samples + report.metrics.missing_latency_samples,
        report.metrics.completed
    );
    Ok(())
}

#[tokio::test]
async fn latency_includes_post_window_completions() -> Result<(), ProbeError> {
    let config = config(&[
        "probe",
        "--scenario",
        "two-balanced",
        "--clients",
        "8",
        "--duration-secs",
        "1",
        "--warmup-secs",
        "0",
        "--slow-ms",
        "80",
        "--slow-in-flight",
        "8",
        "--max-outstanding",
        "32",
    ])?;
    let report = run(config).await?;
    assert!(
        report.metrics.completed_after_window > 0,
        "expected drain completions after the measurement window"
    );
    assert_eq!(
        report.metrics.completed_in_window + report.metrics.completed_after_window,
        report.metrics.completed
    );
    assert_eq!(report.metrics.latency_samples, report.metrics.completed);
    assert!(report.metrics.latency_ns.p99.is_some());
    Ok(())
}

#[tokio::test]
async fn closed_loop_runs_longer_than_drain_timeout() -> Result<(), ProbeError> {
    let duration = DRAIN_SECS + 2;
    let config = config(&[
        "probe",
        "--scenario",
        "single",
        "--clients",
        "2",
        "--duration-secs",
        &duration.to_string(),
        "--warmup-secs",
        "0",
    ])?;
    let started = Instant::now();
    let report = run(config).await?;
    let wall = started.elapsed();
    assert!(
        wall >= Duration::from_secs(duration),
        "expected at least {duration}s of work, wall={wall:?}"
    );
    assert!(report.metrics.completed_in_window > 0);
    assert_eq!(report.metrics.latency_samples, report.metrics.completed);
    assert!(report.metrics.measurement_complete);
    assert_eq!(report.metrics.drain_cancelled, 0);
    Ok(())
}

#[tokio::test]
async fn forced_drain_timeout_reports_incomplete_measurement() -> Result<(), ProbeError> {
    // Stall longer than the drain budget so callers are aborted, but short enough
    // that orphaned buffer workers do not keep the test runtime alive for minutes.
    let stall_ms = (DRAIN_SECS + 1) * 1000;
    let config = config(&[
        "probe",
        "--scenario",
        "two-balanced",
        "--clients",
        "2",
        "--duration-secs",
        "1",
        "--warmup-secs",
        "0",
        "--slow-ms",
        &stall_ms.to_string(),
        "--slow-in-flight",
        "2",
        "--max-outstanding",
        "2",
        "--backend-in-flight",
        "2",
    ])?;
    let report = run(config).await?;
    assert!(
        report.metrics.drain_cancelled > 0,
        "expected drain cancellations for stalled backends"
    );
    assert!(!report.metrics.measurement_complete);
    assert_eq!(
        report.metrics.latency_samples + report.metrics.missing_latency_samples,
        report.metrics.completed
    );
    Ok(())
}

#[tokio::test]
async fn publication_zero_is_a_baseline() -> Result<(), ProbeError> {
    let config = config(&[
        "probe",
        "--scenario",
        "publication",
        "--publications",
        "0",
        "--duration-secs",
        "1",
        "--warmup-secs",
        "0",
        "--clients",
        "2",
    ])?;
    let report = run(config).await?;
    assert_eq!(report.metrics.requested_publications, 0);
    assert_eq!(report.metrics.successful_publications, 0);
    assert_eq!(report.metrics.publication_errors, 0);
    Ok(())
}

#[tokio::test]
async fn publication_schedules_all_requested() -> Result<(), ProbeError> {
    let config = config(&[
        "probe",
        "--scenario",
        "publication",
        "--profile",
        "smoke",
        "--publications",
        "10",
        "--duration-secs",
        "1",
        "--warmup-secs",
        "0",
        "--clients",
        "2",
    ])?;
    let report = run(config).await?;
    assert_eq!(report.metrics.requested_publications, 10);
    assert_eq!(report.metrics.successful_publications, 10);
    Ok(())
}

#[tokio::test]
async fn retention_reports_cancelled_not_throughput() -> Result<(), ProbeError> {
    let config = config(&[
        "probe",
        "--scenario",
        "retention",
        "--publications",
        "5",
        "--retention-mode",
        "paced",
        "--max-outstanding",
        "8",
    ])?;
    let report = run(config).await?;
    assert!(report.metrics.throughput_rps.is_none());
    assert!(report.metrics.publication_rate.is_some());
    assert_eq!(report.metrics.cancelled_requests, 5);
    assert_eq!(report.metrics.admitted, 5);
    assert_eq!(report.metrics.completed, 0);
    assert_eq!(report.metrics.successful_publications, 5);
    assert_eq!(report.metrics.peak_outstanding, 1);
    Ok(())
}

#[test]
fn histogram_records_without_unbounded_growth() -> Result<(), ProbeError> {
    let mut hist = metrics::histogram()?;
    for nanos in [1_000_u64, 2_000, 3_000, 10_000, HISTOGRAM_HIGH_NS + 1] {
        metrics::record(&mut hist, nanos);
    }
    assert_eq!(hist.len(), 5);
    let latency = percentile(&hist);
    assert!(latency.p50.is_some());
    assert_eq!(HISTOGRAM_SIG_FIGS, 3);
    Ok(())
}

#[test]
fn json_invariants_reject_bad_counts() -> Result<(), ProbeError> {
    let config = config(&["probe"])?;
    let report = Report {
        schema_version: 2,
        implementation: config.implementation,
        scenario: config.scenario,
        config,
        environment: environment(),
        metrics: Metrics {
            attempted: 0,
            admitted: 0,
            completed: 1,
            completed_in_window: 1,
            completed_after_window: 0,
            drain_cancelled: 0,
            cancelled_requests: 0,
            rejected_at_capacity: 0,
            admission_other_errors: 0,
            client_dropped: 0,
            runtime_errors: 0,
            throughput_rps: Some(1.0),
            publication_rate: None,
            latency_ns: Latency {
                p50: Some(1000),
                p95: Some(1000),
                p99: Some(1000),
            },
            latency_us: Latency {
                p50: Some(1),
                p95: Some(1),
                p99: Some(1),
            },
            latency_samples: 0,
            missing_latency_samples: 0,
            measurement_complete: true,
            completed_by_backend: vec![1],
            peak_outstanding: 1,
            requested_publications: 0,
            successful_publications: 0,
            publication_errors: 0,
            publications_after_window: 0,
            publication_latency_us: Latency::empty(),
            candidate_build_latency_us: Latency::empty(),
            nominal_arrivals: None,
            scheduled_arrivals: None,
            generated_attempts: None,
            offered_rps_achieved: None,
            histogram_significant_figures: HISTOGRAM_SIG_FIGS,
            histogram_max_ns: HISTOGRAM_HIGH_NS,
            peak_rss_bytes: None,
            peak_live_resources: None,
            final_live_resources: None,
        },
    };
    assert!(report.validate().is_err());
    Ok(())
}
