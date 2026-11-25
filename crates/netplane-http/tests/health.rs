//! HTTP health classification and bounded probe behavior.

use bytes::Bytes;
use http::{Response, StatusCode};
use netplane::health::{HealthOutcome, OutcomeClassifier};
use netplane_http::{health::HttpOutcomeClassifier, upstream::UpstreamError};

#[test]
fn classification_distinguishes_backend_failure_from_application_and_runtime_errors() {
    let classifier = HttpOutcomeClassifier::default();
    for (status, expected) in [
        (StatusCode::OK, HealthOutcome::Success),
        (StatusCode::FOUND, HealthOutcome::Ignored),
        (StatusCode::BAD_REQUEST, HealthOutcome::Ignored),
        (StatusCode::INTERNAL_SERVER_ERROR, HealthOutcome::Failure),
    ] {
        let mut response = Response::new(Bytes::new());
        *response.status_mut() = status;
        assert_eq!(
            classifier.classify(Ok::<_, &UpstreamError>(&response)),
            expected
        );
    }
    assert_eq!(
        classifier.classify(Err::<&Response<Bytes>, _>(&UpstreamError::Timeout)),
        HealthOutcome::Failure
    );
    assert_eq!(
        classifier.classify(Err::<&Response<Bytes>, _>(&UpstreamError::InvalidRequest)),
        HealthOutcome::Ignored
    );
    assert_eq!(
        classifier.classify(Err::<&Response<Bytes>, _>(&UpstreamError::ResponseTooLarge)),
        HealthOutcome::Ignored
    );
    let rejection: tower::BoxError = netplane::AdmissionError::Closed.into();
    assert_eq!(
        classifier.classify(Err::<&Response<Bytes>, _>(&rejection)),
        HealthOutcome::Ignored
    );
}

#[test]
fn application_specific_status_can_override_default_failure() {
    let classifier = HttpOutcomeClassifier::default()
        .with_status(StatusCode::SERVICE_UNAVAILABLE, HealthOutcome::Ignored);
    let mut response = Response::new(Bytes::new());
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    assert_eq!(
        classifier.classify(Ok::<_, &UpstreamError>(&response)),
        HealthOutcome::Ignored
    );
}

#[tokio::test]
async fn default_adapter_translates_no_eligible_backend_through_tower_error_sources()
-> Result<(), tower::BoxError> {
    let graph = netplane::GraphDraft::new(tower::service_fn(|_: http::Request<Bytes>| async {
        Err::<Response<Bytes>, _>(netplane::routing::RoutingError::NoEligibleBackends)
    }))
    .validate()?;
    let mut runtime =
        netplane::Netplane::new(graph, netplane::RuntimeLimits { max_outstanding: 1 })?;
    let adapter = netplane_http::HttpAdapter::identity(
        runtime.handle(),
        netplane_http::HttpLimits::default(),
    )?;
    let response = adapter
        .execute(http::Request::new(axum::body::Body::empty()))
        .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    runtime.shutdown(std::time::Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn probe_compatibility_includes_path_classifier_and_transport() -> Result<(), tower::BoxError>
{
    use netplane::health::{
        ActiveHealthConfig, BackendIdentity, HealthMode, HealthPolicy, HealthProbe, HealthRegistry,
        HealthScheduler, ProbeError,
    };
    use netplane_http::{
        health::HttpHealthProbe,
        upstream::{self, HttpUpstream},
    };
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        time::Duration,
    };
    let scheduler = HealthScheduler::new(ActiveHealthConfig {
        interval: Duration::from_secs(1),
        timeout: Duration::from_secs(1),
        max_concurrent_checks: NonZeroUsize::MIN,
        max_registered_backends: NonZeroUsize::MIN,
    })?;
    let handle = HealthRegistry::default().register_with_mode(
        BackendIdentity {
            service: "s".into(),
            instance: "a".into(),
            incarnation: "pod".into(),
        },
        HealthPolicy {
            failure_threshold: NonZeroU32::MIN,
            recovery_threshold: NonZeroU32::MIN,
            recovery_delay: Duration::ZERO,
        },
        HealthMode::Active,
    )?;
    let upstream = HttpUpstream::new(
        upstream::client(Duration::from_secs(2))?,
        "http://127.0.0.1:1",
        100,
    )?;
    let original = HttpHealthProbe::new(upstream.clone(), "/health")?;
    let registration = scheduler
        .handle()
        .register(handle.clone(), original.clone())?;
    let compatible = scheduler
        .handle()
        .register(handle.clone(), original.clone())?;
    for incompatible in [
        HttpHealthProbe::new(upstream.clone(), "/ready")?,
        original.clone().with_classifier(
            HttpOutcomeClassifier::default()
                .with_status(StatusCode::SERVICE_UNAVAILABLE, HealthOutcome::Ignored),
        ),
        original.clone().with_transport_key("different-timeout"),
    ] {
        assert_ne!(incompatible.identity(), original.identity());
        assert!(matches!(
            scheduler.handle().register(handle.clone(), incompatible),
            Err(ProbeError::IncompatibleProbeConfiguration)
        ));
    }
    // One surviving graph lease still protects the old effective configuration.
    drop(registration);
    assert!(matches!(
        scheduler.handle().register(
            handle.clone(),
            HttpHealthProbe::new(upstream.clone(), "/ready")?
        ),
        Err(ProbeError::IncompatibleProbeConfiguration)
    ));
    drop(compatible);
    let _replacement = scheduler
        .handle()
        .register(handle, HttpHealthProbe::new(upstream, "/ready")?)?;
    Ok(())
}

#[test]
fn active_health_status_defaults_are_stricter_and_overrides_remain_explicit() {
    let passive = HttpOutcomeClassifier::passive_default();
    let active = HttpOutcomeClassifier::probe_default();
    for status in [
        StatusCode::OK,
        StatusCode::FOUND,
        StatusCode::NOT_FOUND,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let mut response = Response::new(Bytes::new());
        *response.status_mut() = status;
        assert_eq!(
            active.classify(Ok::<_, &UpstreamError>(&response)),
            if status.is_success() {
                HealthOutcome::Success
            } else {
                HealthOutcome::Failure
            }
        );
        if status.is_client_error() || status.is_redirection() {
            assert_eq!(
                passive.classify(Ok::<_, &UpstreamError>(&response)),
                HealthOutcome::Ignored
            );
        }
    }
    let mut missing = Response::new(Bytes::new());
    *missing.status_mut() = StatusCode::NOT_FOUND;
    assert_eq!(
        active
            .with_status(StatusCode::NOT_FOUND, HealthOutcome::Ignored)
            .classify(Ok::<_, &UpstreamError>(&missing)),
        HealthOutcome::Ignored
    );
}
