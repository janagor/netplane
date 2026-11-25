//! HTTP observations composed with transport-independent health tracking.

use std::{collections::BTreeMap, future::Future, pin::Pin};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use netplane::health::{HealthOutcome, HealthProbe, OutcomeClassifier, ProbeIdentity};
use tower::BoxError;

use crate::upstream::{HttpUpstream, UpstreamError};

/// Configurable HTTP observations; `Default` uses application-traffic semantics.
///
/// Only upstream transport failures and exchange timeouts count as errors.
/// Response size limits, runtime admission, shutdown and queue errors are
/// ignored. Wrap the actual upstream service before Netplane's bounded queues.
#[derive(Clone, Debug, Default)]
pub struct HttpOutcomeClassifier {
    overrides: BTreeMap<StatusCode, HealthOutcome>,
    fail_non_success: bool,
}

impl HttpOutcomeClassifier {
    /// Ordinary application responses: 2xx succeeds, 5xx fails, others are ignored.
    #[must_use]
    pub fn passive_default() -> Self {
        Self::default()
    }

    /// Dedicated health endpoints must return 2xx; every other status fails.
    /// Explicit status overrides still take precedence.
    #[must_use]
    pub const fn probe_default() -> Self {
        Self {
            overrides: BTreeMap::new(),
            fail_non_success: true,
        }
    }

    /// Override one status without changing other status or error classifications.
    #[must_use]
    pub fn with_status(mut self, status: StatusCode, outcome: HealthOutcome) -> Self {
        self.overrides.insert(status, outcome);
        self
    }

    fn response<B>(&self, response: &Response<B>) -> HealthOutcome {
        self.overrides
            .get(&response.status())
            .copied()
            .unwrap_or_else(|| {
                if response.status().is_success() {
                    HealthOutcome::Success
                } else if self.fail_non_success || response.status().is_server_error() {
                    HealthOutcome::Failure
                } else {
                    HealthOutcome::Ignored
                }
            })
    }

    fn error(mut error: &(dyn std::error::Error + 'static)) -> HealthOutcome {
        loop {
            if let Some(upstream) = error.downcast_ref::<UpstreamError>() {
                return match upstream {
                    UpstreamError::Transport(_) | UpstreamError::Timeout => HealthOutcome::Failure,
                    UpstreamError::InvalidRequest | UpstreamError::ResponseTooLarge => {
                        HealthOutcome::Ignored
                    }
                };
            }
            let Some(source) = error.source() else {
                return HealthOutcome::Ignored;
            };
            error = source;
        }
    }
}

impl<B> OutcomeClassifier<Response<B>, UpstreamError> for HttpOutcomeClassifier {
    fn classify(&self, result: Result<&Response<B>, &UpstreamError>) -> HealthOutcome {
        result.map_or_else(
            |error| Self::error(error),
            |response| self.response(response),
        )
    }
}

impl<B> OutcomeClassifier<Response<B>, BoxError> for HttpOutcomeClassifier {
    fn classify(&self, result: Result<&Response<B>, &BoxError>) -> HealthOutcome {
        result.map_or_else(
            |error| Self::error(error.as_ref()),
            |response| self.response(response),
        )
    }
}

/// GET a fixed health path through a bounded upstream client.
///
/// The scheduler supplies the probe deadline and independent concurrency budget.
/// The upstream also retains its configured exchange timeout and response bound.
/// This probe shares only the client supplied by the application.
#[derive(Clone)]
pub struct HttpHealthProbe {
    upstream: HttpUpstream,
    path: http::Uri,
    classifier: HttpOutcomeClassifier,
    transport_key: Option<String>,
}

impl HttpHealthProbe {
    /// Validate an origin-form path before registering a probe.
    ///
    /// # Errors
    /// Returns an error for invalid paths or an absolute URI.
    pub fn new(upstream: HttpUpstream, path: &str) -> Result<Self, BoxError> {
        let path: http::Uri = path.parse()?;
        if path.scheme().is_some() || path.authority().is_some() || !path.path().starts_with('/') {
            return Err("health probe requires an origin-form path".into());
        }
        Ok(Self {
            upstream,
            path,
            classifier: HttpOutcomeClassifier::probe_default(),
            transport_key: None,
        })
    }

    /// Declare compatibility of separately constructed clients with the same key.
    ///
    /// Include every client setting that affects checks (timeouts, redirects,
    /// retries, proxies, protocols and TLS). The default conservatively shares
    /// only clones of the same upstream, because reqwest clients are opaque.
    /// Origin, path, response bound and classifier are always included separately.
    #[must_use]
    pub fn with_transport_key(mut self, key: impl Into<String>) -> Self {
        self.transport_key = Some(key.into());
        self
    }

    /// Apply application-specific status classification to active checks.
    #[must_use]
    pub fn with_classifier(mut self, classifier: HttpOutcomeClassifier) -> Self {
        self.classifier = classifier;
        self
    }
}

impl HealthProbe for HttpHealthProbe {
    fn identity(&self) -> ProbeIdentity {
        let (origin, bound, opaque) = self.upstream.probe_configuration();
        ProbeIdentity {
            configuration_key: format!(
                "http-probe-v1:{:?}",
                (
                    origin,
                    bound,
                    self.transport_key.as_deref().unwrap_or(&opaque),
                    self.path.to_string(),
                    &self.classifier.overrides,
                    self.classifier.fail_non_success
                )
            ),
        }
    }
    type Future = Pin<Box<dyn Future<Output = HealthOutcome> + Send>>;

    fn check(&self) -> Self::Future {
        let probe = self.clone();
        Box::pin(async move {
            let mut request = Request::new(Bytes::new());
            *request.uri_mut() = probe.path;
            let response = probe.upstream.execute(request).await;
            probe.classifier.classify(response.as_ref())
        })
    }
}
