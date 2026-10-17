//! Internal request observation and publication identity.

use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tower::BoxError;
use tracing::Dispatch;

use crate::{DeadlineExceeded, PublishError, VersionId};

pub fn trace_publish_rejection(runtime_id: u64, error: &PublishError) {
    match error {
        PublishError::ShuttingDown => {
            tracing::debug!(target: "netplane", event = "version.publish_rejected", runtime_id, reason = "shutting_down");
        }
        PublishError::GenerationExhausted => {
            tracing::warn!(target: "netplane", event = "version.publish_rejected", runtime_id, reason = "generation_exhausted");
        }
    }
}

pub fn trace_publication(
    runtime_id: u64,
    previous: VersionId,
    published: VersionId,
    endpoint_ids: &[u64],
) {
    let old_version = previous.get();
    let new_version = published.get();
    tracing::info!(target: "netplane", event = "version.published", runtime_id, old_version, new_version);
    tracing::debug!(target: "netplane", event = "version.replaced", runtime_id, old_version, new_version);
    for &endpoint_id in endpoint_ids {
        tracing::debug!(target: "netplane", event = "version.endpoint", runtime_id, version_id = new_version, endpoint_id);
    }
}

const INITIAL_RUNTIME_ID: u64 = 1;
static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(INITIAL_RUNTIME_ID);

pub fn next_runtime_id() -> u64 {
    NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed)
}

pin_project! {
    pub(crate) struct ObservedExecution<F, R> {
        #[pin]
        inner: F,
        observation: RequestObservation,
        retained: Option<R>,
    }
}

pub struct RequestObservation {
    span: tracing::Span,
    dispatch: Dispatch,
    runtime_id: u64,
    version_id: u64,
    request_id: u64,
    accepted_at: tokio::time::Instant,
    finished: bool,
}

impl RequestObservation {
    pub fn new(runtime_id: u64, version_id: u64, request_id: u64) -> Self {
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let span = tracing::debug_span!(target: "netplane", "netplane.request", runtime_id, version_id, request_id);
        tracing::debug!(target: "netplane", event = "request.admitted", runtime_id, version_id, request_id);
        Self {
            span,
            dispatch,
            runtime_id,
            version_id,
            request_id,
            accepted_at: tokio::time::Instant::now(),
            finished: false,
        }
    }

    fn finish(&mut self, error: Option<&BoxError>) {
        self.finished = true;
        let duration_us = u64::try_from(self.accepted_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        let runtime_id = self.runtime_id;
        let version_id = self.version_id;
        let request_id = self.request_id;
        tracing::dispatcher::with_default(&self.dispatch, || {
            let _entered = self.span.enter();
            if let Some(error) = error {
                let error_kind = if error.is::<DeadlineExceeded>() {
                    "deadline_exceeded"
                } else if error.is::<tower::buffer::error::Closed>() {
                    "buffer_closed"
                } else {
                    "service_error"
                };
                tracing::warn!(target: "netplane", event = "request.failed", runtime_id, version_id, request_id, duration_us, error_kind);
            } else {
                tracing::debug!(target: "netplane", event = "request.completed", runtime_id, version_id, request_id, duration_us);
            }
        });
    }
}

impl Drop for RequestObservation {
    fn drop(&mut self) {
        if !self.finished {
            let runtime_id = self.runtime_id;
            let version_id = self.version_id;
            let request_id = self.request_id;
            let duration_us =
                u64::try_from(self.accepted_at.elapsed().as_micros()).unwrap_or(u64::MAX);
            tracing::dispatcher::with_default(&self.dispatch, || {
                let _entered = self.span.enter();
                tracing::debug!(target: "netplane", event = "request.cancelled", runtime_id, version_id, request_id, duration_us);
            });
        }
    }
}

impl<F, R, T> Future for ObservedExecution<F, R>
where
    F: Future<Output = Result<T, BoxError>>,
{
    type Output = Result<T, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let span = this.observation.span.clone();
        let result = tracing::dispatcher::with_default(&this.observation.dispatch, || {
            let _entered = span.enter();
            this.inner.poll(cx)
        });
        match result {
            Poll::Ready(result) => {
                this.observation.finish(result.as_ref().err());
                this.retained.take();
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub const fn observe<F, R>(
    inner: F,
    observation: RequestObservation,
    retained: R,
) -> ObservedExecution<F, R> {
    ObservedExecution {
        inner,
        observation,
        retained: Some(retained),
    }
}
pub struct ShutdownStarted {
    pub(crate) runtime_id: u64,
    pub(crate) attempt_id: u64,
    pub(crate) grace_period_ms: u64,
    pub(crate) outstanding: usize,
}

impl ShutdownStarted {
    pub(crate) fn emit(self) {
        let Self {
            runtime_id,
            attempt_id: shutdown_attempt_id,
            grace_period_ms,
            outstanding,
        } = self;
        tracing::info!(target: "netplane", event = "shutdown.started", runtime_id, shutdown_attempt_id, grace_period_ms, outstanding);
    }
}
