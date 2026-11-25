use super::{HealthHandle, HealthOutcome, ObservationToken};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower::{Layer, Service};

/// Classify backend outcomes without changing their response or error types.
///
/// Place this layer directly around backend execution, inside Netplane's capacity
/// and admission boundaries. Readiness errors and dropped futures report nothing.
pub trait OutcomeClassifier<Response, Error> {
    /// Interpret a completed execution; application errors may be ignored.
    fn classify(&self, result: Result<&Response, &Error>) -> HealthOutcome;
}
impl<Response, Error, Classifier> OutcomeClassifier<Response, Error> for Classifier
where
    Classifier: Fn(Result<&Response, &Error>) -> HealthOutcome,
{
    fn classify(&self, result: Result<&Response, &Error>) -> HealthOutcome {
        self(result)
    }
}

/// Ordinary Tower middleware that observes only completed backend calls.
#[derive(Clone)]
pub struct HealthLayer<Classifier> {
    handle: HealthHandle,
    classifier: Classifier,
}
impl<Classifier> HealthLayer<Classifier> {
    /// Bind a classifier to one backend incarnation.
    #[must_use]
    pub const fn new(handle: HealthHandle, classifier: Classifier) -> Self {
        Self { handle, classifier }
    }
}
impl<S, Classifier: Clone> Layer<S> for HealthLayer<Classifier> {
    type Service = HealthService<S, Classifier>;
    fn layer(&self, inner: S) -> Self::Service {
        HealthService {
            inner,
            handle: self.handle.clone(),
            classifier: self.classifier.clone(),
        }
    }
}

/// A backend service with transparent readiness and completion observations.
#[derive(Clone)]
pub struct HealthService<S, Classifier> {
    inner: S,
    handle: HealthHandle,
    classifier: Classifier,
}
impl<Request, S, Classifier> Service<Request> for HealthService<S, Classifier>
where
    S: Service<Request>,
    Classifier: OutcomeClassifier<S::Response, S::Error> + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ObservationFuture<S::Future, Classifier>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: Request) -> Self::Future {
        let token = self.handle.begin_observation();
        ObservationFuture {
            inner: self.inner.call(request),
            handle: self.handle.clone(),
            classifier: self.classifier.clone(),
            token: Some(token),
        }
    }
}

pin_project! {
    /// Future reporting at most one completed call; dropping it records nothing.
    pub struct ObservationFuture<F, Classifier> {
        #[pin]
        inner: F,
        handle: HealthHandle,
        classifier: Classifier,
        token: Option<ObservationToken>,
    }
}
impl<F, Response, Error, Classifier> Future for ObservationFuture<F, Classifier>
where
    F: Future<Output = Result<Response, Error>>,
    Classifier: OutcomeClassifier<Response, Error>,
{
    type Output = Result<Response, Error>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = std::task::ready!(this.inner.poll(cx));
        if let Some(token) = this.token.take() {
            this.handle
                .report(token, this.classifier.classify(result.as_ref()));
        }
        Poll::Ready(result)
    }
}
