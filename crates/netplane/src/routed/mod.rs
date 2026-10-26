//! Direct routing to independently bounded backend endpoints.

use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tower::BoxError;

use crate::{Endpoint, endpoint::EndpointExecution};

type Selector<Request> = Arc<dyn Fn(&Request) -> Cow<'static, str> + Send + Sync>;

/// Immutable router with no aggregate per-version execution capacity.
pub struct RoutedGraph<Request: Send, Response> {
    selector: Selector<Request>,
    routes: Arc<BTreeMap<String, Endpoint<Request, Response>>>,
    fallback: Endpoint<Request, Response>,
}

impl<Request, Response> RoutedGraph<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    pub(crate) fn new(
        selector: Box<dyn Fn(&Request) -> Cow<'static, str> + Send + Sync>,
        routes: BTreeMap<String, Endpoint<Request, Response>>,
        fallback: Endpoint<Request, Response>,
    ) -> Self {
        Self {
            selector: Arc::from(selector),
            routes: Arc::new(routes),
            fallback,
        }
    }

    /// Build a future that selects a backend on its first poll.
    ///
    /// Unpolled futures do not invoke the selector or submit to a backend.
    pub(crate) fn execute(&self, request: Request) -> RoutedExecution<Request, Response> {
        RoutedExecution {
            phase: RoutedPhase::Select {
                request: Some(request),
                selector: Arc::clone(&self.selector),
                routes: Arc::clone(&self.routes),
                fallback: self.fallback.clone(),
            },
        }
    }
}

pin_project! {
    /// Backend-only execution: select on first poll, then one endpoint handoff.
    pub struct RoutedExecution<Request, Response>
    where
        Request: Send,
        Request: 'static,
        Response: Send,
        Response: 'static,
    {
        #[pin]
        phase: RoutedPhase<Request, Response>,
    }
}

pin_project! {
    #[project = RoutedPhaseProj]
    enum RoutedPhase<Request, Response>
    where
        Request: Send,
        Request: 'static,
        Response: Send,
        Response: 'static,
    {
        Select {
            request: Option<Request>,
            selector: Selector<Request>,
            routes: Arc<BTreeMap<String, Endpoint<Request, Response>>>,
            fallback: Endpoint<Request, Response>,
        },
        Execute {
            #[pin]
            execution: EndpointExecution<Request, Response>,
        },
        Done,
    }
}

impl<Request, Response> Future for RoutedExecution<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    type Output = Result<Response, BoxError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();
            match this.phase.as_mut().project() {
                RoutedPhaseProj::Select {
                    request,
                    selector,
                    routes,
                    fallback,
                } => {
                    let Some(request) = request.take() else {
                        this.phase.set(RoutedPhase::Done);
                        break Poll::Ready(Err("routed request missing".into()));
                    };
                    let label = selector(&request);
                    let endpoint = routes.get(label.as_ref()).unwrap_or(fallback).clone();
                    let execution = endpoint.execute_owned(request);
                    this.phase.set(RoutedPhase::Execute { execution });
                }
                RoutedPhaseProj::Execute { execution } => match execution.poll(cx) {
                    Poll::Pending => break Poll::Pending,
                    Poll::Ready(result) => {
                        this.phase.set(RoutedPhase::Done);
                        break Poll::Ready(result);
                    }
                },
                RoutedPhaseProj::Done => {
                    break Poll::Ready(Err("routed execution polled after completion".into()));
                }
            }
        }
    }
}
