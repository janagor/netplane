//! Request admission, version selection, and deadline-aware execution.

use std::{future::Future, sync::atomic::Ordering};

use tower::BoxError;

use crate::{
    AdmissionError, DeadlineExceeded, Handle, RuntimeStatus,
    observability::{RequestObservation, observe},
};

impl<Request, Response> Handle<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    /// Sample this runtime's admission count and lifecycle phase.
    ///
    /// `outstanding` includes accepted, unfinished Netplane request futures,
    /// even before first poll, across every handle and publication. It does
    /// not count pending HTTP body reads or work retained independently by a
    /// Tower service. The count and phase come from one admission-state lock.
    #[must_use]
    pub fn status(&self) -> RuntimeStatus {
        self.admission.status()
    }

    /// Admit a request and select its version synchronously.
    ///
    /// The accepted future holds one global slot and its selected version until
    /// it completes or is dropped. Dropping it cancels its queue wait; a
    /// dispatched service determines whether its work can be canceled.
    ///
    /// # Errors
    /// Returns [`AdmissionError::Closed`] after shutdown begins, or
    /// [`AdmissionError::AtCapacity`] when the global budget is full.
    pub fn try_execute(
        &self,
        request: Request,
    ) -> Result<
        impl Future<Output = Result<Response, BoxError>> + Send + 'static + use<Request, Response>,
        AdmissionError,
    > {
        self.try_execute_with_deadline(request, None)
    }

    /// Admit and select a request now, with an absolute execution deadline.
    ///
    /// The deadline starts at this call, including time before the returned
    /// future is first polled. An expired deadline returns [`DeadlineExceeded`]
    /// on polling without polling the service. The accepted future owns one
    /// admission slot and its selected version until completion or drop.
    /// Expiration cancels Netplane's execution future; a dispatched service
    /// may retain independent work.
    ///
    /// # Errors
    /// Returns [`AdmissionError::Closed`] after shutdown begins, or
    /// [`AdmissionError::AtCapacity`] when the global budget is full.
    /// The future returns a boxed [`DeadlineExceeded`] on expiration.
    pub fn try_execute_until(
        &self,
        request: Request,
        deadline: tokio::time::Instant,
    ) -> Result<
        impl Future<Output = Result<Response, BoxError>> + Send + 'static + use<Request, Response>,
        AdmissionError,
    > {
        self.try_execute_with_deadline(request, Some(deadline))
    }

    fn try_execute_with_deadline(
        &self,
        request: Request,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<
        impl Future<Output = Result<Response, BoxError>> + Send + 'static + use<Request, Response>,
        AdmissionError,
    > {
        let guard = match self.admission.try_admit() {
            Ok(guard) => guard,
            Err(AdmissionError::Closed) => {
                tracing::debug!(target: "netplane", event = "request.rejected", runtime_id = self.runtime_id, reason = "admission_closed");
                return Err(AdmissionError::Closed);
            }
            Err(AdmissionError::AtCapacity { limit }) => {
                tracing::warn!(target: "netplane", event = "request.rejected", runtime_id = self.runtime_id, reason = "capacity_exceeded", outstanding_limit = limit);
                return Err(AdmissionError::AtCapacity { limit });
            }
        };
        let version = self.active.load_full().ok_or(AdmissionError::Closed)?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let observation = RequestObservation::new(self.runtime_id, version.id.get(), request_id);
        let execution = version.graph.endpoint.execute(request);
        let execution = async move {
            if let Some(deadline) = deadline {
                if tokio::time::Instant::now() >= deadline {
                    return Err(Box::new(DeadlineExceeded) as BoxError);
                }
                tokio::time::timeout_at(deadline, execution)
                    .await
                    .unwrap_or_else(|_| Err(Box::new(DeadlineExceeded) as BoxError))
            } else {
                execution.await
            }
        };
        Ok(observe(execution, observation, (version, guard)))
    }

    /// Admit the request now and return a future that executes it.
    ///
    /// Admission errors are boxed into the returned future's result. The
    /// accepted future releases its slot on completion or drop.
    ///
    /// # Errors
    /// Returns a boxed [`AdmissionError`] for overload or shutdown, or a
    /// service or buffer worker error after admission.
    pub fn execute(
        &self,
        request: Request,
    ) -> impl Future<Output = Result<Response, BoxError>> + Send + 'static + use<Request, Response>
    {
        let selected = self.try_execute(request);
        async move {
            match selected {
                Ok(execution) => execution.await,
                Err(error) => Err(Box::new(error) as BoxError),
            }
        }
    }

    /// Admit the request now and return a future with an absolute deadline.
    ///
    /// Admission errors are boxed into the future result. The deadline starts
    /// at invocation, even if the future is never polled. Expiration cancels
    /// Netplane's execution future and releases its admission slot.
    ///
    /// # Errors
    /// Returns a boxed [`AdmissionError`] for overload or shutdown, a boxed
    /// [`DeadlineExceeded`] on expiration, or a service or worker error.
    pub fn execute_until(
        &self,
        request: Request,
        deadline: tokio::time::Instant,
    ) -> impl Future<Output = Result<Response, BoxError>> + Send + 'static + use<Request, Response>
    {
        let selected = self.try_execute_until(request, deadline);
        async move {
            match selected {
                Ok(execution) => execution.await,
                Err(error) => Err(Box::new(error) as BoxError),
            }
        }
    }
}
