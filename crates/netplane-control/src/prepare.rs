use std::future::Future;

use netplane::GraphVersion;

/// Translate application-owned desired state into an immutable executable graph.
///
/// Desired state should contain configuration rather than live services. Normalize
/// it before submission so `Eq` represents semantic equality. Preparation should
/// bound its resources and release them when its future is dropped. Independently
/// spawned work remains the application's responsibility.
pub trait GraphPreparer<D, Request: Send + 'static, Response: Send + 'static> {
    /// Application validation or construction failure.
    type Error;

    /// Check application policy without allocating graph workers.
    ///
    /// # Errors
    /// Return an application error for invalid desired state.
    fn validate(&self, desired: &D) -> Result<(), Self::Error>;

    /// Construct and validate a complete replacement outside the controller lock.
    ///
    /// Use `GraphDraft` or `GraphBuilder` for runtime graph validation. This runs
    /// on the caller's executor; offload blocking work explicitly if necessary.
    ///
    /// # Errors
    /// Return an application error if services or the graph cannot be prepared.
    fn prepare(
        &self,
        desired: D,
    ) -> impl Future<Output = Result<GraphVersion<Request, Response>, Self::Error>> + Send;
}
