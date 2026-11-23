//! Apply A, B, duplicate B, and invalid C while retaining an accepted A request.
use std::{error::Error, sync::Arc};

use netplane::{
    Capacity, ConfigError, ControllerOptions, GraphDraft, GraphVersion, Netplane,
    ReconfigurationController, RuntimeLimits,
};
use netplane_control::{ApplyError, ApplyOutcome, GraphPreparer, Reconciler};

#[derive(Clone, Debug, PartialEq, Eq)]
struct GatewayConfig {
    response: &'static str,
    max_in_flight: usize,
}

struct GatewayPreparer;

impl GraphPreparer<GatewayConfig, (), &'static str> for GatewayPreparer {
    type Error = ConfigError;

    fn validate(&self, desired: &GatewayConfig) -> Result<(), Self::Error> {
        if desired.max_in_flight == 0 {
            Err(ConfigError::ZeroConcurrency)
        } else {
            Ok(())
        }
    }

    async fn prepare(
        &self,
        desired: GatewayConfig,
    ) -> Result<GraphVersion<(), &'static str>, Self::Error> {
        GraphDraft::new(tower::service_fn(move |()| async move {
            Ok::<_, std::convert::Infallible>(desired.response)
        }))
        .capacity(Capacity {
            max_in_flight: desired.max_in_flight,
            buffer_capacity: 8,
        })
        .validate()
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let initial = GatewayConfig {
        response: "bootstrap",
        max_in_flight: 2,
    };
    let runtime = Netplane::new(
        GatewayPreparer.prepare(initial).await?,
        RuntimeLimits {
            max_outstanding: 16,
        },
    )?;
    let controller = Arc::new(ReconfigurationController::new(
        runtime,
        ControllerOptions::default(),
    ));
    let reconciler = Reconciler::new(Arc::clone(&controller), GatewayPreparer);
    reconciler
        .apply(GatewayConfig {
            response: "A",
            max_in_flight: 2,
        })
        .await?;
    let handle = controller.handle();
    let accepted_on_a = handle.execute(());
    let desired_b = GatewayConfig {
        response: "B",
        max_in_flight: 4,
    };
    println!("{:?}", reconciler.apply(desired_b.clone()).await?);
    assert!(matches!(
        reconciler.apply(desired_b).await?,
        ApplyOutcome::Unchanged(_)
    ));
    assert!(matches!(
        reconciler
            .apply(GatewayConfig {
                response: "C",
                max_in_flight: 0
            })
            .await,
        Err(ApplyError::Validation(_))
    ));
    assert_eq!(accepted_on_a.await?, "A");
    assert_eq!(handle.execute(()).await?, "B");
    println!("Accepted A request completed; B remains active after invalid C.");
    Ok(())
}
