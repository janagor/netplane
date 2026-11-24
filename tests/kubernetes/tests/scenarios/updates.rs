use crate::support::{Environment, Result};
use kube::api::{Patch, PatchParams};
use serde_json::json;

#[tokio::test]
#[ignore = "requires the isolated Kind cluster"]
async fn equivalent_metadata_is_processed_without_preparation_or_publication() -> Result {
    let env = Environment::create("deduplication").await?;
    let before = env.topology(3).await?;
    let mut final_version = String::new();
    for value in 0..12 {
        final_version = env
            .services()
            .patch(
                "backend",
                &PatchParams::default(),
                &Patch::Merge(
                    json!({"metadata":{"annotations":{"test-update":value.to_string()}}}),
                ),
            )
            .await?
            .metadata
            .resource_version
            .ok_or("Service resource version")?;
    }
    let after = env
        .wait_value("final metadata update processed by discovery", || async {
            let sample = env.get("/observation").await?;
            Ok(
                (sample["processed_resources"]["services/backend"] == final_version
                    && sample["converged"] == true)
                    .then_some(sample),
            )
        })
        .await?;
    assert_eq!(after["publication_id"], before["publication_id"]);
    assert_eq!(after["prepared_sizes"], before["prepared_sizes"]);
    assert_eq!(after["active_preparations"], 0);
    assert_eq!(after["phase"], "Idle");
    assert_eq!(after["desired_revision"], after["applied_revision"]);
    env.assert_no_restart(&before).await?;
    env.cleanup().await
}

#[tokio::test]
#[ignore = "requires the isolated Kind cluster"]
async fn contended_preparation_coalesces_obsolete_topologies_into_latest_state() -> Result {
    let env = Environment::create("coalescing").await?;
    let before = env.topology(3).await?;
    let initial = before["prepared_sizes"]
        .as_array()
        .ok_or("preparation history")?
        .len();
    env.post("/test/preparation", json!(true)).await?;
    env.scale(4).await?;
    env.observed(4).await?;
    env.wait(
        "replacement preparation blocked at fixture gate",
        || async { Ok(env.get("/status").await?["active_preparations"] == 1) },
    )
    .await?;
    // Each intermediate state is observed and submitted while preparation is
    // blocked, proving this exercises the control mailbox rather than Kubernetes
    // coalescing the Deployment writes before any endpoint changes occur.
    env.scale(2).await?;
    env.observed(2).await?;
    env.scale(1).await?;
    let latest = env.observed(1).await?;
    assert_eq!(latest["publication_id"], before["publication_id"]);
    assert_eq!(latest["maximum_preparations"], 1);
    assert_eq!(
        latest["prepared_sizes"].as_array().ok_or("history")?.len(),
        initial + 1
    );
    env.post("/test/preparation", json!(false)).await?;
    let after = env.topology(1).await?;
    assert_eq!(after["applied_revision"], latest["submitted_sequence"]);
    assert_ne!(after["publication_id"], before["publication_id"]);
    assert_eq!(after["maximum_preparations"], 1);
    assert_eq!(after["active_preparations"], 0);
    let sizes = after["prepared_sizes"].as_array().ok_or("history")?;
    assert_eq!(&sizes[initial..], &[json!(4), json!(1)]);
    assert_eq!(env.all_routed_pods().await?, env.expected_pods().await?);
    env.assert_no_restart(&before).await?;
    env.cleanup().await
}
