//! Explicitly ignored: invoke through scripts/kubernetes-e2e.sh, sequentially.
mod support;
#[path = "scenarios/updates.rs"]
mod updates;
use k8s_openapi::api::authorization::v1::SubjectAccessReview;
use kube::{
    Api,
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams},
};
use serde_json::{Value, json};
use support::{Environment, Result};

#[tokio::test]
#[ignore = "requires the isolated Kind cluster"]
async fn discovery_scaling_and_empty_topology() -> Result {
    let env = Environment::create("scaling").await?;
    let original = env.get("/status").await?;
    let sizes = original["prepared_sizes"]
        .as_array()
        .ok_or("preparation history")?;
    assert_eq!(
        sizes.first(),
        Some(&json!(0)),
        "intentional unavailable bootstrap"
    );
    assert!(
        sizes.len() >= 2 && sizes[1..].iter().all(|size| *size == 3),
        "discovery prepared an incomplete initial topology: {sizes:?}"
    );
    assert!(
        env.http
            .get("http://127.0.0.1:18080/readyz")
            .send()
            .await?
            .status()
            .is_success()
    );
    assert_eq!(env.all_routed_pods().await?, env.expected_pods().await?);
    env.scale(4).await?;
    env.topology(4).await?;
    assert_ne!(
        env.get("/status").await?["publication_id"],
        original["publication_id"]
    );
    assert_eq!(env.all_routed_pods().await?, env.expected_pods().await?);
    env.scale(1).await?;
    env.topology(1).await?;
    assert_eq!(env.all_routed_pods().await?, env.expected_pods().await?);
    assert_eq!(env.get("/status").await?["retained_publications"], 0);
    env.scale(0).await?;
    env.topology(0).await?;
    assert_eq!(
        env.http
            .get("http://127.0.0.1:18080/proxy/identity")
            .send()
            .await?
            .status(),
        503
    );
    env.scale(2).await?;
    env.topology(2).await?;
    assert_eq!(env.all_routed_pods().await?, env.expected_pods().await?);
    env.assert_no_restart(&original).await?;
    env.cleanup().await
}

#[tokio::test]
#[ignore = "requires the isolated Kind cluster"]
async fn readiness_and_inflight_publication() -> Result {
    let env = Environment::create("readiness").await?;
    let original = env.get("/status").await?;
    let key = 42;
    let target = env.identity(key).await?;
    let pod = env
        .pods()
        .await?
        .into_iter()
        .find(|p| p.metadata.name.as_deref() == target["pod"].as_str())
        .ok_or("selected Pod")?;
    let request = env
        .http
        .get("http://127.0.0.1:18080/proxy/block")
        .header("x-request-key", key.to_string())
        .send();
    let blocked = tokio::spawn(request);
    env.wait("upstream has accepted blocked request", || async {
        Ok(env.control(&pod, "status", None).await?["active"] == 1)
    })
    .await?;
    env.control(&pod, "readiness", Some(false)).await?;
    let topology = env.topology(2).await?;
    assert!(
        topology["endpoints"]
            .as_array()
            .ok_or("array")?
            .iter()
            .all(|b| b["uid"].as_str() != pod.metadata.uid.as_deref())
    );
    assert_ne!(
        env.get("/status").await?["publication_id"],
        original["publication_id"]
    );
    assert_eq!(env.get("/status").await?["outstanding_requests"], 1);
    for key in 0..32 {
        assert_ne!(env.identity(key).await?["pod"], target["pod"]);
    }
    assert!(
        !env.get("/status").await?["released_publications"]
            .as_array()
            .ok_or("release IDs")?
            .contains(&original["publication_id"])
    );
    env.control(&pod, "release", None).await?;
    assert_eq!(
        blocked.await??.error_for_status()?.json::<Value>().await?["pod"],
        target["pod"]
    );
    env.wait("request ownership released", || async {
        Ok(env.get("/status").await?["outstanding_requests"] == 0)
    })
    .await?;
    env.wait("superseded publication released", || async {
        Ok(env.get("/status").await?["released_publications"]
            .as_array()
            .ok_or("release IDs")?
            .contains(&original["publication_id"]))
    })
    .await?;
    env.control(&pod, "readiness", Some(true)).await?;
    env.topology(3).await?;
    assert_eq!(env.all_routed_pods().await?, env.expected_pods().await?);
    env.assert_no_restart(&original).await?;
    env.cleanup().await
}

#[tokio::test]
#[ignore = "requires the isolated Kind cluster"]
async fn recovery_preserves_graph_and_reconnects() -> Result {
    let env = Environment::create("recovery").await?;
    let original = env.get("/status").await?;
    // A quiet, unchanged Service must recover and route without any controller
    // event rescuing a watcher that remains incorrectly marked Recovering.
    env.post("/test/api", json!(true)).await?;
    env.wait("all API tunnels closed", || async {
        let sample = env.get("/status").await?;
        Ok(sample["fault_disconnected"] == true
            && sample["active_api_tunnels"] == 0
            && sample["discovery_state"]
                .as_str()
                .is_some_and(|s| s.contains("Recovering")))
    })
    .await?;
    env.post("/test/api", json!(false)).await?;
    let recovered = env.topology(3).await?;
    assert_eq!(recovered["publication_id"], original["publication_id"]);
    assert_eq!(env.all_routed_pods().await?, env.expected_pods().await?);
    env.post("/test/api", json!(true)).await?;
    env.wait("watch interruption visible", || async {
        Ok(env.get("/status").await?["discovery_state"]
            .as_str()
            .is_some_and(|s| s.contains("Recovering")))
    })
    .await?;
    for key in 0..16 {
        env.identity(key).await?;
    }
    env.scale(1).await?;
    env.wait("unaffected client sees scale down", || async {
        Ok(env.pods().await?.len() == 1)
    })
    .await?;
    assert_eq!(
        env.get("/status").await?["publication_id"],
        original["publication_id"]
    );
    // Backend survival is separate from publication preservation.
    let survivor = env.pods().await?.into_iter().next().ok_or("survivor")?;
    assert_eq!(env.control(&survivor, "status", None).await?["active"], 0);
    env.post("/test/api", json!(false)).await?;
    env.topology(1).await?;
    assert_eq!(env.all_routed_pods().await?, env.expected_pods().await?);
    env.assert_no_restart(&original).await?;
    env.cleanup().await
}

#[tokio::test]
#[ignore = "requires the isolated Kind cluster"]
async fn rolling_update_converges_during_traffic() -> Result {
    let env = Environment::create("rollout").await?;
    let original = env.get("/status").await?;
    let stop = tokio_util::sync::CancellationToken::new();
    let token = stop.clone();
    let http = env.http.clone();
    let window = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let in_rollout = window.clone();
    let traffic = tokio::spawn(async move {
        let (mut successful, mut failed) = (0, 0);
        loop {
            tokio::select! {
                () = token.cancelled() => break,
                result = http.get("http://127.0.0.1:18080/proxy/identity").send() => {
                    if in_rollout.load(std::sync::atomic::Ordering::SeqCst) {
                        if result.is_ok_and(|r| r.status().is_success()) { successful += 1; } else { failed += 1; }
                    }
                }
            }
        }
        (successful, failed)
    });
    env.deployments()
        .patch(
            "backend",
            &PatchParams::default(),
            &Patch::Strategic(json!({"spec":{"template":{"spec":{"containers":[{
                "name":"backend","env":[{"name":"APP_VERSION","value":"v2"}]
            }]}}}})),
        )
        .await?;
    window.store(true, std::sync::atomic::Ordering::SeqCst);
    env.wait("Deployment rollout complete", || async {
        let deployment = env.deployments().get("backend").await?;
        Ok(deployment.status.is_some_and(|s| {
            s.observed_generation == deployment.metadata.generation
                && s.updated_replicas == Some(3)
                && s.replicas == Some(3)
                && s.available_replicas == Some(3)
        }))
    })
    .await?;
    window.store(false, std::sync::atomic::Ordering::SeqCst);
    env.topology(3).await?;
    for key in 0..32 {
        assert_eq!(env.identity(key).await?["version"], "v2");
    }
    stop.cancel();
    let (success, failure) = traffic.await?;
    eprintln!("rollout successful={success} failed={failure}");
    assert!(success > 0);
    env.assert_no_restart(&original).await?;
    env.cleanup().await
}

#[tokio::test]
#[ignore = "requires the isolated Kind cluster"]
async fn lifecycle_shutdown_during_api_outage() -> Result {
    for outage in [false, true] {
        let env = Environment::create("lifecycle").await?;
        let key = 42;
        let target = env.identity(key).await?;
        let pod = env
            .pods()
            .await?
            .into_iter()
            .find(|p| p.metadata.name.as_deref() == target["pod"].as_str())
            .ok_or("Pod")?;
        let blocked = tokio::spawn(
            env.http
                .get("http://127.0.0.1:18080/proxy/block")
                .header("x-request-key", key.to_string())
                .send(),
        );
        env.wait("accepted block", || async {
            Ok(env.control(&pod, "status", None).await?["active"] == 1)
        })
        .await?;
        if outage {
            env.post("/test/api", json!(true)).await?;
            env.wait("reconnect loop entered", || async {
                Ok(env.get("/status").await?["discovery_state"]
                    .as_str()
                    .is_some_and(|s| s.contains("Recovering")))
            })
            .await?;
        }
        env.post("/test/shutdown", Value::Null).await?;
        env.wait("admission closed", || async {
            Ok(env.get("/status").await?["runtime"] == "Draining")
        })
        .await?;
        assert!(
            !env.http
                .get("http://127.0.0.1:18080/proxy/identity")
                .send()
                .await?
                .status()
                .is_success()
        );
        env.control(&pod, "release", None).await?;
        assert!(blocked.await??.status().is_success());
        env.wait("owned runners stopped", || async {
            let status = env.get("/status").await?;
            Ok(status["background_tasks_stopped"] == true
                && status["runtime"] == "Stopped"
                && status["phase"] == "Stopped"
                && status["discovery_state"]
                    .as_str()
                    .is_some_and(|s| s.contains("Stopped")))
        })
        .await?;
        env.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated Kind cluster"]
async fn permissions_are_read_only_and_namespace_scoped() -> Result {
    let env = Environment::create("permissions").await?;
    let reviews: Api<SubjectAccessReview> = Api::all(env.client.clone());
    for (namespace, group, resource, verb, allowed) in [
        (env.namespace.as_str(), "", "services", "list", true),
        (
            env.namespace.as_str(),
            "discovery.k8s.io",
            "endpointslices",
            "watch",
            true,
        ),
        ("default", "", "services", "list", false),
        (
            env.namespace.as_str(),
            "apps",
            "deployments",
            "patch",
            false,
        ),
        (env.namespace.as_str(), "", "pods", "list", false),
        (env.namespace.as_str(), "", "secrets", "get", false),
    ] {
        let review: SubjectAccessReview = serde_json::from_value(json!({"spec":{
            "user":format!("system:serviceaccount:{}:netplane",env.namespace),
            "resourceAttributes":{"namespace":namespace,"group":group,"resource":resource,"verb":verb}}}))?;
        let result = reviews.create(&PostParams::default(), &review).await?;
        assert_eq!(result.status.ok_or("review status")?.allowed, allowed);
    }
    let foreign = format!("{}-other", env.namespace);
    let namespaces: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(env.client.clone());
    namespaces
        .create(
            &PostParams::default(),
            &serde_json::from_value(
                json!({"metadata":{"name":foreign,"labels":{"netplane-e2e":"true"}}}),
            )?,
        )
        .await?;
    let deployments: Api<k8s_openapi::api::apps::v1::Deployment> =
        Api::namespaced(env.client.clone(), &foreign);
    deployments
        .create(
            &PostParams::default(),
            &serde_json::from_str(include_str!("../manifests/backend.json"))?,
        )
        .await?;
    let services: Api<k8s_openapi::api::core::v1::Service> =
        Api::namespaced(env.client.clone(), &foreign);
    services.create(&PostParams::default(), &serde_json::from_value(json!({"metadata":{"name":"backend"},"spec":{"selector":{"app":"backend"},"ports":[{"name":"http","port":80,"targetPort":8080}]}}))?).await?;
    let foreign_pods: Api<k8s_openapi::api::core::v1::Pod> =
        Api::namespaced(env.client.clone(), &foreign);
    env.wait("foreign namespace has running backends", || async {
        Ok(foreign_pods
            .list(&ListParams::default())
            .await?
            .items
            .iter()
            .filter(|p| {
                p.status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .is_some_and(|c| c.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
            })
            .count()
            == 3)
    })
    .await?;
    env.topology(3).await?;
    namespaces
        .delete(&foreign, &DeleteParams::default())
        .await?;
    // The gateway's complete topology identities must all belong to its namespace.
    assert_eq!(env.all_routed_pods().await?, env.expected_pods().await?);
    env.cleanup().await
}

#[tokio::test]
#[ignore = "requires the isolated Kind cluster"]
async fn terminating_and_unknown_readiness_conditions() -> Result {
    use k8s_openapi::api::discovery::v1::EndpointSlice;
    let env = Environment::create("conditions").await?;
    let pods = env.pods().await?;
    // Replace the selected Service to avoid racing a controller still handling
    // queued events for its previous selector. Old slices have a different owner UID.
    env.services()
        .delete("backend", &DeleteParams::default())
        .await?;
    env.wait("old Service removed", || async {
        Ok(env.services().get_opt("backend").await?.is_none())
    })
    .await?;
    env.services().create(&PostParams::default(), &serde_json::from_value(json!({
        "metadata":{"name":"backend"},"spec":{"ports":[{"name":"http","port":80,"targetPort":8080}]}
    }))?).await?;
    let slices: Api<EndpointSlice> = Api::namespaced(env.client.clone(), &env.namespace);
    slices
        .delete_collection(
            &DeleteParams::default(),
            &kube::api::ListParams::default().labels("kubernetes.io/service-name=backend"),
        )
        .await?;
    env.topology(0).await?;
    let service_uid = env
        .services()
        .get("backend")
        .await?
        .metadata
        .uid
        .ok_or("Service UID")?;
    let endpoints: Vec<_> = pods
        .iter()
        .enumerate()
        .map(|(index, pod)| {
            json!({
                "addresses":[pod.status.as_ref().and_then(|s|s.pod_ip.as_ref())],
                "targetRef":{"kind":"Pod","name":pod.metadata.name,"uid":pod.metadata.uid},
                "conditions":match index {
                    0 => json!({"ready":true,"serving":true,"terminating":true}),
                    1 => json!({"ready":false,"serving":true,"terminating":false}),
                    _ => json!({"serving":true,"terminating":false}),
                }
            })
        })
        .collect();
    let slice: EndpointSlice = serde_json::from_value(
        json!({"metadata":{"name":"controlled","labels":{
        "kubernetes.io/service-name":"backend","endpointslice.kubernetes.io/managed-by":"netplane-e2e"},
        "ownerReferences":[{"apiVersion":"v1","kind":"Service","name":"backend","uid":service_uid}]},
        "addressType":"IPv4","ports":[{"name":"http","port":8080,"protocol":"TCP"}],"endpoints":endpoints}),
    )?;
    slices.create(&PostParams::default(), &slice).await?;
    env.topology(1).await?;
    let eligible = pods[2].metadata.name.as_ref().ok_or("Pod name")?;
    for key in 0..32 {
        assert_eq!(env.identity(key).await?["pod"], *eligible);
    }
    env.cleanup().await
}
