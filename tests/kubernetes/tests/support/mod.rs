use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{Namespace, Pod, Service, ServiceAccount},
    discovery::v1::EndpointSlice,
    rbac::v1::{Role, RoleBinding},
};
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams},
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};
use tower::BoxError;
pub type Result<T = ()> = std::result::Result<T, BoxError>;
pub struct Environment {
    pub client: Client,
    pub namespace: String,
    pub http: reqwest::Client,
}
impl Environment {
    pub async fn create(scenario: &str) -> Result<Self> {
        let env = Self {
            client: Client::try_default().await?,
            namespace: format!("np-{scenario}-{}", std::process::id()),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(100))
                .build()?,
        };
        Api::<Namespace>::all(env.client.clone())
            .create(
                &PostParams::default(),
                &serde_json::from_value(json!({
            "metadata":{"name":env.namespace,"labels":{"netplane-e2e":"true"}}}))?,
            )
            .await?;
        env.deploy_backend().await?;
        env.wait("three ready backend Pods", || async {
            Ok(env.pods().await?.len() == 3)
        })
        .await?;
        env.wait("complete initial EndpointSlice set", || async {
            let slices: Api<EndpointSlice> = Api::namespaced(env.client.clone(), &env.namespace);
            let mut ready = BTreeSet::new();
            for slice in slices
                .list(&ListParams::default().labels("kubernetes.io/service-name=backend"))
                .await?
                .items
            {
                for endpoint in slice.endpoints {
                    if endpoint.conditions.is_some_and(|c| c.ready == Some(true)) {
                        if let Some(uid) = endpoint.target_ref.and_then(|r| r.uid) {
                            ready.insert(uid);
                        }
                    }
                }
            }
            Ok(ready.len() == 3)
        })
        .await?;
        let accounts: Api<ServiceAccount> = Api::namespaced(env.client.clone(), &env.namespace);
        accounts
            .create(
                &PostParams::default(),
                &serde_json::from_value(json!({"metadata":{"name":"netplane"}}))?,
            )
            .await?;
        let roles: Api<Role> = Api::namespaced(env.client.clone(), &env.namespace);
        roles.create(&PostParams::default(), &serde_json::from_value(json!({"metadata":{"name":"discovery"},"rules":[
            {"apiGroups":[""],"resources":["services"],"verbs":["list","watch"]},
            {"apiGroups":["discovery.k8s.io"],"resources":["endpointslices"],"verbs":["list","watch"]}]}))?).await?;
        let bindings: Api<RoleBinding> = Api::namespaced(env.client.clone(), &env.namespace);
        bindings
            .create(
                &PostParams::default(),
                &serde_json::from_value(json!({"metadata":{"name":"discovery"},
            "subjects":[{"kind":"ServiceAccount","name":"netplane","namespace":env.namespace}],
            "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"discovery"}}))?,
            )
            .await?;
        let gateway: Deployment =
            serde_json::from_str(include_str!("../../manifests/gateway.json"))?;
        env.deployments()
            .create(&PostParams::default(), &gateway)
            .await?;
        env.services()
            .create(
                &PostParams::default(),
                &serde_json::from_value(json!({"metadata":{"name":"gateway"},"spec":{
            "type":"NodePort","publishNotReadyAddresses":true,"selector":{"app":"gateway"},
            "ports":[{"port":8080,"targetPort":8080,"nodePort":30080}]}}))?,
            )
            .await?;
        env.topology(3).await?;
        Ok(env)
    }
    pub fn deployments(&self) -> Api<Deployment> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }
    pub fn services(&self) -> Api<Service> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }
    async fn deploy_backend(&self) -> Result {
        let backend: Deployment =
            serde_json::from_str(include_str!("../../manifests/backend.json"))?;
        self.deployments()
            .create(&PostParams::default(), &backend)
            .await?;
        self.services().create(&PostParams::default(), &serde_json::from_value(json!({"metadata":{"name":"backend"},"spec":{
            "selector":{"app":"backend"},"ports":[{"name":"http","port":80,"targetPort":"http"}]}}))?).await?;
        Ok(())
    }
    pub async fn pods(&self) -> Result<Vec<Pod>> {
        Ok(Api::<Pod>::namespaced(self.client.clone(), &self.namespace)
            .list(&ListParams::default().labels("app=backend"))
            .await?
            .items
            .into_iter()
            .filter(|p| {
                p.metadata.deletion_timestamp.is_none()
                    && p.status
                        .as_ref()
                        .and_then(|s| s.conditions.as_ref())
                        .is_some_and(|c| c.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
            })
            .collect())
    }
    pub async fn get(&self, path: &str) -> Result<Value> {
        Ok(self
            .http
            .get(format!("http://127.0.0.1:18080{path}"))
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
    pub async fn post(&self, path: &str, value: Value) -> Result<Value> {
        let response = self
            .http
            .post(format!("http://127.0.0.1:18080{path}"))
            .json(&value)
            .send()
            .await?
            .error_for_status()?;
        let bytes = response.bytes().await?;
        Ok(serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }
    pub async fn control(&self, pod: &Pod, action: &str, ready: Option<bool>) -> Result<Value> {
        let ip = pod
            .status
            .as_ref()
            .and_then(|s| s.pod_ip.as_ref())
            .ok_or("Pod IP")?;
        self.post(
            "/test/control",
            json!({"address":format!("{ip}:8080"),"action":action,"ready":ready}),
        )
        .await
    }
    pub async fn scale(&self, replicas: u32) -> Result {
        self.deployments()
            .patch(
                "backend",
                &PatchParams::default(),
                &Patch::Merge(json!({"spec":{"replicas":replicas}})),
            )
            .await?;
        Ok(())
    }
    pub async fn topology(&self, count: usize) -> Result<Value> {
        self.wait_value(
            &format!("converged topology of {count} endpoints"),
            || async {
                let observation = self.get("/observation").await?;
                let topology = &observation["endpoints"];
                let slices: Api<EndpointSlice> =
                    Api::namespaced(self.client.clone(), &self.namespace);
                let mut eligible = BTreeSet::new();
                for slice in slices
                    .list(&ListParams::default().labels("kubernetes.io/service-name=backend"))
                    .await?
                    .items
                {
                    for endpoint in slice.endpoints {
                        if endpoint
                            .conditions
                            .as_ref()
                            .is_none_or(|c| c.ready != Some(false) && c.terminating != Some(true))
                        {
                            if let Some(uid) = endpoint.target_ref.and_then(|r| r.uid) {
                                eligible.insert(uid);
                            }
                        }
                    }
                }
                let actual: BTreeSet<_> = topology
                    .as_array()
                    .ok_or("topology array")?
                    .iter()
                    .filter_map(|b| b["uid"].as_str().map(str::to_owned))
                    .collect();
                Ok((actual.len() == count
                    && eligible == actual
                    && observation["converged"] == true)
                    .then_some(observation))
            },
        )
        .await
    }
    pub async fn observed(&self, count: usize) -> Result<Value> {
        self.wait_value(
            &format!("discovery submitted {count} endpoints"),
            || async {
                let sample = self.get("/observation").await?;
                Ok((sample["endpoints"]
                    .as_array()
                    .is_some_and(|e| e.len() == count)
                    && sample["submitted_current"] == true)
                    .then_some(sample))
            },
        )
        .await
    }
    pub async fn identity(&self, key: u64) -> Result<Value> {
        Ok(self
            .http
            .get("http://127.0.0.1:18080/proxy/identity")
            .header("x-request-key", key.to_string())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
    pub async fn wait<F, Fut>(&self, description: &str, mut condition: F) -> Result
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        self.wait_value(description, || {
            let future = condition();
            async move { Ok(future.await?.then_some(())) }
        })
        .await
    }
    pub async fn wait_value<T, F, Fut>(&self, description: &str, mut condition: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Option<T>>>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        let mut last_error = String::new();
        loop {
            match tokio::time::timeout_at(deadline, condition()).await {
                Ok(Ok(Some(value))) => return Ok(value),
                Ok(Err(error)) => last_error = error.to_string(),
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("timeout waiting for {description}; last error={last_error}; namespace={}; status={:?}; topology={:?}",
                    self.namespace, self.get("/status").await, self.get("/topology").await).into());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    pub async fn cleanup(self) -> Result {
        Api::<Namespace>::all(self.client.clone())
            .delete(&self.namespace, &DeleteParams::default())
            .await?;
        self.wait("namespace deleted and NodePort released", || async {
            Ok(Api::<Namespace>::all(self.client.clone())
                .get_opt(&self.namespace)
                .await?
                .is_none())
        })
        .await?;
        Ok(())
    }
    pub async fn assert_no_restart(&self, original: &Value) -> Result {
        assert_eq!(self.get("/status").await?["pod"], original["pod"]);
        let pods = Api::<Pod>::namespaced(self.client.clone(), &self.namespace)
            .list(&ListParams::default().labels("app=gateway"))
            .await?;
        assert!(pods.items.iter().all(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.container_statuses.as_ref())
                .is_some_and(|s| s.iter().all(|c| c.restart_count == 0))
        }));
        Ok(())
    }
    pub async fn all_routed_pods(&self) -> Result<BTreeSet<String>> {
        let observation = self.get("/observation").await?;
        let count = observation["endpoints"]
            .as_array()
            .ok_or("endpoints")?
            .len();
        let names: Vec<_> = (0..count)
            .map(|index| format!("endpoint-{index}"))
            .collect();
        let policy = netplane::routing::UniformRouting::try_new(names.clone())?;
        let mut keys = std::collections::BTreeMap::new();
        for key in 0..10_000_u64 {
            keys.entry(policy.select(key.to_string().as_bytes()).to_owned())
                .or_insert(key);
            if keys.len() == count {
                break;
            }
        }
        assert_eq!(
            keys.len(),
            count,
            "could not construct complete selector coverage"
        );
        let mut seen = BTreeSet::new();
        for key in keys.values() {
            seen.insert(
                self.identity(*key).await?["pod"]
                    .as_str()
                    .ok_or("identity")?
                    .to_owned(),
            );
        }
        Ok(seen)
    }
    pub async fn expected_pods(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .pods()
            .await?
            .iter()
            .map(ResourceExt::name_any)
            .collect())
    }
}
