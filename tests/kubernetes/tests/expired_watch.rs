//! Exercise kube-rs HTTP decoding and 410 recovery without cluster compaction.
use axum::{
    Json, Router,
    body::Body,
    extract::{Query, State},
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use kube::{Client, Config};
use netplane_kubernetes::{Discovery, DiscoveryConfig};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tower::BoxError;

#[derive(Clone)]
struct ApiState {
    lists: Arc<AtomicUsize>,
    service_lists: Arc<AtomicUsize>,
    expire: watch::Receiver<bool>,
}
fn service() -> Value {
    json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"backend","namespace":"test","uid":"service","resourceVersion":"1"},
        "spec":{"ports":[{"name":"http","port":80}]}})
}
fn slice(name: &str, ip: &str) -> Value {
    json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":name,"namespace":"test","uid":name,
        "resourceVersion":"2","labels":{"kubernetes.io/service-name":"backend"},
        "ownerReferences":[{"apiVersion":"v1","kind":"Service","name":"backend","uid":"service"}]},
        "addressType":"IPv4","ports":[{"name":"http","port":8080,"protocol":"TCP"}],
        "endpoints":[{"addresses":[ip],"conditions":{"ready":true},"targetRef":{"kind":"Pod","uid":ip}}]})
}
fn pending_watch() -> Response {
    Body::from_stream(stream::pending::<Result<Bytes, Infallible>>()).into_response()
}
async fn services(
    State(state): State<ApiState>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if query.get("watch").is_some_and(|v| v == "true") {
        if state.service_lists.load(Ordering::SeqCst) > 1 {
            return pending_watch();
        }
        let mut expire = state.expire;
        let error = stream::once(async move {
            if !*expire.borrow_and_update() {
                let _ = expire.changed().await;
            }
            Ok::<_, Infallible>(Bytes::from(format!(
                "{}\n",
                json!({"type":"ERROR","object":{
                "apiVersion":"v1","kind":"Status","status":"Failure","reason":"InternalError","message":"interrupted watch","code":500}})
            )))
        });
        return Body::from_stream(error.chain(stream::pending())).into_response();
    }
    state.service_lists.fetch_add(1, Ordering::SeqCst);
    Json(json!({"apiVersion":"v1","kind":"ServiceList","metadata":{"resourceVersion":"1"},"items":[service()]})).into_response()
}
async fn slices(
    State(state): State<ApiState>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if query.get("watch").is_some_and(|v| v == "true") {
        if state.lists.load(Ordering::SeqCst) > 1 {
            return pending_watch();
        }
        let mut expire = state.expire;
        let error = stream::once(async move {
            if !*expire.borrow_and_update() {
                let _ = expire.changed().await;
            }
            Ok::<_, Infallible>(Bytes::from(format!(
                "{}\n",
                json!({"type":"ERROR","object":{
                "apiVersion":"v1","kind":"Status","status":"Failure","reason":"Expired","message":"expired resource version","code":410}})
            )))
        });
        return Body::from_stream(error.chain(stream::pending())).into_response();
    }
    let call = state.lists.fetch_add(1, Ordering::SeqCst);
    let items = if call == 0 {
        vec![slice("old", "10.0.0.1"), slice("retained", "10.0.0.2")]
    } else {
        vec![
            slice("retained", "10.0.0.2"),
            slice("duplicate", "10.0.0.2"),
        ]
    };
    Json(json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{"resourceVersion":if call == 0 {"1"} else {"2"}},"items":items})).into_response()
}
#[tokio::test]
async fn expired_watch_relists_complete_snapshot_without_duplicates() -> Result<(), BoxError> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (expire, rx) = watch::channel(false);
        let state = ApiState {
            lists: Arc::new(AtomicUsize::new(0)),
            service_lists: Arc::new(AtomicUsize::new(0)),
            expire: rx,
        };
        let router = Router::new()
            .route("/api/v1/namespaces/test/services", get(services))
            .route(
                "/apis/discovery.k8s.io/v1/namespaces/test/endpointslices",
                get(slices),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let client = Client::try_from(Config::new(format!("http://{address}").parse()?))?;
        let (discovery, handle) = Discovery::new(
            client,
            DiscoveryConfig {
                namespace: "test".into(),
                service_names: ["backend".into()].into(),
            },
        )?;
        let stop = CancellationToken::new();
        let task = tokio::spawn(discovery.run(stop.clone()));
        let mut snapshots = handle.subscribe();
        loop {
            if handle.snapshot().is_some() {
                break;
            }
            snapshots.changed().await?;
        }
        let initial = handle.snapshot().ok_or("initialized snapshot")?;
        assert_eq!(
            initial
                .services
                .values()
                .next()
                .ok_or("service")?
                .endpoints
                .len(),
            2
        );
        expire.send_replace(true);
        loop {
            let snapshot = handle.snapshot().ok_or("snapshot preserved")?;
            let endpoints = &snapshot
                .services
                .values()
                .next()
                .ok_or("service preserved")?
                .endpoints;
            // Latest-value receivers can miss intermediate values. Exhaustive
            // partial-cache coverage remains in the crate's acknowledged-feed tests.
            assert!(!endpoints.is_empty(), "observed a partial relist");
            if snapshot.revision > initial.revision {
                assert_eq!(endpoints.len(), 1);
                assert_eq!(endpoints[0].address.to_string(), "10.0.0.2");
                break;
            }
            snapshots.changed().await?;
        }
        assert!(state.lists.load(Ordering::SeqCst) >= 2);
        let mut health = handle.subscribe_status();
        loop {
            let relisted = state.service_lists.load(Ordering::SeqCst) >= 2;
            let watching = handle.status().services == netplane_kubernetes::WatchStatus::Watching;
            if relisted && watching {
                break;
            }
            health.changed().await?;
        }
        assert!(
            state.service_lists.load(Ordering::SeqCst) >= 2,
            "quiet Service collection was not relisted"
        );
        stop.cancel();
        task.await??;
        server.abort();
        Ok::<_, BoxError>(())
    })
    .await?
}
