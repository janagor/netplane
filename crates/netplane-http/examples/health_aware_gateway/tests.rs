use super::*;
use netplane::health::{HealthOutcome, HealthProbe};

async fn start_upstreams() -> Result<
    (
        JoinSet<std::io::Result<()>>,
        Vec<(String, HttpUpstream, HealthHandle)>,
    ),
    BoxError,
> {
    let mut servers = JoinSet::new();
    let registry = HealthRegistry::default();
    let client = upstream::client(Duration::from_secs(2))?;
    let mut upstreams = Vec::new();
    let policy = HealthPolicy {
        failure_threshold: NonZeroU32::MIN,
        recovery_threshold: NonZeroU32::new(2).ok_or("threshold")?,
        recovery_delay: Duration::ZERO,
    };
    for name in ["backend-a", "backend-b", "backend-c"] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        servers.spawn(async move { axum::serve(listener, upstream_router(name)).await });
        let upstream = HttpUpstream::new(client.clone(), &format!("http://{address}"), 1024)?;
        let health = registry.register_with_mode(
            BackendIdentity {
                service: "test".into(),
                instance: name.into(),
                incarnation: "test-start".into(),
            },
            policy.clone(),
            HealthMode::Active,
        )?;
        let probe = HttpHealthProbe::new(upstream.clone(), "/health")?;
        for _ in 0..2 {
            let token = health.begin_probe();
            health.report(token, probe.check().await);
        }
        assert!(health.is_eligible());
        upstreams.push((name.into(), upstream, health));
    }
    Ok((servers, upstreams))
}

#[tokio::test]
async fn real_upstreams_fail_recover_and_keep_one_publication() -> Result<(), BoxError> {
    let (mut servers, upstreams) = start_upstreams().await?;
    let mut runtime = Netplane::new(
        build_graph(upstreams.clone())?,
        RuntimeLimits {
            max_outstanding: 64,
        },
    )?;
    let publication = runtime.active_version_id();
    let weights =
        WeightedRouting::try_new([("backend-a", 60), ("backend-b", 30), ("backend-c", 10)])?;
    let key = (0_u64..1000)
        .map(|key| key.to_string())
        .find(|key| weights.select(key.as_bytes()) == "backend-b")
        .ok_or("B key")?;
    let request = || -> Result<Request<Bytes>, BoxError> {
        Ok(Request::builder()
            .header("x-routing-key", &key)
            .body(Bytes::new())?)
    };
    assert_eq!(
        runtime.handle().execute(request()?).await?.body(),
        "backend-b"
    );
    let (_, b, b_health) = &upstreams[1];
    b.execute(
        Request::builder()
            .method("POST")
            .uri("/fail")
            .body(Bytes::new())?,
    )
    .await?;
    assert_eq!(
        runtime.handle().execute(request()?).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(!b_health.is_eligible());
    for _ in 0..32 {
        assert_ne!(
            runtime.handle().execute(request()?).await?.body(),
            "backend-b"
        );
    }
    assert_eq!(runtime.active_version_id(), publication);
    b.execute(
        Request::builder()
            .method("POST")
            .uri("/recover")
            .body(Bytes::new())?,
    )
    .await?;
    let probe = HttpHealthProbe::new(b.clone(), "/health")?;
    for _ in 0..2 {
        let token = b_health.begin_probe();
        b_health.report(token, probe.check().await);
    }
    assert!(b_health.is_eligible());
    assert_eq!(
        runtime.handle().execute(request()?).await?.body(),
        "backend-b"
    );
    for (_, _, health) in &upstreams {
        let token = health.begin_probe();
        health.report(token, HealthOutcome::Failure);
    }
    let error = runtime
        .handle()
        .execute(request()?)
        .await
        .err()
        .ok_or("expected unavailable")?;
    assert_eq!(
        upstream::map_error(&error).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(runtime.active_version_id(), publication);
    runtime.shutdown(Duration::from_secs(1)).await?;
    servers.shutdown().await;
    Ok(())
}
