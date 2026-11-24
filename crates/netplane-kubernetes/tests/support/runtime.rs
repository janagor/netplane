use bytes::Bytes;
use http::{Request, Response};
use netplane::{ControllerOptions, Netplane, ReconfigurationController, RuntimeLimits};
use netplane_control::{GraphPreparer, Reconciler};
use netplane_kubernetes::*;
use std::{collections::BTreeMap, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};
use tower::BoxError;

pub type Result<T = ()> = std::result::Result<T, BoxError>;
pub type Controller = ReconfigurationController<Request<Bytes>, Response<Bytes>>;
pub type Control = Reconciler<DesiredState, HttpGraphPreparer, Request<Bytes>, Response<Bytes>>;
pub fn desired(addresses: &[SocketAddr]) -> Result<DesiredState> {
    desired_for_uid(addresses, "pod")
}
pub fn desired_for_uid(addresses: &[SocketAddr], uid: &str) -> Result<DesiredState> {
    let key = ServiceKey {
        namespace: "default".into(),
        name: "backend".into(),
    };
    let snapshot = DiscoverySnapshot {
        revision: 1,
        services: BTreeMap::from([(
            key.clone(),
            DiscoveredService {
                key,
                uid: "service-1".into(),
                ports: vec![ServicePort {
                    name: Some("http".into()),
                    port: 80,
                    protocol: TransportProtocol::Tcp,
                    app_protocol: None,
                }],
                publish_not_ready_addresses: false,
                endpoints: addresses
                    .iter()
                    .map(|address| DiscoveredEndpoint {
                        address: address.ip(),
                        port: address.port(),
                        target_uid: Some(format!("{uid}-{address}")),
                        port_name: Some("http".into()),
                        protocol: TransportProtocol::Tcp,
                        conditions: EndpointConditions::default(),
                    })
                    .collect(),
            },
        )]),
    };
    Ok(translate(
        &snapshot,
        &ServiceBinding {
            namespace: "default".into(),
            service_name: "backend".into(),
            port: PortSelector::Number(80),
            backend_capacity: NonZeroUsize::MIN,
        },
    )?)
}
pub fn preparer() -> HttpGraphPreparer {
    HttpGraphPreparer::new(
        UpstreamLimits {
            exchange_timeout: Duration::from_secs(3),
            ..UpstreamLimits::default()
        },
        |request| request.uri().path().as_bytes(),
    )
}
pub async fn runtime() -> Result<(Arc<Controller>, Control)> {
    let preparer = preparer();
    let runtime = Netplane::new(
        preparer.prepare(desired(&[])?).await?,
        RuntimeLimits {
            max_outstanding: 16,
        },
    )?;
    let controller = Arc::new(ReconfigurationController::new(
        runtime,
        ControllerOptions {
            max_retained_versions: 0,
        },
    ));
    let reconciler = Reconciler::new(Arc::clone(&controller), preparer);
    Ok((controller, reconciler))
}
pub fn request() -> Request<Bytes> {
    Request::new(Bytes::new())
}
