mod hardening;
mod health;
mod integration;
mod lifecycle;
mod observations;
mod publication_races;
mod recovery;
mod support;
mod topology;
mod translation;

use crate::DiscoveryConfig;
use k8s_openapi::api::{core::v1::Service, discovery::v1::EndpointSlice};
use serde_json::json;
use std::collections::BTreeSet;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;
fn config() -> DiscoveryConfig {
    DiscoveryConfig {
        namespace: "default".into(),
        service_names: BTreeSet::new(),
    }
}
fn service() -> std::result::Result<Service, serde_json::Error> {
    serde_json::from_value(
        json!({"metadata":{"namespace":"default","name":"backend","uid":"uid-1"},"spec":{"ports":[{"name":"http","port":80}]}}),
    )
}
fn slice() -> std::result::Result<EndpointSlice, serde_json::Error> {
    serde_json::from_value(
        json!({"metadata":{"namespace":"default","name":"slice-1","uid":"slice-uid","labels":{"kubernetes.io/service-name":"backend"},"ownerReferences":[{"apiVersion":"v1","kind":"Service","name":"backend","uid":"uid-1"}]},"addressType":"IPv4","ports":[{"name":"http","port":8080}],"endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":true}}]}),
    )
}
