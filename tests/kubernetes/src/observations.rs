//! Consume existing lifecycle events without changing public runtime APIs.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context};
#[derive(Clone, Default)]
pub struct Releases(
    pub Arc<Mutex<BTreeSet<u64>>>,
    pub Arc<Mutex<BTreeMap<String, String>>>,
);
#[derive(Default)]
struct Fields {
    released: bool,
    id: Option<u64>,
    processed: bool,
    collection: String,
    name: String,
    version: String,
}
impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "event" {
            self.released = value == "version.released";
            self.processed = value == "discovery.resource_processed";
        }
        match field.name() {
            "collection" => value.clone_into(&mut self.collection),
            "resource_name" => value.clone_into(&mut self.name),
            "resource_version" => value.clone_into(&mut self.version),
            _ => {}
        }
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "version_id" {
            self.id = Some(value);
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
impl<S: Subscriber> Layer<S> for Releases {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.processed {
            let mut versions = self
                .1
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if versions.len() < 256 {
                versions.insert(
                    format!("{}/{}", fields.collection, fields.name),
                    fields.version,
                );
                drop(versions);
            }
        }
        if fields.released {
            if let Some(id) = fields.id {
                let mut ids = self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if ids.len() < 256 {
                    ids.insert(id);
                }
            }
        }
    }
}

pub fn install() -> Releases {
    use tracing_subscriber::prelude::*;
    let releases = Releases::default();
    tracing_subscriber::registry()
        .with(
            releases
                .clone()
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    metadata.target() == "netplane" && *metadata.level() <= tracing::Level::DEBUG
                })),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        )
        .init();
    releases
}
