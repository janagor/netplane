//! Validate independently updated observations before exposing a fixture sample.
use super::App;
use axum::http::StatusCode;
use netplane_kubernetes::{IntegrationStatus, WatchStatus, translate};
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};

pub fn observation(app: &App) -> Result<Value, StatusCode> {
    let observations = app.discovery.subscribe_observations();
    for _ in 0..16 {
        // Acknowledgements must precede the control state they synchronize:
        // never combine a newer processed-resource marker with an older state.
        let processed_resources = app
            .releases
            .1
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let discovery = observations.borrow().clone();
        let integration = app.integration.borrow().clone();
        let reconciliation = app.reconciliation.borrow().clone();
        let inspection = app.controller.inspect();
        let revision = discovery.snapshot.as_ref().map(|s| s.revision);
        let endpoints = discovery
            .snapshot
            .as_ref()
            .and_then(|s| translate(s, &app.binding).ok())
            .map(|d| {
                d.backends()
                    .iter()
                    .map(|b| json!({"uid": b.id.target_uid, "address": b.id.address.to_string()}))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let submitted = match integration {
            IntegrationStatus::Submitted {
                revision, sequence, ..
            } => Some((revision, sequence)),
            _ => None,
        };
        let healthy = discovery.status.services == WatchStatus::Watching
            && discovery.status.endpoint_slices == WatchStatus::Watching;
        let submitted_current = submitted.is_some_and(|(r, _)| Some(r) == revision);
        let converged = healthy
            && submitted_current
            && reconciliation.is_converged()
            && submitted.is_some_and(|(_, s)| Some(s) == reconciliation.applied_revision)
            && inspection.active.as_ref().map(|v| v.id) == reconciliation.active_publication;
        let runtime = app.controller.handle().status();
        let sample = json!({
            "discovery_revision": revision,
            "submitted_revision": submitted.map(|(r,_)|r),
            "submitted_sequence": submitted.map(|(_,s)|s),
            "submitted_current": submitted_current,
            "endpoints": endpoints,
            "converged": converged,
            "desired_revision": reconciliation.desired_revision,
            "applied_revision": reconciliation.applied_revision,
            "publication_id": inspection.active.as_ref().map(|v|v.id.get()),
            "retained_publications": inspection.retained.len(),
            "phase": format!("{:?}", reconciliation.phase),
            "discovery_state": format!("{:?}", discovery.status),
            "outstanding_requests": runtime.outstanding,
            "runtime": format!("{:?}", runtime.lifecycle),
            "background_tasks_stopped": app.stopped.load(Ordering::SeqCst),
            "fault_disconnected": app.fault.disconnected(),
            "active_api_tunnels": app.fault.active(),
            "active_preparations": app.preparations.active.load(Ordering::SeqCst),
            "maximum_preparations": app.preparations.maximum.load(Ordering::SeqCst),
            "preparation_paused": *app.preparations.paused.borrow(),
            "prepared_sizes": app.preparations.sizes.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
            "released_publications": app.releases.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).iter().copied().collect::<Vec<_>>(),
            "processed_resources": processed_resources,
            "pod": std::env::var("POD_NAME").unwrap_or_default(),
        });
        // Include synchronization-sensitive diagnostics in the collection window.
        // Compare the whole reconciliation transition sequence, publication and
        // discovery Arc around collection. Holding the Arc prevents pointer ABA.
        if !Arc::ptr_eq(&discovery, &observations.borrow())
            || integration != *app.integration.borrow()
            || reconciliation != *app.reconciliation.borrow()
            || inspection != app.controller.inspect()
        {
            continue;
        }
        return Ok(sample);
    }
    Err(StatusCode::SERVICE_UNAVAILABLE)
}
