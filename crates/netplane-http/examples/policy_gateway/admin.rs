use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};

use super::control::{
    ControlError, GatewayControl, OutcomeView, ReconfigureRequest, RollbackRequest, StatusView,
};

#[derive(Clone)]
struct AdminState {
    control: Arc<GatewayControl>,
    token: Arc<str>,
}

pub fn router(control: Arc<GatewayControl>, token: String) -> Router {
    let state = AdminState {
        control,
        token: token.into(),
    };
    Router::new()
        .route("/status", get(status))
        .route("/reconfigure", post(reconfigure))
        .route("/rollback", post(rollback))
        .route("/shutdown", post(shutdown))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(state)
}

#[derive(Debug)]
enum AdminError {
    Unauthorized,
    BadRequest,
    TooLarge,
    Control(ControlError),
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Control(ControlError::InvalidPolicy) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Control(ControlError::Conflict) => StatusCode::CONFLICT,
            Self::Control(ControlError::ShuttingDown) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Control(ControlError::NotFound) => StatusCode::NOT_FOUND,
            Self::Control(ControlError::Internal) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        status.into_response()
    }
}

impl From<ControlError> for AdminError {
    fn from(value: ControlError) -> Self {
        Self::Control(value)
    }
}

fn authenticated(headers: &HeaderMap, expected: &str) -> Result<(), AdminError> {
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or("");
    let expected = expected.as_bytes();
    let supplied = supplied.as_bytes();
    let mut difference = expected.len() ^ supplied.len();
    for index in 0..expected.len().max(supplied.len()) {
        difference |= usize::from(
            expected.get(index).copied().unwrap_or(0) ^ supplied.get(index).copied().unwrap_or(0),
        );
    }
    if difference == 0 {
        Ok(())
    } else {
        Err(AdminError::Unauthorized)
    }
}

fn json_error(error: &JsonRejection) -> AdminError {
    if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
        AdminError::TooLarge
    } else {
        AdminError::BadRequest
    }
}

async fn status(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<StatusView>, AdminError> {
    authenticated(&headers, &state.token)?;
    Ok(Json(state.control.status().await?))
}

async fn reconfigure(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<ReconfigureRequest>, JsonRejection>,
) -> Result<Json<OutcomeView>, AdminError> {
    authenticated(&headers, &state.token)?;
    Ok(Json(
        state
            .control
            .reconfigure(body.map_err(|error| json_error(&error))?.0)
            .await?,
    ))
}

async fn rollback(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<RollbackRequest>, JsonRejection>,
) -> Result<Json<OutcomeView>, AdminError> {
    authenticated(&headers, &state.token)?;
    Ok(Json(
        state
            .control
            .rollback(body.map_err(|error| json_error(&error))?.0)
            .await?,
    ))
}

async fn shutdown(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<StatusCode, AdminError> {
    authenticated(&headers, &state.token)?;
    state.control.shutdown().await?;
    Ok(StatusCode::ACCEPTED)
}
