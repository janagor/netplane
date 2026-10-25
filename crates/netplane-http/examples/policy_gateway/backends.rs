use std::convert::Infallible;

use bytes::Bytes;
use http::{HeaderValue, Method, Response, StatusCode};
use tower::{ServiceBuilder, ServiceExt, service_fn, util::BoxService};

use super::{
    HttpRequest, HttpResponse,
    config::{BackendBehavior, PolicyConfig},
};

fn response(status: StatusCode, body: Bytes) -> HttpResponse {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response
}

fn processing_backend(
    name: &'static str,
    behavior: BackendBehavior,
    revision: HeaderValue,
) -> BoxService<HttpRequest, HttpResponse, Infallible> {
    let service = service_fn(move |request: HttpRequest| {
        let behavior = behavior.clone();
        async move {
            if !behavior.delay.is_zero() {
                tokio::time::sleep(behavior.delay).await;
            }
            if behavior.fail {
                return Ok::<_, Infallible>(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::new(),
                ));
            }
            Ok(response(
                StatusCode::OK,
                Bytes::from(format!(
                    "backend={name};processed_bytes={}",
                    request.body().len()
                )),
            ))
        }
    });
    ServiceBuilder::new()
        .map_response(move |mut response: HttpResponse| {
            response
                .headers_mut()
                .insert("x-backend", HeaderValue::from_static(name));
            response
                .headers_mut()
                .insert("x-policy-revision", revision.clone());
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain"),
            );
            response
        })
        .service(service)
        .boxed()
}

pub fn stable_backend(policy: &PolicyConfig) -> BoxService<HttpRequest, HttpResponse, Infallible> {
    processing_backend(
        "stable",
        policy.stable.clone(),
        revision_header(policy.revision),
    )
}

pub fn canary_backend(policy: &PolicyConfig) -> BoxService<HttpRequest, HttpResponse, Infallible> {
    processing_backend(
        "canary",
        policy.canary.clone(),
        revision_header(policy.revision),
    )
}

fn revision_header(revision: u64) -> HeaderValue {
    HeaderValue::from_str(&revision.to_string()).unwrap_or_else(|_| HeaderValue::from_static("0"))
}

pub fn fallback_backend(
    policy: &PolicyConfig,
) -> BoxService<HttpRequest, HttpResponse, Infallible> {
    let revision = revision_header(policy.revision);
    ServiceBuilder::new()
        .map_response(move |mut response: HttpResponse| {
            response
                .headers_mut()
                .insert("x-backend", HeaderValue::from_static("fallback"));
            response
                .headers_mut()
                .insert("x-policy-revision", revision.clone());
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain"),
            );
            response
        })
        .service(service_fn(|request: HttpRequest| async move {
            let status = if request.uri().path() != "/process" {
                StatusCode::NOT_FOUND
            } else if request.method() != Method::POST {
                StatusCode::METHOD_NOT_ALLOWED
            } else {
                StatusCode::BAD_REQUEST
            };
            let mut response = response(status, Bytes::new());
            if status == StatusCode::METHOD_NOT_ALLOWED {
                response
                    .headers_mut()
                    .insert(http::header::ALLOW, HeaderValue::from_static("POST"));
            }
            Ok::<_, Infallible>(response)
        }))
        .boxed()
}
