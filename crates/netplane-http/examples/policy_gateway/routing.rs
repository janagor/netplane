use http::Method;

use netplane::WeightedRouting;

use super::HttpRequest;

pub fn valid_request_key(key: &[u8]) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && key
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

pub fn select_backend<'a>(request: &HttpRequest, policy: &'a WeightedRouting) -> &'a str {
    if request.uri().path() != "/process" || request.method() != Method::POST {
        return "unmatched";
    }
    let Some(key) = request.headers().get("x-request-key") else {
        return "invalid";
    };
    if !valid_request_key(key.as_bytes()) {
        return "invalid";
    }
    policy.select(key.as_bytes())
}
