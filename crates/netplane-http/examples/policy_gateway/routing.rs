use http::Method;

use super::{HttpRequest, config::PolicyConfig};

pub fn valid_request_key(key: &[u8]) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && key
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

pub fn canary_bucket(key: &[u8]) -> u8 {
    let hash = key.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (hash % 100) as u8
}

pub fn select_backend(request: &HttpRequest, policy: &PolicyConfig) -> &'static str {
    if request.uri().path() != "/process" || request.method() != Method::POST {
        return "unmatched";
    }
    let Some(key) = request.headers().get("x-request-key") else {
        return "invalid";
    };
    if !valid_request_key(key.as_bytes()) {
        return "invalid";
    }
    if canary_bucket(key.as_bytes()) < policy.canary_percent {
        "canary"
    } else {
        "stable"
    }
}
