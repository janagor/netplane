//! Parse complete HTTP/1.1 responses from the loopback test client.

use std::io;

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Response, StatusCode, header};
use tower::BoxError;

pub fn parse(raw: &[u8]) -> Result<Response<Bytes>, BoxError> {
    let mut parsed_headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Response::new(&mut parsed_headers);
    let offset = match parsed.parse(raw)? {
        httparse::Status::Complete(offset) => offset,
        httparse::Status::Partial => return Err(invalid("incomplete HTTP response")),
    };
    let status = StatusCode::from_u16(parsed.code.ok_or_else(|| invalid("missing status"))?)?;
    let mut response = Response::new(Bytes::new());
    *response.status_mut() = status;
    for field in parsed.headers.iter() {
        response.headers_mut().append(
            HeaderName::from_bytes(field.name.as_bytes())?,
            HeaderValue::from_bytes(field.value)?,
        );
    }

    let wire_body = &raw[offset..];
    let body = if response
        .headers()
        .get(header::TRANSFER_ENCODING)
        .is_some_and(|value| {
            value.to_str().is_ok_and(|text| {
                text.split(',')
                    .any(|coding| coding.trim().eq_ignore_ascii_case("chunked"))
            })
        }) {
        decode_chunked(wire_body)?
    } else {
        if let Some(length) = response.headers().get(header::CONTENT_LENGTH) {
            let expected: usize = length.to_str()?.parse()?;
            if wire_body.len() != expected {
                return Err(invalid("content length mismatch"));
            }
        }
        Bytes::copy_from_slice(wire_body)
    };
    *response.body_mut() = body;
    Ok(response)
}

fn decode_chunked(mut wire: &[u8]) -> Result<Bytes, BoxError> {
    let mut decoded = Vec::new();
    loop {
        let line_end = wire
            .windows(2)
            .position(|pair| pair == b"\r\n")
            .ok_or_else(|| invalid("missing chunk size terminator"))?;
        let size_text = std::str::from_utf8(&wire[..line_end])?;
        let size = usize::from_str_radix(size_text.split(';').next().unwrap_or(""), 16)?;
        wire = &wire[line_end + 2..];
        if size == 0 {
            break;
        }
        let end = size
            .checked_add(2)
            .ok_or_else(|| invalid("chunk size overflow"))?;
        if wire.len() < end || &wire[size..end] != b"\r\n" {
            return Err(invalid("incomplete chunk"));
        }
        decoded.extend_from_slice(&wire[..size]);
        wire = &wire[end..];
    }
    Ok(Bytes::from(decoded))
}

fn invalid(message: &'static str) -> BoxError {
    io::Error::new(io::ErrorKind::InvalidData, message).into()
}

#[test]
fn decodes_chunked_wire_body() -> Result<(), BoxError> {
    let wire = b"HTTP/1.1 201 Created\r\nTransfer-Encoding: chunked\r\nX-Test: okay\r\n\r\n4\r\ntest\r\n0\r\n\r\n";
    let response = parse(wire)?;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()["x-test"], "okay");
    assert_eq!(response.body(), &Bytes::from_static(b"test"));
    Ok(())
}
