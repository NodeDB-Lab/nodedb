// SPDX-License-Identifier: BUSL-1.1

//! In-band NDJSON error lines.
//!
//! Every NDJSON error line is `{"error": "...", "code": "NDB-<n>"}` plus a
//! newline. The code is the public error code an HTTP JSON error body
//! carries, so a client tells error classes apart without parsing the text.

use nodedb_types::NodeDbError;

use crate::control::gateway::GatewayErrorMap;

/// The error object for `e`: the gateway message and the public code.
pub(super) fn error_value(e: &crate::Error) -> serde_json::Value {
    let (_status, message) = GatewayErrorMap::to_http(e);
    coded_value(&message, &crate::error_classify::classify(e))
}

/// The NDJSON error line for `e`.
pub(super) fn error_line(e: &crate::Error) -> String {
    line(error_value(e))
}

/// The NDJSON error line for `e`, with `context` before its message. The
/// code stays the error's own.
pub(super) fn error_line_in_context(context: &str, e: &crate::Error) -> String {
    let (_status, message) = GatewayErrorMap::to_http(e);
    line(coded_value(
        &format!("{context}: {message}"),
        &crate::error_classify::classify(e),
    ))
}

/// The NDJSON error line for a shaping error, with the shaper's own code.
pub(super) fn shape_error_line(e: &NodeDbError) -> String {
    line(coded_value(e.message(), e))
}

/// `{"error": message, "code": "NDB-<n>"}`, the code taken from `coded`.
fn coded_value(message: &str, coded: &NodeDbError) -> serde_json::Value {
    serde_json::json!({ "error": message, "code": coded.code().to_string() })
}

/// One NDJSON line: the object and a newline.
fn line(value: serde_json::Value) -> String {
    format!("{value}\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> serde_json::Value {
        assert!(line.ends_with('\n'), "an NDJSON line ends with a newline");
        sonic_rs::from_str(line.trim_end()).expect("error line is JSON")
    }

    fn dispatch_error() -> crate::Error {
        crate::Error::Dispatch {
            detail: "boom".into(),
        }
    }

    #[test]
    fn a_line_carries_the_public_code() {
        let value = parse(&error_line(&dispatch_error()));
        assert_eq!(value["code"], "NDB-9002");
        let message = value["error"].as_str().expect("message is text");
        assert!(
            message.contains("boom"),
            "message keeps the detail: {message}"
        );
    }

    #[test]
    fn a_context_line_prefixes_the_message_and_keeps_the_code() {
        let value = parse(&error_line_in_context(
            "malformed response batch",
            &dispatch_error(),
        ));
        assert_eq!(value["code"], "NDB-9002");
        let message = value["error"].as_str().expect("message is text");
        assert!(
            message.starts_with("malformed response batch: "),
            "context comes first: {message}"
        );
        assert!(
            message.contains("boom"),
            "message keeps the detail: {message}"
        );
    }

    #[test]
    fn a_shape_error_line_keeps_the_shaper_code() {
        let value = parse(&shape_error_line(&NodeDbError::division_by_zero()));
        assert_eq!(value["code"], "NDB-1204");
        assert_eq!(value["error"], "division by zero");

        let value = parse(&shape_error_line(&NodeDbError::data_exception(
            "column \"ts\" holds an integer where a timestamp is required",
        )));
        assert_eq!(value["code"], "NDB-1208");
    }

    #[test]
    fn the_error_value_carries_the_public_code() {
        let value = error_value(&dispatch_error());
        assert_eq!(value["code"], "NDB-9002");
        assert!(value["error"].is_string());
    }
}
