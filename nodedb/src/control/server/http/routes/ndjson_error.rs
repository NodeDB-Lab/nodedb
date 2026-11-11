// SPDX-License-Identifier: BUSL-1.1

//! In-band NDJSON error lines.
//!
//! Every NDJSON error line is `{"error": "...", "code": "NDB-<n>"}` plus a
//! newline. The code is the public error code an HTTP JSON error body
//! carries, so a client tells error classes apart without parsing the text.
//! The error of statement `k` of a request also carries `sqlstate`,
//! `statement` and `position`.

use nodedb_types::NodeDbError;

use crate::control::gateway::GatewayErrorMap;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::shared::statement_exec::StatementAt;

/// The error object for `e`: the gateway message and the public code.
pub(super) fn error_value(e: &crate::Error) -> serde_json::Value {
    let (_status, message) = GatewayErrorMap::to_http(e);
    coded_value(&message, &crate::error_classify::classify(e))
}

/// The NDJSON error line for `e`.
pub(super) fn error_line(e: &crate::Error) -> String {
    line(error_value(e))
}

/// The error object for `e`, raised by statement `at` of the request text
/// `sql`.
///
/// An error with no statement answers [`error_value`]. An error with a
/// statement also answers `sqlstate`, `statement` (1-based) and `position`.
/// `position` is the 1-based character offset where the statement starts.
pub(super) fn statement_error_value(
    e: &crate::Error,
    at: Option<StatementAt>,
    sql: &str,
) -> serde_json::Value {
    let mut value = error_value(e);
    let Some(at) = at else {
        return value;
    };
    if let Some(object) = value.as_object_mut() {
        let (_severity, sqlstate, _message) = error_to_sqlstate(e);
        object.insert("sqlstate".to_owned(), sqlstate.into());
        object.insert("statement".to_owned(), at.number().into());
        object.insert("position".to_owned(), at.position(sql).into());
    }
    value
}

/// [`statement_error_value`] as one NDJSON line.
pub(super) fn statement_error_line(e: &crate::Error, at: Option<StatementAt>, sql: &str) -> String {
    line(statement_error_value(e, at, sql))
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
    fn an_error_of_the_second_statement_answers_its_sqlstate_and_statement() {
        let value = parse(&statement_error_line(
            &dispatch_error(),
            StatementAt::of(1, 2),
            "SELECT 1; SELECT 2",
        ));
        assert_eq!(value["code"], "NDB-9002", "{value}");
        assert_eq!(
            value["sqlstate"],
            error_to_sqlstate(&dispatch_error()).1,
            "{value}"
        );
        assert_eq!(value["statement"], 2, "{value}");
        assert_eq!(value["position"], 11, "{value}");
    }

    #[test]
    fn an_error_with_no_statement_answers_the_error_value() {
        let value = statement_error_value(&dispatch_error(), None, "SELECT 1");
        assert_eq!(value, error_value(&dispatch_error()));
    }

    #[test]
    fn the_error_value_carries_the_public_code() {
        let value = error_value(&dispatch_error());
        assert_eq!(value["code"], "NDB-9002");
        assert!(value["error"].is_string());
    }
}
