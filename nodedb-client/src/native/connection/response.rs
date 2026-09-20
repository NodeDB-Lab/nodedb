//! Response-frame helpers: I/O error mapping, error-frame surfacing, result decoding.

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{NativeResponse, ResponseStatus};
use nodedb_types::result::QueryResult;

pub(super) fn io_err(e: std::io::Error) -> NodeDbError {
    NodeDbError::sync_connection_failed(format!("I/O: {e}"))
}

/// Surface an error frame as a typed error.
///
/// Every operation that reads a response must call this before interpreting
/// it: `send` returns an error frame as `Ok(resp)` (the frame arrived; it is
/// the *server* that refused), so an operation that ignores `status` reports
/// success for work the server rejected, and one that reads `rows` from it
/// reads an empty result — indistinguishable from "no such row".
pub(crate) fn check_error(resp: &NativeResponse) -> NodeDbResult<()> {
    if resp.status == ResponseStatus::Error {
        return Err(error_frame_to_typed(resp.error.as_ref(), "unknown error"));
    }
    Ok(())
}

/// Rebuild the server's typed error from an error frame.
///
/// The numeric NodeDB code is the authoritative classification: SQLSTATE is
/// many-to-one, so reconstructing from it would collapse a duplicate key and
/// a duplicate idempotency key into one condition and everything unclassified
/// into `XX000`. A `0` code means the peer predates that field, and guessing
/// is worse than reporting a generic internal failure — so only then does the
/// error become `internal`.
fn error_frame_to_typed(
    payload: Option<&nodedb_types::protocol::ErrorPayload>,
    fallback: &str,
) -> NodeDbError {
    let Some(payload) = payload else {
        return NodeDbError::internal(fallback);
    };
    if payload.ndb_code == 0 {
        return NodeDbError::internal(payload.message.clone());
    }
    NodeDbError::from_wire(
        nodedb_types::error::ErrorCode(payload.ndb_code),
        payload.message.clone(),
    )
}

pub(super) fn response_to_query_result(resp: NativeResponse) -> NodeDbResult<QueryResult> {
    if resp.status == ResponseStatus::Error {
        return Err(error_frame_to_typed(resp.error.as_ref(), "query failed"));
    }
    Ok(QueryResult {
        columns: resp.columns.unwrap_or_default(),
        rows: resp.rows.unwrap_or_default(),
        rows_affected: resp.rows_affected.unwrap_or(0),
        command: resp.command,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_to_query_result_ok() {
        let resp = NativeResponse::from_query_result(
            1,
            QueryResult {
                columns: vec!["x".into()],
                rows: vec![vec![nodedb_types::Value::Integer(42)]],
                rows_affected: 0,
                command: None,
            },
            0,
        );
        let qr = response_to_query_result(resp).unwrap();
        assert_eq!(qr.columns, vec!["x"]);
        assert_eq!(qr.rows[0][0].as_i64(), Some(42));
    }

    #[test]
    fn response_to_query_result_error() {
        let resp = NativeResponse::error(1, "42P01", "not found");
        let err = response_to_query_result(resp).unwrap_err();
        assert!(format!("{err}").contains("not found"));
    }

    #[test]
    fn check_error_ok() {
        let resp = NativeResponse::ok(1);
        assert!(check_error(&resp).is_ok());
    }

    #[test]
    fn check_error_fail() {
        let resp = NativeResponse::error(1, "XX000", "boom");
        assert!(check_error(&resp).is_err());
    }
}
