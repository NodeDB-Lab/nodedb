// SPDX-License-Identifier: BUSL-1.1

//! Small response/error-shaping helpers shared by the Document and KV clone
//! write-interception paths.

use nodedb_types::{DatabaseId, Lsn};

use crate::bridge::envelope::{Response, Status};
use crate::types::RequestId;

/// Build a synthetic OK response reporting `affected` rows.
///
/// A copy-on-write clone satisfies a DELETE by recording a tombstone instead of
/// dispatching a row removal, so this is the only place that write's affected
/// count comes from. It has to be a real count: the rows a tombstone hides are
/// rows the statement removed from the clone's view, and a tombstone recorded
/// for a key that was never visible removed nothing.
pub(super) fn synthetic_affected_response(
    request_id: RequestId,
    watermark_lsn: Lsn,
    affected: u64,
) -> Response {
    // Built with the same infallible msgpack primitives the Data Plane's
    // `response_affected` uses, so the count can never go missing here.
    let mut payload = Vec::with_capacity(16);
    nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
    nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", affected as i64);
    Response {
        request_id,
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: payload.into(),
        watermark_lsn,
        error_code: None,
        stage_vote: None,
        read_versions: crate::types::ReadVersions::new(),
        write_set: Vec::new(),
    }
}

/// Strip the `"<db_id>/"` db-qualified prefix.
pub(super) fn strip_db_prefix(db_id: DatabaseId, qualified: &str) -> &str {
    if db_id == DatabaseId::DEFAULT {
        return qualified;
    }
    let prefix = format!("{}/", db_id.as_u64());
    qualified.strip_prefix(prefix.as_str()).unwrap_or(qualified)
}

/// Wrap a clone write-path failure as a typed dispatch error.
pub(super) fn write_err(msg: impl Into<String>) -> crate::Error {
    crate::Error::Dispatch { detail: msg.into() }
}
