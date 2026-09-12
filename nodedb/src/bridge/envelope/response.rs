// SPDX-License-Identifier: BUSL-1.1

//! Data -> Control response envelope and its per-row write-set entries.

use super::error_code::ErrorCode;
use super::payload::Payload;
use super::status::Status;
use crate::types::{Lsn, RequestId};
use nodedb_types::RowIdentity;

/// One row-level effect of an applied write, carried back from the Data Plane
/// so the Control Plane can mint a durable redo record *after* apply.
///
/// Populated only by write handlers whose autocommit path mints no WAL redo of
/// its own but whose effect must still survive a WAL-only restart — today, a
/// `PointUpdate` on a document collection carrying a secondary vector (HNSW)
/// index (see `data::executor::handlers::point::update`). `value` is the
/// post-image body for a put; empty and ignored when `is_delete`.
#[derive(Debug, Clone)]
pub struct WriteSetEntry {
    /// The row's stable global surrogate.
    pub surrogate: u32,
    /// The row's client identity, by the rule INSERT mints it with.
    ///
    /// The redo record journals this text as its `document_id`, so a WAL
    /// replay names the same row a live event names.
    pub identity: RowIdentity,
    /// `true` for a delete effect (no body), `false` for a put (post-image in
    /// `value`).
    pub is_delete: bool,
    /// Post-image body for a put; empty for a delete.
    pub value: Vec<u8>,
    /// Collection this entry's row belongs to.
    ///
    /// `None` means the statement's own collection, which is every entry a
    /// single-collection write produces. `Some(c)` marks a cross-collection
    /// side effect — a row written into a different collection as a
    /// consequence of this statement — whose redo record must name `c` rather
    /// than the plan's collection, and which homes to a different vShard.
    pub collection: Option<String>,
}

/// Response envelope: Data Plane -> Control Plane.
///
/// Every field is mandatory.
#[derive(Debug, Clone)]
pub struct Response {
    /// Echoed request identifier for correlation.
    pub request_id: RequestId,

    /// Outcome status.
    pub status: Status,

    /// Attempt number (for retry tracking).
    pub attempt: u32,

    /// Whether this is a partial result (more coming).
    pub partial: bool,

    /// Payload bytes produced by this response chunk.
    pub payload: Payload,

    /// Watermark LSN at the time of read (for snapshot consistency tracking).
    pub watermark_lsn: Lsn,

    /// Per-collection read-version LSN (the scanned collection's `coll_write_lsn`
    /// at read time, a WAL LSN) — the sound comparand for cross-shard OCC read
    /// validation. Distinct from `watermark_lsn` (core-global max, used for
    /// snapshot/SI reporting).
    ///
    /// On a WRITE response it is the POST-write version of the written
    /// collection (the handlers record before responding), which is how the Raft
    /// apply path returns a committed write's own version to its proposer.
    /// `Lsn::ZERO` when the plan names no single user collection.
    pub read_version_lsn: Lsn,

    /// Error code if status is not Ok.
    pub error_code: Option<Box<ErrorCode>>,

    /// Whether this response's originating transaction found its slice of the
    /// versioned read-set still current against the local write versions.
    /// `Some(true)` = still current (or no reads observed for this slice);
    /// `Some(false)` = at least one read was superseded; `None` = the response
    /// did not carry a read-set check (reads, control ops, and every
    /// non-transaction response).
    ///
    /// For the direct-apply (dependent/active, fast-path) path this is reporting
    /// only — the apply commits regardless. For a staged static Calvin
    /// transaction it is the LOCAL COMMIT VOTE: the scheduler flushes the staged
    /// buffer to base on `Some(true)` and drops it on `Some(false)`.
    pub read_set_valid: Option<bool>,

    /// Row-level effects the Control Plane must turn into durable redo records
    /// *after* the Data Plane applied them. Empty for every response that owns
    /// its durability on the pre-dispatch WAL path (the common case); non-empty
    /// only for post-apply-redo writes (see [`WriteSetEntry`]).
    pub write_set: Vec<WriteSetEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_ok() {
        let resp = Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(b"result".to_vec()),
            watermark_lsn: Lsn::new(42),
            error_code: None,
            read_set_valid: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        };
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.watermark_lsn, Lsn::new(42));
        assert_eq!(&*resp.payload, b"result");
    }

    #[test]
    fn response_error() {
        let resp = Response {
            request_id: RequestId::new(2),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: Some(Box::new(ErrorCode::DeadlineExceeded)),
            read_set_valid: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        };
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::DeadlineExceeded)
        );
    }
}
