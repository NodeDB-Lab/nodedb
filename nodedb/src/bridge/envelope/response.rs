// SPDX-License-Identifier: BUSL-1.1

//! Data -> Control response envelope and its per-row write-set entries.

use super::error_code::ErrorCode;
use super::payload::Payload;
use super::stage_vote::StageVote;
use super::status::Status;
use crate::types::{Lsn, RequestId};
use nodedb_types::RowIdentity;

/// One row-level effect of an applied document write, carried back from the
/// Data Plane so the Control Plane journals it *after* apply.
///
/// Every document and edge write handler reports every row it stores or
/// removes that its pre-dispatch WAL record does not carry exactly: the
/// post-image of an update, a bulk or batch write, a derived
/// materialized-sum target row, the stamped image of a versioned row, and an
/// edge version or tombstone at its decided ordinal. The Control
/// Plane journals the entries, in entry order, as the parts of the write's
/// record group, and WAL replay applies them in LSN order.
#[derive(Debug, Clone)]
pub struct WriteSetEntry {
    /// The row's stable global surrogate.
    pub surrogate: u32,
    /// The row's client identity, by the rule INSERT mints it with.
    ///
    /// The redo record journals this text as its `document_id`, so a WAL
    /// replay names the same row a live event names.
    pub identity: RowIdentity,
    /// What the write did to the row.
    pub effect: RowEffect,
    /// Collection this entry's row belongs to.
    ///
    /// `None` means the statement's own collection, which is every entry a
    /// single-collection write produces. `Some(c)` marks a cross-collection
    /// side effect — a row written into a different collection as a
    /// consequence of this statement — whose redo record must name `c` rather
    /// than the plan's collection, and which homes to a different vShard.
    pub collection: Option<String>,
}

/// The effect one [`WriteSetEntry`] reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowEffect {
    /// The row holds `value` now. `value` is the MessagePack body the write
    /// path encodes into storage, never a strict Binary Tuple: replay hands
    /// it to that same encoder.
    Put {
        value: Vec<u8>,
        /// The version key of a row on a `bitemporal=true` collection.
        version: Option<RowVersion>,
    },
    /// The row is gone.
    Delete {
        /// The system time of the tombstone of a row on a `bitemporal=true`
        /// collection.
        system_from_ms: Option<i64>,
    },
    /// Replay must not apply the write's own pre-dispatch record: the apply
    /// wrote nothing, or another entry of this write set carries the row's
    /// exact image. The Control Plane cancels that record with a
    /// `WriteAborted` marker once every image of the write set is appended.
    CancelForward,
    /// A graph edge version the apply wrote, at the ordinal it decided. The
    /// entry's collection is `None`: an edge record homes to the write's own
    /// vShard.
    Edge(EdgeImage),
}

/// The graph edge versions an apply wrote, in the payload shape their WAL
/// sub-record carries. Every ordinal is the one the apply decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeImage {
    /// One edge version. `system_from` is always `Some`.
    Put(crate::wal::EdgePutRedo),
    /// One edge tombstone. `system_from` is always `Some`.
    Delete(crate::wal::EdgeDeleteRedo),
}

/// The version key a `bitemporal=true` row landed at. Replay installs the row
/// at exactly this key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowVersion {
    pub sys_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
}

impl RowVersion {
    /// The key of a version valid for all time, written at `sys_from_ms`.
    pub fn open(sys_from_ms: i64) -> Self {
        Self {
            sys_from_ms,
            valid_from_ms: i64::MIN,
            valid_until_ms: i64::MAX,
        }
    }
}

impl WriteSetEntry {
    /// The row identified by `surrogate` now holds `value`.
    pub fn put(surrogate: u32, identity: RowIdentity, value: Vec<u8>) -> Self {
        Self {
            surrogate,
            identity,
            effect: RowEffect::Put {
                value,
                version: None,
            },
            collection: None,
        }
    }

    /// The row identified by `surrogate` is gone.
    pub fn delete(surrogate: u32, identity: RowIdentity) -> Self {
        Self {
            surrogate,
            identity,
            effect: RowEffect::Delete {
                system_from_ms: None,
            },
            collection: None,
        }
    }

    /// The write's pre-dispatch record for the row identified by `surrogate`
    /// must not replay.
    pub fn cancel_forward(surrogate: u32, identity: RowIdentity) -> Self {
        Self {
            surrogate,
            identity,
            effect: RowEffect::CancelForward,
            collection: None,
        }
    }

    /// The edge version an apply wrote. The entry names the edge by its
    /// source endpoint.
    pub fn edge(image: EdgeImage) -> Self {
        let (src_surrogate, src_id) = match &image {
            EdgeImage::Put(put) => (put.src_surrogate, put.src_id.as_str()),
            EdgeImage::Delete(delete) => (delete.src_surrogate, delete.src_id.as_str()),
        };
        Self {
            surrogate: src_surrogate,
            identity: RowIdentity::from_user_key(src_id),
            effect: RowEffect::Edge(image),
            collection: None,
        }
    }

    /// This entry, at the version a `bitemporal=true` collection wrote it at.
    /// `None` leaves the entry unversioned.
    pub fn versioned(mut self, version: Option<RowVersion>) -> Self {
        match &mut self.effect {
            RowEffect::Put { version: slot, .. } => *slot = version,
            RowEffect::Delete { system_from_ms } => {
                *system_from_ms = version.map(|v| v.sys_from_ms);
            }
            RowEffect::CancelForward | RowEffect::Edge(_) => {}
        }
        self
    }

    /// This entry, naming a row of `collection` rather than the statement's.
    pub fn in_collection(mut self, collection: String) -> Self {
        self.collection = Some(collection);
        self
    }
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

    /// The local commit vote of a Calvin stage response.
    ///
    /// Every Calvin stage path sets it. `None` means the response is not a
    /// Calvin stage outcome. It never means an unknown vote.
    pub stage_vote: Option<StageVote>,

    /// Row-level effects the Control Plane turns into durable redo records
    /// *after* the Data Plane applied them (see [`WriteSetEntry`]). Empty for
    /// a response whose pre-dispatch WAL record carries its whole effect.
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
            stage_vote: None,
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
            stage_vote: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        };
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::DeadlineExceeded)
        );
    }
}
