// SPDX-License-Identifier: BUSL-1.1

//! What a Calvin participant reports to its coordinator in its
//! `CompletionAck`.
//!
//! Every sequencer replica applies every `CompletionAck`, so the coordinator
//! reads a participant's report whether or not its own node holds a replica
//! of that participant. The report carries the participant's timeseries
//! install counts and, for a slice that holds the statement's own write, the
//! statement's answer: the rows it returns when it carries `RETURNING`, else
//! its plain answer with the affected count. Rows past the result limit
//! travel as their size alone, and the coordinator fails the statement with
//! the error a local `RETURNING` over the limit gives.

use crate::bridge::envelope::Response;
use crate::engine::timeseries::install_counts::TsInstallCounts;

/// The largest row payload one `CompletionAck` carries. The ack is a
/// sequencer log entry, and every entry travels in one RPC frame, so the
/// rows leave room in that frame for the rest of the entry.
pub const ACK_ROWS_FRAME_BUDGET: u64 = (nodedb_cluster::rpc_codec::MAX_RPC_PAYLOAD_SIZE as u64) / 2;

/// The `RETURNING` answer of a participant's slice.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum AckReturning {
    /// The encoded rows, as the install answered them.
    Rows { rows: Vec<u8> },
    /// The rows took `bytes`, past `limit`.
    OverLimit { bytes: u64, limit: u64 },
    /// The install stored the rows, and rendering them failed.
    Failed { detail: String },
}

/// A participant's report to its coordinator.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CalvinAckResult {
    /// The encoded [`TsInstallCounts`] of the apply. Empty when it installed
    /// no resolved timeseries batch.
    pub counts: Vec<u8>,
    /// The slice's `RETURNING` answer. `None` when no statement of the slice
    /// carries `RETURNING`.
    pub returning: Option<AckReturning>,
    /// The plain answer of a slice that holds the statement's own write and
    /// carries no `RETURNING`: its affected count. `None` for any other
    /// slice.
    pub reply: Option<AckReply>,
}

/// The plain answer of a primary-write slice.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum AckReply {
    /// The encoded answer, as the install answered it.
    Payload { payload: Vec<u8> },
    /// The install stored the writes, and rendering the answer failed.
    Failed { detail: String },
}

/// What a participant's slice answers for its statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckSlice {
    /// The slice holds the statement's own write, not only a derived one.
    pub primary_write: bool,
    /// The slice's statement carries `RETURNING`.
    pub returning: bool,
}

impl CalvinAckResult {
    /// The report of an install that answered `response` for `slice`. `limit`
    /// bounds the rows it carries.
    ///
    /// A primary-write slice always reports its answer: its rows, or its
    /// plain answer. So a coordinator on a node that hosts none of the
    /// transaction's participants still answers the statement with its
    /// affected count.
    pub fn of(response: &Response, slice: AckSlice, limit: u64) -> Self {
        let payload = response.payload.as_bytes();
        let counts = if TsInstallCounts::from_payload(payload).is_some() {
            payload.to_vec()
        } else {
            Vec::new()
        };
        let has_returning = slice.primary_write && slice.returning;
        let reply = (slice.primary_write && !slice.returning && counts.is_empty()).then(|| {
            match crate::control::local_dispatch::reject_data_plane_error(response) {
                Err(error) => AckReply::Failed {
                    detail: error.to_string(),
                },
                Ok(()) => AckReply::Payload {
                    payload: payload.to_vec(),
                },
            }
        });
        let returning = has_returning.then(|| {
            if let Err(error) = crate::control::local_dispatch::reject_data_plane_error(response) {
                return AckReturning::Failed {
                    detail: error.to_string(),
                };
            }
            let bytes = payload.len() as u64;
            if bytes > limit {
                AckReturning::OverLimit { bytes, limit }
            } else {
                AckReturning::Rows {
                    rows: payload.to_vec(),
                }
            }
        });
        Self {
            counts,
            returning,
            reply,
        }
    }

    /// Whether the report carries nothing for the coordinator.
    pub fn is_empty(&self) -> bool {
        self.counts.is_empty() && self.returning.is_none() && self.reply.is_none()
    }

    /// The encoded report. Empty for a report that carries nothing.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("calvin ack result: {e}"),
        })
    }

    /// The report `bytes` encode. `None` for empty or foreign bytes.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }
        zerompk::from_msgpack(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::engine::timeseries::install_counts::TsInstallCount;

    const PLAIN: AckSlice = AckSlice {
        primary_write: true,
        returning: false,
    };
    const RETURNING: AckSlice = AckSlice {
        primary_write: true,
        returning: true,
    };

    fn response(payload: Vec<u8>) -> Response {
        Response {
            request_id: crate::types::RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: crate::bridge::envelope::Payload::from_vec(payload),
            watermark_lsn: crate::types::Lsn::ZERO,
            error_code: None,
            stage_vote: None,
            read_versions: crate::types::ReadVersions::new(),
            write_set: Vec::new(),
        }
    }

    #[test]
    fn a_report_carries_counts_and_rows_within_the_limit() {
        let counts = TsInstallCounts::new(vec![TsInstallCount {
            collection: "cpu".into(),
            accepted: 1,
            rejected: 2,
        }])
        .to_bytes()
        .expect("encode counts");
        let report = CalvinAckResult::of(&response(counts.clone()), PLAIN, 1024);
        assert_eq!(report.counts, counts);
        assert!(report.returning.is_none());
        assert!(report.reply.is_none(), "the counts are the answer");
        let decoded =
            CalvinAckResult::from_bytes(&report.to_bytes().expect("encode")).expect("decode");
        assert_eq!(decoded, report);

        let rows = CalvinAckResult::of(&response(vec![0x90; 8]), RETURNING, 1024);
        assert_eq!(
            rows.returning,
            Some(AckReturning::Rows {
                rows: vec![0x90; 8]
            })
        );
        let decoded =
            CalvinAckResult::from_bytes(&rows.to_bytes().expect("encode")).expect("decode");
        assert_eq!(decoded, rows);
    }

    #[test]
    fn rows_past_the_limit_travel_as_their_size() {
        let report = CalvinAckResult::of(&response(vec![0x90; 64]), RETURNING, 16);
        assert_eq!(
            report.returning,
            Some(AckReturning::OverLimit {
                bytes: 64,
                limit: 16
            })
        );
    }

    /// A derived slice, such as an implicit edge beside the statement's own
    /// write, reports nothing.
    #[test]
    fn a_derived_slice_reports_nothing() {
        let derived = AckSlice {
            primary_write: false,
            returning: false,
        };
        let report = CalvinAckResult::of(&response(vec![0x81]), derived, 16);
        assert!(report.to_bytes().expect("encode").is_empty());
        assert!(CalvinAckResult::from_bytes(&[]).is_none());
    }

    /// A primary plain write reports its answer, so a coordinator that
    /// hosts no participant reads its affected count.
    #[test]
    fn a_primary_plain_write_reports_its_answer() {
        let count = crate::data::executor::response_codec::encode_count("affected", 3)
            .expect("encode count");
        let report = CalvinAckResult::of(&response(count.clone()), PLAIN, 16);
        assert_eq!(
            report.reply,
            Some(AckReply::Payload {
                payload: count.clone()
            })
        );
        let decoded =
            CalvinAckResult::from_bytes(&report.to_bytes().expect("encode")).expect("decode");
        assert_eq!(decoded, report);
    }
}
