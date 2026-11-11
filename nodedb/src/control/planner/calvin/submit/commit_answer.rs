// SPDX-License-Identifier: BUSL-1.1

//! The answer a COMMIT flush takes from a Calvin group that completed.
//!
//! Each statement of a block answered when it ran: its count, or its
//! `RETURNING` rows. The group commits after that, and the COMMIT returns
//! no rows. It reads only the timeseries install counts the participants
//! report, to raise a notice for the lines an install rejected.
//!
//! So a participant's rows past the result limit, two participants with
//! rows, or a reply that did not render never turns the committed group
//! into an error. An error there tells the client to retry a block that
//! applied, and the retry applies it twice.

use crate::bridge::envelope::Response;
use crate::control::state::CalvinAckResult;
use crate::engine::timeseries::install_counts::merge_count_payloads;

/// The merged install counts `ack_results` report, as a response. `None`
/// when no participant reports a count.
///
/// A report that does not decode adds no count. The COMMIT then raises its
/// notice from the counts its own resolve took.
pub(super) fn install_counts(ack_results: &[Vec<u8>]) -> Option<Response> {
    merged_counts(ack_results).map(super::local::synthetic_returning_response)
}

fn merged_counts(ack_results: &[Vec<u8>]) -> Option<Vec<u8>> {
    let mut held: Option<Vec<u8>> = None;
    for bytes in ack_results.iter().filter(|bytes| !bytes.is_empty()) {
        let Some(report) = CalvinAckResult::from_bytes(bytes) else {
            tracing::warn!(
                bytes = bytes.len(),
                "a Calvin participant's completion report does not decode; the COMMIT \
                 takes no install count from it"
            );
            continue;
        };
        if report.counts.is_empty() {
            continue;
        }
        held = match held {
            None => merge_count_payloads(&report.counts, &[]),
            Some(held) => merge_count_payloads(&held, &report.counts),
        };
    }
    held
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::state::{AckReply, AckReturning};

    fn report(result: CalvinAckResult) -> Vec<u8> {
        result.to_bytes_or_failed()
    }

    /// A rows report past the limit, two rows reports, and a failed reply
    /// carry no count, and none of them is an error.
    #[test]
    fn rows_over_limit_and_failed_reports_answer_no_count() {
        let reports = vec![
            report(CalvinAckResult {
                returning: Some(AckReturning::OverLimit {
                    bytes: 64,
                    limit: 16,
                }),
                ..CalvinAckResult::default()
            }),
            report(CalvinAckResult {
                returning: Some(AckReturning::Rows {
                    rows: vec![0x91, 0x01],
                }),
                ..CalvinAckResult::default()
            }),
            report(CalvinAckResult {
                returning: Some(AckReturning::Rows {
                    rows: vec![0x91, 0x02],
                }),
                ..CalvinAckResult::default()
            }),
            report(CalvinAckResult {
                reply: Some(AckReply::Failed {
                    detail: "render".into(),
                }),
                ..CalvinAckResult::default()
            }),
        ];
        assert!(install_counts(&reports).is_none());
    }

    fn counts(collection: &str, accepted: u64, rejected: u64) -> Vec<u8> {
        use crate::engine::timeseries::install_counts::{TsInstallCount, TsInstallCounts};
        TsInstallCounts::new(vec![TsInstallCount {
            collection: collection.into(),
            accepted,
            rejected,
        }])
        .to_bytes()
        .expect("encode install counts")
    }

    /// The counts of every participant merge, beside a rows report past the
    /// limit on one of them.
    #[test]
    fn counts_of_every_participant_merge_beside_rows_over_limit() {
        let reports = vec![
            report(CalvinAckResult {
                counts: counts("metrics_a", 3, 1),
                returning: Some(AckReturning::OverLimit {
                    bytes: 64,
                    limit: 16,
                }),
                ..CalvinAckResult::default()
            }),
            report(CalvinAckResult {
                counts: counts("metrics_b", 2, 0),
                ..CalvinAckResult::default()
            }),
        ];
        let answer = install_counts(&reports).expect("the merged counts");
        let merged = crate::engine::timeseries::install_counts::TsInstallCounts::from_payload(
            answer.payload.as_bytes(),
        )
        .expect("the answer holds install counts");
        let by_collection = merged.by_collection();
        assert_eq!(by_collection.get("metrics_a"), Some(&(3, 1)));
        assert_eq!(by_collection.get("metrics_b"), Some(&(2, 0)));
    }

    /// A report that does not decode and an empty report add nothing.
    #[test]
    fn unreadable_and_empty_reports_answer_no_count() {
        let reports = vec![
            crate::control::state::UNREADABLE_ACK_REPORT.to_vec(),
            Vec::new(),
        ];
        assert!(install_counts(&reports).is_none());
        assert!(install_counts(&[]).is_none());
    }
}
