// SPDX-License-Identifier: BUSL-1.1

//! The statement's answer from a completed Calvin transaction: the local
//! participants' deposits and every participant's completion report.

use nodedb_cluster::calvin::types::TxClass;

use super::local::synthetic_returning_response;
use crate::Error;
use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::server::shared::sql::staging_predicates::extract_affected_count;
use crate::control::state::CalvinApplyResult;

/// The applied answer the coordinator hands back, from what every
/// participant's `CompletionAck` reported.
///
/// The sidecar holds the applies of the participants this node hosts
/// replicas of. The acks reach this node from every participant:
/// - their timeseries install counts replace the sidecar's plain answer,
///   and stand alone when no local participant deposited one. A plain slice's
///   count joins them;
/// - every `RETURNING` participant's rows answer the statement, a local
///   deposit in place of its participant's report. Several participants
///   answer the union of their frozen replies, in statement order (see
///   `super::returning_answer`);
/// - every primary-write participant's plain answer holds the affected count of
///   its own slice. The sum of them answers the statement, in place of the one
///   slice a local deposit knows.
///
/// So a coordinator that hosts none of the participants answers with the
/// same result as one that hosts them all.
///
/// Under [`ReplyFold::SumOwnedEdges`] the answer is the sum of every home's
/// owned-edge count instead.
pub(crate) fn with_reported_results(
    applied: Option<CalvinApplyResult>,
    ack_results: &[Vec<u8>],
    fold: ReplyFold,
) -> crate::Result<Option<Response>> {
    use crate::control::state::{AckReply, AckReturning, CalvinAckResult};
    use crate::engine::timeseries::install_counts::{add_plain_count, merge_count_payloads};
    let CalvinApplyResult {
        plain: local_plain,
        returning: local_rows,
    } = applied.unwrap_or_default();
    // An installed txn whose reply failed to render deposits it as an error
    // for the statement.
    for response in local_plain.iter().chain(local_rows.values()) {
        crate::control::local_dispatch::reject_data_plane_error(response)?;
    }
    let mut reports: Vec<CalvinAckResult> = Vec::with_capacity(ack_results.len());
    for bytes in ack_results.iter().filter(|bytes| !bytes.is_empty()) {
        // A participant's count that does not read leaves the statement's
        // count unknown, so it is never a smaller number.
        reports.push(
            CalvinAckResult::from_bytes(bytes).ok_or_else(|| Error::Internal {
                detail: format!(
                    "a Calvin participant's completion report of {} bytes does not decode, so \
                     the statement's count is unknown; read the rows to see what it wrote",
                    bytes.len()
                ),
            })?,
        );
    }
    if fold == ReplyFold::SumOwnedEdges {
        return sum_owned_edges(&reports);
    }
    let plain = summed_replies(&reports)?;
    let counts = reports
        .iter()
        .filter(|report| !report.counts.is_empty())
        .fold(None::<Vec<u8>>, |held, report| match held {
            None => merge_count_payloads(&report.counts, &[]),
            Some(held) => merge_count_payloads(&held, &report.counts),
        });
    // A plain slice beside a timeseries install adds its count to the install's.
    let counts = match (counts, plain.as_deref().and_then(extract_affected_count)) {
        (Some(counts), Some(affected)) => {
            Some(add_plain_count(&counts, affected).ok_or_else(|| {
                Error::Internal {
                    detail: "a Calvin transaction's install counts did not encode with its plain \
                         count; read the rows to see what it wrote"
                        .to_owned(),
                }
            })?)
        }
        (counts, _) => counts,
    };
    let reported: Vec<&AckReturning> = reports
        .iter()
        .filter_map(|report| report.returning.as_ref())
        .collect();
    let rows = super::returning_answer::returning_answer(local_rows, &reported)?;
    Ok(match (local_plain, rows, counts) {
        (_, Some(rows), _) => Some(rows),
        (Some(response), None, Some(counts)) => Some(Response {
            payload: crate::bridge::envelope::Payload::from_vec(counts),
            ..response
        }),
        (Some(response), None, None) => Some(match plain {
            Some(payload) => Response {
                payload: crate::bridge::envelope::Payload::from_vec(payload),
                ..response
            },
            None => response,
        }),
        (None, None, Some(counts)) => Some(synthetic_returning_response(counts)),
        (None, None, None) => plain
            .or_else(|| {
                reports.iter().find_map(|report| match &report.reply {
                    Some(AckReply::Payload { payload }) => Some(payload.clone()),
                    Some(AckReply::Failed { .. }) | None => None,
                })
            })
            .map(synthetic_returning_response),
    })
}

/// The sum of the plain answers `reports` hold, in participant order. Each
/// participant's answer is the one its redo entry fixed, so every coordinator
/// reads the same counts. `None` when no answer carries a count. An install
/// that stored its writes and failed to render its answer fails the statement.
fn summed_replies(
    reports: &[crate::control::state::CalvinAckResult],
) -> crate::Result<Option<Vec<u8>>> {
    use crate::control::server::shared::sql::affected_payload::merge_affected_payloads;
    use crate::control::state::AckReply;
    let mut total: Option<Vec<u8>> = None;
    for report in reports {
        match &report.reply {
            Some(AckReply::Failed { detail }) => {
                return Err(Error::Internal {
                    detail: format!("calvin reply: {detail}"),
                });
            }
            Some(AckReply::Payload { payload }) if extract_affected_count(payload).is_some() => {
                total = Some(match total {
                    Some(held) => {
                        merge_affected_payloads(&held, payload).ok_or_else(|| Error::Internal {
                            detail: "calvin reply: two affected counts did not add".into(),
                        })?
                    }
                    None => payload.clone(),
                });
            }
            Some(AckReply::Payload { .. }) | None => {}
        }
    }
    Ok(total)
}

/// How the coordinator folds its participants' answers into the
/// statement's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyFold {
    /// Every participant that holds the statement's own write answers its own
    /// slice's count, and the statement's count is their sum. One participant's
    /// `RETURNING` rows answer in place of a count.
    SumSlices,
    /// The transaction writes edges and nothing of its own. Each home
    /// counts only the edges it owns, so the sum of every home's count is
    /// the statement's count, each edge counted once.
    SumOwnedEdges,
}

impl ReplyFold {
    /// The fold of a transaction over `plans`.
    pub(crate) fn of_plans<'a>(plans: impl IntoIterator<Item = &'a PhysicalPlan> + Clone) -> Self {
        let writes_edges = plans
            .clone()
            .into_iter()
            .any(crate::control::planner::calvin::is_edge_write);
        if writes_edges
            && !crate::control::planner::calvin::write_class::plans_have_user_write(plans)
        {
            Self::SumOwnedEdges
        } else {
            Self::SumSlices
        }
    }

    /// The fold of `tx_class`, whose decoded plans are `plans`. A class
    /// split into parts reads its manifest, and one with no edge key reads
    /// nothing.
    pub(crate) fn of_prepared(tx_class: &TxClass, plans: &[PhysicalPlan]) -> Self {
        if !super::edge_slices::writes_edges(tx_class) {
            return Self::SumSlices;
        }
        if let Some(manifest) = &tx_class.multi_part {
            return if manifest.user_write {
                Self::SumSlices
            } else {
                Self::SumOwnedEdges
            };
        }
        Self::of_plans(plans.iter())
    }
}

/// The sum of every home's owned-edge count, as the statement's answer.
fn sum_owned_edges(
    reports: &[crate::control::state::CalvinAckResult],
) -> crate::Result<Option<Response>> {
    use crate::control::state::AckReply;
    let mut total: Option<u64> = None;
    for report in reports {
        match &report.reply {
            Some(AckReply::Payload { payload }) => {
                let count = extract_affected_count(payload).ok_or_else(|| Error::Internal {
                    detail: "calvin edge write: a home's answer carries no affected count"
                        .to_owned(),
                })?;
                total = Some(total.unwrap_or(0).saturating_add(count));
            }
            Some(AckReply::Failed { detail }) => {
                return Err(Error::Internal {
                    detail: format!("calvin reply: {detail}"),
                });
            }
            None => {}
        }
    }
    let Some(total) = total else {
        return Ok(None);
    };
    let count = usize::try_from(total).map_err(|_| Error::Internal {
        detail: format!("calvin edge write: a count of {total} edges does not fit"),
    })?;
    let payload = crate::data::executor::response_codec::encode_count("affected", count)?;
    Ok(Some(synthetic_returning_response(payload)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::state::{AckReturning, CalvinAckResult};
    use crate::engine::timeseries::install_counts::{TsInstallCount, TsInstallCounts};

    fn counts(collection: &str, accepted: u64, rejected: u64) -> Vec<u8> {
        TsInstallCounts::new(vec![TsInstallCount {
            collection: collection.into(),
            accepted,
            rejected,
        }])
        .to_bytes()
        .expect("encode install counts")
    }

    fn ack(counts: Vec<u8>, returning: Option<AckReturning>) -> Vec<u8> {
        CalvinAckResult {
            counts,
            returning,
            reply: None,
        }
        .to_bytes()
        .expect("encode ack result")
    }

    /// A sidecar entry that holds the plain answer `response`.
    fn plain(response: Response) -> Option<CalvinApplyResult> {
        Some(CalvinApplyResult {
            plain: Some(response),
            ..CalvinApplyResult::default()
        })
    }

    /// A sidecar entry that holds one participant's `RETURNING` answer.
    fn returning(response: Response) -> Option<CalvinApplyResult> {
        Some(CalvinApplyResult {
            returning: std::collections::BTreeMap::from([(0, response)]),
            ..CalvinApplyResult::default()
        })
    }

    fn replied(affected: usize) -> Vec<u8> {
        let payload = crate::data::executor::response_codec::encode_count("affected", affected)
            .expect("encode count");
        CalvinAckResult {
            counts: Vec::new(),
            returning: None,
            reply: Some(crate::control::state::AckReply::Payload { payload }),
        }
        .to_bytes()
        .expect("encode ack result")
    }

    /// A Calvin write whose coordinator hosts no participant vShard answers
    /// with the affected count its primary participant reported in its ack.
    /// The acks hold every participant's slice, so they stand over the one
    /// slice a local deposit knows.
    #[test]
    fn a_coordinator_with_no_participant_answers_the_reported_count() {
        use crate::control::server::shared::sql::staging_predicates::extract_affected_count;
        let answer = with_reported_results(
            None,
            &[ack(Vec::new(), None), replied(3)],
            ReplyFold::SumSlices,
        )
        .expect("a report")
        .expect("an answer");
        assert_eq!(extract_affected_count(answer.payload.as_bytes()), Some(3));

        let local_payload = crate::data::executor::response_codec::encode_count("affected", 5)
            .expect("encode count");
        let local = synthetic_returning_response(local_payload);
        let answer = with_reported_results(plain(local), &[replied(3)], ReplyFold::SumSlices)
            .expect("a report")
            .expect("an answer");
        assert_eq!(extract_affected_count(answer.payload.as_bytes()), Some(3));
    }

    /// Each participant's slice answers its own count, so the statement's
    /// count is their sum. A slice that matched no row adds nothing.
    #[test]
    fn plain_answers_of_several_participants_add_up() {
        use crate::control::server::shared::sql::staging_predicates::extract_affected_count;
        let local = synthetic_returning_response(
            crate::data::executor::response_codec::encode_count("affected", 1)
                .expect("encode count"),
        );
        let answer = with_reported_results(
            plain(local),
            &[replied(1), replied(0), replied(2)],
            ReplyFold::SumSlices,
        )
        .expect("a report")
        .expect("an answer");
        assert_eq!(extract_affected_count(answer.payload.as_bytes()), Some(3));

        let answer = with_reported_results(None, &[replied(0), replied(4)], ReplyFold::SumSlices)
            .expect("a report")
            .expect("an answer");
        assert_eq!(extract_affected_count(answer.payload.as_bytes()), Some(4));
    }

    /// A report that does not decode leaves the count unknown, so it fails the
    /// statement instead of answering a smaller count.
    #[test]
    fn an_unreadable_report_fails_the_statement() {
        let unreadable = crate::control::state::UNREADABLE_ACK_REPORT.to_vec();
        assert!(
            with_reported_results(None, &[replied(1), unreadable], ReplyFold::SumSlices).is_err()
        );
    }

    /// A plain slice's count joins a timeseries slice's install counts, and
    /// `accepted` stays the rows the installs stored.
    #[test]
    fn a_plain_slice_beside_a_timeseries_install_adds_its_count() {
        use crate::control::server::shared::sql::staging_predicates::extract_affected_count;
        let answer = with_reported_results(
            None,
            &[counted("cpu", 2, 1), replied(3)],
            ReplyFold::SumSlices,
        )
        .expect("a report")
        .expect("an answer");
        assert_eq!(extract_affected_count(answer.payload.as_bytes()), Some(5));
        let reported = decoded(&answer);
        assert_eq!((reported.accepted, reported.rejected), (2, 1));
    }

    /// An install that failed to render its answer fails the statement, even
    /// beside a local deposit.
    #[test]
    fn a_failed_answer_fails_the_statement() {
        let failed = CalvinAckResult {
            counts: Vec::new(),
            returning: None,
            reply: Some(crate::control::state::AckReply::Failed {
                detail: "render".to_owned(),
            }),
        }
        .to_bytes()
        .expect("encode ack result");
        let local = synthetic_returning_response(
            crate::data::executor::response_codec::encode_count("affected", 1)
                .expect("encode count"),
        );
        assert!(
            with_reported_results(plain(local), &[replied(1), failed], ReplyFold::SumSlices)
                .is_err()
        );
    }

    /// An edge write with no write of its own sums every home's owned
    /// count, whatever a local home deposited: each edge counts once, at
    /// its owner.
    #[test]
    fn an_edge_only_write_sums_its_homes_owned_counts() {
        use crate::control::server::shared::sql::staging_predicates::extract_affected_count;
        let local = synthetic_returning_response(
            crate::data::executor::response_codec::encode_count("affected", 2)
                .expect("encode count"),
        );
        let answer = with_reported_results(
            plain(local),
            &[replied(2), replied(0), replied(5)],
            ReplyFold::SumOwnedEdges,
        )
        .expect("a report")
        .expect("an answer");
        assert_eq!(extract_affected_count(answer.payload.as_bytes()), Some(7));
    }

    fn counted(collection: &str, accepted: u64, rejected: u64) -> Vec<u8> {
        ack(counts(collection, accepted, rejected), None)
    }

    fn decoded(response: &Response) -> TsInstallCounts {
        TsInstallCounts::from_payload(response.payload.as_bytes()).expect("install counts")
    }

    /// With one replica per vShard, the coordinator's node hosts no replica of
    /// either participant, so its sidecar holds nothing. The counts both
    /// participants' acks carried still reach the statement.
    #[test]
    fn remote_participants_report_their_counts_through_their_acks() {
        let answer = with_reported_results(
            None,
            &[counted("cpu", 2, 1), counted("mem", 1, 3)],
            ReplyFold::SumSlices,
        )
        .expect("a report")
        .expect("an answer");
        let reported = decoded(&answer);
        assert_eq!((reported.accepted, reported.rejected), (3, 4));
        assert_eq!(reported.by_collection().get("mem"), Some(&(1, 3)));
    }

    /// A local participant's sidecar answer takes every participant's
    /// counts. A `RETURNING` answer keeps its rows. A write whose acks carry
    /// no counts keeps its answer, or has none.
    #[test]
    fn acked_counts_replace_a_plain_answer_and_keep_rows() {
        let local = synthetic_returning_response(counts("cpu", 2, 0));
        let answer = with_reported_results(
            plain(local),
            &[counted("cpu", 2, 0), counted("mem", 0, 2)],
            ReplyFold::SumSlices,
        )
        .expect("a report")
        .expect("an answer");
        assert_eq!(decoded(&answer).rejected, 2);

        let rows = synthetic_returning_response(vec![0x90]);
        let reported = ack(
            counts("cpu", 1, 1),
            Some(AckReturning::Rows { rows: vec![0x90] }),
        );
        let answer = with_reported_results(returning(rows), &[reported], ReplyFold::SumSlices)
            .expect("a report")
            .expect("an answer");
        assert_eq!(answer.payload.as_bytes(), &[0x90]);

        assert!(
            with_reported_results(None, &[], ReplyFold::SumSlices)
                .expect("a report")
                .is_none()
        );
    }

    /// A participant on another node answers a `RETURNING` statement with
    /// the rows its ack carried, next to a plain participant's counts.
    #[test]
    fn remote_rows_answer_the_statement() {
        let rows = vec![0x91, 0x01];
        let answer = with_reported_results(
            plain(synthetic_returning_response(Vec::new())),
            &[
                ack(
                    counts("cpu", 1, 1),
                    Some(AckReturning::Rows { rows: rows.clone() }),
                ),
                counted("mem", 2, 0),
            ],
            ReplyFold::SumSlices,
        )
        .expect("a report")
        .expect("an answer");
        assert_eq!(answer.payload.as_bytes(), rows.as_slice());
    }

    /// Remote rows past the limit fail the statement with the error a local
    /// `RETURNING` over the limit gives. Two participants whose rows are no
    /// frozen replies cannot form a union.
    #[test]
    fn remote_rows_past_the_limit_or_unfrozen_rows_of_two_participants_fail() {
        let over = ack(
            Vec::new(),
            Some(AckReturning::OverLimit {
                vshard: 0,
                bytes: 64,
                limit: 16,
            }),
        );
        match with_reported_results(None, &[over], ReplyFold::SumSlices) {
            Err(Error::ExecutionLimitExceeded { detail }) => assert_eq!(
                detail,
                "query result exceeded max_query_result_bytes (64 > 16 bytes)"
            ),
            other => panic!("expected the result limit error, got {other:?}"),
        }

        let one = ack(Vec::new(), Some(AckReturning::Rows { rows: vec![0x90] }));
        match with_reported_results(None, &[one.clone(), one], ReplyFold::SumSlices) {
            Err(Error::Internal { detail }) => {
                assert!(detail.contains("no frozen reply"), "{detail}")
            }
            other => panic!("expected the union error, got {other:?}"),
        }
    }
}
