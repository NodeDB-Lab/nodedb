// SPDX-License-Identifier: BUSL-1.1

//! Local submit-and-await primitive for the Calvin cross-shard write path.
//!
//! PRECONDITION for everything here: this node is the sequencer-group leader.
//! Its sequencer service assigns and its registry receives the replicated
//! completion ack. A submit on a non-leader is drained and discarded, so
//! non-leader callers must route through [`super::routed`].

use std::time::Duration;

use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::calvin::{
    Assignment, AssignmentReceiver, AttemptOutcome, CalvinCompletionRegistry, TxnId,
};

use super::stream::{PartStream, StreamTarget, stream_parts};
use crate::Error;
use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::planner::calvin::abort_error::calvin_abort_error;
use crate::control::state::{CalvinApplyResult, SharedState};

/// Build a minimal Control-Plane [`Response`] carrying only the RETURNING
/// `payload` bytes forwarded over the cross-node routed-submit RPC.
///
/// The coordinator only reads `.payload` (and derives the plan kind from the
/// task) when shaping RETURNING rows, so the other fields are placeholders: the
/// authoritative status/LSN already lived on the leader that applied the txn.
pub(super) fn synthetic_returning_response(payload_bytes: Vec<u8>) -> Response {
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::{Lsn, RequestId};

    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Payload::from_vec(payload_bytes),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        stage_vote: None,
        read_versions: crate::types::ReadVersions::new(),
        write_set: Vec::new(),
    }
}

/// Raise `tx_class`'s metadata floor to this node's metadata floor, which
/// covers the batch being applied now (see `AppliedIndexWatcher::floor`).
///
/// The coordinator stamps the catalog it planned against. The leader raises
/// it again, which only makes replicas wait longer. Each replica's scheduler
/// holds the transaction until its own metadata apply reached the floor.
pub(crate) fn raise_metadata_floor(state: &SharedState, tx_class: &mut TxClass) {
    let floor = state
        .applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID)
        .floor();
    tx_class.metadata_floor = tx_class.metadata_floor.max(floor);
}

/// Stamp the incarnation this node's catalog holds for every user collection
/// `tx_class`'s plans name. The coordinator stamps first, against the catalog
/// it planned with. A forwarded submit arrives stamped and keeps its stamps.
pub(crate) fn stamp_incarnations(state: &SharedState, tx_class: &mut TxClass) -> crate::Result<()> {
    // A split class was stamped by its coordinator before the split, and no
    // longer carries its plans in `plans`.
    if !tx_class.incarnations.is_empty() || tx_class.is_multi_part() {
        return Ok(());
    }
    let plans =
        nodedb_physical::physical_plan::wire::decode_batch(&tx_class.plans).map_err(|e| {
            Error::Serialization {
                format: "msgpack".into(),
                detail: format!("calvin incarnation stamp: plan decode: {e}"),
            }
        })?;
    let mut named = std::collections::BTreeSet::new();
    for plan in &plans {
        // Array cell writes route by the array's own incarnation.
        if matches!(plan, PhysicalPlan::Array(_) | PhysicalPlan::ClusterArray(_)) {
            continue;
        }
        named.extend(plan.named_collections().into_iter().map(str::to_owned));
    }
    let catalog = state.credentials.catalog();
    let tenant_id = tx_class.tenant_id.as_u64();
    let mut incarnations = Vec::with_capacity(named.len());
    for collection in named {
        let incarnation = catalog.incarnation_of(tx_class.database_id, tenant_id, &collection)?;
        incarnations.push(nodedb_cluster::calvin::types::CalvinIncarnation {
            collection,
            incarnation,
        });
    }
    tx_class.set_incarnations(incarnations);
    Ok(())
}

/// Await the sequencer assignment of submission `inbox_seq`, bounded by
/// `timeout`.
///
/// A closed channel means the sequencer rejected or discarded the submission
/// without sequencing it: a read/write cycle in its epoch, a leadership
/// change, or a halted sequencer. Nothing applied, so the caller gets a
/// retryable refusal. On timeout the registration is dropped, so no sender
/// stays behind. A timed-out submission can still be sequenced later.
pub(crate) async fn await_assignment(
    registry: &CalvinCompletionRegistry,
    inbox_seq: u64,
    assignment_rx: AssignmentReceiver,
    timeout: Duration,
) -> crate::Result<Assignment> {
    match tokio::time::timeout(timeout, assignment_rx).await {
        Ok(Ok(assignment)) => Ok(assignment),
        Ok(Err(_)) => Err(Error::RetryableRefusal {
            reason: "the Calvin sequencer did not sequence the transaction; nothing was \
                     applied"
                .to_owned(),
        }),
        Err(_) => {
            registry.drop_assignment(inbox_seq);
            Err(Error::Internal {
                detail: "timed out waiting for Calvin sequencer assignment".to_owned(),
            })
        }
    }
}

/// Submit `tx_class` to THIS node's Calvin sequencer inbox and await completion.
///
/// PRECONDITION: this node is the sequencer-group leader (its service assigns;
/// its registry receives the replicated completion ack). Callers that are not
/// the leader MUST route via [`super::routed::submit_calvin_routed`].
///
/// The assignment + completion waits are bounded by
/// `state.tuning.network.default_deadline_secs`.
pub async fn submit_and_await_calvin(
    state: &SharedState,
    tx_class: TxClass,
) -> crate::Result<Option<Response>> {
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    submit_and_await_calvin_with_timeout(state, tx_class, timeout).await
}

/// [`submit_and_await_calvin`] with an explicit deadline budget.
///
/// Used by the leader-side RPC handler so the forwarded submit-and-await is
/// bounded by the coordinator's remaining deadline rather than this node's full
/// default deadline.
pub async fn submit_and_await_calvin_with_timeout(
    state: &SharedState,
    mut tx_class: TxClass,
    timeout: Duration,
) -> crate::Result<Option<Response>> {
    raise_metadata_floor(state, &mut tx_class);
    stamp_incarnations(state, &mut tx_class)?;
    super::unique_claims::stamp_unique_claims(state, &mut tx_class)?;
    let stream = super::parts::split_into_parts(state, &mut tx_class)?;
    submit_prepared_and_await(state, tx_class, stream, timeout).await
}

/// Submit a stamped `tx_class` to this node's sequencer, stream its parts
/// when it carries them as parts, and await its completion.
///
/// PRECONDITION: this node is the sequencer-group leader.
pub(crate) async fn submit_prepared_and_await(
    state: &SharedState,
    tx_class: TxClass,
    stream: Option<PartStream>,
    timeout: Duration,
) -> crate::Result<Option<Response>> {
    #[cfg(feature = "failpoints")]
    crate::control::fail_gate::after_calvin_stamp(state.node_id, &tx_class).await;
    let fold = ReplyFold::of_tx_class(&tx_class)?;
    let inbox = state
        .sequencer_inbox
        .get()
        .ok_or(Error::SequencerUnavailable)?;
    let registry = state
        .calvin_completion_registry
        .get()
        .ok_or(Error::SequencerUnavailable)?;

    // A write to a permission-tree source is acknowledged only once it binds
    // every node.
    let binds_authorization = tx_class
        .write_set
        .participating_vshards_in_database(tx_class.database_id)
        .map_err(|e| Error::BadRequest {
            detail: format!("Calvin transaction write set: {e}"),
        })?
        .iter()
        .any(|vshard| {
            state
                .authorization_fence
                .sources()
                .is_source_vshard(vshard.as_u32())
        });

    let (inbox_seq, assignment_rx) =
        inbox
            .submit_with(tx_class, registry)
            .map_err(|e| Error::BadRequest {
                detail: format!("Calvin sequencer rejected transaction: {e}"),
            })?;
    let (epoch, position, participants) =
        await_assignment(registry, inbox_seq, assignment_rx, timeout).await?;
    // The header holds its locks on every participant until the parts
    // arrive. A lost stream aborts the transaction, and the completion
    // below reports it.
    if let Some(stream) = &stream {
        stream_parts(state, StreamTarget::Local, stream, true)
            .await
            .into_result()?;
    }

    let completion_rx =
        registry.register_completion_report(TxnId::new(epoch, position), participants);
    let report = tokio::time::timeout(timeout, completion_rx)
        .await
        .map_err(|_| {
            let err = Error::Internal {
                detail: "timed out waiting for Calvin transaction completion".to_owned(),
            };
            // This timeout is the only signal a silently-never-completed
            // Calvin write ever produces; file it as a structured report at
            // the one site that detects it, since the error alone gives an
            // operator no clue which transaction or participant stalled.
            crate::diag::calvin_completion_timeout(
                &err,
                epoch,
                position,
                participants,
                timeout.as_secs(),
            );
            err
        })?
        .map_err(|_| Error::Internal {
            detail: "Calvin completion channel closed".to_owned(),
        })?;
    // The global cross-shard verdict was ABORT and the writes were dropped.
    // The verdict's reason picks the error the client sees.
    match report.outcome {
        AttemptOutcome::Completed => {}
        AttemptOutcome::Aborted { reason } => return Err(calvin_abort_error(reason)),
    }
    if binds_authorization {
        crate::control::security::auth_lease::calvin_write_barrier(state).await?;
    }

    // Completion fired: the scheduler deposited the applied Response (with any
    // RETURNING rows) into the sidecar BEFORE proposing the ack that woke this
    // waiter, so the entry is present now if this write carried RETURNING.
    // Drain it (removing the entry) and hand it back so the coordinator can emit
    // DATA-ROW output instead of a bare command tag. `None` for plain writes; a
    // `Conflict` (>1 RETURNING participant) fails loudly rather than returning a
    // partial cross-shard union.
    let drained = state
        .calvin
        .apply_results
        .take(&TxnId::new(epoch, position));
    let applied = match drained {
        Some(CalvinApplyResult::Single {
            response,
            has_returning,
        }) => {
            // An installed txn whose reply failed to render deposits it as an
            // error for the statement.
            crate::control::local_dispatch::reject_data_plane_error(&response)?;
            Some((response, has_returning))
        }
        Some(CalvinApplyResult::Conflict) => {
            return Err(Error::Internal {
                detail: "multi-participant cross-shard RETURNING not supported".to_owned(),
            });
        }
        None => None,
    };
    with_reported_results(applied, &report.ack_results, fold)
}

/// The applied answer the coordinator hands back, from what every
/// participant's `CompletionAck` reported.
///
/// The sidecar holds the applies of the participants this node hosts
/// replicas of. The acks reach this node from every participant:
/// - their timeseries install counts replace the sidecar's plain answer,
///   and stand alone when no local participant deposited one;
/// - a participant's `RETURNING` rows answer the statement when no local
///   participant deposited them. Rows past the result limit fail the
///   statement with the error a local `RETURNING` over the limit gives, and
///   two participants with rows are a cross-shard `RETURNING` union, which
///   is unsupported;
/// - a primary-write participant's plain answer, with its affected count,
///   answers the statement when no local participant deposited one. The
///   first one reported stands, as the first deposit does in the sidecar.
///
/// So a coordinator that hosts none of the participants answers with the
/// same result as one that hosts them all.
///
/// Under [`ReplyFold::SumOwnedEdges`] the answer is the sum of every home's
/// owned-edge count instead.
pub(crate) fn with_reported_results(
    applied: Option<(Response, bool)>,
    ack_results: &[Vec<u8>],
    fold: ReplyFold,
) -> crate::Result<Option<Response>> {
    use crate::control::state::{AckReply, AckReturning, CalvinAckResult};
    use crate::engine::timeseries::install_counts::merge_count_payloads;
    let reports: Vec<CalvinAckResult> = ack_results
        .iter()
        .filter_map(|bytes| CalvinAckResult::from_bytes(bytes))
        .collect();
    if fold == ReplyFold::SumOwnedEdges {
        return sum_owned_edges(&reports);
    }
    let counts = reports
        .iter()
        .filter(|report| !report.counts.is_empty())
        .fold(None::<Vec<u8>>, |held, report| match held {
            None => merge_count_payloads(&report.counts, &[]),
            Some(held) => merge_count_payloads(&held, &report.counts),
        });
    let mut returning = reports
        .iter()
        .filter_map(|report| report.returning.as_ref());
    let reported = returning.next();
    if returning.next().is_some() {
        return Err(Error::Internal {
            detail: "multi-participant cross-shard RETURNING not supported".to_owned(),
        });
    }
    // A local replica's sidecar rows answer the statement, and the local
    // response path enforces the result limit on them.
    let local_rows = matches!(applied, Some((_, true)));
    let rows = match reported {
        Some(_) if local_rows => None,
        None => None,
        Some(AckReturning::Rows { rows }) => Some(rows.clone()),
        Some(AckReturning::OverLimit { bytes, limit }) => {
            return Err(Error::ExecutionLimitExceeded {
                detail: format!(
                    "query result exceeded max_query_result_bytes ({bytes} > {limit} bytes)"
                ),
            });
        }
        Some(AckReturning::Failed { detail }) => {
            return Err(Error::Internal {
                detail: format!("calvin RETURNING: {detail}"),
            });
        }
    };
    Ok(match (applied, rows, counts) {
        (Some((response, true)), _, _) => Some(response),
        (_, Some(rows), _) => Some(synthetic_returning_response(rows)),
        (Some((response, false)), None, Some(counts)) => Some(Response {
            payload: crate::bridge::envelope::Payload::from_vec(counts),
            ..response
        }),
        (Some((response, false)), None, None) => Some(response),
        (None, None, Some(counts)) => Some(synthetic_returning_response(counts)),
        (None, None, None) => match reports.iter().find_map(|report| report.reply.as_ref()) {
            Some(AckReply::Payload { payload }) => {
                Some(synthetic_returning_response(payload.clone()))
            }
            Some(AckReply::Failed { detail }) => {
                return Err(Error::Internal {
                    detail: format!("calvin reply: {detail}"),
                });
            }
            None => None,
        },
    })
}

/// How the coordinator folds its participants' answers into the
/// statement's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyFold {
    /// A participant that holds the statement's own write answers it. The
    /// first answer stands.
    First,
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
            Self::First
        }
    }

    /// The fold of `tx_class`. A class split into parts reads its manifest,
    /// and one with no edge key needs no decode.
    pub(crate) fn of_tx_class(tx_class: &TxClass) -> crate::Result<Self> {
        if !super::edge_slices::writes_edges(tx_class) {
            return Ok(Self::First);
        }
        if let Some(manifest) = &tx_class.multi_part {
            return Ok(if manifest.user_write {
                Self::First
            } else {
                Self::SumOwnedEdges
            });
        }
        let plans =
            nodedb_physical::physical_plan::wire::decode_batch(&tx_class.plans).map_err(|e| {
                Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("calvin reply fold: plan decode: {e}"),
                }
            })?;
        Ok(Self::of_plans(plans.iter()))
    }
}

/// The sum of every home's owned-edge count, as the statement's answer.
fn sum_owned_edges(
    reports: &[crate::control::state::CalvinAckResult],
) -> crate::Result<Option<Response>> {
    use crate::control::server::shared::sql::staging_predicates::extract_affected_count;
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
    /// A local deposit still stands over a reported answer.
    #[test]
    fn a_coordinator_with_no_participant_answers_the_reported_count() {
        use crate::control::server::shared::sql::staging_predicates::extract_affected_count;
        let answer =
            with_reported_results(None, &[ack(Vec::new(), None), replied(3)], ReplyFold::First)
                .expect("a report")
                .expect("an answer");
        assert_eq!(extract_affected_count(answer.payload.as_bytes()), Some(3));

        let local_payload = crate::data::executor::response_codec::encode_count("affected", 5)
            .expect("encode count");
        let local = synthetic_returning_response(local_payload);
        let answer = with_reported_results(Some((local, false)), &[replied(3)], ReplyFold::First)
            .expect("a report")
            .expect("an answer");
        assert_eq!(extract_affected_count(answer.payload.as_bytes()), Some(5));
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
            Some((local, false)),
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

    /// The sequencer dropped the assignment of a submission it did not
    /// sequence. The caller fails at once with a retryable refusal, long
    /// before its timeout.
    #[tokio::test]
    async fn an_unsequenced_submission_is_a_retryable_refusal() {
        let registry = CalvinCompletionRegistry::new_detached();
        let rx = registry.register_submission(7);
        registry.drop_assignment(7);
        match await_assignment(&registry, 7, rx, Duration::from_secs(3600)).await {
            Err(Error::RetryableRefusal { .. }) => {}
            other => panic!("expected a retryable refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_assigned_submission_returns_its_assignment() {
        let registry = CalvinCompletionRegistry::new_detached();
        let rx = registry.register_submission(3);
        registry.note_assigned(3, TxnId::new(5, 1), 2);
        let assignment = await_assignment(&registry, 3, rx, Duration::from_secs(3600))
            .await
            .expect("assigned");
        assert_eq!(assignment, (5, 1, 2));
    }

    /// A wait that times out reports the timeout, not a refusal: the
    /// submission can still be sequenced.
    #[tokio::test]
    async fn a_timed_out_wait_is_not_a_refusal() {
        let registry = CalvinCompletionRegistry::new_detached();
        let rx = registry.register_submission(8);
        match await_assignment(&registry, 8, rx, Duration::from_millis(1)).await {
            Err(Error::Internal { detail }) => assert!(detail.contains("timed out"), "{detail}"),
            other => panic!("expected the timeout error, got {other:?}"),
        }
    }

    /// With one replica per vShard, the coordinator's node hosts no replica of
    /// either participant, so its sidecar holds nothing. The counts both
    /// participants' acks carried still reach the statement.
    #[test]
    fn remote_participants_report_their_counts_through_their_acks() {
        let answer = with_reported_results(
            None,
            &[counted("cpu", 2, 1), counted("mem", 1, 3)],
            ReplyFold::First,
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
            Some((local, false)),
            &[counted("cpu", 2, 0), counted("mem", 0, 2)],
            ReplyFold::First,
        )
        .expect("a report")
        .expect("an answer");
        assert_eq!(decoded(&answer).rejected, 2);

        let rows = synthetic_returning_response(vec![0x90]);
        let reported = ack(
            counts("cpu", 1, 1),
            Some(AckReturning::Rows { rows: vec![0x90] }),
        );
        let answer = with_reported_results(Some((rows, true)), &[reported], ReplyFold::First)
            .expect("a report")
            .expect("an answer");
        assert_eq!(answer.payload.as_bytes(), &[0x90]);

        assert!(
            with_reported_results(None, &[], ReplyFold::First)
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
            Some((synthetic_returning_response(Vec::new()), false)),
            &[
                ack(
                    counts("cpu", 1, 1),
                    Some(AckReturning::Rows { rows: rows.clone() }),
                ),
                counted("mem", 2, 0),
            ],
            ReplyFold::First,
        )
        .expect("a report")
        .expect("an answer");
        assert_eq!(answer.payload.as_bytes(), rows.as_slice());
    }

    /// Remote rows past the limit fail the statement with the error a local
    /// `RETURNING` over the limit gives. Two participants with rows fail it
    /// as an unsupported cross-shard union.
    #[test]
    fn remote_rows_past_the_limit_or_from_two_participants_fail() {
        let over = ack(
            Vec::new(),
            Some(AckReturning::OverLimit {
                bytes: 64,
                limit: 16,
            }),
        );
        match with_reported_results(None, &[over], ReplyFold::First) {
            Err(Error::ExecutionLimitExceeded { detail }) => assert_eq!(
                detail,
                "query result exceeded max_query_result_bytes (64 > 16 bytes)"
            ),
            other => panic!("expected the result limit error, got {other:?}"),
        }

        let one = ack(Vec::new(), Some(AckReturning::Rows { rows: vec![0x90] }));
        match with_reported_results(None, &[one.clone(), one], ReplyFold::First) {
            Err(Error::Internal { detail }) => {
                assert!(detail.contains("cross-shard RETURNING"), "{detail}")
            }
            other => panic!("expected the cross-shard error, got {other:?}"),
        }
    }
}
