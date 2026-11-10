// SPDX-License-Identifier: BUSL-1.1

//! Local submit-and-await primitive for the Calvin cross-shard write path.
//!
//! PRECONDITION for everything here: this node is the sequencer-group leader.
//! Its sequencer service assigns and its registry receives the replicated
//! completion ack. A submit on a non-leader is drained and discarded, so
//! non-leader callers must route through [`super::routed`].

use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::calvin::{AttemptOutcome, TxnId};

use super::prepare::{Prepared, prepare_submit};
use super::reported_results::with_reported_results;
use super::stream::{StreamTarget, stream_parts};
use super::wait::{await_assignment, await_completion};
use crate::Error;
use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::planner::calvin::abort_error::calvin_abort_error;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;

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
        read_vshards: crate::types::ReadShards::new(),
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

/// Stamp the incarnation this node's catalogs hold for every user collection
/// and every array `plans` name. `plans` are `tx_class`'s decoded plans. The
/// coordinator stamps first, against the catalogs it planned with. A
/// forwarded submit arrives stamped and keeps its stamps.
pub(crate) fn stamp_incarnations(
    state: &SharedState,
    tx_class: &mut TxClass,
    plans: &[PhysicalPlan],
) -> crate::Result<()> {
    use nodedb_cluster::calvin::types::{CalvinIncarnation, IncarnationKind};

    // A split class was stamped by its coordinator before the split, and no
    // longer carries its plans in `plans`.
    if !tx_class.incarnations.is_empty() || tx_class.is_multi_part() {
        return Ok(());
    }
    let named =
        crate::control::planner::calvin::incarnation_names::incarnation_names(plans, |plan| {
            plan.named_collections()
                .into_iter()
                .map(str::to_owned)
                .collect()
        });
    let catalog = state.credentials.catalog();
    let mirror =
        crate::control::array_catalog::named_incarnation::read_mirror(&state.array_catalog);
    let tenant_id = tx_class.tenant_id.as_u64();
    let mut incarnations = Vec::with_capacity(named.len());
    for (collection, kind) in named {
        let incarnation = match kind {
            IncarnationKind::Collection => {
                catalog.incarnation_of(tx_class.database_id, tenant_id, &collection)?
            }
            IncarnationKind::Array => {
                crate::control::array_catalog::named_incarnation::array_incarnation_of(
                    &mirror,
                    tx_class.tenant_id,
                    tx_class.database_id,
                    &collection,
                )?
            }
        };
        incarnations.push(CalvinIncarnation {
            collection,
            incarnation,
            kind,
        });
    }
    drop(mirror);
    tx_class.set_incarnations(incarnations);
    Ok(())
}

/// Submit `tx_class` to THIS node's Calvin sequencer inbox and await completion.
///
/// PRECONDITION: this node is the sequencer-group leader (its service assigns;
/// its registry receives the replicated completion ack). Callers that are not
/// the leader MUST route via [`super::routed::submit_calvin_routed`].
///
/// The assignment and completion waits end at the running statement's
/// deadline.
pub async fn submit_and_await_calvin(
    state: &SharedState,
    tx_class: TxClass,
) -> crate::Result<Option<Response>> {
    let deadline = StatementDeadline::of_statement(state).tokio_instant();
    submit_and_await_calvin_until(state, tx_class, deadline).await
}

/// [`submit_and_await_calvin`] with an explicit deadline.
///
/// The leader-side RPC handler passes the coordinator's remaining deadline,
/// so the forwarded submit-and-await never outlives the statement.
pub async fn submit_and_await_calvin_until(
    state: &SharedState,
    mut tx_class: TxClass,
    deadline: tokio::time::Instant,
) -> crate::Result<Option<Response>> {
    let prepared = prepare_submit(state, &mut tx_class)?;
    submit_prepared_and_await(state, tx_class, prepared, deadline).await
}

/// Submit a stamped `tx_class` to this node's sequencer, stream its parts
/// when it carries them as parts, and await its completion. `prepared`
/// holds the parts and the reply fold [`prepare_submit`] derived.
///
/// PRECONDITION: this node is the sequencer-group leader.
///
/// A deadline spent before the submit refuses it: nothing was submitted. A
/// wait that reaches the deadline after the submit is an unknown outcome.
pub(crate) async fn submit_prepared_and_await(
    state: &SharedState,
    tx_class: TxClass,
    prepared: Prepared,
    deadline: tokio::time::Instant,
) -> crate::Result<Option<Response>> {
    #[cfg(feature = "failpoints")]
    crate::control::fail_gate::after_calvin_stamp(state.node_id, &tx_class).await;
    let Prepared { stream, fold } = prepared;
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

    super::budget::budget_ms_until(deadline)?;
    let (inbox_seq, assignment_rx) =
        inbox
            .submit_with(tx_class, registry)
            .map_err(|e| Error::BadRequest {
                detail: format!("Calvin sequencer rejected transaction: {e}"),
            })?;
    let (epoch, position, participants) =
        await_assignment(registry, inbox_seq, assignment_rx, deadline).await?;
    // The header holds its locks on every participant until the parts
    // arrive. A lost stream aborts the transaction, and the completion
    // below reports it.
    if let Some(stream) = &stream {
        stream_parts(state, StreamTarget::Local, stream, true, deadline)
            .await
            .into_result()?;
    }

    let report = await_completion(
        registry,
        TxnId::new(epoch, position),
        participants,
        deadline,
    )
    .await?;
    // The global cross-shard verdict was ABORT and the writes were dropped.
    // The verdict's reason picks the error the client sees.
    match report.outcome {
        AttemptOutcome::Completed => {}
        AttemptOutcome::Aborted(cause) => return Err(calvin_abort_error(cause)),
    }
    if binds_authorization {
        crate::control::security::auth_lease::calvin_write_barrier(
            state,
            crate::control::server::shared::session::StatementDeadline::of_statement(state)
                .instant(),
        )
        .await?;
    }

    // Completion fired: every local participant deposited its applied
    // Response (with any RETURNING rows) into the sidecar BEFORE proposing the
    // ack that woke this waiter. Drain them (removing the entry) and hand the
    // statement's answer back, so the coordinator can emit DATA-ROW output
    // instead of a bare command tag.
    let applied = state
        .calvin
        .apply_results
        .take(&TxnId::new(epoch, position));
    with_reported_results(applied, &report.ack_results, fold)
}
