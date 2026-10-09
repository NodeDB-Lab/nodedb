// SPDX-License-Identifier: BUSL-1.1

//! Calvin multi-shard arm of the neutral COMMIT orchestrator.
//!
//! Routes a transaction's buffered writes through the Calvin sequencer. Strict
//! mode routes the whole buffered batch to the sequencer-group leader through
//! the universal strict atomic task-set entry point — one transaction bound by
//! the durable Vote/Verdict barrier. Best-effort mode groups writes by vShard
//! and submits each group
//! as an INDEPENDENT single-vShard Calvin transaction (via
//! `build_single_vshard_tx_class` then `submit_calvin_routed`) — the SAME
//! deterministic sequencer funnel, so each vShard gets an epoch-anchored
//! bitemporal stamp and a `TransactionRedo` WAL record, while remaining
//! non-atomic ACROSS vShards (no global vote binds them; a failure on one vShard
//! does not roll back another).

use crate::control::planner::calvin::{
    CrossShardTxnMode, TxnDispatchPosition, TxnProvenance, build_single_vshard_tx_class,
    dispatch_strict_atomic_tasks_to_calvin, submit_calvin_routed,
};
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use nodedb_physical::physical_task::PhysicalTask;

use super::commit::ts_rejections::RejectedByCollection;
use super::connection::SessionId;
use super::outcome::AbortReason;
use super::store::SessionStore;

/// What one COMMIT hands Calvin.
pub(super) struct CalvinCommit<'a> {
    pub buffered: &'a [PhysicalTask],
    pub tenant_id: crate::types::TenantId,
    pub reads: &'a [ReadSetEntry],
    pub event_source: crate::event::EventSource,
    /// The messages the transaction's trigger bodies published. One
    /// participant's redo record carries them.
    pub publishes: &'a [crate::wal::RedoPublish],
    /// The cross-shard request the transaction applies, with the vShard the
    /// request addresses. That vShard's redo record carries the key.
    pub applied_key: Option<(crate::wal::CrossShardAppliedKey, u32)>,
    /// The lines the statements' stage-time previews rejected, by
    /// collection.
    pub ts_preview: &'a super::commit::ts_rejections::RejectedByCollection,
}

/// Dispatch a multi-shard transaction batch through Calvin. Strict commits the
/// whole batch atomically through the leader-routed Vote/Verdict barrier;
/// best-effort submits one independent single-vShard Calvin transaction per
/// vShard. Returns `Some(reason)` on failure, `None` on success.
///
/// Every timeseries ingest resolves to its rows here, once, before any
/// submit: every replica resolves a sequenced transaction on its own. This
/// resolve is authoritative. Once the transaction commits, a collection
/// whose resolve rejected more lines than its statements reported raises a
/// notice.
pub(super) async fn run_commit_calvin(
    sessions: &SessionStore,
    session_id: SessionId,
    state: &SharedState,
    commit: CalvinCommit<'_>,
) -> Option<AbortReason> {
    let resolved =
        match crate::control::write_resolve::resolve_tasks_for_log(state, commit.buffered).await {
            Ok(resolved) => resolved,
            Err(e) => return Some(AbortReason::Dispatch(e)),
        };
    let buffered = resolved.as_deref().unwrap_or(commit.buffered);
    let ts_committed = match super::commit::ts_rejections::tasks_rejected_by_collection(buffered) {
        Ok(committed) => committed,
        Err(e) => return Some(AbortReason::Dispatch(e)),
    };
    let ts_preview = commit.ts_preview;
    let outcome = submit_commit_calvin(
        sessions,
        session_id,
        state,
        CalvinCommit { buffered, ..commit },
    )
    .await;
    match outcome {
        Ok(applied) => {
            // Each install's count covers its resolve's and adds the rows it
            // rejected at its log position, so it wins where it is known.
            let committed = super::commit::ts_rejections::with_applied(ts_committed, &applied);
            super::commit::ts_rejections::raise_commit_rejections(ts_preview, &committed);
            None
        }
        Err(reason) => Some(reason),
    }
}

/// Submit `commit`'s resolved batch through Calvin, strict or best-effort.
/// Returns the lines and rows the applies report their timeseries installs
/// rejected, by collection.
async fn submit_commit_calvin(
    sessions: &SessionStore,
    session_id: SessionId,
    state: &SharedState,
    commit: CalvinCommit<'_>,
) -> Result<RejectedByCollection, AbortReason> {
    let CalvinCommit {
        buffered,
        tenant_id,
        reads,
        event_source,
        publishes,
        applied_key,
        ts_preview: _,
    } = commit;
    let publishes =
        crate::wal::RedoPublish::encode_all(publishes).map_err(AbortReason::Dispatch)?;
    let applied_key = applied_key
        .map(|(key, vshard)| encode_applied_key(&key).map(|bytes| (bytes, vshard)))
        .transpose()
        .map_err(AbortReason::Dispatch)?;
    // A cross-shard request's writes commit together or not at all, so a
    // keyed transaction always takes the atomic path.
    let cross_shard_mode = if applied_key.is_some() {
        CrossShardTxnMode::Strict
    } else {
        sessions.cross_shard_txn_mode(session_id)
    };
    // The session's read-reservation owner `R`, taken at read time. Fetched once
    // and stamped onto every Calvin submit below so each commit batch acquires
    // its keys as `R` and self-upgrades the shared reservations — never
    // recomputed at commit.
    let reservation_owner = sessions.current_reservation_owner(session_id);

    match cross_shard_mode {
        CrossShardTxnMode::Strict => {
            // One universal strict admission path builds either a single- or
            // multi-participant class and submits exactly once to the sequencer.
            let result = dispatch_strict_atomic_tasks_to_calvin(
                state,
                buffered,
                tenant_id,
                TxnDispatchPosition::CommitFlush,
                reads,
                reservation_owner,
                TxnProvenance {
                    event_source,
                    body_tasks: body_task_indexes(sessions, session_id),
                    publishes,
                    applied_key,
                },
            )
            .await;
            match result {
                Ok(applied) => Ok(applied_rejections(applied.as_ref())),
                Err(crate::Error::CalvinSerializationConflict) => {
                    super::hot_key::record_read_set_aborts(state, reads);
                    Err(AbortReason::Serialization)
                }
                Err(e) => Err(AbortReason::Dispatch(e)),
            }
        }
        CrossShardTxnMode::BestEffortNonAtomic => {
            // Group the buffered writes by vShard. Each group becomes ONE
            // independent single-vShard Calvin transaction, sequenced through the
            // SAME deterministic funnel the contended point-write path uses
            // (`build_single_vshard_tx_class` + `submit_calvin_routed`): the
            // scheduler resolves it into a `TransactionRedo` and proposes it as a
            // stamped data-group entry. The install sets `epoch_system_ms`, so
            // every engine's bitemporal stamp is epoch-anchored and
            // byte-identical on replay.
            //
            // Non-atomic ACROSS vShards is preserved by construction: each group
            // is a separate submit-and-await with its own single-participant
            // verdict (no cross-shard vote barrier). On the FIRST failure we
            // surface the reason and stop — we do NOT roll back vShards that have
            // already committed, exactly as the mode's contract requires.
            let body = sessions.body_tasks(session_id);
            let mut by_vshard: std::collections::BTreeMap<u32, (Vec<PhysicalTask>, Vec<u32>)> =
                std::collections::BTreeMap::new();
            for (index, task) in buffered.iter().enumerate() {
                let (tasks, body_tasks) = by_vshard.entry(task.vshard_id.as_u32()).or_default();
                if body.contains(&index)
                    && let Ok(local) = u32::try_from(tasks.len())
                {
                    body_tasks.push(local);
                }
                tasks.push(task.clone());
            }
            // The first group's commit carries the messages: a best-effort
            // transaction commits group by group, and the first group is the
            // first to commit.
            let mut publishes = Some(publishes);
            let mut applied = RejectedByCollection::new();
            // A group only trigger bodies wrote, in a transaction a client
            // also wrote, commits under the source a body's row takes beside
            // the client's in one transaction.
            let client_wrote = (0..buffered.len()).any(|index| !body.contains(&index));
            for (_vshard_u32, (tasks, body_tasks)) in by_vshard {
                let group_source = if client_wrote && body_tasks.len() == tasks.len() {
                    event_source.committed_row_override(crate::event::EventSource::Trigger)
                } else {
                    event_source
                };
                // Empty read-set: best-effort performs no cross-shard OCC (the
                // multi-shard COMMIT path never ran `si_conflict_abort`), so each
                // group carries no versioned reads — matching the single-vShard
                // submit `route_write_to_calvin` uses.
                // A timeseries ingest resolves to its rows here, before it is
                // sequenced: every replica resolves a sequenced group on its own.
                let tasks =
                    match crate::control::write_resolve::resolve_tasks_for_log(state, &tasks).await
                    {
                        Ok(resolved) => resolved.unwrap_or(tasks),
                        Err(e) => return Err(AbortReason::Dispatch(e)),
                    };
                let mut tx_class = match build_single_vshard_tx_class(&tasks, tenant_id, &[]) {
                    Ok(tc) => tc,
                    Err(e) => return Err(AbortReason::Dispatch(e)),
                };
                // Each per-vShard group acquires under `R` too, so it self-upgrades
                // its slice of the session's shared reservations.
                tx_class.set_lock_owner(reservation_owner);
                tx_class.set_event_source(group_source.wal_code());
                tx_class.set_body_plans(body_tasks);
                if let Some(publishes) = publishes.take() {
                    tx_class.set_publishes(publishes);
                }
                match submit_calvin_routed(state, tx_class).await {
                    Ok(group) => {
                        applied = super::commit::ts_rejections::with_applied(
                            applied,
                            &applied_rejections(group.as_ref()),
                        );
                    }
                    Err(crate::Error::CalvinSerializationConflict) => {
                        super::hot_key::record_read_set_aborts(state, reads);
                        return Err(AbortReason::Serialization);
                    }
                    Err(e) => return Err(AbortReason::Dispatch(e)),
                }
            }
            Ok(applied)
        }
    }
}

/// The lines and rows the timeseries installs of `applied` rejected, by
/// collection. Empty when the answer carries no install counts.
fn applied_rejections(applied: Option<&crate::bridge::envelope::Response>) -> RejectedByCollection {
    applied
        .map(|response| {
            super::commit::ts_rejections::applied_rejected_by_collection(
                response.payload.as_bytes(),
            )
        })
        .unwrap_or_default()
}

/// The opaque bytes a transaction class carries an applied key in.
fn encode_applied_key(key: &crate::wal::CrossShardAppliedKey) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(key).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("cross-shard applied key encode: {e}"),
    })
}

/// Indexes into the buffered tasks of the tasks a trigger body buffered.
fn body_task_indexes(sessions: &SessionStore, session_id: SessionId) -> Vec<u32> {
    sessions
        .body_tasks(session_id)
        .into_iter()
        .filter_map(|index| u32::try_from(index).ok())
        .collect()
}
