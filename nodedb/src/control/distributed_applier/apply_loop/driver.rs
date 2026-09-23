// SPDX-License-Identifier: BUSL-1.1

//! Per-batch loop driver: pulls batches off the apply channel, dispatches
//! each entry to the Array CRDT / Calvin fast path or the generic write
//! path, and lands the batch's durable applied floor once every entry in it
//! has been applied.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::control::array_sync::ArrayOpTarget;
use crate::control::array_sync::raft_apply::{AppliedPosition, ArraySchemaPayload};
use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::distributed_applier::applied_index::AppliedPrefix;
use crate::control::distributed_applier::applier::ApplyBatch;
use crate::control::distributed_applier::propose_tracker::ProposeTracker;
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId};

use super::array_dispatch::{apply_array_op_entry, apply_array_schema_entry};
use super::bookkeeping::record_durable_apply;
use super::calvin_read_result::{CalvinReadResultFields, forward_calvin_read_result};
use super::proposal_gate::{EntryOutcome, ProposalGate};
use super::transaction_redo::apply_transaction_redo_entry;
use super::write_dispatch::apply_generic_entry;
use crate::control::distributed_applier::proposal_ledger::{
    PROPOSAL_LEDGER_CAPACITY, ProposalLedger,
};

/// Run the background loop that applies committed Raft entries to the local Data Plane.
///
/// This task reads from the apply channel, deserializes each entry, dispatches
/// the write to the Data Plane via SPSC, and notifies proposers.
pub async fn run_apply_loop(
    mut apply_rx: mpsc::Receiver<ApplyBatch>,
    state: Arc<SharedState>,
    tracker: Arc<ProposeTracker>,
    calvin_read_result_senders: Arc<
        std::sync::Mutex<std::collections::BTreeMap<u32, mpsc::Sender<ReadResultEvent>>>,
    >,
) {
    // Proposals this node already applied, recovered from its WAL before any
    // entry is delivered: every record an entry's apply appended carries the
    // entry's idempotency key in its header.
    let records = match state.wal.replay() {
        Ok(records) => records,
        Err(error) => {
            // Without the keys an entry re-delivered above the durable floor,
            // or a second committed copy of a proposal, would apply a second
            // time. Refuse to apply anything rather than risk it: the loop
            // stops, and every propose waiter surfaces the stall.
            tracing::error!(
                %error,
                "data-group apply loop cannot read its WAL to recover applied proposals; \
                 refusing to apply committed entries"
            );
            return;
        }
    };
    let mut ledger = ProposalLedger::from_records(&records, PROPOSAL_LEDGER_CAPACITY);
    drop(records);
    while let Some(batch) = apply_rx.recv().await {
        // The floor is saved ONCE per batch, after the loop — never per entry.
        // `save_applied_index` lands a redb transaction, and redb commits at
        // `Durability::Immediate`, so a per-entry save puts one synchronous
        // fsync per applied entry directly on the raft apply path. That stalls
        // the raft loop hard enough to delay heartbeats and keep elections from
        // stabilizing under a multi-node write load. One fsync per batch
        // amortizes the cost across every entry in it and keeps the critical
        // path free.
        //
        // `AppliedPrefix` computes WHICH index is safe to save: the highest
        // contiguous successfully-applied entry, stopping at the first failure
        // and never advancing past it. Every branch below must therefore report
        // its outcome — `record` for the ones whose success means a durable
        // redo record, `skip` for the ones that apply no durable state at all.
        let mut prefix = AppliedPrefix::new();
        let mut gate = ProposalGate {
            ledger: &mut ledger,
            group_id: batch.group_id,
        };
        for entry in &batch.entries {
            // Decode once; reused for both the idempotency key and the
            // Array/Calvin fast-path match below. Returns 0 for
            // unparseable / pre-key entries; the tracker treats 0 as
            // "no key" (no mismatch detection).
            let replicated_opt = ReplicatedEntry::from_bytes(&entry.data);
            let applied_key = replicated_opt
                .as_ref()
                .map(|e| e.idempotency_key)
                .unwrap_or(0);

            // Database scope for the entry, read from the wire. `0` decodes to
            // `DatabaseId::DEFAULT` (the pre-`database_id` legacy shape). The
            // generic decode path (`from_replicated_entry`) returns only
            // `(tenant, vshard, plan, resolved_now_ms)`, so the scope is taken
            // from the entry itself — a WAL redo appended under the wrong
            // database scope replays into the wrong catalog namespace.
            let database_id = replicated_opt
                .as_ref()
                .map(|e| DatabaseId::new(e.database_id))
                .unwrap_or(DatabaseId::DEFAULT);

            // A second committed copy of a proposal this node already applied
            // (a re-proposal after a leader change whose first copy also
            // committed) resolves its waiter with the first copy's result and
            // applies nothing.
            if gate.skip_duplicate(&tracker, &mut prefix, entry.index, applied_key) {
                continue;
            }

            // ── Array CRDT variants — handled on the Control Plane, bypass Data Plane ──
            if let Some(replicated) = replicated_opt {
                let target_vshard = replicated.vshard_id;
                let pos = AppliedPosition {
                    group_id: batch.group_id,
                    log_index: entry.index,
                    applied_key,
                };
                match replicated.write {
                    ReplicatedWrite::ArrayOp {
                        ref array,
                        ref op_bytes,
                        ref provenance,
                        ..
                    } => {
                        let applied_ok = apply_array_op_entry(
                            &state,
                            &tracker,
                            pos,
                            ArrayOpTarget {
                                tenant_id: TenantId::new(replicated.tenant_id),
                                database_id: DatabaseId::new(replicated.database_id),
                                array,
                            },
                            op_bytes,
                            provenance.as_deref(),
                        )
                        .await;
                        // Advance the durable prefix only when the op durably
                        // applied — same safe-watermark rule as the Data Plane
                        // write path below, and the same funnel: the op path
                        // submits through `submit_write`, so its redo is fsynced
                        // before it reports success. A failure breaks the
                        // prefix: the entry must stay replayable.
                        let outcome = EntryOutcome::Applied {
                            durable: applied_ok,
                            result: None,
                        };
                        gate.settle(&mut prefix, entry.index, applied_key, outcome);
                        continue;
                    }
                    ReplicatedWrite::ArraySchema {
                        ref array,
                        ref snapshot_payload,
                        schema_hlc_bytes,
                    } => {
                        let applied_ok = apply_array_schema_entry(
                            &state,
                            &tracker,
                            pos,
                            ArraySchemaPayload {
                                tenant_id: TenantId::new(replicated.tenant_id),
                                database_id: DatabaseId::new(replicated.database_id),
                                array,
                                snapshot_payload,
                                schema_hlc_bytes,
                            },
                        );
                        // Advance the durable prefix only when the schema
                        // snapshot durably imported.
                        //
                        // This is the one applied branch that mints no WAL redo
                        // record, and it needs none: its entire effect is two
                        // fsync-committed redb transactions — the schema
                        // registry's snapshot row and the array catalog's entry
                        // — both written before it reports success. The floor's
                        // invariant ("this entry's state survives a restart, so
                        // Raft need not redeliver it") is therefore already met
                        // by the registries themselves. The cell paths have no
                        // such durable store behind them: their state lives in
                        // Data-Plane memtables and exists on disk only as the
                        // redo record the funnel appends, which is why they must
                        // route through `submit_write`.
                        let outcome = EntryOutcome::Applied {
                            durable: applied_ok,
                            result: None,
                        };
                        gate.settle(&mut prefix, entry.index, applied_key, outcome);
                        continue;
                    }
                    ReplicatedWrite::TransactionRedo { .. } => {
                        let outcome =
                            apply_transaction_redo_entry(&state, &tracker, pos, &replicated).await;
                        // Advance the durable prefix when the entry's outcome is
                        // durable: its keyed redo record fsynced, or a final
                        // refusal cancelled in the WAL.
                        gate.settle(&mut prefix, entry.index, applied_key, outcome);
                        continue;
                    }
                    ReplicatedWrite::CalvinReadResult {
                        epoch,
                        position,
                        passive_vshard,
                        tenant_id,
                        ref values,
                    } => {
                        forward_calvin_read_result(
                            &tracker,
                            &calvin_read_result_senders,
                            pos,
                            CalvinReadResultFields {
                                target_vshard,
                                epoch,
                                position,
                                passive_vshard,
                                tenant_id,
                                values,
                            },
                        );
                        // A read result is forwarded to an in-memory Calvin
                        // scheduler and writes nothing durable, so it neither
                        // advances the prefix nor breaks it. Advancing on it
                        // would assert a redo record that does not exist;
                        // breaking on it would stall the floor behind an entry
                        // that a re-delivery could not usefully replay anyway —
                        // the epoch it belongs to does not survive a restart —
                        // and force every later write in the batch to be applied
                        // twice on the next boot.
                        prefix.skip();
                        continue;
                    }
                    _ => {}
                }
            }

            let outcome = apply_generic_entry(
                &state,
                &tracker,
                batch.group_id,
                entry,
                applied_key,
                database_id,
            )
            .await;
            gate.settle(&mut prefix, entry.index, applied_key, outcome);
        }

        // One save + one compaction check per batch, against the contiguous
        // prefix. Compaction is deliberately driven by the same index the floor
        // was just saved at — never the batch's last delivered index — so it can
        // never discard an entry the next boot still has to replay. Compacting
        // on the raft commit index while the SPSC apply lags would likewise let
        // the `SnapshotBuilder` serialize incomplete engine state and corrupt a
        // lagging follower's snapshot.
        if let Some(applied_index) = prefix.floor() {
            record_durable_apply(&state, batch.group_id, applied_index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::distributed_applier::apply_loop::helpers::deterministic_crdt_fence_noop;
    use crate::control::distributed_applier::propose_tracker::AppliedWrite;

    #[test]
    fn fenced_frontier_mismatch_completes_retry_and_advances_durable_prefix() {
        let result: crate::Result<AppliedWrite> = Err(crate::Error::DataPlane(
            crate::bridge::envelope::ErrorCode::CrdtFrontierMismatch {
                expected: [1; 32],
                actual: [2; 32],
            },
        ));
        assert!(deterministic_crdt_fence_noop(&result));
        assert!(matches!(
            result,
            Err(crate::Error::DataPlane(
                crate::bridge::envelope::ErrorCode::CrdtFrontierMismatch { .. }
            ))
        ));

        let mut prefix = AppliedPrefix::new();
        prefix.record(17, deterministic_crdt_fence_noop(&result));
        assert_eq!(prefix.floor(), Some(17));
    }
}
