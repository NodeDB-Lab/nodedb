// SPDX-License-Identifier: BUSL-1.1

//! Static and active (dependent-read) txn dispatch to the Data Plane.

use std::time::Instant;

use tracing::{debug, error};

use nodedb_cluster::calvin::types::SequencedTxn;

use super::routing::PlanRouting;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::types::{DatabaseId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

/// Whether this vShard's slice carries a PRIMARY user data write — the write
/// whose applied `Response` (affected-count + any RETURNING rows) the
/// coordinator surfaces.
///
/// A primary write is a Document / KV / Vector / Timeseries / Columnar / Array
/// write — NOT the implicit graph-edge cleanup (`EdgePut` / `EdgeDelete`) that
/// dual-homes alongside a document delete/update. For a single-collection user
/// DML (plus its implicit edges) exactly ONE participant carries the primary
/// write, so only it deposits the applied `Response` into the coordinator's
/// sidecar and the edge participants never clobber the entry.
///
/// This gate subsumes the RETURNING case (a RETURNING write IS a primary write,
/// so its rows are still deposited) while ALSO carrying the affected-count of a
/// plain (non-RETURNING) write — which a RETURNING-only gate dropped, making a
/// routed plain write report zero rows affected.
fn participant_change_sets(
    plans: &[PhysicalPlan],
    tenant_id: crate::types::TenantId,
    vshard_id: u32,
) -> Vec<crate::control::server::dispatch_utils::WriteChangeSet> {
    plans
        .iter()
        .filter(|plan| match plan {
            // Edge plans are dual-homed; only the source participant publishes
            // the one logical Control-Plane event.
            PhysicalPlan::Graph(
                nodedb_physical::physical_plan::GraphOp::EdgePut { src_id, .. }
                | nodedb_physical::physical_plan::GraphOp::EdgeDelete { src_id, .. },
            ) => VShardId::from_key(src_id.as_bytes()).as_u32() == vshard_id,
            _ => true,
        })
        .map(|plan| {
            crate::control::server::dispatch_utils::extract_write_change_set(plan, tenant_id)
        })
        .collect()
}

/// Whether this vShard's slice carries the USER'S own write, as opposed to a
/// derived side effect the Control Plane appended alongside it.
///
/// It gates the applied-response deposit, and that is the whole reason the
/// distinction has to be made: a statement's `CommandComplete` is shaped from
/// ONE deposited response, primary-write participants coalesce first-wins, and
/// a derived participant's response describes a row the user's statement never
/// named. A balance write that won that race handed an `INSERT` tag a count —
/// or, when its flush found the commit already resolved and answered with an
/// empty payload, no count at all — belonging to a different write entirely.
///
/// `is_derived_side_effect` is the named predicate rather than an inline
/// `!matches!(plan, PhysicalPlan::Graph(_))`: the implicit graph edge and the
/// cross-shard balance are the same concept, and spelling it inline here is why
/// the second one never inherited the exclusion.
pub(crate) fn plans_have_primary_write(plans: &[PhysicalPlan]) -> bool {
    plans.iter().any(|plan| {
        crate::control::planner::calvin::is_write_plan(plan)
            && !crate::control::planner::calvin::write_class::is_derived_side_effect(plan)
    })
}

/// Whether this vShard's slice carries a RETURNING-bearing write — a plan whose
/// applied response is DATA-ROWs rather than a bare affected-count. Uses the
/// SAME `describe_plan` classification the coordinator's response-shaping uses,
/// so the two never disagree about which participant owns the returned rows.
pub(crate) fn plans_have_returning(plans: &[PhysicalPlan]) -> bool {
    use crate::control::server::response_shape::types::{PlanKind, describe_plan};
    plans
        .iter()
        .any(|plan| matches!(describe_plan(plan), PlanKind::ReturningRows))
}

impl Scheduler {
    /// Whether THIS node is currently the leader of the data-group owning this
    /// scheduler's vshard.
    ///
    /// Stamped into the `CalvinExecute{Static,Active}` MetaOp at dispatch time
    /// so the Data Plane runs the OLLP optimistic-lock verification (and emits
    /// `OllpRetryRequired`) ONLY on the leader, while every replica applies the
    /// carried predicted write-set verbatim — preserving Calvin determinism.
    ///
    /// Resolved via the existing routing → group-role check (no new election).
    /// On a poisoned lock the inner guard is recovered; a momentarily-unknown
    /// leadership (e.g. mid-election) resolves to `false`, i.e. follower-style
    /// apply against the predicted set, which is always determinism-safe.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn is_group_leader(
        &self,
    ) -> bool {
        let mr = match self.multi_raft.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        mr.vshard_role_is_leader(self.vshard_id)
    }

    /// Broadcast a terminal, NON-retryable routing-failure signal via the
    /// sequencer-group Raft so every replica's `CalvinCompletionRegistry`
    /// fires `note_routing_failed`, waking the coordinator's completion
    /// waiter immediately with the reason instead of leaving it to burn the
    /// full deadline and report a generic timeout. Mirrors the OllpMismatch
    /// broadcast in `handle_executor_response`. Shared by `dispatch_txn` and
    /// `dispatch_active_txn`.
    fn propose_routing_failure(
        &self,
        epoch: u64,
        position: u32,
        txn_id: TxnId,
        err: &crate::Error,
    ) {
        self.propose_sequencer_entry(
            nodedb_cluster::calvin::SequencerEntry::TxnRoutingFailed {
                epoch,
                position,
                detail: err.to_string(),
            },
            txn_id,
            "txn routing-failure signal",
        );
    }

    /// Filter a transaction's write plans down to the slice that homes to this
    /// scheduler's vShard.
    ///
    /// Returns an EMPTY vector when no write plan homes here — that is NOT an
    /// error: a read-only participant (writes elsewhere, only READS this vShard)
    /// legitimately has no local write slice yet must still validate its reads.
    /// Each caller decides what an empty slice means for its path. A genuinely
    /// malformed plan (control-plane-only, unroutable, or a non-write inside a
    /// write txn) is still a hard `Err`.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn local_calvin_plans(
        &self,
        plans: Vec<PhysicalPlan>,
        database_id: DatabaseId,
        epoch: u64,
        position: u32,
    ) -> crate::Result<Vec<PhysicalPlan>> {
        let mut local = Vec::new();
        for plan in plans {
            match super::routing::plan_vshard_in_database(&plan, database_id) {
                PlanRouting::Vshards(vshards) => {
                    if vshards.iter().any(|v| v.as_u32() == self.vshard_id) {
                        local.push(plan);
                    }
                }
                PlanRouting::ControlPlaneOnly => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "calvin txn {epoch}/{position} for vshard {} carries a \
                             control-plane-only plan that must never reach the Data \
                             Plane: {plan:?}",
                            self.vshard_id
                        ),
                    });
                }
                PlanRouting::Unroutable(reason) => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "calvin txn {epoch}/{position} for vshard {} contains an \
                             unroutable plan ({reason}): {plan:?}",
                            self.vshard_id
                        ),
                    });
                }
                PlanRouting::NotAWrite => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "calvin txn {epoch}/{position} for vshard {} contains a \
                             non-write (read/DDL) plan inside a Calvin write \
                             transaction: {plan:?}",
                            self.vshard_id
                        ),
                    });
                }
            }
        }

        Ok(local)
    }

    /// Dispatch a static-set ready transaction to the Data Plane executor.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_txn(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
    ) {
        let tenant_id = txn.tx_class.tenant_id;
        let epoch = txn.epoch;
        let position = txn.position;

        let plans = match super::super::helpers::decode_plans(&txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: plan decode failed; releasing locks and skipping txn"
                );
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let local = match self.local_calvin_plans(plans, txn.tx_class.database_id, epoch, position)
        {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: static txn routing failed; releasing locks"
                );
                self.propose_routing_failure(epoch, position, txn_id, &e);
                self.on_txn_complete(txn_id);
                return;
            }
        };

        // A participant with no local WRITE slice is either a READ-ONLY
        // participant — writes home elsewhere, but a read homes HERE, so it must
        // still validate its slice of the read-set and cast a real commit/abort
        // vote — or a routing bug (neither writes nor reads home here). Only the
        // latter is an error; the former stages a validate-only task below.
        if local.is_empty()
            && !super::routing::homes_versioned_read(
                &txn.tx_class.versioned_reads,
                txn.tx_class.database_id,
                self.vshard_id,
            )
        {
            let e = crate::Error::Internal {
                detail: format!(
                    "calvin txn {epoch}/{position} homes no local write plans or reads \
                     for vshard {}",
                    self.vshard_id
                ),
            };
            error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin scheduler: static txn homes no local work; releasing locks"
            );
            self.propose_routing_failure(epoch, position, txn_id, &e);
            self.on_txn_complete(txn_id);
            return;
        }

        // Write participant (`local` non-empty) or validate-only read
        // participant (`local` empty, a read homes here): both STAGE through the
        // identical static path so each casts a real commit/abort Vote through
        // stage -> resolve -> verdict. The validate-only task stages no plans;
        // its response carries only the read-set vote.
        self.dispatch_calvin_static(txn, txn_id, lock_owner, tenant_id, local);
    }

    /// Build and dispatch a `CalvinExecuteStatic` task, then park the txn in
    /// `pending` as `Staged`.
    ///
    /// Shared by the write path (`plans` = this vShard's local write slice) and
    /// the validate-only read path (`plans` empty). Both carry the txn's FULL
    /// `versioned_reads` to the apply core, which validates the LOCAL slice of
    /// the read-set — whether or not `plans` is empty — and returns the commit
    /// vote on `read_set_valid`. A validate-only task has `has_primary_write ==
    /// false`, so it deposits no result sidecar entry, exactly as intended.
    fn dispatch_calvin_static(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
        tenant_id: crate::types::TenantId,
        plans: Vec<PhysicalPlan>,
    ) {
        // The apply-slot identity (used in the CalvinExecuteStatic task and
        // error logs) is exactly `txn_id`; deriving it here keeps the two in
        // lockstep instead of passing the pair redundantly.
        let epoch = txn_id.epoch;
        let position = txn_id.position;
        let request_id = self.next_request_id();
        let has_primary_write = plans_have_primary_write(&plans);
        let has_returning = plans_have_returning(&plans);
        let change_sets = participant_change_sets(&plans, tenant_id, self.vshard_id);
        let plan = PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
            epoch,
            position,
            tenant_id,
            plans,
            epoch_system_ms: txn.epoch_system_ms,
            is_group_leader: self.is_group_leader(),
            // The replicated read-set travels to the apply core so each
            // participant can check, at apply, whether its slice of the reads was
            // still current. Empty for pure-write / autocommit transactions.
            versioned_reads: txn.tx_class.versioned_reads.as_slice().to_vec(),
        });

        // Calvin allocates the CalvinApplied WAL LSN post-apply (in the
        // scheduler's response handler), so no committed LSN is known at
        // dispatch time to stamp here.
        let request =
            self.build_exempt_request(request_id, tenant_id, txn.tx_class.database_id, plan, None);

        let resp_rx = self.shared.tracker.register(request_id);

        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            if matches!(e, crate::Error::DispatchCapacityBusy { .. }) {
                // Retryable capacity condition (issue 352): nothing was
                // enqueued. Demote the log, count it, and pace the re-drive via
                // the scheduler's drain gate instead of spinning on ERROR.
                debug!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: dispatch deferred (capacity busy)"
                );
                self.metrics.record_dispatch_busy();
                let attempts = self.dispatch_busy_attempts.saturating_add(1);
                self.dispatch_busy_attempts = attempts;
                self.dispatch_busy_until = Some(
                    // no-determinism: the backoff deadline is scheduler observability, not Calvin WAL data
                    Instant::now()
                        + super::scheduler::Scheduler::busy_backoff(
                            attempts,
                            self.vshard_id,
                            epoch,
                        ),
                );
                self.on_txn_complete(txn_id);
                return;
            }
            error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin scheduler: dispatch failed; releasing locks"
            );
            self.on_txn_complete(txn_id);
            return;
        }
        self.dispatch_busy_attempts = 0;
        self.dispatch_busy_until = None;

        self.metrics.record_dispatch();

        // no-determinism: executor latency observability, off-WAL path
        let dispatch_instant = Instant::now();

        self.spawn_response_bridge(txn_id, request_id, resp_rx);

        self.pending.insert(
            txn_id,
            super::super::types::PendingTxn {
                txn,
                lock_owner,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: dispatch_instant,
                has_primary_write,
                has_returning,
                change_sets,
                // This dispatch STAGED the txn (validate + buffer, no apply);
                // its response carries the local commit vote that drives the
                // subsequent flush-or-drop.
                commit_state: Some(super::super::types::CommitState::Staged),
                // Set only once the txn parks in `AwaitingVerdict`.
                verdict_deadline: None,
            },
        );
    }

    /// Dispatch an active dependent-read txn once all passive results are in.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_active_txn(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
        injected_reads: std::collections::BTreeMap<
            nodedb_physical::physical_plan::meta::PassiveReadKeyId,
            nodedb_types::Value,
        >,
    ) {
        let request_id = self.next_request_id();
        let tenant_id = txn.tx_class.tenant_id;
        let epoch = txn.epoch;
        let position = txn.position;

        let plans = match super::super::helpers::decode_plans(&txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active plan decode failed; releasing locks"
                );
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let plans = match self.local_calvin_plans(plans, txn.tx_class.database_id, epoch, position)
        {
            Ok(p) if !p.is_empty() => p,
            Ok(_) => {
                // A dependent-read active txn dispatched here always carries a
                // local write slice (the OLLP orchestrator only routes the write
                // participant through this path). An empty local slice is a
                // routing bug, not a read-only participant — surface it as a
                // terminal routing failure rather than dispatching an
                // active task with nothing to apply.
                let e = crate::Error::Internal {
                    detail: format!(
                        "calvin active txn {epoch}/{position} homes no local write plans \
                         for vshard {}",
                        self.vshard_id
                    ),
                };
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active txn homes no local writes; releasing locks"
                );
                self.propose_routing_failure(epoch, position, txn_id, &e);
                self.on_txn_complete(txn_id);
                return;
            }
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active txn routing failed; releasing locks"
                );
                self.propose_routing_failure(epoch, position, txn_id, &e);
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let has_primary_write = plans_have_primary_write(&plans);
        let has_returning = plans_have_returning(&plans);
        let change_sets = participant_change_sets(&plans, tenant_id, self.vshard_id);
        let plan = PhysicalPlan::Meta(MetaOp::CalvinExecuteActive {
            epoch,
            position,
            tenant_id,
            plans,
            injected_reads,
            epoch_system_ms: txn.epoch_system_ms,
            is_group_leader: self.is_group_leader(),
        });

        // Calvin allocates the CalvinApplied WAL LSN post-apply (in the
        // scheduler's response handler), so no committed LSN is known at
        // dispatch time to stamp here.
        let request =
            self.build_exempt_request(request_id, tenant_id, txn.tx_class.database_id, plan, None);

        let resp_rx = self.shared.tracker.register(request_id);

        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            if self.note_dispatch_busy(&e, epoch) {
                debug!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active dispatch deferred (capacity busy)"
                );
                self.on_txn_complete(txn_id);
                return;
            }
            error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin scheduler: active dispatch failed; releasing locks"
            );
            self.on_txn_complete(txn_id);
            return;
        }

        self.metrics.record_dispatch();

        // no-determinism: executor latency observability, off-WAL path
        let dispatch_instant = Instant::now();

        self.spawn_response_bridge(txn_id, request_id, resp_rx);

        self.pending.insert(
            txn_id,
            super::super::types::PendingTxn {
                txn,
                lock_owner,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: dispatch_instant,
                has_primary_write,
                has_returning,
                change_sets,
                // The dependent-read active path now STAGES (leader-verify OLLP
                // + buffer, no base apply); its response drives the same
                // resolve → redo → flush as the static path, restoring
                // WAL-only-restart durability. `resolve_staged_commit` reads the
                // `read_set_valid: None` the active handler returns as "commit".
                commit_state: Some(super::super::types::CommitState::Staged),
                // Set only once the txn parks in `AwaitingVerdict`.
                verdict_deadline: None,
            },
        );
    }
}
