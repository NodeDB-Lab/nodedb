// SPDX-License-Identifier: BUSL-1.1

//! Calvin deterministic executor handlers.
//!
//! Handler entry points:
//!
//! - [`CoreLoop::execute_calvin_execute_static`]: static-set multi-shard txn
//!   (the common case). It VALIDATES the read-set to compute the local commit
//!   vote and STAGES the transaction's plans into the commit-pending buffer
//!   WITHOUT mutating base or firing side effects, then returns the vote.
//!   [`CoreLoop::execute_calvin_flush`] later replays the staged plans through
//!   the durable apply funnel, or [`CoreLoop::execute_calvin_drop`] discards
//!   them.
//!
//! - [`CoreLoop::execute_calvin_execute_passive`]: passive participant for a
//!   dependent-read txn. Reads each declared key from the local engine and
//!   returns a msgpack-encoded `Vec<(PassiveReadKeyId, Value)>` payload. The
//!   Control Plane scheduler proposes a `CalvinReadResult` Raft entry after
//!   receiving this response.
//!
//! - [`CoreLoop::execute_calvin_execute_active`]: active participant for a
//!   dependent-read txn. Executes the physical plans with the injected read
//!   values already resolved. Performs an OLLP verification hook: if the
//!   active participant detects that the declared predicate no longer matches
//!   the current engine state, it returns `OllpRetryRequired` WITHOUT writing.
//!   The OLLP orchestrator on the Control Plane retries via `Inbox::submit`.
//!
//! The `CalvinApplied` WAL record is written on the Control Plane side (in the
//! scheduler's response path) after a successful response is received through
//! the SPSC bridge; not here in the Data Plane.

use std::panic::{AssertUnwindSafe, catch_unwind};

use tracing::{debug, info_span};

use nodedb_cluster::calvin::types::PassiveReadKey;
use nodedb_types::Value;
use nodedb_types::calvin::VersionedReadEntry;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::commit_pending::PendingCommit;
use crate::data::executor::handlers::transaction::overlay::BitemporalStamp;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::calvin_txn_id::calvin_synthetic_txn_id;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::PassiveReadKeyId;

use std::collections::BTreeMap;

/// Execution context shared by both static and active Calvin handler variants.
///
/// Bundles the epoch-scoped parameters that repeat across
/// `execute_calvin_execute_static` and `execute_calvin_execute_active`,
/// keeping each function's argument count within the lint budget.
pub(in crate::data::executor) struct CalvinExecCtx {
    pub epoch: u64,
    pub position: u32,
    pub epoch_system_ms: i64,
    pub is_group_leader: bool,
}

impl CoreLoop {
    /// Validate a static-set Calvin transaction and stage it for commit.
    ///
    /// Computes the local commit vote by checking whether this participant's
    /// slice of the transaction's LSN-versioned read-set is still current
    /// against the per-core write versions, then STAGES the write plans into
    /// the commit-pending buffer keyed by `(epoch, position)`. It performs NO
    /// base mutation and fires NO side effects — nothing is observable until a
    /// subsequent [`CoreLoop::execute_calvin_flush`] replays the staged plans
    /// (or [`CoreLoop::execute_calvin_drop`] discards them). The response
    /// carries the vote on `read_set_valid`; the deterministic time anchor and
    /// leadership scope are captured with the staged plans and restored at
    /// flush time (when the actual apply — and any time-dependent writes — run).
    pub(in crate::data::executor) fn execute_calvin_execute_static(
        &mut self,
        task: &ExecutionTask,
        ctx: CalvinExecCtx,
        tenant_id: &TenantId,
        plans: &[PhysicalPlan],
        versioned_reads: &[VersionedReadEntry],
    ) -> Response {
        let CalvinExecCtx {
            epoch,
            position,
            epoch_system_ms,
            is_group_leader,
        } = ctx;
        let vshard_id = task.request.vshard_id.as_u32();
        debug!(
            core = self.core_id,
            epoch,
            position,
            epoch_system_ms,
            vshard_id,
            is_group_leader,
            plan_count = plans.len(),
            read_count = versioned_reads.len(),
            "calvin stage for commit"
        );
        let _stage_span = info_span!(
            "executor_stage",
            epoch,
            position,
            vshard = vshard_id,
            tenant_id = tenant_id.as_u64(),
            trace_id = ?task.request.trace_id,
        )
        .entered();

        // Derive the synthetic transaction identity before ANY mutation. A
        // representational failure must not leave either a pending buffer or an
        // overlay behind for a transaction that cannot later be resolved.
        let synthetic_txn_id = match calvin_synthetic_txn_id(epoch, position, vshard_id) {
            Ok(id) => id,
            Err(error) => {
                return self.calvin_stage_failure(task, epoch, position, vshard_id, error);
            }
        };

        // Stage every plan before publishing `PendingCommit`. Panic isolation
        // cleans both staging representations after any failure.
        let stage_result = catch_unwind(AssertUnwindSafe(|| {
            for plan in plans {
                self.stage_calvin_overlay(task, synthetic_txn_id, *tenant_id, plan)?;
                // Test-only fault boundary after a potentially-mutating stage.
                crate::fail_point!("calvin_static::during_overlay_stage");
            }
            Ok::<(), ErrorCode>(())
        }));
        match stage_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return self.calvin_stage_failure(task, epoch, position, vshard_id, error);
            }
            Err(payload) => {
                return self.calvin_stage_failure(
                    task,
                    epoch,
                    position,
                    vshard_id,
                    ErrorCode::Internal {
                        detail: format!(
                            "panic while staging static Calvin transaction: {}",
                            calvin_panic_payload_to_string(payload.as_ref())
                        ),
                    },
                );
            }
        }

        // Local commit vote: is this participant's slice of the read-set still
        // current against the local write versions? Empty read-set is vacuously
        // current. Read-only — no base mutation here. A stale-read false vote
        // retains the fully staged state until the durable global verdict.
        let vote = self.read_set_still_current(task, tenant_id.as_u64(), versioned_reads);

        // Publish only a fully staged transaction. The verdict-driven flush
        // replays this raw plan buffer; a global abort drops it and its overlay.
        self.commit_pending.insert(
            (epoch, position, vshard_id),
            PendingCommit {
                plans: plans.to_vec(),
                tenant_id: *tenant_id,
                epoch_system_ms,
                is_group_leader,
            },
        );

        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            error_code: None,
            read_set_valid: Some(vote),
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    /// Clean failed static staging and return an explicit abort vote.
    /// Defensive removal clears document/KV and graph overlays plus their gauge.
    fn calvin_stage_failure<E>(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
        vshard_id: u32,
        error: E,
    ) -> Response
    where
        E: Into<ErrorCode>,
    {
        self.commit_pending.remove(&(epoch, position, vshard_id));
        self.drop_calvin_synthetic_overlay(epoch, position, vshard_id);
        let mut response = self.response_error(task, error.into());
        // Scheduler treats this as a durable local abort vote and still waits
        // for the authoritative global verdict before issuing any drop.
        response.read_set_valid = Some(false);
        response
    }

    /// Flush a staged Calvin transaction to base storage.
    ///
    /// Pops the plans staged by [`CoreLoop::execute_calvin_execute_static`]
    /// under `(epoch, position)` and replays them through the durable apply
    /// funnel (`execute_transaction_batch`) — the same funnel the single-shard
    /// commit and recovery use — so base mutation, side effects, and
    /// version recording all run exactly once here. The deterministic epoch
    /// time anchor and leadership scope captured at stage time are restored
    /// around the apply so time-dependent writes stay identical across
    /// replicas. An absent key (already flushed or dropped, e.g. a duplicate
    /// dispatch) is an idempotent no-op returning `Ok`.
    pub(in crate::data::executor) fn execute_calvin_flush(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
    ) -> Response {
        let vshard_id = task.request.vshard_id.as_u32();
        // Capture the resolve-time bitemporal stamps (if `CalvinResolve` staged
        // them into the synthetic overlay) BEFORE the overlay is dropped, so the
        // base install below reuses the exact stamp the redo carries rather than
        // minting a fresh one. Empty when this transaction wrote no bitemporal
        // document rows or resolve never ran.
        let synthetic_txn_id = calvin_synthetic_txn_id(epoch, position, vshard_id).ok();
        let bitemporal_stamps: Vec<(u32, BitemporalStamp)> = synthetic_txn_id
            .and_then(|synthetic| self.txn_overlays.get(&synthetic))
            .map(|overlay| overlay.all_bitemporal_stamps().collect())
            .unwrap_or_default();
        let graph_system_from = synthetic_txn_id
            .and_then(|synthetic| self.graph_txn_overlays.get(&synthetic))
            .and_then(|overlay| overlay.resolved_system_from());
        // Drop the synthetic overlay entry staged by
        // `execute_calvin_execute_static` unconditionally, before the apply
        // below: idempotent no-op on a duplicate dispatch.
        self.drop_calvin_synthetic_overlay(epoch, position, vshard_id);
        let Some(pending) = self.commit_pending.remove(&(epoch, position, vshard_id)) else {
            debug!(
                core = self.core_id,
                epoch, position, vshard_id, "calvin flush: no staged commit (already resolved)"
            );
            return self.response_ok(task);
        };
        let _apply_span = info_span!(
            "executor_apply",
            epoch,
            position,
            vshard = vshard_id,
            tenant_id = pending.tenant_id.as_u64(),
            trace_id = ?task.request.trace_id,
        )
        .entered();
        const NANOS_PER_MS: i64 = 1_000_000;
        self.hlc
            .update_from_remote(pending.epoch_system_ms.saturating_mul(NANOS_PER_MS));
        self.epoch_system_ms = Some(pending.epoch_system_ms);
        // Scope OLLP verification to this participant's staged leadership for the
        // batch, then restore the resting (authoritative) state.
        let prev_group_leader = self.ollp_is_group_leader;
        self.ollp_is_group_leader = pending.is_group_leader;
        // Install the captured resolve-time stamps into apply scratch; the
        // batch consumes them for its bitemporal document puts and clears the
        // scratch when it returns. `txn_id = None`: the synthetic overlay was
        // already dropped above, so the stamps are threaded in directly here.
        for (surrogate, stamp) in bitemporal_stamps {
            self.active_bitemporal_stamps.insert(surrogate, stamp);
        }
        self.active_graph_system_from = graph_system_from;
        // The read-set was already validated at stage time and drives the
        // flush/drop decision; the replay itself carries no read-set to re-check.
        // Scope the flush key so `record_batch_index_write_values` stages this
        // batch's index tuples (the apply carries `wal_lsn: None`); the post-apply
        // `RecordCalvinWriteVersions` op drains them at the replicated applied LSN.
        self.calvin_flush_key = Some((epoch, position, vshard_id));
        let result = self.execute_transaction_batch(
            task,
            pending.tenant_id.as_u64(),
            &pending.plans,
            &[],
            None,
        );
        self.calvin_flush_key = None;
        self.ollp_is_group_leader = prev_group_leader;
        self.epoch_system_ms = None;
        result
    }

    /// Discard a staged Calvin transaction.
    ///
    /// Removes the plans staged under `(epoch, position, vshard)` from the
    /// commit-pending buffer and fires nothing — no base mutation, no side
    /// effects. An
    /// absent key (already flushed or dropped) is an idempotent no-op.
    pub(in crate::data::executor) fn execute_calvin_drop(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
    ) -> Response {
        let vshard_id = task.request.vshard_id.as_u32();
        let existed = self
            .commit_pending
            .remove(&(epoch, position, vshard_id))
            .is_some();
        // Discard the synthetic overlay entry alongside the raw plan buffer;
        // idempotent no-op if it was never staged or already removed.
        self.drop_calvin_synthetic_overlay(epoch, position, vshard_id);
        debug!(
            core = self.core_id,
            epoch, position, vshard_id, existed, "calvin drop: discarding staged commit"
        );
        self.response_ok(task)
    }

    /// Execute a passive-participant dependent-read Calvin txn.
    ///
    /// Reads each key from the local engine state and returns a
    /// msgpack-encoded `Vec<(PassiveReadKeyId, Value)>` as the response
    /// payload. The Control Plane scheduler collects these values and
    /// proposes a `ReplicatedWrite::CalvinReadResult` entry to the
    /// per-vshard Raft group so all replicas see the same read results.
    ///
    /// `Instant::now()` is intentionally absent here — this is a
    /// synchronous Data Plane read with no timer interaction.
    pub(in crate::data::executor) fn execute_calvin_execute_passive(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
        tenant_id: &TenantId,
        keys_to_read: &[PassiveReadKey],
    ) -> Response {
        debug!(
            core = self.core_id,
            epoch,
            position,
            vshard_id = task.request.vshard_id.as_u32(),
            key_count = keys_to_read.len(),
            "calvin execute passive: reading keys"
        );

        let mut results: Vec<(PassiveReadKeyId, Value)> = Vec::with_capacity(keys_to_read.len());

        for passive_key in keys_to_read {
            // Build a PassiveReadKeyId for each surrogate in the engine key set.
            // For this v1 handler the engine key set carries single surrogates per
            // key (as specified in the design); we iterate all surrogates to be safe.
            let values = self.read_passive_key(tenant_id, &passive_key.engine_key);
            results.extend(values);
        }

        match response_codec::encode_serde(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("calvin passive read encode: {e}"),
                },
            ),
        }
    }

    /// Stage an active-participant dependent-read Calvin txn for commit.
    ///
    /// Mirrors [`CoreLoop::execute_calvin_execute_static`]: it performs NO base
    /// mutation and fires NO side effects — it buffers the write plans in
    /// `commit_pending` and stages each into `txn_overlays` under the synthetic
    /// `TxnId`, so a subsequent `CalvinResolve` reconstitutes them as one
    /// replayable `RedoRecord` and [`CoreLoop::execute_calvin_flush`] applies
    /// them. This restores WAL-only-restart durability for the dependent-read
    /// path, which previously applied directly with `wal_lsn: None` (only a
    /// non-replayable `CalvinApplied` marker survived).
    ///
    /// The one divergence from the static path: OLLP predicate verification
    /// (leader-only) runs HERE, before staging, via
    /// [`CoreLoop::verify_calvin_active_ollp`]. The dependent-read path has no
    /// LSN-versioned read-set to vote on; its conflict detector is the OLLP
    /// `actual != predicted` re-check. Running it at stage time (not flush)
    /// ensures a mismatch returns `OllpRetryRequired` and stages nothing —
    /// otherwise a stale redo would be WAL-appended before the flush-time check
    /// (whose retry signal is swallowed as a degraded shard). The Control Plane
    /// scheduler releases locks and re-recons on `OllpRetryRequired`.
    ///
    /// `injected_reads` is retained on the wire for future plan variants that
    /// reference resolved read values by `PassiveReadKeyId`; in v1 the
    /// coordinator baked the read values into concrete point ops / the predicted
    /// surrogate set at recon, so the plans are self-contained and stage
    /// byte-identically to the static path.
    pub(in crate::data::executor) fn execute_calvin_execute_active(
        &mut self,
        task: &ExecutionTask,
        ctx: CalvinExecCtx,
        tenant_id: &TenantId,
        plans: &[PhysicalPlan],
        injected_reads: &BTreeMap<PassiveReadKeyId, Value>,
    ) -> Response {
        let CalvinExecCtx {
            epoch,
            position,
            epoch_system_ms,
            is_group_leader,
        } = ctx;
        let vshard_id = task.request.vshard_id.as_u32();
        debug!(
            core = self.core_id,
            epoch,
            position,
            epoch_system_ms,
            vshard_id,
            is_group_leader,
            plan_count = plans.len(),
            injected_count = injected_reads.len(),
            "calvin execute active"
        );
        let _stage_span = info_span!(
            "executor_stage",
            epoch,
            position,
            vshard = vshard_id,
            tenant_id = tenant_id.as_u64(),
            trace_id = ?task.request.trace_id,
        )
        .entered();

        // OLLP verification runs HERE, before staging, so a predicate-drift
        // mismatch surfaces on THIS stage response (where the scheduler releases
        // locks and re-recons) and nothing is staged, resolved, or WAL-appended.
        // Scoped to this replica's staged leadership for the check, then the
        // resting (authoritative) state is restored. A read-only scan needs no
        // time anchor, so `epoch_system_ms`/`hlc` stay unset until flush
        // (mirroring the static path, where they ride `PendingCommit`).
        let prev_group_leader = self.ollp_is_group_leader;
        self.ollp_is_group_leader = is_group_leader;
        let verified = self.verify_calvin_active_ollp(task, tenant_id.as_u64(), plans);
        self.ollp_is_group_leader = prev_group_leader;
        match verified {
            Ok(true) => {}
            Ok(false) => return self.response_error(task, ErrorCode::OllpRetryRequired),
            Err(e) => return self.response_error(task, e),
        }

        // Stage exactly like `execute_calvin_execute_static`: buffer the plans in
        // `commit_pending` (the sole durable apply the flush replays) and stage
        // each write into `txn_overlays` under the synthetic `TxnId` (producer
        // side for `CalvinResolve`). No base mutation, no side effects; the time
        // anchor + leadership scope captured here are restored at flush time.
        self.commit_pending.insert(
            (epoch, position, vshard_id),
            PendingCommit {
                plans: plans.to_vec(),
                tenant_id: *tenant_id,
                epoch_system_ms,
                is_group_leader,
            },
        );
        let synthetic_txn_id = match calvin_synthetic_txn_id(epoch, position, vshard_id) {
            Ok(id) => id,
            Err(e) => return self.response_error(task, e),
        };
        for plan in plans {
            if let Err(e) = self.stage_calvin_overlay(task, synthetic_txn_id, *tenant_id, plan) {
                return self.response_error(task, e);
            }
        }

        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            error_code: None,
            // The dependent-read path carries no versioned read-set; `None` maps
            // to "commit" in `resolve_staged_commit` (`read_set_valid != Some(false)`).
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }
}
fn calvin_panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "<non-string panic payload>".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_physical::physical_plan::{DocumentOp, TimeseriesOp};
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::*;
    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::doc_format;
    use crate::data::executor::handlers::transaction::overlay::Staged;
    use crate::types::{DatabaseId, RequestId, TraceId, VShardId};

    /// A minimal `ExecutionTask` homing to vShard 0, tenant 1, database
    /// DEFAULT -- everything a Calvin static-execute handler needs beyond
    /// its explicit `CalvinExecCtx` / `tenant_id` / `plans` arguments.
    fn make_task() -> ExecutionTask {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
            document_id: "y".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        let request = Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        };
        ExecutionTask::new(request)
    }

    fn doc_value(field: &str, val: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(field.to_string(), Value::String(val.into()));
        zerompk::to_msgpack_vec(&Value::Object(obj)).unwrap()
    }

    fn point_insert_plan(collection: &str, document_id: &str, surrogate: u32) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: document_id.to_string(),
            value: doc_value("a", "1"),
            if_absent: false,
            surrogate: Surrogate::new(surrogate),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn canonical_ilp_plan(collection: &str, lines: Vec<&str>, tokens: Vec<u32>) -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            payload: zerompk::to_msgpack_vec(&lines).expect("canonical ILP payload"),
            format: "ilp-msgpack".to_owned(),
            wal_lsn: None,
            surrogates: tokens.into_iter().map(Surrogate::new).collect(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn bulk_delete_plan(collection: &str, predicted: Option<Vec<u32>>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: predicted,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        })
    }

    /// Seed a row directly into base storage (bypassing Calvin staging), the
    /// pre-existing state the active-path OLLP verifier scans against.
    fn seed_row(core: &mut CoreLoop, collection: &str, surrogate: u32) {
        let doc_id = nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate));
        let body = doc_format::canonicalize_document_for_storage(&doc_value("a", "1"));
        core.sparse
            .put(DatabaseId::DEFAULT.as_u64(), 1, collection, &doc_id, &body)
            .expect("seed row");
    }

    #[test]
    fn calvin_execute_static_stages_point_insert_into_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let task = make_task();
        let tenant_id = TenantId::new(1);
        let plans = vec![point_insert_plan("orders", "o1", 7)];
        let ctx = CalvinExecCtx {
            epoch: 1,
            position: 0,
            epoch_system_ms: 0,
            is_group_leader: true,
        };

        let resp = core.execute_calvin_execute_static(&task, ctx, &tenant_id, &plans, &[]);
        assert_eq!(resp.status, Status::Ok);

        let vshard_id = task.request.vshard_id.as_u32();

        // `commit_pending` is unchanged -- it still holds the raw plans that
        // drive the base install at flush time.
        assert!(
            core.commit_pending.contains_key(&(1, 0, vshard_id)),
            "commit_pending must still be populated exactly as before this unit"
        );

        // The synthetic overlay entry additionally holds the resolved
        // post-image for the concrete point-write plan.
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();
        let coll_key = (DatabaseId::DEFAULT, tenant_id, "orders".to_string());
        let expected_body = doc_format::canonicalize_document_for_storage(&doc_value("a", "1"));
        assert_eq!(
            core.txn_overlays
                .get(&synthetic)
                .and_then(|o| o.get(&coll_key, 7)),
            Some(&Staged::Put(expected_body)),
            "the Calvin write plan must be staged into the synthetic-TxnId overlay"
        );
    }

    #[test]
    fn synthetic_id_failure_leaves_no_pending_or_overlay_state() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let epoch_outside_synthetic_range = 1_u64 << 33;

        let response = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: epoch_outside_synthetic_range,
                position: 0,
                epoch_system_ms: 0,
                is_group_leader: true,
            },
            &tenant_id,
            &[point_insert_plan("orders", "o1", 7)],
            &[],
        );

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.read_set_valid, Some(false));
        assert!(core.commit_pending.is_empty());
        assert!(core.txn_overlays.is_empty());
        assert!(core.graph_txn_overlays.is_empty());
    }

    #[test]
    fn static_stage_error_cleans_all_prior_overlay_state_before_voting_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let metrics = std::sync::Arc::new(crate::control::metrics::SystemMetrics::new());
        core.metrics = Some(metrics.clone());
        let gauge = || {
            metrics
                .active_txn_overlays
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        let baseline = gauge();
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let ctx = CalvinExecCtx {
            epoch: 9,
            position: 3,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let vshard = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(9, 3, vshard).unwrap();
        core.graph_txn_overlay_mut(synthetic);
        assert_eq!(
            gauge(),
            baseline + 1,
            "graph staging must increment the gauge"
        );
        // The first plan adds a document overlay; the second is invalid without
        // an OLLP prediction and must clean both overlays atomically.
        let plans = vec![
            point_insert_plan("orders", "o1", 7),
            bulk_delete_plan("orders", None),
        ];

        let response = core.execute_calvin_execute_static(&task, ctx, &tenant_id, &plans, &[]);

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.read_set_valid, Some(false));
        assert!(!core.commit_pending.contains_key(&(9, 3, vshard)));
        assert!(!core.txn_overlays.contains_key(&synthetic));
        assert!(!core.graph_txn_overlays.contains_key(&synthetic));
        assert_eq!(
            gauge(),
            baseline,
            "failed staging must restore the overlay gauge"
        );
    }

    #[test]
    fn static_stage_error_also_cleans_existing_graph_overlay_for_synthetic_id() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let vshard = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(9, 5, vshard).unwrap();
        // Use the graph-overlay choke point so this regression also covers its
        // gauge accounting without requiring a graph catalog fixture.
        core.graph_txn_overlay_mut(synthetic);

        let response = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 9,
                position: 5,
                epoch_system_ms: 0,
                is_group_leader: true,
            },
            &tenant_id,
            &[bulk_delete_plan("orders", None)],
            &[],
        );

        assert_eq!(response.read_set_valid, Some(false));
        assert!(!core.graph_txn_overlays.contains_key(&synthetic));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn static_stage_panic_cleans_prior_overlay_state_before_voting_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let metrics = std::sync::Arc::new(crate::control::metrics::SystemMetrics::new());
        core.metrics = Some(metrics.clone());
        let gauge = || {
            metrics
                .active_txn_overlays
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        let baseline = gauge();
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let ctx = CalvinExecCtx {
            epoch: 9,
            position: 4,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let vshard = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(9, 4, vshard).unwrap();
        core.graph_txn_overlay_mut(synthetic);
        assert_eq!(
            gauge(),
            baseline + 1,
            "graph staging must increment the gauge"
        );
        let _fail = crate::fail_point::FailGuard::install(
            "calvin_static::during_overlay_stage",
            crate::fail_point::FailAction::Panic,
        );

        let response = core.execute_calvin_execute_static(
            &task,
            ctx,
            &tenant_id,
            &[point_insert_plan("orders", "o1", 7)],
            &[],
        );

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.read_set_valid, Some(false));
        assert!(!core.commit_pending.contains_key(&(9, 4, vshard)));
        assert!(!core.txn_overlays.contains_key(&synthetic));
        assert!(!core.graph_txn_overlays.contains_key(&synthetic));
        assert_eq!(
            gauge(),
            baseline,
            "panic cleanup must restore the overlay gauge"
        );
    }

    #[test]
    fn static_calvin_ilp_staging_is_all_or_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();
        let tenant = TenantId::new(1);
        let vshard = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(21, 1, vshard).expect("synthetic transaction id");

        let malformed = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cpu"),
            payload: vec![0xc1],
            format: "ilp-msgpack".to_owned(),
            wal_lsn: None,
            surrogates: vec![Surrogate::new(1)],
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let failed = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 21,
                position: 1,
                epoch_system_ms: 0,
                is_group_leader: true,
            },
            &tenant,
            &[malformed],
            &[],
        );
        assert_eq!(failed.read_set_valid, Some(false));
        assert!(matches!(
            failed.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ));
        assert!(!core.commit_pending.contains_key(&(21, 1, vshard)));
        assert!(!core.txn_overlays.contains_key(&synthetic));

        let valid = canonical_ilp_plan("cpu", vec!["cpu value=1i", "cpu value=2i"], vec![1, 2]);
        let staged = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 21,
                position: 1,
                epoch_system_ms: 0,
                is_group_leader: true,
            },
            &tenant,
            &[valid],
            &[],
        );
        assert_eq!(staged.read_set_valid, Some(true));
        assert_eq!(
            core.txn_overlays
                .get(&synthetic)
                .map(|overlay| overlay.len()),
            Some(2)
        );

        let mismatch = canonical_ilp_plan("cpu", vec!["memory value=1i"], vec![3]);
        let failed = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 21,
                position: 2,
                epoch_system_ms: 0,
                is_group_leader: true,
            },
            &tenant,
            &[mismatch],
            &[],
        );
        let mismatch_id = calvin_synthetic_txn_id(21, 2, vshard).expect("synthetic transaction id");
        assert_eq!(failed.read_set_valid, Some(false));
        assert!(!core.commit_pending.contains_key(&(21, 2, vshard)));
        assert!(!core.txn_overlays.contains_key(&mismatch_id));

        core.ts_tuning.max_tag_cardinality = 1;
        let overflow = canonical_ilp_plan(
            "cpu",
            vec!["cpu,host=a value=1i", "cpu,host=b value=2i"],
            vec![4, 5],
        );
        let failed = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 21,
                position: 3,
                epoch_system_ms: 0,
                is_group_leader: true,
            },
            &tenant,
            &[overflow],
            &[],
        );
        let overflow_id = calvin_synthetic_txn_id(21, 3, vshard).expect("synthetic transaction id");
        assert_eq!(failed.status, Status::Error);
        assert_eq!(failed.read_set_valid, Some(false));
        assert!(matches!(
            failed.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ));
        assert!(
            !core.commit_pending.contains_key(&(21, 3, vshard)),
            "a rejected stage cannot reach the TransactionRedo-producing flush path"
        );
        assert!(!core.txn_overlays.contains_key(&overflow_id));
    }

    #[test]
    fn calvin_flush_drops_synthetic_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let task = make_task();
        let tenant_id = TenantId::new(1);
        let plans = vec![point_insert_plan("orders", "o1", 7)];
        let ctx = CalvinExecCtx {
            epoch: 1,
            position: 0,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let resp = core.execute_calvin_execute_static(&task, ctx, &tenant_id, &plans, &[]);
        assert_eq!(resp.status, Status::Ok);

        let vshard_id = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();
        assert!(core.txn_overlays.contains_key(&synthetic));

        let flush_resp = core.execute_calvin_flush(&task, 1, 0);
        assert_eq!(flush_resp.status, Status::Ok);

        assert!(
            !core.txn_overlays.contains_key(&synthetic),
            "flush must drop the synthetic overlay entry alongside commit_pending"
        );
    }

    #[test]
    fn calvin_drop_discards_synthetic_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let task = make_task();
        let tenant_id = TenantId::new(1);
        let plans = vec![point_insert_plan("orders", "o1", 7)];
        let ctx = CalvinExecCtx {
            epoch: 1,
            position: 0,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let resp = core.execute_calvin_execute_static(&task, ctx, &tenant_id, &plans, &[]);
        assert_eq!(resp.status, Status::Ok);

        let vshard_id = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();
        assert!(core.txn_overlays.contains_key(&synthetic));

        let drop_resp = core.execute_calvin_drop(&task, 1, 0);
        assert_eq!(drop_resp.status, Status::Ok);

        assert!(
            !core.txn_overlays.contains_key(&synthetic),
            "drop must discard the synthetic overlay entry alongside commit_pending"
        );
    }

    /// The dependent-read ACTIVE path STAGES its writes (into `commit_pending` +
    /// the synthetic overlay) instead of applying them to base directly. This is
    /// the direct regression guard for U-CAL5: before it, this handler called
    /// `execute_transaction_batch` inline (`wal_lsn: None`), so a Calvin-committed
    /// dependent-read write left only a non-replayable `CalvinApplied` marker and
    /// was lost on a WAL-only restart. Staging routes it through the same
    /// resolve → redo → flush the static path uses.
    #[test]
    fn calvin_execute_active_stages_point_insert_into_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let task = make_task();
        let tenant_id = TenantId::new(1);
        let plans = vec![point_insert_plan("orders", "o1", 7)];
        let ctx = CalvinExecCtx {
            epoch: 1,
            position: 0,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let injected = BTreeMap::new();

        let resp = core.execute_calvin_execute_active(&task, ctx, &tenant_id, &plans, &injected);
        assert_eq!(resp.status, Status::Ok);
        // The dependent-read path carries no versioned read-set; `None` maps to
        // "commit" in `resolve_staged_commit`.
        assert_eq!(resp.read_set_valid, None);

        let vshard_id = task.request.vshard_id.as_u32();

        // STAGED, not applied: the plans are buffered for the flush replay.
        assert!(
            core.commit_pending.contains_key(&(1, 0, vshard_id)),
            "active-path write must be STAGED into commit_pending, not applied directly"
        );

        // No base mutation at stage time — the row appears only after flush.
        let doc_id = nodedb_types::StorageKey::for_surrogate(Surrogate::new(7));
        assert!(
            core.sparse
                .get(DatabaseId::DEFAULT.as_u64(), 1, "orders", &doc_id)
                .expect("base get")
                .is_none(),
            "staging the active-path write must NOT mutate base storage"
        );

        // The synthetic overlay holds the resolved post-image (producer side for
        // `CalvinResolve` → redo).
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();
        let coll_key = (DatabaseId::DEFAULT, tenant_id, "orders".to_string());
        let expected_body = doc_format::canonicalize_document_for_storage(&doc_value("a", "1"));
        assert_eq!(
            core.txn_overlays
                .get(&synthetic)
                .and_then(|o| o.get(&coll_key, 7)),
            Some(&Staged::Put(expected_body)),
            "the active Calvin write plan must be staged into the synthetic-TxnId overlay"
        );
    }

    /// On the data-group leader, a predicate-write plan whose carried OLLP
    /// predicted set no longer matches live state returns `OllpRetryRequired`
    /// BEFORE staging anything — so no stale redo is WAL-appended and the
    /// coordinator can re-recon under a fresh attempt. Verifying at stage time
    /// (not flush) is the one divergence from the static path.
    #[test]
    fn calvin_execute_active_ollp_drift_returns_retry_and_stages_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        // Live match-all set is {10, 11}; the plan predicts only {10} → drift.
        seed_row(&mut core, "orders", 10);
        seed_row(&mut core, "orders", 11);

        let task = make_task();
        let tenant_id = TenantId::new(1);
        let plans = vec![bulk_delete_plan("orders", Some(vec![10]))];
        let ctx = CalvinExecCtx {
            epoch: 1,
            position: 0,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let injected = BTreeMap::new();

        let resp = core.execute_calvin_execute_active(&task, ctx, &tenant_id, &plans, &injected);
        assert_eq!(resp.status, Status::Error);
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );

        // Drift stages NOTHING — neither the raw buffer nor the overlay.
        let vshard_id = task.request.vshard_id.as_u32();
        assert!(
            !core.commit_pending.contains_key(&(1, 0, vshard_id)),
            "an OLLP-drift retry must not leave a staged commit buffer"
        );
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();
        assert!(
            !core.txn_overlays.contains_key(&synthetic),
            "an OLLP-drift retry must not leave a staged overlay"
        );
    }
}
