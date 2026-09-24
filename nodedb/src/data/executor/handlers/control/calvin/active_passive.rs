// SPDX-License-Identifier: BUSL-1.1

//! Passive and active participants for a dependent-read Calvin transaction.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use tracing::{debug, info_span};

use nodedb_cluster::calvin::types::PassiveReadKey;
use nodedb_types::Value;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::commit_pending::PendingCommit;
use crate::data::executor::handlers::control::calvin_reply::CalvinReply;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::PassiveReadKeyId;

use crate::data::executor::handlers::control::calvin_txn_id::calvin_synthetic_txn_id;
use crate::data::panic_payload::panic_payload_to_string;

use super::shared::CalvinExecCtx;

impl CoreLoop {
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
    /// mutation and fires NO side effects. It stages each write plan into
    /// `txn_overlays` under the synthetic `TxnId` and buffers the plans in
    /// `commit_pending`, so a subsequent `CalvinResolve` builds one redo record
    /// and [`CoreLoop::execute_calvin_flush`] installs it.
    ///
    /// The one divergence from the static path: OLLP predicate verification
    /// (leader-only) runs HERE, before staging, via
    /// [`CoreLoop::verify_calvin_active_ollp`]. The dependent-read path has no
    /// LSN-versioned read-set to vote on; its conflict detector is the OLLP
    /// `actual != predicted` re-check. A mismatch returns `OllpRetryRequired`
    /// and stages nothing, so no redo record is appended for it. The Control
    /// Plane scheduler releases locks and re-recons on `OllpRetryRequired`.
    ///
    /// `injected_reads` is retained on the wire for future plan variants that
    /// reference resolved read values by `PassiveReadKeyId`. The coordinator
    /// bakes the read values into concrete point ops and the predicted
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
        // resting (authoritative) state is restored.
        let prev_group_leader = self.calvin.ollp_is_group_leader;
        self.calvin.ollp_is_group_leader = is_group_leader;
        let verified = self.verify_calvin_active_ollp(task, tenant_id.as_u64(), plans);
        self.calvin.ollp_is_group_leader = prev_group_leader;
        match verified {
            Ok(true) => {}
            Ok(false) => return self.response_error(task, ErrorCode::OllpRetryRequired),
            Err(e) => return self.response_error(task, e),
        }

        let synthetic_txn_id = match calvin_synthetic_txn_id(epoch, position, vshard_id) {
            Ok(id) => id,
            Err(e) => return self.calvin_stage_failure(task, epoch, position, vshard_id, e),
        };
        // Staging reads the epoch's time anchor, so every replica stages the
        // same images.
        let prev_epoch_ms = self.epoch_system_ms;
        self.epoch_system_ms = Some(epoch_system_ms);
        // A panic while staging drops the staged state like any refusal,
        // instead of unwinding past a half-staged overlay.
        let staged = catch_unwind(AssertUnwindSafe(|| {
            let mut reply = CalvinReply::default();
            for plan in plans {
                self.stage_calvin_plan(task, synthetic_txn_id, *tenant_id, plan, &mut reply)?;
            }
            Ok::<CalvinReply, ErrorCode>(reply)
        }));
        self.epoch_system_ms = prev_epoch_ms;
        let reply = match staged {
            Ok(Ok(reply)) => reply,
            Ok(Err(e)) => return self.calvin_stage_failure(task, epoch, position, vshard_id, e),
            Err(payload) => {
                return self.calvin_stage_failure(
                    task,
                    epoch,
                    position,
                    vshard_id,
                    ErrorCode::Internal {
                        detail: format!(
                            "panic while staging active Calvin transaction: {}",
                            panic_payload_to_string(payload.as_ref())
                        ),
                    },
                );
            }
        };

        // Publish only a fully staged transaction. Resolve reads these plans
        // against the overlay; a global abort drops both.
        self.calvin.commit_pending.insert(
            (epoch, position, vshard_id),
            PendingCommit {
                plans: plans.to_vec(),
                tenant_id: *tenant_id,
                epoch_system_ms,
                reply,
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
            // The dependent-read path carries no versioned read-set; `None` maps
            // to "commit" in `resolve_staged_commit` (`read_set_valid != Some(false)`).
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::doc_format;
    use crate::data::executor::handlers::transaction::overlay::Staged;
    use crate::types::DatabaseId;

    use super::super::shared::test_support::{
        bulk_delete_plan, doc_value, make_task, point_insert_plan, seed_row,
    };

    /// The dependent-read ACTIVE path STAGES its writes (into `commit_pending` +
    /// the synthetic overlay) instead of applying them to base directly, so a
    /// Calvin-committed dependent-read write reaches the WAL as a redo record.
    /// Staging routes it through the same resolve → redo → flush the static
    /// path uses.
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
            core.calvin.commit_pending.contains_key(&(1, 0, vshard_id)),
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
            !core.calvin.commit_pending.contains_key(&(1, 0, vshard_id)),
            "an OLLP-drift retry must not leave a staged commit buffer"
        );
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();
        assert!(
            !core.txn_overlays.contains_key(&synthetic),
            "an OLLP-drift retry must not leave a staged overlay"
        );
    }
}
