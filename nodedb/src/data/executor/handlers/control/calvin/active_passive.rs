// SPDX-License-Identifier: BUSL-1.1

//! Passive and active participants for a dependent-read Calvin transaction.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use tracing::{debug, info_span};

use nodedb_cluster::calvin::types::PassiveReadKey;
use nodedb_types::Value;

use crate::bridge::envelope::{ErrorCode, Payload, Response, StageVote, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::commit_pending::PendingCommit;
use crate::data::executor::handlers::control::calvin_reply::{CalvinReply, CalvinStaging};
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
    /// Reads each key from base storage and returns a zerompk-encoded
    /// `Vec<(PassiveReadKeyId, Value)>` as the response payload. The
    /// scheduler holds the transaction's locks on these keys, so no other
    /// write changes them before the transaction's verdict. A successful
    /// read votes commit: a passive participant writes nothing. The scheduler
    /// proposes the payload as a `ReplicatedWrite::CalvinReadResult` entry
    /// to the data group of every active vShard.
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

        let database_id = task.request.database_id.as_u64();
        let mut results: Vec<(PassiveReadKeyId, Value)> = Vec::with_capacity(keys_to_read.len());
        for passive_key in keys_to_read {
            match self.read_passive_key(database_id, tenant_id, &passive_key.engine_key) {
                Ok(values) => results.extend(values),
                Err(code) => return self.response_error(task, code),
            }
        }

        match zerompk::to_msgpack_vec(&results) {
            Ok(payload) => {
                let mut response = self.response_with_payload(task, payload);
                response.stage_vote = Some(StageVote::Commit);
                response
            }
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
    /// and [`CoreLoop::install_calvin_redo`] installs it.
    ///
    /// The one divergence from the static path: OLLP predicate verification
    /// (leader-only) runs HERE, before staging, via
    /// [`CoreLoop::verify_calvin_active_ollp`]. The dependent-read path has no
    /// write-versioned read-set to vote on; its conflict detector is the OLLP
    /// `actual != predicted` re-check. A mismatch returns `OllpRetryRequired`
    /// and stages nothing, so no redo entry exists for it. The Control
    /// Plane scheduler releases locks and re-recons on `OllpRetryRequired`.
    ///
    /// The coordinator bakes the values it read into concrete point ops, so
    /// the plans are self-contained and stage byte-identically to the static
    /// path. The scheduler compared `injected_reads` with those values before
    /// it dispatched this stage, and a mismatch never reaches the core.
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
        } = ctx;
        let vshard_id = task.request.vshard_id.as_u32();
        debug!(
            core = self.core_id,
            epoch,
            position,
            epoch_system_ms,
            vshard_id,
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
        // locks and re-recons) and nothing is staged or resolved.
        let verified = self.verify_calvin_active_ollp(task, tenant_id.as_u64(), plans);
        match verified {
            Ok(true) => {}
            Ok(false) => return self.calvin_stage_refusal(task, ErrorCode::OllpRetryRequired),
            Err(e) => return self.calvin_stage_refusal(task, e.into()),
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
            let mut staging = CalvinStaging::default();
            for plan in plans {
                self.stage_calvin_plan(task, synthetic_txn_id, *tenant_id, plan, &mut staging)?;
            }
            Ok::<CalvinReply, ErrorCode>(staging.reply)
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
            // The dependent-read path carries no versioned read-set. Its OLLP
            // check passed above, so the slice votes commit.
            stage_vote: Some(StageVote::Commit),
            read_versions: crate::types::ReadVersions::new(),
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

    fn kv_keys(collection: &str, keys: &[&[u8]]) -> PassiveReadKey {
        PassiveReadKey {
            engine_key: nodedb_cluster::calvin::types::EngineKeySet::Kv {
                collection: collection.to_owned(),
                keys: nodedb_cluster::calvin::types::SortedVec::new(
                    keys.iter().map(|key| key.to_vec()).collect(),
                ),
            },
        }
    }

    fn decode_reads(response: &Response) -> Vec<(PassiveReadKeyId, Value)> {
        zerompk::from_msgpack(response.payload.as_ref()).expect("passive payload decodes")
    }

    /// A passive read returns each stored key-value row as its stored bytes
    /// and an absent row as `Null`, and votes commit.
    #[test]
    fn calvin_execute_passive_reads_stored_kv_bytes_and_votes_commit() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tenant_id: 1,
                collection: "items",
                key: b"alice:sword",
                value: b"stored-bytes",
                ttl_ms: 0,
                now_ms: crate::engine::kv::current_ms(),
                surrogate: Surrogate::new(5),
            })
            .expect("seed kv row");

        let task = make_task();
        let resp = core.execute_calvin_execute_passive(
            &task,
            1,
            0,
            &TenantId::new(1),
            &[kv_keys("items", &[b"alice:sword", b"bob:sword"])],
        );

        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.stage_vote, Some(StageVote::Commit));
        let collection = nodedb_types::QualifiedCollection::from_stored("items".to_owned());
        let reads: BTreeMap<PassiveReadKeyId, Value> = decode_reads(&resp).into_iter().collect();
        assert_eq!(
            reads.get(&PassiveReadKeyId::kv(
                collection.clone(),
                b"alice:sword".to_vec()
            )),
            Some(&Value::Bytes(b"stored-bytes".to_vec()))
        );
        assert_eq!(
            reads.get(&PassiveReadKeyId::kv(collection, b"bob:sword".to_vec())),
            Some(&Value::Null)
        );
    }

    /// A passive read returns a stored document row as its stored bytes.
    #[test]
    fn calvin_execute_passive_reads_stored_document_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed_row(&mut core, "orders", 11);
        let stored = core
            .sparse
            .get(
                DatabaseId::DEFAULT.as_u64(),
                1,
                "orders",
                &nodedb_types::StorageKey::for_surrogate(Surrogate::new(11)),
            )
            .expect("read seeded row")
            .expect("seeded row present");

        let task = make_task();
        let keys = [PassiveReadKey {
            engine_key: nodedb_cluster::calvin::types::EngineKeySet::Document {
                collection: "orders".to_owned(),
                surrogates: nodedb_cluster::calvin::types::SortedVec::new(vec![11]),
            },
        }];
        let resp = core.execute_calvin_execute_passive(&task, 1, 0, &TenantId::new(1), &keys);

        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            decode_reads(&resp),
            vec![(
                PassiveReadKeyId::surrogate(
                    nodedb_types::QualifiedCollection::from_stored("orders".to_owned()),
                    11
                ),
                Value::Bytes(stored)
            )]
        );
    }

    /// A key set that names no readable row refuses the read, so the
    /// passive participant votes abort instead of reporting a value.
    #[test]
    fn calvin_execute_passive_refuses_a_lock_only_key_set() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();
        let keys = [PassiveReadKey {
            engine_key: nodedb_cluster::calvin::types::EngineKeySet::Collection {
                collection: "orders".to_owned(),
                vshards: nodedb_cluster::calvin::types::SortedVec::new(Vec::new()),
            },
        }];
        let resp = core.execute_calvin_execute_passive(&task, 1, 0, &TenantId::new(1), &keys);

        assert_eq!(resp.status, Status::Error);
        assert_eq!(resp.stage_vote, None);
        assert!(matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::Unsupported { .. })
        ));
    }

    /// The dependent-read ACTIVE path STAGES its writes (into `commit_pending` +
    /// the synthetic overlay) instead of applying them to base directly, so a
    /// Calvin-committed dependent-read write reaches the WAL as a redo record.
    /// Staging routes it through the same resolve → redo → install the static
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
        };
        let injected = BTreeMap::new();

        let resp = core.execute_calvin_execute_active(&task, ctx, &tenant_id, &plans, &injected);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.stage_vote, Some(StageVote::Commit));

        let vshard_id = task.request.vshard_id.as_u32();

        // STAGED, not applied: the plans are buffered for the resolve.
        assert!(
            core.calvin.commit_pending.contains_key(&(1, 0, vshard_id)),
            "active-path write must be STAGED into commit_pending, not applied directly"
        );

        // No base mutation at stage time — the row appears only after install.
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
    /// (not install) is the one divergence from the static path.
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
        };
        let injected = BTreeMap::new();

        let resp = core.execute_calvin_execute_active(&task, ctx, &tenant_id, &plans, &injected);
        assert_eq!(resp.status, Status::Error);
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
        assert_eq!(resp.stage_vote, Some(StageVote::PredictionDrift));

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

    #[test]
    fn calvin_execute_active_stage_error_votes_participant_error() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let epoch_outside_synthetic_range = 1_u64 << 33;
        let ctx = CalvinExecCtx {
            epoch: epoch_outside_synthetic_range,
            position: 0,
            epoch_system_ms: 0,
        };

        let resp = core.execute_calvin_execute_active(
            &task,
            ctx,
            &tenant_id,
            &[point_insert_plan("orders", "o1", 7)],
            &BTreeMap::new(),
        );

        assert_eq!(resp.status, Status::Error);
        assert_eq!(resp.stage_vote, Some(StageVote::ParticipantError));
        assert!(core.calvin.commit_pending.is_empty());
        assert!(core.txn_overlays.is_empty());
    }
}
