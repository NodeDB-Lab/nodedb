// SPDX-License-Identifier: BUSL-1.1

//! Flush a staged Calvin transaction: install its committed redo record.

use nodedb_physical::physical_plan::RedoSumTargets;
use tracing::{debug, info_span};

use crate::bridge::envelope::{Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::redo_apply::CommittedRedo;
use crate::data::executor::task::ExecutionTask;

/// The `MetaOp::CalvinFlush` fields the flush reads.
pub(in crate::data::executor) struct CalvinFlushRedo<'a> {
    pub epoch: u64,
    pub position: u32,
    /// The encoded `RedoRecord` the scheduler appended at the request's LSN.
    /// Empty when the transaction wrote nothing on this vShard.
    pub redo: &'a [u8],
    /// Every collection the local slice of the transaction writes.
    pub collections: &'a [String],
    /// The materialized-sum targets the local slice folds into.
    pub sum_targets: &'a [RedoSumTargets],
}

impl CoreLoop {
    /// Flush the Calvin transaction staged under `(epoch, position)`.
    ///
    /// The flush installs the transaction's committed redo record through
    /// [`CoreLoop::install_committed_redo`], the install every committed
    /// transaction and restart replay run: validate, install with undo, then
    /// settle and cover. An absent staged entry means a duplicate dispatch,
    /// which answers `Ok` and installs nothing.
    ///
    /// The staged entry and its overlay stay until the install succeeds. A
    /// refused install rolled every write back, so the scheduler can send the
    /// same flush again.
    ///
    /// The reply reads at the epoch instant, so every replica sees the same
    /// live rows. The response carries the reply the plans decided when they
    /// staged. A document or CRDT `RETURNING` row is read from base after the
    /// install, so it reports the row the install stored. A reply that fails
    /// to render leaves the flush `Ok`, since the record installed. Its error
    /// travels in `error_code` for the statement to report.
    pub(in crate::data::executor) fn execute_calvin_flush(
        &mut self,
        task: &ExecutionTask,
        flush: CalvinFlushRedo<'_>,
    ) -> Response {
        let CalvinFlushRedo {
            epoch,
            position,
            redo,
            collections,
            sum_targets,
        } = flush;
        let vshard_id = task.request.vshard_id.as_u32();
        let key = (epoch, position, vshard_id);
        let Some((tenant_id, epoch_system_ms)) = self
            .calvin
            .commit_pending
            .get(&key)
            .map(|pending| (pending.tenant_id, pending.epoch_system_ms))
        else {
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
            tenant_id = tenant_id.as_u64(),
            trace_id = ?task.request.trace_id,
        )
        .entered();
        const NANOS_PER_MS: i64 = 1_000_000;
        self.hlc
            .update_from_remote(epoch_system_ms.saturating_mul(NANOS_PER_MS));

        let mut response = if redo.is_empty() {
            self.response_ok(task)
        } else {
            self.install_committed_redo(
                task,
                tenant_id.as_u64(),
                CommittedRedo {
                    redo,
                    collections,
                    sum_targets,
                },
            )
        };
        if response.status != Status::Ok {
            return response;
        }
        // The record installed: the staged entry and its overlay are spent.
        let reply = self
            .calvin
            .commit_pending
            .remove(&key)
            .map(|pending| pending.reply)
            .unwrap_or_default();
        self.drop_calvin_synthetic_overlay(epoch, position, vshard_id);
        // Writes waiting on the rows this transaction owned run next.
        self.calvin.fence.note_resolved(key, task.wal_lsn());
        let prev_epoch_ms = self.epoch_system_ms;
        self.epoch_system_ms = Some(epoch_system_ms);
        let rendered = self.calvin_reply_payload(task, tenant_id.as_u64(), reply);
        self.epoch_system_ms = prev_epoch_ms;
        match rendered {
            Ok(payload) => response.payload = Payload::from_vec(payload),
            Err(error) => response.error_code = Some(Box::new(error)),
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::control::calvin_txn_id::calvin_synthetic_txn_id;
    use crate::types::{DatabaseId, Lsn, TenantId};
    use nodedb_physical::physical_plan::{
        DocumentOp, PhysicalPlan, ReturningColumns, ReturningSpec,
    };
    use nodedb_types::{QualifiedCollection, StorageKey, Surrogate};

    use crate::engine::document::store::{CollectionConfig, IndexPath};

    use super::super::shared::CalvinExecCtx;
    use super::super::shared::test_support::{
        bulk_delete_plan, doc_value, make_task, point_insert_plan, seed_row,
    };

    const CTX: CalvinExecCtx = CalvinExecCtx {
        epoch: 1,
        position: 0,
        epoch_system_ms: 0,
        is_group_leader: true,
    };

    /// Stage `plans` at `(1, 0)`, resolve them, and return the redo bytes.
    fn stage_and_resolve(core: &mut CoreLoop, plans: &[PhysicalPlan]) -> Vec<u8> {
        let task = make_task();
        let staged = core.execute_calvin_execute_static(&task, CTX, &TenantId::new(1), plans, &[]);
        assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);
        let resolved = core.execute_calvin_resolve(&task, 1, 0);
        assert_eq!(resolved.status, Status::Ok, "{:?}", resolved.error_code);
        resolved.payload.as_bytes().to_vec()
    }

    fn flush_at(core: &mut CoreLoop, lsn: u64, redo: &[u8], collections: &[String]) -> Response {
        let mut task = make_task();
        task.wal_lsn = Some(Lsn::new(lsn));
        core.execute_calvin_flush(
            &task,
            CalvinFlushRedo {
                epoch: 1,
                position: 0,
                redo,
                collections,
                sum_targets: &[],
            },
        )
    }

    fn base_row(core: &CoreLoop, collection: &str, surrogate: u32) -> Option<Vec<u8>> {
        core.sparse
            .get(
                DatabaseId::DEFAULT.as_u64(),
                1,
                collection,
                &StorageKey::for_surrogate(Surrogate::new(surrogate)),
            )
            .expect("read base row")
    }

    #[test]
    fn calvin_flush_drops_synthetic_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let redo = stage_and_resolve(&mut core, &[point_insert_plan("orders", "o1", 7)]);

        let vshard_id = make_task().request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();
        assert!(core.txn_overlays.contains_key(&synthetic));

        let flushed = flush_at(&mut core, 40, &redo, &["orders".to_string()]);
        assert_eq!(flushed.status, Status::Ok, "{:?}", flushed.error_code);

        assert!(
            !core.txn_overlays.contains_key(&synthetic),
            "flush must drop the synthetic overlay entry alongside commit_pending"
        );
        assert!(
            base_row(&core, "orders", 7).is_some(),
            "the flush installs the staged row from the redo record"
        );
    }

    /// The flush installs the redo record, not the staged plans. A row that
    /// joins the bulk delete's predicate after staging stays, and the
    /// predicted row goes.
    #[test]
    fn a_flush_installs_the_resolved_record_not_a_replay_of_the_plans() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed_row(&mut core, "orders", 1);
        let redo = stage_and_resolve(&mut core, &[bulk_delete_plan("orders", Some(vec![1]))]);

        seed_row(&mut core, "orders", 2);
        let flushed = flush_at(&mut core, 41, &redo, &["orders".to_string()]);

        assert_eq!(flushed.status, Status::Ok, "{:?}", flushed.error_code);
        assert!(base_row(&core, "orders", 1).is_none());
        assert!(base_row(&core, "orders", 2).is_some());
    }

    /// The flush answers with the reply the staged statement computed:
    /// RETURNING rows for a point insert that asked for them.
    #[test]
    fn a_calvin_flush_answers_the_returning_rows_its_statement_staged() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            document_id: "o9".to_string(),
            value: doc_value("a", "returned"),
            if_absent: false,
            surrogate: Surrogate::new(9),
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        let redo = stage_and_resolve(&mut core, &[plan]);

        let flushed = flush_at(&mut core, 42, &redo, &["orders".to_string()]);

        assert_eq!(flushed.status, Status::Ok, "{:?}", flushed.error_code);
        let payload = flushed.payload.as_bytes();
        assert!(
            payload.windows(b"returned".len()).any(|w| w == b"returned"),
            "the flush reply carries the inserted row"
        );
        assert!(base_row(&core, "orders", 9).is_some());
    }

    /// The install notes the index values of every document row at the
    /// flush's LSN, so no flush-time staging carries them to a later op.
    #[test]
    fn a_calvin_flush_notes_index_values_at_the_redo_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut config = CollectionConfig::new("orders");
        config.index_paths.push(IndexPath::new("a"));
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(1), "orders".to_string()),
            config,
        );
        let redo = stage_and_resolve(&mut core, &[point_insert_plan("orders", "o1", 7)]);

        let flushed = flush_at(&mut core, 43, &redo, &["orders".to_string()]);

        assert_eq!(flushed.status, Status::Ok, "{:?}", flushed.error_code);
        assert_eq!(
            core.write_index.index_values.value_lsn(
                DatabaseId::DEFAULT,
                TenantId::new(1),
                "orders",
                "a",
                "1"
            ),
            Some(Lsn::new(43)),
            "the install records the row's index value at the redo LSN"
        );
    }

    /// A flush whose record fails to install answers the install's error and
    /// leaves nothing of the record in base.
    #[test]
    fn a_calvin_flush_whose_record_cannot_install_answers_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        stage_and_resolve(&mut core, &[point_insert_plan("orders", "o1", 7)]);

        let flushed = flush_at(&mut core, 44, &[0xff, 0x00], &["orders".to_string()]);

        assert_eq!(flushed.status, Status::Error);
        assert!(!matches!(
            flushed.error_code.as_deref(),
            Some(ErrorCode::OllpRetryRequired)
        ));
        assert!(base_row(&core, "orders", 7).is_none());
    }

    /// A refused install keeps the staged entry and its overlay, so the
    /// scheduler's resent flush installs the record.
    #[test]
    fn a_refused_install_keeps_the_staged_commit_for_the_resent_flush() {
        use crate::data::executor::handlers::transaction::redo_apply::test_commit::failing_install_sub_record;
        use crate::wal::RedoRecord;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let redo = stage_and_resolve(&mut core, &[point_insert_plan("orders", "o1", 7)]);
        let mut failing = RedoRecord::from_bytes(&redo).expect("decode redo");
        failing.ops.push(failing_install_sub_record());
        let failing = failing.to_bytes().expect("encode redo");
        let vshard_id = make_task().request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();

        let refused = flush_at(&mut core, 45, &failing, &["orders".to_string()]);

        assert!(
            matches!(
                refused.error_code.as_deref(),
                Some(ErrorCode::RetryableRefusal { .. })
            ),
            "{:?}",
            refused.error_code
        );
        assert!(core.calvin.commit_pending.contains_key(&(1, 0, vshard_id)));
        assert!(core.txn_overlays.contains_key(&synthetic));
        assert!(base_row(&core, "orders", 7).is_none());

        let flushed = flush_at(&mut core, 46, &redo, &["orders".to_string()]);

        assert_eq!(flushed.status, Status::Ok, "{:?}", flushed.error_code);
        assert!(base_row(&core, "orders", 7).is_some());
        assert!(!core.calvin.commit_pending.contains_key(&(1, 0, vshard_id)));
        assert!(!core.txn_overlays.contains_key(&synthetic));
    }

    /// A reply that fails to render after a successful install leaves the
    /// flush `Ok` and carries the render error for the statement.
    #[test]
    fn a_reply_render_error_leaves_the_installed_flush_ok() {
        use crate::data::executor::handlers::control::calvin_reply::CalvinReply;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let redo = stage_and_resolve(&mut core, &[point_insert_plan("orders", "o1", 7)]);
        let vshard_id = make_task().request.vshard_id.as_u32();
        core.calvin
            .commit_pending
            .get_mut(&(1, 0, vshard_id))
            .expect("staged commit")
            .reply = CalvinReply::unrenderable_for_test("orders");

        let flushed = flush_at(&mut core, 47, &redo, &["orders".to_string()]);

        assert_eq!(flushed.status, Status::Ok);
        assert!(
            matches!(
                flushed.error_code.as_deref(),
                Some(ErrorCode::Internal { .. })
            ),
            "{:?}",
            flushed.error_code
        );
        assert!(base_row(&core, "orders", 7).is_some());
        assert!(!core.calvin.commit_pending.contains_key(&(1, 0, vshard_id)));
    }
    /// An autocommit put of row `surrogate` in "orders" at `wal_lsn`.
    fn autocommit_put(surrogate: u32, value: &str, wal_lsn: u64) -> ExecutionTask {
        let mut task = make_task();
        task.request.plan = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            document_id: "o1".to_string(),
            value: doc_value("a", value),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        task.wal_lsn = Some(Lsn::new(wal_lsn));
        task
    }

    /// The flush of the transaction staged at `(1, 0)`, as a queued task.
    fn flush_task(redo: Vec<u8>, lsn: u64) -> ExecutionTask {
        let mut task = make_task();
        task.request.plan =
            PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::CalvinFlush {
                epoch: 1,
                position: 0,
                redo,
                collections: vec!["orders".to_string()],
                sum_targets: Vec::new(),
            });
        task.wal_lsn = Some(Lsn::new(lsn));
        task
    }

    fn holds(core: &CoreLoop, needle: &[u8]) -> bool {
        base_row(core, "orders", 7)
            .is_some_and(|row| row.windows(needle.len()).any(|w| w == needle))
    }

    /// An autocommit write to a row a staged Calvin transaction owns waits
    /// for the flush, then applies on top of it.
    #[test]
    fn a_write_to_an_owned_row_applies_after_the_flush() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let redo = stage_and_resolve(&mut core, &[point_insert_plan("orders", "o1", 7)]);

        core.task_queue.push(autocommit_put(7, "autocommit", 60));
        core.tick();
        assert_eq!(core.calvin.fence.len(), 1, "the write waits for the owner");
        assert!(base_row(&core, "orders", 7).is_none());

        core.task_queue.push(flush_task(redo, 50));
        core.tick();

        assert_eq!(core.calvin.fence.len(), 0);
        assert!(
            holds(&core, b"autocommit"),
            "the waiting write applies after the flush and is not lost"
        );
    }

    /// A waiting write whose record sits below the flush's redo record is
    /// refused: restart replay would apply it before the flush.
    #[test]
    fn a_waiting_write_below_the_flush_lsn_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let redo = stage_and_resolve(&mut core, &[point_insert_plan("orders", "o1", 7)]);

        core.task_queue.push(autocommit_put(7, "autocommit", 40));
        core.task_queue.push(flush_task(redo, 50));
        core.tick();

        assert_eq!(core.calvin.fence.len(), 0);
        assert!(base_row(&core, "orders", 7).is_some());
        assert!(
            !holds(&core, b"autocommit"),
            "the refused write never applies"
        );
    }

    /// A write to a row the staged transaction does not own runs at once.
    #[test]
    fn a_write_to_an_unowned_row_does_not_wait() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        stage_and_resolve(&mut core, &[point_insert_plan("orders", "o1", 7)]);

        core.task_queue.push(autocommit_put(8, "autocommit", 60));
        core.tick();

        assert_eq!(core.calvin.fence.len(), 0);
        assert!(base_row(&core, "orders", 8).is_some());
    }
}
