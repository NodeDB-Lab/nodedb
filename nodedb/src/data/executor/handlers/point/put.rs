// SPDX-License-Identifier: BUSL-1.1

//! PointPut: insert or overwrite one document, committing storage + indexes
//! + stats in a single redb transaction via `apply_point_put`.

use tracing::debug;

use crate::bridge::envelope::{Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::versioned_point_images;
use crate::data::executor::enforcement::chain_guard::{self, AbandonedWrite, ChainGuard};
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};
use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec};
use nodedb_types::Surrogate;

/// Dispatch-side arguments for [`CoreLoop::execute_point_put`].
pub(in crate::data::executor) struct PointPutExec<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    /// When `Some`, project the STORED post-image per spec instead of
    /// reporting a bare affected count.
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this write may touch, resolved on the Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_put(
        &mut self,
        task: &ExecutionTask,
        params: PointPutExec<'_>,
    ) -> Response {
        let PointPutExec {
            tid,
            collection,
            document_id,
            surrogate,
            value,
            returning,
            rls_filters,
            resolved_sum_targets,
        } = params;
        if let Some(refusal) = crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
            "document", collection, surrogate,
        ) {
            return self.response_error(task, refusal);
        }
        let storage_key = StorageKey::for_surrogate(surrogate);
        let document_identity = RowIdentity::from_user_key(document_id);
        debug!(core = self.core_id, %collection, %document_id, "point put");

        let database_id = task.request.database_id.as_u64();
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };

        // A PUT is an upsert, so whether it is a chain link depends on whether a
        // row is already there. The link is written while the body is built, so
        // that question has to be answered BEFORE the write — `apply_point_put`'s
        // outcome comes too late for it. The probe is paid only by a collection that
        // actually declares `HASH_CHAIN`.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);
        if chain.enabled() {
            let existing = match self.current_row(database_id, tid, collection, &storage_key) {
                Ok(existing) => existing,
                Err(e) => return self.response_error(task, e),
            };
            if existing.is_none()
                && let Err(e) = chain.chain_insert(self, surrogate, value)
            {
                return self.response_error(task, e);
            }
        }

        // Unified write transaction: document + inverted index + stats in one commit.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                chain.restore(self);
                return self.response_error(task, e);
            }
        };

        let mut prior = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid,
                collection,
                storage_key,
                surrogate,
                value,
                index_text: true,
                user_roles: &task.request.user_roles,
                enforce: true,
                unique: crate::data::executor::enforcement::unique::UniqueJudge::Row,
                wal_lsn: task.wal_lsn(),
                resolved_targets: resolved_sum_targets,
            },
        ) {
            Ok(p) => p,
            Err(e) => {
                let e = chain_guard::abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key),
                    e,
                );
                return self.response_error(task, e);
            }
        };

        if let Err(e) = chain
            .settle(self, surrogate, &prior.stored_value)
            .and_then(|()| chain.persist_head(self, &txn))
        {
            let e = chain_guard::abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(std::mem::take(&mut prior.memory_undo)),
                e,
            );
            return self.response_error(task, e);
        }

        // `prior.prior_value` is what discriminates insert from update here —
        // the same bitemporal-aware pre-image that drives `emit_put_event`
        // below. An enforcement that only saw the post-image would account
        // every overwrite as a fresh insert and double-count its amount.
        let images = match prior.prior_value {
            Some(ref old) => WriteImages::Update {
                old: ImageBody::Stored(old),
                new: ImageBody::Submitted(value),
            },
            None => WriteImages::Insert {
                new: ImageBody::Submitted(value),
            },
        };
        let enforcement = match write_hook::run(self, &txn, &hook_ctx, images) {
            Ok(outcome) => outcome,
            Err(e) => {
                let e = chain_guard::abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(std::mem::take(&mut prior.memory_undo)),
                    e,
                );
                return self.response_error(task, e);
            }
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);
        let target_writes = enforcement.target_writes;

        // Settled before the commit: an autocommit statement is its own
        // transaction boundary, so a put that leaves a journal group unbalanced
        // is refused with nothing written.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            let e = chain_guard::abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(std::mem::take(&mut prior.memory_undo))
                    .targets(target_writes),
                e,
            );
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            let e = chain_guard::abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(std::mem::take(&mut prior.memory_undo))
                    .targets(target_writes),
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("commit: {e}"),
                },
            );
            return self.response_error(task, e);
        }

        // Record the committed write's version against its surrogate + collection.
        self.note_surrogate_write(task, tid, collection, surrogate.as_u32());

        // Record the touched secondary-index values into the per-index
        // write-value substrate (added ∪ removed ∪ bitemporal tuples).
        if let Some(stamp) = self.task_write_stamp(task) {
            let mut tuples = std::mem::take(&mut prior.secondary_index_added);
            tuples.append(&mut prior.secondary_index_removed);
            tuples.append(&mut prior.bitemporal_index_tuples);
            self.note_index_write_values(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
                &tuples,
                stamp,
            );
        }

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Emit write event to Event Plane. Insert vs Update is derived
        // from whether `prior` was present — a PointPut onto an existing
        // row is an Update from every downstream consumer's perspective.
        // The plan's `document_id` is the row's client identity, and the
        // identity the WAL journals for it.
        self.emit_put_event(
            task,
            tid,
            collection,
            document_identity.clone(),
            value,
            prior.prior_value.as_deref(),
        );

        let mut response = if let Some(spec) = returning {
            let strict_schema = self.strict_schema_for(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
            );
            self.stored_returning_response(
                task,
                spec,
                rls_filters,
                strict_schema.as_ref(),
                &self.identity_column(database_id, tid, collection),
                &[(&document_identity, prior.stored_value.as_slice())],
            )
        } else {
            // An upsert always writes the row, whether or not one was there before.
            self.response_affected(task, 1)
        };
        // A versioned row's key was decided here, so its stamped image
        // replaces the unstamped pre-dispatch record.
        if let Some(sys_from_ms) = prior.bitemporal_sys_from_ms {
            response.write_set = versioned_point_images(
                WriteSetEntry::put(surrogate.as_u32(), document_identity, value.to_vec()),
                sys_from_ms,
            );
        }
        response.write_set.extend(target_write_set);
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, TenantId};

    const DB: u64 = 0;
    const TID: u64 = 1;
    const SOURCE: &str = "point_txns";
    const TARGET: &str = "point_holders";

    /// The premise every test below rests on.
    #[test]
    fn the_fixture_is_co_resident() {
        assert!(
            crate::query::sum_target_is_co_resident(
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, SOURCE),
                TARGET,
            ),
            "'{SOURCE}' and '{TARGET}' must share a vShard: a cross-shard binding's balance \
             travels on its own task and is never folded into the source write's transaction"
        );
    }
    const A1: &str = "a1";
    const T1: Surrogate = Surrogate(4001);

    fn binding() -> nodedb_physical::physical_plan::MaterializedSumBinding {
        nodedb_physical::physical_plan::MaterializedSumBinding {
            target_collection: TARGET.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
            declared_primary_key: None,
        }
    }

    fn resolved() -> Vec<ResolvedSumTarget> {
        vec![ResolvedSumTarget::new(TARGET, A1, T1)]
    }

    fn config_key(collection: &str) -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
        )
    }

    /// A source collection bound to the sum, and a target row starting at zero.
    fn seeded_core(dir: &std::path::Path) -> CoreLoop {
        let (mut core, _req, _resp) = make_core_with_dir(dir);

        let mut source = CollectionConfig::new(SOURCE);
        source.enforcement.materialized_sum_sources = vec![binding()];
        core.doc_configs.insert(config_key(SOURCE), source);
        core.doc_configs
            .insert(config_key(TARGET), CollectionConfig::new(TARGET));

        let seed = serde_json::json!({"id": A1, "balance": "0"});
        core.sparse
            .put(
                DB,
                TID,
                TARGET,
                &nodedb_types::StorageKey::for_surrogate(T1),
                &doc_format::encode_to_msgpack(&seed),
            )
            .expect("seed target row");
        core
    }

    /// A source row body, in the MessagePack every handler receives.
    fn entry(account: &str, amount: i64) -> Vec<u8> {
        doc_format::encode_to_msgpack(&serde_json::json!({
            "account_id": account,
            "amount": amount,
        }))
    }

    /// The balance the target row currently holds.
    fn balance(core: &CoreLoop, surrogate: Surrogate) -> String {
        let stored = core
            .sparse
            .get(
                DB,
                TID,
                TARGET,
                &nodedb_types::StorageKey::for_surrogate(surrogate),
            )
            .expect("read target")
            .expect("target row must exist");
        doc_format::decode_document(&stored)
            .expect("target row must decode")
            .get("balance")
            .and_then(|v| v.as_str())
            .expect("target row must carry a balance")
            .to_string()
    }

    /// One PUT of `body` onto the same source row.
    fn put(core: &mut CoreLoop, task: &ExecutionTask, body: &[u8]) -> Status {
        let targets = resolved();
        core.execute_point_put(
            task,
            PointPutExec {
                tid: TID,
                collection: SOURCE,
                document_id: "e1",
                surrogate: Surrogate(21),
                value: body,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &targets,
            },
        )
        .status
    }

    #[test]
    fn point_put_credits_an_insert_and_deltas_an_overwrite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        let task = make_default_task();

        assert_eq!(put(&mut core, &task, &entry(A1, 10)), Status::Ok);
        assert_eq!(put(&mut core, &task, &entry(A1, 30)), Status::Ok);

        assert_eq!(
            balance(&core, T1),
            "30",
            "an overwrite must delta the total, not add the new amount on top of \
             the old one"
        );
    }

    /// A put refused after `apply_point_put` ran puts the row's in-memory
    /// index back. The second put replaces the row's vector node, then its
    /// materialized-sum fold is refused: the plan resolves no target row.
    /// The prior node must be live and bound to the row again, the new node
    /// gone, and the target balance unmoved.
    #[test]
    fn a_put_refused_after_apply_restores_the_prior_vector_node() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        core.vector_params.insert(
            config_key(SOURCE),
            crate::engine::vector::hnsw::HnswParams::default(),
        );
        let task = make_default_task();
        let body = |amount: i64, embedding: &[f64]| {
            doc_format::encode_to_msgpack(&serde_json::json!({
                "account_id": A1,
                "amount": amount,
                "embedding": embedding,
            }))
        };
        let row = Surrogate(21);

        assert_eq!(
            put(&mut core, &task, &body(10, &[1.0, 0.0, 0.0])),
            Status::Ok
        );
        let key = CoreLoop::vector_index_key(DB, TID, SOURCE, "embedding");
        let prior = core.vector_collections[&key]
            .local_for_surrogate(row)
            .expect("the first put binds a node");

        let refused = core.execute_point_put(
            &task,
            PointPutExec {
                tid: TID,
                collection: SOURCE,
                document_id: "e1",
                surrogate: row,
                value: &body(30, &[0.0, 1.0, 0.0]),
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(
            refused.status,
            Status::Error,
            "the unresolved fold must refuse"
        );

        let coll = &core.vector_collections[&key];
        assert_eq!(coll.live_count(), 1, "the new node must be gone");
        assert_eq!(
            coll.local_for_surrogate(row),
            Some(prior),
            "the prior node must be live and bound to the row again"
        );
        let doc_key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            SOURCE.to_string(),
            "embedding".to_string(),
            StorageKey::for_surrogate(row),
        );
        assert_eq!(core.vector_doc_map.get(&doc_key).copied(), Some(prior));
        assert_eq!(balance(&core, T1), "10", "the refused put moves no total");
    }

    /// A put that carries `Surrogate::ZERO` is refused before any write: no
    /// row lands under the zero key and no target total moves.
    #[test]
    fn an_unbound_document_put_is_refused_and_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        let task = make_default_task();
        let targets = resolved();

        let response = core.execute_point_put(
            &task,
            PointPutExec {
                tid: TID,
                collection: SOURCE,
                document_id: "e1",
                surrogate: Surrogate::ZERO,
                value: &entry(A1, 10),
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &targets,
            },
        );
        assert!(matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ));
        let stored = core
            .sparse
            .get(
                DB,
                TID,
                SOURCE,
                &nodedb_types::StorageKey::for_surrogate(Surrogate::ZERO),
            )
            .expect("read source");
        assert!(stored.is_none(), "nothing is stored under the zero key");
        assert_eq!(balance(&core, T1), "0", "no target total moves");
    }
}
