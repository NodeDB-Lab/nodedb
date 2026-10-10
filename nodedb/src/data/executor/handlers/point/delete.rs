// SPDX-License-Identifier: BUSL-1.1

//! PointDelete: remove one document plus its cascading side-effects across
//! inverted, secondary, graph, and spatial indexes.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::versioned_point_images;
use crate::data::executor::enforcement::chain_guard::{AbandonedWrite, abandon_write};
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::partial_refusal::refusal_after_partial_apply;
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec, StorageMode};
use nodedb_types::Surrogate;

/// Borrowed arguments for [`CoreLoop::execute_point_delete`], grouped so the
/// handler stays within the argument-count limit.
pub(in crate::data::executor) struct PointDeleteExec<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    /// `None` when the key is unbound in this database: the delete matches
    /// no row.
    pub surrogate: Option<Surrogate>,
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled RLS read policy gating the `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Compiled RLS write policy gating the REMOVAL, decided against the row's
    /// pre-deletion image — the only image a delete has. A separate slot from
    /// `rls_filters`: that one bounds what may be shown back, this one bounds
    /// what may be removed.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this delete must debit, resolved on the Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_delete(
        &mut self,
        task: &ExecutionTask,
        args: PointDeleteExec<'_>,
    ) -> Response {
        let PointDeleteExec {
            tid,
            collection,
            document_id,
            surrogate,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = args;
        debug!(core = self.core_id, %collection, %document_id, "point delete");
        // A key unbound in this database names no row here.
        let Some(surrogate) = surrogate else {
            return self.point_delete_matched_nothing(task, returning, rls_filters);
        };

        let database_id = task.request.database_id.as_u64();
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };

        // Gate the removal on the collection's write policy, decided against
        // the row's pre-deletion image. The read happens BEFORE the write: the
        // removal is staged into the transaction below, so checking a value
        // read back through it would decide a row already gone.
        if let Err(e) = self.gate_point_delete(task, tid, collection, surrogate, rls_write_check) {
            return self.response_error(task, e);
        }

        // Doc-store write + all index cascades, via `apply_point_delete`, in a
        // transaction this handler owns: every sparse-database write the delete
        // performs lands on commit below, or none of it does if any step fails
        // (the txn is dropped un-committed on every early return).
        let txn = match self.sparse.begin_write() {
            Ok(txn) => txn,
            Err(e) => return self.response_error(task, e),
        };
        let mut outcome = match self.apply_point_delete(
            &txn,
            PointDeleteParams {
                database_id,
                tid,
                collection,
                document_id,
                surrogate,
                user_roles: &task.request.user_roles,
                enforce: true,
                resolved_targets: resolved_sum_targets,
            },
        ) {
            Ok(outcome) => outcome,
            Err(e) => return self.response_error(task, e),
        };
        // Every abort below drops `txn` uncommitted, which reverses the
        // durable writes only. `abandon_write` reverses the in-memory
        // cascades and the target rows' cache and index entries.
        let storage_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);
        let memory_undo = std::mem::take(&mut outcome.memory_undo);
        // Image-folding enforcement, inside the SAME transaction the removal was
        // staged in: a materialized-sum target write is itself a document write,
        // so the debit and the row's removal land or roll back together. A
        // delete that matched nothing changes no total and folds nothing —
        // `apply_point_delete` reports that as a `None` pre-image.
        //
        // `outcome.prior_value` is the pre-image, which is the ONLY image a
        // delete has. An enforcement API that can report only a post-image
        // cannot express this write at all: a deleted row's contribution would
        // stay on the total forever.
        let enforcement = match outcome.prior_value {
            Some(ref old) => match write_hook::run(
                self,
                &txn,
                &hook_ctx,
                WriteImages::Delete {
                    old: ImageBody::Stored(old),
                },
            ) {
                Ok(enforcement) => enforcement,
                Err(e) => {
                    let e = abandon_write(
                        self,
                        AbandonedWrite::row(database_id, tid, collection, &storage_key)
                            .undo(memory_undo),
                        e,
                    );
                    return self.response_error(task, e);
                }
            },
            None => Default::default(),
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);
        let target_writes = enforcement.target_writes;

        // A delete subtracts the removed row's amount, so removing one leg of a
        // balanced journal on its own is a violation. Settled before the commit,
        // and dropping `txn` un-committed reverses the removal.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            let e = abandon_write(
                self,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(memory_undo)
                    .targets(target_writes),
                e,
            );
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            let e = abandon_write(
                self,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(memory_undo)
                    .targets(target_writes),
                crate::Error::DataPlane(ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                }),
            );
            return self.response_error(task, e);
        }
        let prior = outcome.prior_value;

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Record the committed delete's version against its surrogate +
        // collection, but only when a row was actually removed — a delete that
        // matched nothing changes no state and creates no OCC conflict.
        if prior.is_some() {
            self.note_surrogate_write(task, tid, collection, surrogate.as_u32());

            // Record the removed secondary-index values into the per-index
            // write-value substrate (plain cascade ∪ bitemporal tombstones).
            if let Some(stamp) = self.task_write_stamp(task) {
                let mut tuples = outcome.secondary_index_tuples;
                tuples.extend(outcome.bitemporal_index_tuples);
                self.note_index_write_values(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                    &tuples,
                    stamp,
                );
            }
        }

        // Emit delete event to Event Plane if the row actually existed.
        // `apply_point_delete` returns the prior bytes — we thread them
        // through so CDC/trigger consumers see the pre-delete state as
        // `old_value`. A delete against a non-existent key is a true
        // no-op and emits nothing.
        let document_identity =
            crate::engine::document::store::RowIdentity::from_user_key(document_id);
        // A versioned removal's tombstone key was decided here, so its stamped
        // entry replaces the unstamped pre-dispatch record. Then one entry per
        // target row this delete debited: the statement's own record names
        // only the removed row.
        let mut write_set = match (prior.is_some(), outcome.bitemporal_sys_from_ms) {
            (true, Some(sys_from_ms)) => versioned_point_images(
                WriteSetEntry::delete(surrogate.as_u32(), document_identity.clone()),
                sys_from_ms,
            ),
            _ => Vec::new(),
        };
        write_set.extend(target_write_set);
        if let Some(prior_bytes) = prior.as_deref() {
            // `document_identity` is read again below for `RETURNING`'s `id`
            // field, so the event-emit boundary gets a clone rather than the
            // move.
            self.emit_document_delete_event(
                task,
                tid,
                collection,
                document_identity.clone(),
                Some(prior_bytes),
            );
        }

        let mut response = if let (Some(spec), Some(prior_bytes)) = (returning, prior.as_deref()) {
            // Decode the pre-deletion image with the collection's storage mode:
            // on a strict collection the prior bytes are a Binary Tuple, which
            // the MessagePack decoder accepts without erroring and turns into a
            // document with none of the row's real columns. The schema borrow is
            // scoped so the response build below can take `self` mutably.
            let doc = {
                let identity_column = self.identity_column(database_id, tid, collection);
                let strict_schema = self
                    .doc_configs
                    .get(&(
                        task.request.database_id,
                        crate::types::TenantId::new(tid),
                        collection.to_string(),
                    ))
                    .and_then(|c| match &c.storage_mode {
                        StorageMode::Strict { schema } => Some(schema),
                        StorageMode::Schemaless => None,
                    });
                returning_doc::from_stored(
                    prior_bytes,
                    &document_identity,
                    strict_schema,
                    &identity_column,
                )
            };
            let doc = match doc {
                Ok(doc) => doc,
                // The removal committed: the refusal keeps its record and
                // carries its entries.
                Err(e) => {
                    let code = refusal_after_partial_apply(e.into());
                    return self.refusal_with_landed_rows(task, code, write_set);
                }
            };
            match returning_rows::build_rows_payload(spec, rls_filters, &[doc]) {
                Ok(payload) => self.response_with_payload(task, payload),
                // The removal committed: the refusal carries its entries.
                Err(e) => {
                    return self.refusal_with_landed_rows(
                        task,
                        ErrorCode::Internal {
                            detail: format!("RETURNING encode: {e}"),
                        },
                        write_set,
                    );
                }
            }
        } else if returning.is_some() {
            // Row did not exist — return empty rows payload.
            self.point_delete_matched_nothing(task, returning, rls_filters)
        } else {
            // No RETURNING: report the count the doc-store write actually
            // produced. `prior` is `None` when the row was already gone, which
            // is a genuine no-op — the plan resolved a surrogate for the
            // primary key (surrogates outlive the row they were assigned to),
            // so the surrogate is no evidence a row was there to remove.
            self.response_affected(task, u64::from(prior.is_some()))
        };
        response.write_set = write_set;
        response
    }

    /// The answer of a point delete that removed no row: an empty `RETURNING`
    /// row set, or zero rows affected.
    pub(in crate::data::executor) fn point_delete_matched_nothing(
        &mut self,
        task: &ExecutionTask,
        returning: Option<&ReturningSpec>,
        rls_filters: &[u8],
    ) -> Response {
        let Some(spec) = returning else {
            return self.response_affected(task, 0);
        };
        match returning_rows::build_rows_payload(spec, rls_filters, &[]) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }

    /// Decide a single row's removal against the compiled write policy.
    ///
    /// A row that is already absent is admitted: the delete removes nothing, so
    /// there is no image for the policy to restrict and no state change to
    /// refuse. Reads through the same current-state view the delete cascade
    /// uses, so a bitemporal collection is decided on its live version rather
    /// than a superseded one.
    fn gate_point_delete(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> crate::Result<()> {
        if matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            return Ok(());
        }
        let database_id = task.request.database_id.as_u64();
        let storage_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);
        let stored = if self.is_bitemporal(database_id, tid, collection) {
            self.sparse
                .versioned_get_current(database_id, tid, collection, &storage_key)?
        } else {
            self.sparse
                .get(database_id, tid, collection, &storage_key)?
        };
        let Some(body) = stored else {
            return Ok(());
        };
        let strict_schema = self
            .doc_configs
            .get(&(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection.to_string(),
            ))
            .and_then(|c| match &c.storage_mode {
                StorageMode::Strict { schema } => Some(schema),
                StorageMode::Schemaless => None,
            });
        rls_write_gate::admit_stored_row(
            rls_write_check,
            &body,
            &storage_key.to_identity(),
            strict_schema,
            &self.identity_column(database_id, tid, collection),
            tid,
            collection,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::data::executor::handlers::point::insert::PointInsertParams;
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

    fn insert(
        core: &mut CoreLoop,
        task: &ExecutionTask,
        surrogate: Surrogate,
        body: &[u8],
    ) -> Status {
        let targets = resolved();
        let document_id = format!("e{}", surrogate.as_u32());
        core.execute_point_insert(PointInsertParams {
            task,
            tid: TID,
            collection: SOURCE,
            document_id: &document_id,
            surrogate,
            value: body,
            if_absent: false,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &targets,
            deferred_sum_targets: &[],
        })
        .status
    }

    #[test]
    fn point_delete_takes_the_row_back_off_the_total() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        let task = make_default_task();
        let targets = resolved();

        assert_eq!(
            insert(&mut core, &task, Surrogate(31), &entry(A1, 40)),
            Status::Ok
        );
        assert_eq!(balance(&core, T1), "40");

        let resp = core.execute_point_delete(
            &task,
            PointDeleteExec {
                tid: TID,
                collection: SOURCE,
                document_id: "e31",
                surrogate: Some(Surrogate(31)),
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &targets,
            },
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            balance(&core, T1),
            "0",
            "a deleted row's contribution must come back off the total"
        );
    }

    /// A hash-chained collection, exactly as DDL builds one: `HASH_CHAIN` implies
    /// `APPEND_ONLY`.
    fn chained_core(dir: &std::path::Path) -> CoreLoop {
        let (mut core, _req, _resp) = make_core_with_dir(dir);
        let mut config = CollectionConfig::new(SOURCE);
        config.enforcement.append_only = true;
        config.enforcement.hash_chain = true;
        core.doc_configs.insert(config_key(SOURCE), config);
        core
    }

    fn insert_chained(core: &mut CoreLoop, task: &ExecutionTask) -> Status {
        core.execute_point_insert(PointInsertParams {
            task,
            tid: TID,
            collection: SOURCE,
            document_id: "e1",
            surrogate: Surrogate(91),
            value: &entry(A1, 10),
            if_absent: false,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
            deferred_sum_targets: &[],
        })
        .status
    }

    /// A removed row reads as tampering to `VERIFY_HASH_CHAIN`, so the delete is
    /// refused.
    #[test]
    fn a_delete_on_a_hash_chained_collection_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = chained_core(dir.path());
        let task = make_default_task();
        assert_eq!(insert_chained(&mut core, &task), Status::Ok);

        let resp = core.execute_point_delete(
            &task,
            PointDeleteExec {
                tid: TID,
                collection: SOURCE,
                document_id: "e1",
                surrogate: Some(Surrogate(91)),
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(resp.status, Status::Error);
        assert!(
            core.sparse
                .get(
                    DB,
                    TID,
                    SOURCE,
                    &nodedb_types::StorageKey::for_surrogate(Surrogate(91))
                )
                .expect("read back")
                .is_some(),
            "a refused delete must leave the chained row in place"
        );
    }

    /// The in-memory index state of one source row.
    #[derive(Debug, PartialEq)]
    struct RowIndexes {
        rtree: Vec<(u64, nodedb_types::BoundingBox)>,
        spatial_doc: Option<String>,
        live_vectors: usize,
        bound_vector: Option<u32>,
        recorded_vector: Option<u32>,
        node_deleted: bool,
    }

    fn row_indexes(core: &CoreLoop, surrogate: Surrogate, document_id: &str) -> RowIndexes {
        let storage_key = nodedb_types::StorageKey::for_surrogate(surrogate);
        let entry_id =
            crate::data::executor::handlers::point::apply_put::SpatialEntryId::from_storage_key(
                storage_key,
            )
            .as_u64();
        let db = DatabaseId::DEFAULT;
        let tenant = TenantId::new(TID);
        let rtree: Vec<(u64, nodedb_types::BoundingBox)> = core
            .spatial_indexes
            .get(&(db, tenant, SOURCE.to_string(), "loc".to_string()))
            .map(|rt| rt.entries().into_iter().map(|e| (e.id, e.bbox)).collect())
            .unwrap_or_default();
        let spatial_doc = core
            .spatial_doc_map
            .get(&(db, tenant, SOURCE.to_string(), "loc".to_string(), entry_id))
            .cloned();
        let vector_key = CoreLoop::vector_index_key(DB, TID, SOURCE, "embedding");
        let vectors = core.vector_collections.get(&vector_key);
        RowIndexes {
            rtree,
            spatial_doc,
            live_vectors: vectors.map(|c| c.live_count()).unwrap_or(0),
            bound_vector: vectors.and_then(|c| c.local_for_surrogate(surrogate)),
            recorded_vector: core
                .vector_doc_map
                .get(&(
                    db,
                    tenant,
                    SOURCE.to_string(),
                    "embedding".to_string(),
                    storage_key,
                ))
                .copied(),
            node_deleted: core.is_node_deleted(DB, TID, SOURCE, document_id),
        }
    }

    /// A delete refused after `apply_point_delete` ran leaves the row's
    /// R-tree entry, vector node, reverse maps and node mark as they were.
    /// The refusal here is the fold's: the target row it debits is gone.
    #[test]
    fn a_delete_refused_after_apply_leaves_every_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        let task = make_default_task();
        let targets = resolved();
        core.vector_params.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), SOURCE.to_string()),
            crate::engine::vector::hnsw::HnswParams::default(),
        );

        let surrogate = Surrogate(31);
        let body = doc_format::encode_to_msgpack(&serde_json::json!({
            "account_id": A1,
            "amount": 40,
            "loc": {"type": "Point", "coordinates": [1.0, 2.0]},
            "embedding": [1.0, 0.0, 0.0],
        }));
        assert_eq!(insert(&mut core, &task, surrogate, &body), Status::Ok);
        let before = row_indexes(&core, surrogate, "e31");
        assert_eq!(before.rtree.len(), 1, "the insert indexed the geometry");
        assert!(before.spatial_doc.is_some());
        assert_eq!(before.live_vectors, 1, "the insert indexed the vector");
        assert!(before.bound_vector.is_some());
        assert_eq!(before.recorded_vector, before.bound_vector);
        assert!(!before.node_deleted);

        let target_key = nodedb_types::StorageKey::for_surrogate(T1);
        core.sparse
            .delete(DB, TID, TARGET, &target_key)
            .expect("drop the target row");
        core.doc_cache.invalidate(DB, TID, TARGET, &target_key);

        let resp = core.execute_point_delete(
            &task,
            PointDeleteExec {
                tid: TID,
                collection: SOURCE,
                document_id: "e31",
                surrogate: Some(surrogate),
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &targets,
            },
        );
        assert_eq!(
            resp.status,
            Status::Error,
            "the fold must refuse the delete"
        );
        assert!(
            !matches!(
                resp.error_code.as_deref(),
                Some(ErrorCode::RollbackFailed { .. })
            ),
            "every in-memory entry must reverse, got {:?}",
            resp.error_code
        );
        assert!(
            core.sparse
                .get(
                    DB,
                    TID,
                    SOURCE,
                    &nodedb_types::StorageKey::for_surrogate(surrogate)
                )
                .expect("read back")
                .is_some(),
            "the refused delete leaves the row"
        );
        assert_eq!(
            row_indexes(&core, surrogate, "e31"),
            before,
            "the refused delete leaves every in-memory index as it was"
        );
    }

    /// [`seeded_core`] with a strict source that declares a `SparseVector`
    /// column.
    fn strict_sparse_core(dir: &std::path::Path) -> CoreLoop {
        use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
        let mut core = seeded_core(dir);
        let schema = StrictSchema::new(vec![
            ColumnDef::required("_rowid", ColumnType::Int64),
            ColumnDef::nullable("account_id", ColumnType::String),
            ColumnDef::nullable("amount", ColumnType::Int64),
            ColumnDef::nullable("terms", ColumnType::SparseVector),
        ])
        .expect("schema");
        let mut source = CollectionConfig::new(SOURCE)
            .with_storage_mode(nodedb_physical::physical_plan::StorageMode::Strict { schema });
        source.enforcement.materialized_sum_sources = vec![binding()];
        core.doc_configs.insert(config_key(SOURCE), source);
        core
    }

    /// A delete refused after `apply_point_delete` ran leaves the row's
    /// sparse-vector postings on a strict collection. The refusal here is the
    /// fold's: the target row it debits is gone.
    #[test]
    fn a_delete_refused_after_apply_leaves_the_sparse_postings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = strict_sparse_core(dir.path());
        let task = make_default_task();
        let targets = resolved();

        let surrogate = Surrogate(32);
        let body = doc_format::encode_to_msgpack(&serde_json::json!({
            "account_id": A1,
            "amount": 40,
            "terms": "{3:0.5, 7:1.5}",
        }));
        assert_eq!(insert(&mut core, &task, surrogate, &body), Status::Ok);
        let sparse_key = CoreLoop::sparse_index_key(DB, TID, SOURCE, "terms");
        let row_key = nodedb_types::StorageKey::for_surrogate(surrogate).to_string();
        let postings = |core: &CoreLoop| {
            core.sparse_vector_indexes.get(&sparse_key).map(|index| {
                (
                    index.doc_count(),
                    index.doc_image(&row_key),
                    index.next_internal_id(),
                )
            })
        };
        let before = postings(&core);
        let Some((doc_count, image, _)) = &before else {
            panic!("the insert must create the sparse index");
        };
        assert_eq!(*doc_count, 1, "the insert indexed the sparse vector");
        assert!(image.is_some(), "the row holds sparse postings");

        let target_key = nodedb_types::StorageKey::for_surrogate(T1);
        core.sparse
            .delete(DB, TID, TARGET, &target_key)
            .expect("drop the target row");
        core.doc_cache.invalidate(DB, TID, TARGET, &target_key);

        let resp = core.execute_point_delete(
            &task,
            PointDeleteExec {
                tid: TID,
                collection: SOURCE,
                document_id: "e32",
                surrogate: Some(surrogate),
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &targets,
            },
        );
        assert_eq!(
            resp.status,
            Status::Error,
            "the fold must refuse the delete"
        );
        assert!(
            !matches!(
                resp.error_code.as_deref(),
                Some(ErrorCode::RollbackFailed { .. })
            ),
            "every in-memory entry must reverse, got {:?}",
            resp.error_code
        );
        assert!(
            core.sparse
                .get(
                    DB,
                    TID,
                    SOURCE,
                    &nodedb_types::StorageKey::for_surrogate(surrogate)
                )
                .expect("read back")
                .is_some(),
            "the refused delete leaves the row"
        );
        assert_eq!(
            postings(&core),
            before,
            "the refused delete leaves the sparse postings as they were"
        );
    }
}
