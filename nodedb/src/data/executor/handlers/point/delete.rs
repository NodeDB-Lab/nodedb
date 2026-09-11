// SPDX-License-Identifier: BUSL-1.1

//! PointDelete: remove one document plus its cascading side-effects across
//! inverted, secondary, graph, and spatial indexes.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
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
    pub surrogate: Surrogate,
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
        let outcome = match self.apply_point_delete(
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
                    // `apply_point_delete` already invalidated this row's cache
                    // entry, and dropping `txn` reverses every durable write it
                    // staged, so nothing else has to be undone here.
                    return self.response_error(task, e);
                }
            },
            None => Default::default(),
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);

        // A delete subtracts the removed row's amount, so removing one leg of a
        // balanced journal on its own is a violation. Settled before the commit,
        // and dropping `txn` un-committed reverses the removal.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                },
            );
        }
        let prior = outcome.prior_value;

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Record the committed delete's version against its surrogate +
        // collection, but only when a row was actually removed — a delete that
        // matched nothing changes no state and creates no OCC conflict.
        if prior.is_some() {
            self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());

            // Record the removed secondary-index values into the per-index
            // write-value substrate (plain cascade ∪ bitemporal tombstones).
            if let Some(lsn) = task.wal_lsn() {
                let mut tuples = outcome.secondary_index_tuples;
                tuples.extend(outcome.bitemporal_index_tuples);
                self.note_index_write_values(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                    &tuples,
                    lsn,
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
        if let Some(prior_bytes) = prior.as_deref() {
            let old_converted = self.resolve_event_payload(
                task.request.database_id.as_u64(),
                tid,
                collection,
                prior_bytes,
            );
            // `document_identity` is read again below for `RETURNING`'s `id`
            // field, so the event-emit boundary gets a clone rather than the
            // move.
            self.emit_document_delete_event(
                task,
                collection,
                document_identity.clone(),
                Some(old_converted.as_deref().unwrap_or(prior_bytes)),
            );
        }

        let mut response = if let (Some(spec), Some(prior_bytes)) = (returning, prior.as_deref()) {
            // Decode the pre-deletion image with the collection's storage mode:
            // on a strict collection the prior bytes are a Binary Tuple, which
            // the MessagePack decoder accepts without erroring and turns into a
            // document with none of the row's real columns. The schema borrow is
            // scoped so the response build below can take `self` mutably.
            let doc = {
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
                returning_doc::from_stored(prior_bytes, &document_identity, strict_schema)
            };
            let doc = match doc {
                Ok(doc) => doc,
                Err(e) => return self.response_error(task, e),
            };
            match returning_rows::build_rows_payload(spec, rls_filters, &[doc]) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else if let Some(spec) = returning {
            // Row did not exist — return empty rows payload.
            match returning_rows::build_rows_payload(spec, rls_filters, &[]) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else {
            // No RETURNING: report the count the doc-store write actually
            // produced. `prior` is `None` when the row was already gone, which
            // is a genuine no-op — the plan resolved a surrogate for the
            // primary key (surrogates outlive the row they were assigned to),
            // so the surrogate is no evidence a row was there to remove.
            self.response_affected(task, u64::from(prior.is_some()))
        };
        // Redo entries for the target rows this delete debited: the statement's
        // own redo names only the removed row, so without these a WAL-only
        // restart replays the removal and leaves every total still carrying the
        // contribution of a row that is gone.
        if !target_write_set.is_empty() {
            response.write_set = target_write_set;
        }
        response
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
        let row_key = storage_key.to_string();
        let row_key = row_key.as_str();
        let stored = if self.is_bitemporal(database_id, tid, collection) {
            self.sparse
                .versioned_get_current(database_id, tid, collection, row_key)?
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
            crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, TARGET),
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
                surrogate: Surrogate(31),
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

    /// Removing a link makes `verify_chain` report the row AFTER it as broken, so
    /// the delete is refused rather than allowed to accuse an untampered row.
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
                surrogate: Surrogate(91),
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
}
