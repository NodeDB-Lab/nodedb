// SPDX-License-Identifier: BUSL-1.1

//! PointInsert: write one document, probing existence under the same
//! write transaction so duplicate primary keys surface as
//! `unique_violation` (SQLSTATE 23505) instead of silently overwriting.
//!
//! Distinct from `PointPut` — that handler is by-design an upsert.
//! `PointInsert` is routed from SQL `INSERT` (and `INSERT ... ON CONFLICT
//! DO NOTHING` with `if_absent=true`).

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_guard::{self, ChainGuard};
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};
use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec};
use nodedb_types::Surrogate;

/// Parameters for [`CoreLoop::execute_point_insert`].
pub(in crate::data::executor) struct PointInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    /// `INSERT ... ON CONFLICT DO NOTHING` flag: silently skip on a duplicate
    /// primary key instead of raising a `unique` constraint violation.
    pub if_absent: bool,
    /// When `Some`, project the STORED post-image per spec instead of
    /// reporting a bare affected count.
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this insert may credit, resolved on the Control Plane at plan time. The
    /// Data Plane never derives it: the primary-key → surrogate map is catalog
    /// state that lives on the other side of the bridge.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// Materialized-sum TARGET collections whose delta the Control Plane
    /// settled at plan time and appended as its own `ApplyBalanceDelta` task,
    /// homed on the target's vShard. This handler must not apply them as well.
    pub deferred_sum_targets: &'a [String],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_insert(
        &mut self,
        p: PointInsertParams<'_>,
    ) -> Response {
        let PointInsertParams {
            task,
            tid,
            collection,
            document_id,
            surrogate,
            value,
            if_absent,
            returning,
            rls_filters,
            resolved_sum_targets,
            deferred_sum_targets,
        } = p;
        let storage_key = StorageKey::for_surrogate(surrogate);
        let row_key = storage_key.to_string();
        let document_identity = RowIdentity::from_user_key(document_id);
        debug!(
            core = self.core_id,
            %collection, %document_id, if_absent,
            "point insert"
        );

        let database_id = task.request.database_id.as_u64();
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets,
            wal_lsn: task.wal_lsn(),
        };

        // Hash chaining rewrites the BODY (it injects `_chain_hash`), so it runs
        // before the body is encoded and stored — not through the image funnel,
        // which only sees a write that has already been applied. This handler is
        // INSERT-shaped by construction, so every write it performs is a link.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);
        let chained = match chain.chain_insert(self, database_id, tid, document_id, value) {
            Ok(chained) => chained,
            Err(e) => return self.response_error(task, e),
        };
        let effective_value: &[u8] = chained.as_deref().unwrap_or(value);

        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                chain.restore(self);
                return self.response_error(task, e);
            }
        };

        // Existence probe inside the write transaction: linearizable with
        // the apply_point_put commit — no other writer can insert between
        // this check and our insert commit. Probe uses `document_id` as
        // the row key, which is how the primary key is encoded for strict
        // and schemaless collections alike (see `dml::convert_insert`).
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        let exists_result = if bitemporal {
            self.sparse.versioned_exists_current_in_txn(
                &txn,
                database_id,
                tid,
                collection,
                &storage_key,
            )
        } else {
            self.sparse
                .exists_in_txn(&txn, database_id, tid, collection, &storage_key)
        };
        match exists_result {
            Ok(true) => {
                // Drop the txn without committing — no-op on redb. The chain
                // head was advanced before the probe, so put it back: no row
                // lands, so no link exists for it to cover.
                chain.restore(self);
                if if_absent {
                    // `INSERT ... ON CONFLICT DO NOTHING`: the row already
                    // exists, so nothing is inserted and the statement affects
                    // 0 rows. Reporting no count here let the renderer assume
                    // the default 1 and claim an insert that never happened.
                    //
                    // A `RETURNING` on the same statement must likewise ship an
                    // empty row set rather than the count shape: nothing was
                    // written, so there is no post-image to project, and a
                    // count payload would be decoded as a row set of the wrong
                    // shape by the RETURNING renderer.
                    if let Some(spec) = returning {
                        return self.stored_returning_response(task, spec, rls_filters, None, &[]);
                    }
                    return self.response_affected(task, 0);
                }
                return self.response_error(
                    task,
                    crate::Error::RejectedConstraint {
                        collection: collection.to_string(),
                        constraint: "unique".to_string(),
                        detail: format!(
                            "duplicate key value '{document_id}' violates primary-key \
                             uniqueness on '{collection}'"
                        ),
                    },
                );
            }
            Ok(false) => {}
            Err(e) => {
                chain.restore(self);
                return self.response_error(task, e);
            }
        }

        // `apply_point_put` returns prior bytes if any — for PointInsert that
        // is `None` because the probe above already rejected the conflict case.
        // The outcome's index tuples are consumed below to record touched
        // secondary-index values.
        let mut outcome = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id: task.request.database_id.as_u64(),
                tid,
                collection,
                document_id: &row_key,
                surrogate,
                value: effective_value,
                index_text: true,
                user_roles: &task.request.user_roles,
                enforce: true,
                wal_lsn: task.wal_lsn(),
                resolved_targets: resolved_sum_targets,
            },
        ) {
            Ok(o) => o,
            Err(e) => {
                chain_guard::abort_after_apply(
                    self,
                    &chain,
                    database_id,
                    tid,
                    collection,
                    &storage_key,
                );
                return self.response_error(task, e);
            }
        };

        // The advanced head lands in the SAME transaction as the row whose hash
        // it is, so head and row commit or roll back as one unit.
        if let Err(e) = chain.persist_head(self, &txn) {
            chain_guard::abort_after_apply(
                self,
                &chain,
                database_id,
                tid,
                collection,
                &storage_key,
            );
            return self.response_error(task, e);
        }

        // Image-folding enforcement runs one level ABOVE `apply_point_put` and
        // inside THIS transaction, so a materialized-sum target write lands or
        // rolls back with the row that credited it. The post-image is the
        // SUBMITTED body, never the chained one: `_chain_hash` is a wrapper the
        // chain adds around the row and no constraint is declared over it.
        let enforcement = match write_hook::run(
            self,
            &txn,
            &hook_ctx,
            WriteImages::Insert {
                new: ImageBody::Submitted(value),
            },
        ) {
            Ok(outcome) => outcome,
            Err(e) => {
                chain_guard::abort_after_apply(
                    self,
                    &chain,
                    database_id,
                    tid,
                    collection,
                    &storage_key,
                );
                return self.response_error(task, e);
            }
        };
        // Redo entries for the target rows this insert credited, attached to the
        // response below so the Control Plane journals each derived write
        // against its OWN collection — the statement's own redo names only the
        // source row.
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);

        // BALANCED is settled before the commit, so a single-row insert of one
        // journal leg — unbalanced by the constraint's own definition when the
        // statement is its own transaction — leaves nothing behind.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            chain_guard::abort_after_apply(
                self,
                &chain,
                database_id,
                tid,
                collection,
                &storage_key,
            );
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("commit: {e}"),
                },
            );
        }

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());

        // The exact bytes storage now holds, taken before the index tuples are
        // consumed below. A `RETURNING` projection reads these rather than
        // `value`, so it reports the generated columns and injected `_rowid`
        // the encode pipeline added on the way to disk.
        let stored_value = std::mem::take(&mut outcome.stored_value);

        // Record the touched secondary-index values into the per-index
        // write-value substrate (added ∪ removed ∪ bitemporal tuples).
        if let Some(lsn) = task.wal_lsn() {
            let mut tuples = outcome.secondary_index_added;
            tuples.extend(outcome.secondary_index_removed);
            tuples.extend(outcome.bitemporal_index_tuples);
            self.note_index_write_values(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
                &tuples,
                lsn,
            );
        }

        // Implicit graph-edge extraction now lives on the Control Plane
        // (`control/planner/implicit_edges/`): a `_from`/`_to` document is
        // mirrored as a `GraphOp::EdgePut` task BEFORE dispatch, so the edge is
        // homed and surrogate-resolved per endpoint and routes through the same
        // single-home/Calvin path as an explicit edge. The PointInsert handler
        // only writes the document; it no longer derives edges (which mis-homed
        // cross-shard edges by the document's vShard).

        self.emit_put_event(
            task,
            tid,
            collection,
            storage_key.to_identity(),
            value,
            None,
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
                &[(&document_identity, stored_value.as_slice())],
            )
        } else {
            // The row was inserted: exactly one row affected.
            self.response_affected(task, 1)
        };
        if !target_write_set.is_empty() {
            response.write_set = target_write_set;
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, TenantId};

    const DB: u64 = 0;
    const TID: u64 = 1;
    /// The collection that drives the binding.
    const SOURCE: &str = "point_txns";
    /// The collection whose `balance` the binding maintains, sharing `SOURCE`'s
    /// vShard. Every test asserts the INLINE fold: the target row is seeded
    /// into, and read back out of, the SOURCE core's own document store.
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

    /// `SUM(amount)` per `account_id`, materialized onto the target's `balance`.
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
    fn point_insert_credits_the_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        let task = make_default_task();

        assert_eq!(
            insert(&mut core, &task, Surrogate(11), &entry(A1, 25)),
            Status::Ok
        );
        assert_eq!(
            insert(&mut core, &task, Surrogate(12), &entry(A1, 75)),
            Status::Ok
        );

        assert_eq!(balance(&core, T1), "100", "both inserts must be totalled");
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

    /// The chain must actually be built by the autocommit INSERT path, or the
    /// refusal tests in `point/delete.rs` and `point/update/exec.rs` would pass
    /// over rows that carry no link at all.
    #[test]
    fn a_point_insert_links_the_row_into_the_chain() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = chained_core(dir.path());
        let task = make_default_task();

        assert_eq!(insert_chained(&mut core, &task), Status::Ok);

        let stored = core
            .sparse
            .get(
                DB,
                TID,
                SOURCE,
                &nodedb_types::StorageKey::for_surrogate(Surrogate(91)),
            )
            .expect("read back")
            .expect("row must exist");
        let doc = doc_format::decode_document(&stored).expect("decode");
        assert!(
            doc.get("_chain_hash").and_then(|v| v.as_str()).is_some(),
            "an autocommit INSERT into a hash-chained collection must store its link"
        );
        assert!(
            core.sparse
                .get_chain_head(DB, TID, SOURCE)
                .expect("read head")
                .is_some(),
            "and must persist the advanced head"
        );
    }
}
