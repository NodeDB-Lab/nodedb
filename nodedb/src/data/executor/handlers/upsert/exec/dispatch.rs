// SPDX-License-Identifier: BUSL-1.1

//! The upsert handler entry point: probe for an existing row and dispatch to
//! the overwrite branch ([`overwrite`]) or the insert branch ([`insert`]).
//!
//! Works for schemaless and strict collections. All internal transport
//! uses nodedb_types::Value + zerompk (msgpack). No JSON roundtrips.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook::HookCtx;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::Surrogate;

use super::insert::InsertCtx;
use super::overwrite::OverwriteCtx;

/// Parameters for `execute_upsert`.
pub(in crate::data::executor) struct UpsertParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    pub on_conflict_updates: &'a [(String, nodedb_physical::physical_plan::UpdateValue)],
    /// Compiled RLS write policy gating the PERSIST, decided against whichever
    /// body this call actually stores — the merged row on the conflict branch,
    /// the incoming body on the insert branch.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// When `Some`, project the STORED post-image per spec: the merged row on
    /// the conflict branch, the inserted row otherwise. Never the submitted
    /// body — on a conflict the caller's values are only part of the result.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this upsert may touch, resolved on the Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Upsert: insert if absent, merge fields if present.
    ///
    /// If a document with `document_id` exists, merges `value` fields into the
    /// existing document (preserving fields not in `value`). If it doesn't exist,
    /// inserts as a new document (identical to PointPut).
    ///
    /// `value` is msgpack-encoded (zerompk). Strict collections decode binary
    /// tuples for existing docs, merge, and re-encode via `apply_point_put`.
    pub(in crate::data::executor) fn execute_upsert(
        &mut self,
        task: &ExecutionTask,
        params: UpsertParams<'_>,
    ) -> Response {
        let UpsertParams {
            tid,
            collection,
            document_id,
            surrogate,
            value,
            on_conflict_updates,
            rls_write_check,
            returning,
            rls_filters,
            resolved_sum_targets,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %document_id,
            has_on_conflict = !on_conflict_updates.is_empty(),
            "upsert"
        );

        let database_id = task.request.database_id.as_u64();
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };

        // Detect strict storage mode for this collection.
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let strict_schema = self.doc_configs.get(&config_key).and_then(|config| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Check if document already exists. Bitemporal collections consult
        // the versioned table's current-state view (reverse-scan to newest
        // non-tombstone); non-bitemporal collections use the legacy point
        // lookup.
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        // Computed once for the whole statement: the schemaless half of this
        // check is an unindexed `vector_params` scan, so it must not be paid
        // per branch. Gates the live HNSW re-index + the post-apply redo
        // write-set below; a non-vector collection pays neither.
        let has_vectors = self.collection_has_vectors(database_id, tid, collection);
        let key = nodedb_types::StorageKey::for_surrogate(surrogate);
        let existing = if bitemporal {
            self.sparse
                .versioned_get_current(database_id, tid, collection, &key)
        } else {
            self.sparse.get(database_id, tid, collection, &key)
        };

        match existing {
            Ok(Some(current_bytes)) => self.execute_upsert_overwrite(
                task,
                OverwriteCtx {
                    tid,
                    collection,
                    document_id,
                    surrogate,
                    value,
                    on_conflict_updates,
                    rls_write_check,
                    returning,
                    rls_filters,
                    database_id,
                    hook_ctx: &hook_ctx,
                    has_vectors,
                    strict_schema: strict_schema.as_ref(),
                    current_bytes,
                },
            ),
            Ok(None) => self.execute_upsert_insert(
                task,
                InsertCtx {
                    tid,
                    collection,
                    document_id,
                    surrogate,
                    value,
                    rls_write_check,
                    returning,
                    rls_filters,
                    database_id,
                    hook_ctx: &hook_ctx,
                    has_vectors,
                    strict_schema: strict_schema.as_ref(),
                },
            ),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
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

    /// One UPSERT of `body` onto the same source row.
    fn upsert(core: &mut CoreLoop, task: &ExecutionTask, body: &[u8]) -> Status {
        let targets = resolved();
        core.execute_upsert(
            task,
            UpsertParams {
                tid: TID,
                collection: SOURCE,
                document_id: "e61",
                surrogate: Surrogate(61),
                value: body,
                on_conflict_updates: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &targets,
            },
        )
        .status
    }

    #[test]
    fn upsert_credits_on_insert_and_deltas_on_conflict() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        let task = make_default_task();

        assert_eq!(upsert(&mut core, &task, &entry(A1, 20)), Status::Ok);
        assert_eq!(balance(&core, T1), "20", "the insert arm must credit");

        assert_eq!(upsert(&mut core, &task, &entry(A1, 50)), Status::Ok);
        assert_eq!(
            balance(&core, T1),
            "50",
            "the conflict arm must delta the total against the pre-merge row"
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

    /// An UPSERT that lands on an existing chained row is an update, and must be
    /// refused on the same terms — an arm that writes with a bare `sparse.put`
    /// runs no admission at all.
    #[test]
    fn an_upsert_onto_an_existing_hash_chained_row_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = chained_core(dir.path());
        let task = make_default_task();
        assert_eq!(insert_chained(&mut core, &task), Status::Ok);

        let resp = core.execute_upsert(
            &task,
            UpsertParams {
                tid: TID,
                collection: SOURCE,
                document_id: "e1",
                surrogate: Surrogate(91),
                value: &entry(A1, 999),
                on_conflict_updates: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(resp.status, Status::Error);
    }
}
