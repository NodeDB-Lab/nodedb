// SPDX-License-Identifier: BUSL-1.1

//! The PointUpdate statement itself: read the row, decide, write, answer.
//!
//! Holds only the sequencing — read the current row, decide what the update
//! makes it, gate the write policy on that decision, persist, then re-index,
//! emit, and project. The two halves it delegates to are the ones with their
//! own rules: `post_image` computes bytes and may not touch storage, `persist`
//! touches storage and may not reinterpret bytes. Keeping the order here, in
//! one readable pass, is what makes the "nothing is written before the policy
//! gate" property checkable at a glance.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec, StorageMode, UpdateValue};
use nodedb_types::Surrogate;

use super::super::update_reindex_secondary::UpdateSecondaryReindex;
use super::persist::PointUpdatePersist;
use super::post_image::PointUpdateImage;

/// Parameters for `execute_point_update`.
pub(in crate::data::executor) struct PointUpdateParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub updates: &'a [(String, UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled RLS read policy gating the `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Compiled RLS write policy gating the PERSIST, decided against the
    /// post-update image. A separate slot from `rls_filters`: that one bounds
    /// what may be shown back, this one bounds what may be written.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this update may touch — both sides of a join-key change. Resolved on the
    /// Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// Declared `PRIMARY KEY` column of a schemaless collection, `None`
    /// otherwise — see `PointUpdateImage::declared_primary_key`.
    pub declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_update(
        &mut self,
        task: &ExecutionTask,
        params: PointUpdateParams<'_>,
    ) -> Response {
        let PointUpdateParams {
            tid,
            collection,
            document_id,
            surrogate,
            updates,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
        } = params;
        let row_key = surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        debug!(
            core = self.core_id,
            %collection,
            %document_id,
            fields = updates.len(),
            has_returning = returning.is_some(),
            "point update"
        );

        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // `Some` exactly when the collection stores Binary Tuples. Held (not
        // just a bool) because the RETURNING projection below has to decode the
        // re-encoded post-image, and the MessagePack decoder accepts a Binary
        // Tuple without erroring — it would return a document with none of the
        // row's real columns rather than fail.
        let strict_schema = self
            .doc_configs
            .get(&config_key)
            .and_then(|c| match &c.storage_mode {
                StorageMode::Strict { schema } => Some(schema.clone()),
                StorageMode::Schemaless => None,
            });
        let is_strict = strict_schema.is_some();

        // Reject direct updates to generated columns.
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) = crate::data::executor::handlers::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )
        {
            return self.response_error(task, e);
        }

        // Refuse the statement outright on a collection that declared its rows
        // immutable. A hash-chained collection is refused here for the reason
        // its links exist: each link covers its predecessor's hash, so rewriting
        // a row makes `verify_chain` report the row AFTER it as broken, and the
        // tamper-evidence would accuse an untampered row.
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) = crate::data::executor::enforcement::append_only::check_point_update(
                collection,
                &config.enforcement,
            )
        {
            return self.response_error(task, e);
        }

        // Any non-literal assignment forces the slow decode→eval→re-encode path,
        // because we need the current document to evaluate against.
        let has_expr = updates
            .iter()
            .any(|(_, v)| matches!(v, UpdateValue::Expr(_)));

        let bitemporal = self.is_bitemporal(task.request.database_id.as_u64(), tid, collection);
        let sys_from_for_encode = if bitemporal {
            self.bitemporal_now_ms()
        } else {
            0
        };
        let database_id = task.request.database_id.as_u64();
        let get_result = if bitemporal {
            self.sparse
                .versioned_get_current(database_id, tid, collection, row_key)
        } else {
            self.sparse.get(database_id, tid, collection, row_key)
        };
        match get_result {
            Ok(Some(current_bytes)) => {
                let has_generated = self.doc_configs.get(&config_key).is_some_and(|c| {
                    !c.enforcement.generated_columns.is_empty()
                        && crate::data::executor::handlers::generated::needs_recomputation(
                            updates,
                            &c.enforcement.generated_columns,
                        )
                });

                let updated_bytes = match self.build_point_update_image(PointUpdateImage {
                    config_key: &config_key,
                    current_bytes: &current_bytes,
                    updates,
                    is_strict,
                    has_generated,
                    has_expr,
                    bitemporal,
                    sys_from_ms: sys_from_for_encode,
                    declared_primary_key,
                }) {
                    Ok(bytes) => bytes,
                    Err(e) => return self.response_error(task, e),
                };

                // Gate the persist on the collection's write policy, decided
                // against the post-update image the row will actually hold.
                // Placed after the generated columns are recomputed — a policy
                // may reference one — and before any store or index is touched,
                // so a rejected row leaves nothing behind.
                if let Err(e) = rls_write_gate::admit_stored_row(
                    rls_write_check,
                    &updated_bytes,
                    document_id,
                    strict_schema.as_ref(),
                    tid,
                    collection,
                ) {
                    return self.response_error(task, e);
                }

                let write_result = self.persist_point_update(PointUpdatePersist {
                    config_key: &config_key,
                    database_id,
                    tid,
                    collection,
                    row_key,
                    current_bytes: &current_bytes,
                    updated_bytes: &updated_bytes,
                    bitemporal,
                    sys_from_ms: sys_from_for_encode,
                    wal_lsn: task.wal_lsn(),
                    resolved_sum_targets,
                });
                match write_result {
                    Ok(target_write_set) => {
                        self.doc_cache.put(
                            task.request.database_id.as_u64(),
                            tid,
                            collection,
                            row_key,
                            &updated_bytes,
                        );

                        let has_vectors = self.collection_has_vectors(database_id, tid, collection);
                        if let Err(e) =
                            self.update_reindex_vector_and_sparse(UpdateSecondaryReindex {
                                database_id,
                                tid,
                                collection,
                                row_key,
                                surrogate,
                                new_body: &updated_bytes,
                                is_strict,
                                has_vectors,
                            })
                        {
                            return self.response_error(task, e);
                        }

                        // Emit update event to Event Plane. `current_bytes`
                        // is the pre-update row already read above; the
                        // helper derives `WriteOp::Update` from the Some
                        // prior + Some new pair and handles strict→msgpack
                        // conversion on both sides.
                        self.emit_put_event(
                            task,
                            tid,
                            collection,
                            row_key,
                            &updated_bytes,
                            Some(&current_bytes),
                        );

                        // Build the response for both the RETURNING and
                        // non-RETURNING branches first, then — only when the
                        // collection carries a secondary vector index — carry the
                        // surrogate + post-image back in the write-set so the
                        // Control Plane can mint a post-apply `Put` redo record.
                        // The autocommit WAL path mints none for a PointUpdate, so
                        // without this a WAL-only restart rebuilds the HNSW from the
                        // pre-update body and resurrects the old embedding.
                        // `updated_bytes` is moved in as its last use.
                        let mut response = if let Some(spec) = returning {
                            // Post-update image, decoded in the collection's
                            // storage mode; the user-visible key only fills in
                            // as `id` when the row declares none of its own.
                            let doc = match returning_doc::from_stored(
                                &updated_bytes,
                                document_id,
                                strict_schema.as_ref(),
                            ) {
                                Ok(doc) => doc,
                                Err(e) => return self.response_error(task, e),
                            };
                            match returning_rows::build_rows_payload(spec, rls_filters, &[doc]) {
                                Ok(payload) => self.response_with_payload(task, payload),
                                Err(e) => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: format!("RETURNING encode: {e}"),
                                        },
                                    );
                                }
                            }
                        } else {
                            let mut payload = Vec::with_capacity(16);
                            nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
                            nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", 1);
                            self.response_with_payload(task, payload)
                        };
                        if has_vectors {
                            response.write_set = vec![WriteSetEntry {
                                surrogate: surrogate.as_u32(),
                                is_delete: false,
                                value: updated_bytes,
                                collection: None,
                            }];
                        }
                        // Derived target rows live in a DIFFERENT collection
                        // than this statement's, so each carries its own
                        // `Some(collection)` and homes to that collection's
                        // vShard. Appended rather than replacing: the row's own
                        // vector redo above and these are both required.
                        response.write_set.extend(target_write_set);
                        response
                    }
                    Err(e) => self.response_error(task, e),
                }
            }
            Ok(None) => {
                let mut payload = Vec::with_capacity(16);
                nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
                nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", 0);
                self.response_with_payload(task, payload)
            }
            Err(e) => self.response_error(task, e),
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
    use crate::engine::document::store::{CollectionConfig, surrogate_to_doc_id};
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
    const A2: &str = "a2";
    const T1: Surrogate = Surrogate(4001);
    const T2: Surrogate = Surrogate(4002);

    fn binding() -> nodedb_physical::physical_plan::MaterializedSumBinding {
        nodedb_physical::physical_plan::MaterializedSumBinding {
            target_collection: TARGET.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        }
    }

    fn resolved() -> Vec<ResolvedSumTarget> {
        vec![
            ResolvedSumTarget::new(TARGET, A1, T1),
            ResolvedSumTarget::new(TARGET, A2, T2),
        ]
    }

    fn config_key(collection: &str) -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
        )
    }

    /// A source collection bound to the sum, and two target rows starting at
    /// zero.
    fn seeded_core(dir: &std::path::Path) -> CoreLoop {
        let (mut core, _req, _resp) = make_core_with_dir(dir);

        let mut source = CollectionConfig::new(SOURCE);
        source.enforcement.materialized_sum_sources = vec![binding()];
        core.doc_configs.insert(config_key(SOURCE), source);
        core.doc_configs
            .insert(config_key(TARGET), CollectionConfig::new(TARGET));

        for (id, surrogate) in [(A1, T1), (A2, T2)] {
            let seed = serde_json::json!({"id": id, "balance": "0"});
            core.sparse
                .put(
                    DB,
                    TID,
                    TARGET,
                    &surrogate_to_doc_id(surrogate),
                    &doc_format::encode_to_msgpack(&seed),
                )
                .expect("seed target row");
        }
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
            .get(DB, TID, TARGET, &surrogate_to_doc_id(surrogate))
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
    fn point_update_moves_the_amount_when_the_join_key_changes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        let task = make_default_task();
        let targets = resolved();

        assert_eq!(
            insert(&mut core, &task, Surrogate(41), &entry(A1, 60)),
            Status::Ok
        );
        assert_eq!(balance(&core, T1), "60");

        let moved = nodedb_types::json_to_msgpack(&serde_json::json!(A2)).expect("encode");
        let updates = vec![("account_id".to_string(), UpdateValue::Literal(moved))];
        let resp = core.execute_point_update(
            &task,
            PointUpdateParams {
                tid: TID,
                collection: SOURCE,
                document_id: "e41",
                surrogate: Surrogate(41),
                updates: &updates,
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &targets,
                declared_primary_key: None,
            },
        );
        assert_eq!(resp.status, Status::Ok);

        assert_eq!(
            balance(&core, T1),
            "0",
            "the account the row left must lose the amount"
        );
        assert_eq!(
            balance(&core, T2),
            "60",
            "the account the row joined must gain it"
        );
    }

    #[test]
    fn point_update_deltas_the_amount_in_place() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        let task = make_default_task();
        let targets = resolved();

        assert_eq!(
            insert(&mut core, &task, Surrogate(51), &entry(A1, 10)),
            Status::Ok
        );

        let raised = nodedb_types::json_to_msgpack(&serde_json::json!(35)).expect("encode");
        let updates = vec![("amount".to_string(), UpdateValue::Literal(raised))];
        let resp = core.execute_point_update(
            &task,
            PointUpdateParams {
                tid: TID,
                collection: SOURCE,
                document_id: "e51",
                surrogate: Surrogate(51),
                updates: &updates,
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &targets,
                declared_primary_key: None,
            },
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            balance(&core, T1),
            "35",
            "the total must move by the difference, not by the new amount"
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

    /// Rewriting a link has the same effect as removing one.
    #[test]
    fn an_update_on_a_hash_chained_collection_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = chained_core(dir.path());
        let task = make_default_task();
        assert_eq!(insert_chained(&mut core, &task), Status::Ok);

        let raised = nodedb_types::json_to_msgpack(&serde_json::json!(999)).expect("encode");
        let updates = vec![("amount".to_string(), UpdateValue::Literal(raised))];
        let resp = core.execute_point_update(
            &task,
            PointUpdateParams {
                tid: TID,
                collection: SOURCE,
                document_id: "e1",
                surrogate: Surrogate(91),
                updates: &updates,
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &[],
                declared_primary_key: None,
            },
        );
        assert_eq!(resp.status, Status::Error);

        let stored = core
            .sparse
            .get(DB, TID, SOURCE, &surrogate_to_doc_id(Surrogate(91)))
            .expect("read back")
            .expect("row must exist");
        let doc = doc_format::decode_document(&stored).expect("decode");
        assert_eq!(
            doc.get("amount").and_then(|v| v.as_i64()),
            Some(10),
            "a refused update must leave the chained row byte-identical"
        );
    }
}
