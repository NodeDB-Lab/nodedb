// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::{SqlValue, WriteRoute};
use nodedb_types::Surrogate;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use super::super::super::convert::ConvertContext;
use super::super::super::value::{expand_row_defaults, row_to_msgpack, rows_to_msgpack_array};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::identity::{
    columnar_row_surrogates, declared_primary_key_name, is_auto_rowid_pk,
    resolve_doc_identity_with_declared,
};
use super::schema::build_schema_bytes;

/// Bundled arguments for [`convert_insert`].
pub(crate) struct ConvertInsertArgs<'a> {
    pub collection: &'a str,
    /// The lowering these rows take, decided by `nodedb-sql`.
    pub route: WriteRoute,
    pub rows: &'a [Vec<(String, SqlValue)>],
    pub column_defaults: &'a [(String, String)],
    pub column_schema: &'a [(String, String)],
    pub if_absent: bool,
    pub primary_key: Option<&'a str>,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(crate) fn convert_insert(args: ConvertInsertArgs<'_>) -> crate::Result<Vec<PhysicalTask>> {
    let ConvertInsertArgs {
        collection,
        route,
        rows,
        column_defaults,
        column_schema,
        if_absent,
        primary_key,
        tenant_id,
        ctx,
    } = args;
    let coll_qualified = super::super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let mut tasks = Vec::new();
    let mut columnar_rows: Vec<&Vec<(String, SqlValue)>> = Vec::new();

    // Both INSERT routing gates, read from the catalog once for the whole
    // statement (never re-hit per row).
    //
    // `IF NOT EXISTS` (ON CONFLICT DO NOTHING → `if_absent`) cannot be honored by
    // `CrdtOp::DocUpsert`, which is an unconditional LWW full-replace: reject.
    let gates = super::super::balanced_gate::document_collection_write_gates(ctx, collection)?;
    let is_crdt = gates.crdt;
    if is_crdt && if_absent {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "INSERT ... IF NOT EXISTS on CRDT collection '{collection}' is not supported; \
                 CRDT documents converge via last-writer-wins full replace"
            ),
        });
    }

    // A balanced collection's rows are judged as a set, so the statement lowers
    // to ONE page rather than one task per row — see `balanced_gate`.
    let is_balanced = gates.balanced && !is_crdt;
    // `ON CONFLICT DO NOTHING` skips rows whose key already exists, and which
    // rows those are is decided per row at apply time. A journal that silently
    // loses one leg that way is exactly the unbalanced state the constraint
    // exists to refuse, and the page shape cannot express the per-row skip, so
    // the combination is rejected rather than half-honored.
    if is_balanced && if_absent {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "INSERT ... IF NOT EXISTS on BALANCED collection '{collection}' is not \
                 supported; a row skipped on conflict would leave its journal unbalanced"
            ),
        });
    }
    // Rows of a balanced INSERT, accumulated across the loop below and emitted
    // as one `BatchInsert` task after it.
    let mut balanced_documents: Vec<(String, Vec<u8>)> = Vec::new();
    let mut balanced_surrogates: Vec<Surrogate> = Vec::new();

    // Every engine's rows expand their DEFAULTs here, ahead of identity
    // derivation. A DEFAULT materialized after the primary-key NOT NULL gate
    // refuses a key the declaration supplies.
    let expanded_rows = expand_row_defaults(rows, column_defaults, tenant_id, ctx)?;

    // One catalog read for the whole statement. `_rowid` carries no
    // declaration, so it skips the read.
    let declared_pk = if is_auto_rowid_pk(primary_key) {
        None
    } else {
        declared_primary_key_name(ctx, collection)?
    };

    for row in &expanded_rows {
        match route {
            WriteRoute::ColumnarFamily => {
                columnar_rows.push(row);
            }
            WriteRoute::Document => {
                let value_bytes = row_to_msgpack(row)?;
                let (doc_id, surrogate) = resolve_doc_identity_with_declared(
                    ctx,
                    collection,
                    primary_key,
                    declared_pk.as_deref(),
                    row,
                )?;
                // One page for the whole statement: the rows of a balanced
                // INSERT are judged together, so they may not be split across
                // one task — one boundary — per row.
                if is_balanced {
                    balanced_documents.push((doc_id, value_bytes));
                    balanced_surrogates.push(surrogate);
                    continue;
                }
                let plan = if is_crdt {
                    PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                        collection: qualified_collection.clone(),
                        document_id: doc_id,
                        fields_json: super::super::crdt_gate::row_to_fields_json(row)?,
                        surrogate,
                        partial: false,
                        returning: None,
                        rls_filters: Vec::new(),
                    })
                } else {
                    PhysicalPlan::Document(DocumentOp::PointInsert {
                        collection: qualified_collection.clone(),
                        document_id: doc_id,
                        value: value_bytes,
                        if_absent,
                        surrogate,
                        // Both filled in after conversion: the RETURNING spec
                        // by the protocol layer's injection pass, the read
                        // filter by the RLS injection pass.
                        returning: None,
                        rls_filters: Vec::new(),
                        // Filled by the materialized-sum resolution pass,
                        // which runs after conversion (it needs the catalog
                        // and, in cluster mode, a routed lookup).
                        resolved_sum_targets: Vec::new(),
                        deferred_sum_targets: Vec::new(),
                    })
                };
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    database_id: ctx.database_id,
                    plan,
                    post_set_op: PostSetOp::None,
                    txn_id: None,
                });
            }
        }
    }

    if !balanced_documents.is_empty() {
        tasks.push(super::super::balanced_gate::balanced_batch_task(
            super::super::balanced_gate::BalancedBatch {
                collection,
                tenant_id,
                vshard,
                documents: balanced_documents,
                surrogates: balanced_surrogates,
            },
            ctx.database_id,
        ));
    }

    if !columnar_rows.is_empty() {
        let payload = rows_to_msgpack_array(&columnar_rows)?;
        // `ON CONFLICT DO NOTHING` means skip, not refuse: `if_absent` keeps
        // `InsertIfAbsent` regardless of the declared key. Otherwise, a
        // `PRIMARY KEY` declared on a natural key column (not `id` /
        // `document_id`) refuses a duplicate rather than tombstoning it.
        let intent = if if_absent {
            ColumnarInsertIntent::InsertIfAbsent
        } else if declared_pk
            .as_deref()
            .is_some_and(|pk| pk != "id" && pk != "document_id")
        {
            ColumnarInsertIntent::InsertUnique
        } else {
            ColumnarInsertIntent::Insert
        };
        let surrogates = columnar_row_surrogates(
            ctx,
            collection,
            &columnar_rows,
            primary_key,
            declared_pk.as_deref(),
        )?;
        let schema_bytes = build_schema_bytes(column_schema, declared_pk.as_deref());
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: qualified_collection.clone(),
                payload,
                format: "msgpack".into(),
                intent,
                on_conflict_updates: Vec::new(),
                surrogates,
                schema_bytes,
                provenance: None,
                wal_lsn: None,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                // Both slots are filled by later passes over the built plan —
                // `inject_returning_spec` from the statement's RETURNING list,
                // and the row-level-security injector from the collection's read
                // policy. Filling either here would duplicate a decision that
                // has one owner.
                returning: None,
                rls_filters: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }

    Ok(tasks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;

    /// Build a `ConvertContext` whose credential store carries a catalog with
    /// three collections under tenant 0 / DEFAULT database: `edges`
    /// (`has_implicit_edges = true`), `plain` (all flags false), and `crdt_coll`
    /// (`crdt = true`). The returned `TempDir` must be kept alive for the lifetime
    /// of the context (it backs the catalog's redb file).
    fn ctx_with_catalog() -> (ConvertContext, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            CredentialStore::open(&dir.path().join("system.redb")).expect("open credential store");
        {
            let catalog = store.catalog();
            let mut edges = StoredCollection::new(0, "edges", "owner");
            edges.has_implicit_edges = true;
            catalog
                .put_collection(crate::types::DatabaseId::DEFAULT, &edges)
                .expect("put edges collection");
            let plain = StoredCollection::new(0, "plain", "owner");
            catalog
                .put_collection(crate::types::DatabaseId::DEFAULT, &plain)
                .expect("put plain collection");
            let mut crdt_coll = StoredCollection::new(0, "crdt_coll", "owner");
            crdt_coll.crdt = true;
            catalog
                .put_collection(crate::types::DatabaseId::DEFAULT, &crdt_coll)
                .expect("put crdt collection");
        }

        let ctx = ConvertContext {
            purpose: crate::control::planner::sql_plan_convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: None,
            credentials: Some(Arc::new(store)),
            wal: None,
            surrogate_assigner: None,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            sql_catalog: None,
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        };
        (ctx, dir)
    }

    fn crdt_row(id: &str) -> Vec<(String, SqlValue)> {
        vec![
            ("id".to_string(), SqlValue::String(id.to_string())),
            ("name".to_string(), SqlValue::String("alice".to_string())),
        ]
    }

    #[test]
    fn insert_into_crdt_collection_routes_doc_upsert() {
        let (ctx, _dir) = ctx_with_catalog();
        let rows = vec![crdt_row("k1")];
        let tasks = convert_insert(ConvertInsertArgs {
            collection: "crdt_coll",
            route: WriteRoute::Document,
            rows: &rows,
            column_defaults: &[],
            column_schema: &[],
            if_absent: false,
            primary_key: Some("id"),
            tenant_id: TenantId::new(0),
            ctx: &ctx,
        })
        .expect("convert_insert");
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                document_id,
                fields_json,
                partial,
                ..
            }) => {
                assert_eq!(document_id, "k1");
                assert!(!partial, "INSERT must be a full-replace DocUpsert");
                assert!(fields_json.contains("alice"));
            }
            other => panic!("expected CrdtOp::DocUpsert, got {other:?}"),
        }
    }

    #[test]
    fn insert_into_non_crdt_collection_routes_point_insert() {
        let (ctx, _dir) = ctx_with_catalog();
        let rows = vec![crdt_row("k1")];
        let tasks = convert_insert(ConvertInsertArgs {
            collection: "plain",
            route: WriteRoute::Document,
            rows: &rows,
            column_defaults: &[],
            column_schema: &[],
            if_absent: false,
            primary_key: Some("id"),
            tenant_id: TenantId::new(0),
            ctx: &ctx,
        })
        .expect("convert_insert");
        assert_eq!(tasks.len(), 1);
        assert!(
            matches!(
                &tasks[0].plan,
                PhysicalPlan::Document(DocumentOp::PointInsert { .. })
            ),
            "non-crdt INSERT must remain a PointInsert"
        );
    }
}
