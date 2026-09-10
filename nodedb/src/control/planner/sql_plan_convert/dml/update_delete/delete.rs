// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::Delete` → `PhysicalTask` lowering.

use nodedb_sql::types::{EngineType, Filter, SqlValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::control::planner::sql_plan_convert::filter::serialize_filters;
use crate::control::planner::sql_plan_convert::value::{sql_value_to_bytes, sql_value_to_string};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::shared::{
    delete_effective_filter, document_collection_is_edge_bearing, pk_effective_filter,
};

pub(in crate::control::planner::sql_plan_convert) fn convert_delete(
    collection: &str,
    engine: &EngineType,
    filters: &[Filter],
    target_keys: &[SqlValue],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = crate::control::planner::sql_plan_convert::convert::db_qualified(
        ctx.database_id,
        collection,
    );
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);

    if matches!(engine, EngineType::KeyValue) {
        // A KV collection has no document store; a WHERE with no primary key
        // must still route to the KV engine, not `DocumentOp::BulkDelete`.
        if target_keys.is_empty() {
            let filter_bytes = serialize_filters(filters)?;
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                database_id: ctx.database_id,
                plan: PhysicalPlan::Kv(KvOp::PredicateDelete {
                    collection: qualified_collection.clone(),
                    filters: filter_bytes,
                    rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                }),
                post_set_op: PostSetOp::None,
                txn_id: None,
            }]);
        }
        let keys: Vec<Vec<u8>> = target_keys.iter().map(sql_value_to_bytes).collect();
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Kv(KvOp::Delete {
                collection: qualified_collection.clone(),
                keys,
                // Filled by the RLS injection pass, which runs after plan
                // conversion.
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // `TimeseriesRules::plan_delete` already rejects this; this guard keeps
    // the rejection true for any other caller, not a fall-through to
    // `DocumentOp::BulkDelete` over the empty document store.
    if matches!(engine, EngineType::Timeseries) {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "DELETE is not supported on timeseries collection '{collection}'; \
                 expire rows with CREATE RETENTION POLICY"
            ),
        });
    }

    // Columnar and spatial engines have no document store; route to
    // `ColumnarOp::Delete` regardless of whether the WHERE reduces to PK keys
    // (mirrors the columnar/spatial UPDATE routing in `convert_update`). Without
    // this a columnar/spatial DELETE falls through to `DocumentOp::BulkDelete`,
    // which scans the empty document store and matches nothing.
    if matches!(engine, EngineType::Columnar | EngineType::Spatial) {
        let filter_bytes = serialize_filters(filters)?;
        let effective_filter = pk_effective_filter(filter_bytes, target_keys)?;
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection: qualified_collection.clone(),
                filters: effective_filter,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // CRDT gate: a `crdt = true` collection routes to `CrdtOp::DocDelete`.
    // Only a PK-targeted DELETE is representable; a predicate DELETE is
    // rejected — no silent fallthrough that would bypass CRDT convergence.
    let is_crdt = super::super::crdt_gate::document_collection_is_crdt(ctx, collection)?;
    if is_crdt && target_keys.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "predicate (non-primary-key) DELETE on CRDT collection '{collection}' is not \
                 supported; target rows by primary key"
            ),
        });
    }

    // Edge-bearing gate: a PK-equality delete on a collection with implicit
    // edges must not lower to a static `PointDelete` — that bypasses OLLP
    // and leaks the edge. Route as `BulkDelete` instead so the edge-bearing
    // gate sends it through the Calvin/OLLP coordinator. Non-edge-bearing
    // collections keep the fast `PointDelete` path below.
    if !is_crdt && !target_keys.is_empty() && document_collection_is_edge_bearing(ctx, collection)?
    {
        let effective_filter = delete_effective_filter(filters, target_keys)?;
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: qualified_collection.clone(),
                filters: effective_filter,
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                // Filled in by the materialized-sum resolution pass, which
                // recon-scans the rows this predicate matches.
                resolved_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    if !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            let pk_string = sql_value_to_string(key);
            let pk_bytes = pk_string.clone().into_bytes();
            // Read-only resolution: a task always exists (the write hook still
            // runs, an unbound row_key affects 0 rows, and the clone CoW
            // resolver intercepts the ZERO sentinel), but a key this statement
            // never creates must never mint a binding.
            let surrogate = ctx.surrogate_for_existing_pk(collection, &pk_bytes)?;
            let plan = if is_crdt {
                PhysicalPlan::Crdt(CrdtOp::DocDelete {
                    collection: qualified_collection.clone(),
                    document_id: pk_string,
                    surrogate,
                    returning: None,
                    rls_filters: Vec::new(),
                })
            } else {
                PhysicalPlan::Document(DocumentOp::PointDelete {
                    collection: qualified_collection.clone(),
                    document_id: pk_string,
                    surrogate,
                    pk_bytes,
                    returning: None,
                    rls_filters: Vec::new(),
                    rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                    resolved_sum_targets: Vec::new(),
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
        Ok(tasks)
    } else {
        let filter_bytes = serialize_filters(filters)?;
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: qualified_collection,
                filters: filter_bytes,
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                // Filled in by the materialized-sum resolution pass, which
                // recon-scans the rows this predicate matches.
                resolved_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }])
    }
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

    #[test]
    fn pk_delete_on_edge_bearing_collection_routes_bulk_delete() {
        let (ctx, _dir) = ctx_with_catalog();
        let keys = vec![SqlValue::String("edge_3".to_string())];
        let tasks = convert_delete(
            "edges",
            &EngineType::DocumentSchemaless,
            &[],
            &keys,
            TenantId::new(0),
            &ctx,
        )
        .expect("convert_delete");
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::BulkDelete { filters, .. }) => {
                // Synthesized PK filter is never empty for a non-empty target_keys.
                assert!(
                    !filters.is_empty(),
                    "edge-bearing PK delete must carry a non-empty filter"
                );
            }
            other => panic!("expected BulkDelete, got {other:?}"),
        }
    }

    #[test]
    fn pk_delete_on_non_edge_collection_routes_point_delete() {
        let (ctx, _dir) = ctx_with_catalog();
        let keys = vec![SqlValue::String("row_1".to_string())];
        let tasks = convert_delete(
            "plain",
            &EngineType::DocumentSchemaless,
            &[],
            &keys,
            TenantId::new(0),
            &ctx,
        )
        .expect("convert_delete");
        assert_eq!(tasks.len(), 1);
        assert!(
            matches!(
                &tasks[0].plan,
                PhysicalPlan::Document(DocumentOp::PointDelete { .. })
            ),
            "non-edge-bearing PK delete must remain a PointDelete"
        );
    }

    #[test]
    fn delete_by_pk_on_crdt_routes_doc_delete() {
        let (ctx, _dir) = ctx_with_catalog();
        let keys = vec![SqlValue::String("k1".to_string())];
        let tasks = convert_delete(
            "crdt_coll",
            &EngineType::DocumentSchemaless,
            &[],
            &keys,
            TenantId::new(0),
            &ctx,
        )
        .expect("convert_delete");
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Crdt(CrdtOp::DocDelete { document_id, .. }) => {
                assert_eq!(document_id, "k1");
            }
            other => panic!("expected CrdtOp::DocDelete, got {other:?}"),
        }
    }

    #[test]
    fn predicate_delete_on_crdt_rejects() {
        let (ctx, _dir) = ctx_with_catalog();
        let err = convert_delete(
            "crdt_coll",
            &EngineType::DocumentSchemaless,
            &[],
            &[],
            TenantId::new(0),
            &ctx,
        )
        .expect_err("predicate DELETE on crdt must reject");
        assert!(matches!(err, crate::Error::BadRequest { .. }));
    }
}
