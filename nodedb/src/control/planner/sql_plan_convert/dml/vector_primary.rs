// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::VectorPrimary{Insert,Delete,Truncate,Update}` → `PhysicalTask` lowering.
//!
//! A vector-primary row is keyed by its declared primary key, through the
//! same identity path a document row takes: the key content-addresses a
//! surrogate, the surrogate keys the HNSW node and the payload sidecar. A
//! point `DELETE` / `UPDATE` resolves its keys read-only, so a key this
//! statement never created mints no binding.

use nodedb_sql::types::{Filter, SqlExpr, SqlValue, VectorPrimaryInsertIntent, VectorPrimaryRow};
use nodedb_types::{RlsWriteCheck, Surrogate};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::{UpdateValue, VectorOp, VectorWriteTargets};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::convert::{ConvertContext, db_qualified};
use super::super::filter::serialize_filters;
use super::super::value::{
    assignments_to_update_values, sql_value_to_nodedb_value, sql_value_to_string,
};
use super::insert::{
    declared_primary_key_name, is_auto_rowid_pk, resolve_doc_identity_with_declared,
};

/// Collection-level settings every vector-primary write carries.
pub(in super::super) struct VectorPrimaryCfg<'a> {
    pub field: &'a str,
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
}

/// Inputs to [`convert_vector_primary_insert`].
pub(in super::super) struct VectorPrimaryInsertArgs<'a> {
    pub collection: &'a str,
    pub cfg: &'a VectorPrimaryCfg<'a>,
    pub rows: &'a [VectorPrimaryRow],
    pub intent: VectorPrimaryInsertIntent,
    pub on_conflict_updates: &'a [(String, SqlExpr)],
    /// Resolved primary-key column; `id` by convention when nothing is declared.
    pub primary_key: &'a str,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

/// The routing every vector-primary task shares.
struct Routing {
    qualified: nodedb_types::QualifiedCollection,
    collection: String,
    vshard: VShardId,
}

fn routing(ctx: &ConvertContext, collection: &str) -> Routing {
    let qualified = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = db_qualified(ctx.database_id, collection);
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection.as_str());
    Routing {
        qualified,
        collection,
        vshard,
    }
}

fn task(tenant_id: TenantId, r: &Routing, ctx: &ConvertContext, op: VectorOp) -> PhysicalTask {
    PhysicalTask {
        tenant_id,
        vshard_id: r.vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Vector(op),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

/// The declared `PRIMARY KEY` column, read once per statement. `_rowid`
/// carries no declaration.
fn declared_pk(
    ctx: &ConvertContext,
    collection: &str,
    primary_key: &str,
) -> crate::Result<Option<String>> {
    if is_auto_rowid_pk(primary_key) {
        Ok(None)
    } else {
        declared_primary_key_name(ctx, collection)
    }
}

/// Encode a row's non-vector columns as the payload image the Data Plane
/// stores verbatim as the sidecar.
fn encode_payload(fields: &std::collections::HashMap<String, SqlValue>) -> crate::Result<Vec<u8>> {
    if fields.is_empty() {
        return Ok(Vec::new());
    }
    let value_map: std::collections::HashMap<String, nodedb_types::Value> = fields
        .iter()
        .map(|(k, v)| (k.clone(), sql_value_to_nodedb_value(v)))
        .collect();
    zerompk::to_msgpack_vec(&value_map).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("vector-primary payload: {e}"),
    })
}

pub(in super::super) fn convert_vector_primary_insert(
    args: VectorPrimaryInsertArgs<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let VectorPrimaryInsertArgs {
        collection,
        cfg,
        rows,
        intent,
        on_conflict_updates,
        primary_key,
        tenant_id,
        ctx,
    } = args;
    let r = routing(ctx, collection);
    let collection = r.collection.as_str();
    let declared = declared_pk(ctx, collection, primary_key)?;
    let update_values = if on_conflict_updates.is_empty() {
        Vec::new()
    } else {
        assignments_to_update_values(on_conflict_updates)?
    };
    let mut tasks = Vec::with_capacity(rows.len());
    for row in rows {
        // Enforce the per-tenant vector dimension quota before building any
        // task. 0 means unlimited.
        if ctx.max_vector_dim > 0 {
            let dim = row.vector.len() as u32;
            if dim > ctx.max_vector_dim {
                return Err(crate::Error::TenantVectorDimExceeded {
                    dim,
                    limit: ctx.max_vector_dim,
                });
            }
        }
        // Identity comes from the declared primary key, exactly as it does
        // for a document row: the key content-addresses the surrogate, and
        // a row with no key mints a fresh one and carries its identity under
        // the key column so a later point read finds it.
        let row_fields: Vec<(String, SqlValue)> = row
            .payload_fields
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let (doc_id, surrogate) = resolve_doc_identity_with_declared(
            ctx,
            collection,
            primary_key,
            declared.as_deref(),
            &row_fields,
        )?;
        let key_column = declared.as_deref().unwrap_or(primary_key);
        let mut fields = row.payload_fields.clone();
        if !is_auto_rowid_pk(primary_key)
            && !fields
                .get(key_column)
                .is_some_and(|v| !matches!(v, SqlValue::Null))
        {
            fields.insert(key_column.to_string(), SqlValue::String(doc_id.clone()));
        }
        let pk_bytes = doc_id.into_bytes();
        let payload = encode_payload(&fields)?;

        // `returning` and `rls_filters` are filled in after conversion: the
        // RETURNING spec by the protocol layer's injection pass, the read
        // filter by the RLS injection pass.
        let op = match intent {
            VectorPrimaryInsertIntent::Insert => VectorOp::DirectInsert {
                collection: r.qualified.clone(),
                field: cfg.field.to_string(),
                surrogate,
                pk_bytes,
                vector: row.vector.clone(),
                payload,
                quantization: cfg.quantization,
                storage_dtype: cfg.storage_dtype,
                payload_indexes: cfg.payload_indexes.to_vec(),
                returning: None,
                rls_filters: Vec::new(),
            },
            VectorPrimaryInsertIntent::InsertIfAbsent => VectorOp::DirectInsertIfAbsent {
                collection: r.qualified.clone(),
                field: cfg.field.to_string(),
                surrogate,
                pk_bytes,
                vector: row.vector.clone(),
                payload,
                quantization: cfg.quantization,
                storage_dtype: cfg.storage_dtype,
                payload_indexes: cfg.payload_indexes.to_vec(),
                returning: None,
                rls_filters: Vec::new(),
            },
            VectorPrimaryInsertIntent::Upsert => VectorOp::DirectUpsert {
                collection: r.qualified.clone(),
                field: cfg.field.to_string(),
                surrogate,
                pk_bytes,
                vector: row.vector.clone(),
                payload,
                quantization: cfg.quantization,
                storage_dtype: cfg.storage_dtype,
                payload_indexes: cfg.payload_indexes.to_vec(),
                returning: None,
                rls_filters: Vec::new(),
                on_conflict_updates: update_values.clone(),
                rls_write_check: RlsWriteCheck::pending_injection(),
            },
        };
        tasks.push(task(tenant_id, &r, ctx, op));
    }
    Ok(tasks)
}

/// Resolve `target_keys` read-only to the surrogates they are bound to, or
/// serialize `filters` for the Data Plane to evaluate on the sidecar rows.
fn write_targets(
    ctx: &ConvertContext,
    collection: &str,
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> crate::Result<VectorWriteTargets> {
    if target_keys.is_empty() {
        return Ok(VectorWriteTargets::Predicate(serialize_filters(filters)?));
    }
    let mut surrogates: Vec<Surrogate> = Vec::with_capacity(target_keys.len());
    for key in target_keys {
        let pk_bytes = sql_value_to_string(key).into_bytes();
        surrogates.push(ctx.surrogate_for_existing_pk(collection, &pk_bytes)?);
    }
    Ok(VectorWriteTargets::Surrogates(surrogates))
}

pub(in super::super) fn convert_vector_primary_delete(
    collection: &str,
    field: &str,
    filters: &[Filter],
    target_keys: &[SqlValue],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let r = routing(ctx, collection);
    let targets = write_targets(ctx, r.collection.as_str(), filters, target_keys)?;
    Ok(vec![task(
        tenant_id,
        &r,
        ctx,
        VectorOp::DirectDelete {
            collection: r.qualified.clone(),
            field: field.to_string(),
            targets,
            // Attached by `inject_returning_spec` after plan conversion; the
            // RLS injection pass fills the two policy slots.
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::pending_injection(),
        },
    )])
}

/// Lower a vector-primary `TRUNCATE`. The Data Plane resolves every live
/// surrogate of the primary index itself, so no key is bound here.
pub(in super::super) fn convert_vector_primary_truncate(
    collection: &str,
    field: &str,
    restart_identity: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> Vec<PhysicalTask> {
    let r = routing(ctx, collection);
    vec![task(
        tenant_id,
        &r,
        ctx,
        VectorOp::DirectTruncate {
            collection: r.qualified.clone(),
            field: field.to_string(),
            restart_identity,
        },
    )]
}

/// Inputs to [`convert_vector_primary_update`].
pub(in super::super) struct VectorPrimaryUpdateArgs<'a> {
    pub collection: &'a str,
    pub cfg: &'a VectorPrimaryCfg<'a>,
    pub new_vector: Option<&'a [f32]>,
    pub assignments: &'a [(String, SqlExpr)],
    pub filters: &'a [Filter],
    pub target_keys: &'a [SqlValue],
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(in super::super) fn convert_vector_primary_update(
    args: VectorPrimaryUpdateArgs<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let VectorPrimaryUpdateArgs {
        collection,
        cfg,
        new_vector,
        assignments,
        filters,
        target_keys,
        tenant_id,
        ctx,
    } = args;
    if let Some(vector) = new_vector
        && ctx.max_vector_dim > 0
        && vector.len() as u32 > ctx.max_vector_dim
    {
        return Err(crate::Error::TenantVectorDimExceeded {
            dim: vector.len() as u32,
            limit: ctx.max_vector_dim,
        });
    }
    let r = routing(ctx, collection);
    let targets = write_targets(ctx, r.collection.as_str(), filters, target_keys)?;
    let payload_patch: Vec<(String, UpdateValue)> = assignments_to_update_values(assignments)?;
    Ok(vec![task(
        tenant_id,
        &r,
        ctx,
        VectorOp::DirectUpdate {
            collection: r.qualified.clone(),
            field: cfg.field.to_string(),
            targets,
            new_vector: new_vector.map(<[f32]>::to_vec),
            payload_patch,
            quantization: cfg.quantization,
            storage_dtype: cfg.storage_dtype,
            payload_indexes: cfg.payload_indexes.to_vec(),
            // See `convert_vector_primary_delete`.
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::pending_injection(),
        },
    )])
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types::VectorPrimaryRow;
    use nodedb_types::VectorQuantization;

    fn make_ctx(max_vector_dim: u32) -> ConvertContext {
        ConvertContext {
            purpose: super::super::super::convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: None,
            credentials: None,
            wal: None,
            surrogate_assigner: None,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        }
    }

    fn row(dim: usize, id: &str) -> VectorPrimaryRow {
        let mut payload_fields = std::collections::HashMap::new();
        payload_fields.insert("id".to_string(), SqlValue::String(id.to_string()));
        VectorPrimaryRow {
            surrogate: nodedb_types::Surrogate::ZERO,
            vector: vec![0.0f32; dim],
            payload_fields,
        }
    }

    fn cfg() -> VectorPrimaryCfg<'static> {
        VectorPrimaryCfg {
            field: "emb",
            quantization: VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: &[],
        }
    }

    fn convert(
        ctx: &ConvertContext,
        rows: &[VectorPrimaryRow],
        intent: VectorPrimaryInsertIntent,
    ) -> crate::Result<Vec<PhysicalTask>> {
        convert_vector_primary_insert(VectorPrimaryInsertArgs {
            collection: "vecs",
            cfg: &cfg(),
            rows,
            intent,
            on_conflict_updates: &[],
            primary_key: "id",
            tenant_id: crate::types::TenantId::new(1),
            ctx,
        })
    }

    #[test]
    fn tenant_vector_dim_under_bound_succeeds() {
        let ctx = make_ctx(128);
        let rows = vec![row(64, "a"), row(128, "b")];
        let result = convert(&ctx, &rows, VectorPrimaryInsertIntent::Insert);
        assert!(result.is_ok(), "dimensions under/at cap must succeed");
    }

    #[test]
    fn tenant_vector_dim_exceeded_rejected() {
        let ctx = make_ctx(64);
        let rows = vec![row(65, "a")];
        let result = convert(&ctx, &rows, VectorPrimaryInsertIntent::Insert);
        match result {
            Err(crate::Error::TenantVectorDimExceeded { dim, limit }) => {
                assert_eq!(dim, 65);
                assert_eq!(limit, 64);
            }
            other => panic!("expected TenantVectorDimExceeded, got {other:?}"),
        }
    }

    #[test]
    fn tenant_vector_dim_zero_means_unlimited() {
        let ctx = make_ctx(0);
        let rows = vec![row(99999, "a")];
        let result = convert(&ctx, &rows, VectorPrimaryInsertIntent::Insert);
        assert!(result.is_ok(), "limit=0 means unlimited, must succeed");
    }

    /// The row's primary key, not its vector, is the identity carried to the
    /// Data Plane: two rows with the same vector and different keys are two
    /// rows, and the key bytes travel for followers to bind.
    #[test]
    fn identity_is_the_primary_key_not_the_vector() {
        let ctx = make_ctx(0);
        let rows = vec![row(3, "r1"), row(3, "r2")];
        let tasks = convert(&ctx, &rows, VectorPrimaryInsertIntent::Insert).expect("convert");
        let keys: Vec<Vec<u8>> = tasks
            .iter()
            .map(|t| match &t.plan {
                PhysicalPlan::Vector(VectorOp::DirectInsert { pk_bytes, .. }) => pk_bytes.clone(),
                other => panic!("expected DirectInsert, got {other:?}"),
            })
            .collect();
        assert_eq!(keys, vec![b"r1".to_vec(), b"r2".to_vec()]);
    }

    #[test]
    fn intent_selects_the_physical_op() {
        let ctx = make_ctx(0);
        let rows = vec![row(3, "r1")];
        let absent =
            convert(&ctx, &rows, VectorPrimaryInsertIntent::InsertIfAbsent).expect("convert");
        assert!(matches!(
            absent[0].plan,
            PhysicalPlan::Vector(VectorOp::DirectInsertIfAbsent { .. })
        ));
        let upsert = convert(&ctx, &rows, VectorPrimaryInsertIntent::Upsert).expect("convert");
        assert!(matches!(
            upsert[0].plan,
            PhysicalPlan::Vector(VectorOp::DirectUpsert { .. })
        ));
    }

    /// With no primary-key equality the WHERE clause travels as a predicate
    /// for the Data Plane to resolve against the sidecar rows.
    #[test]
    fn delete_without_point_keys_carries_the_predicate() {
        let ctx = make_ctx(0);
        let tasks = convert_vector_primary_delete(
            "vecs",
            "emb",
            &[],
            &[],
            crate::types::TenantId::new(1),
            &ctx,
        )
        .expect("convert");
        assert!(matches!(
            &tasks[0].plan,
            PhysicalPlan::Vector(VectorOp::DirectDelete {
                targets: VectorWriteTargets::Predicate(bytes),
                ..
            }) if bytes.is_empty()
        ));
    }
}
