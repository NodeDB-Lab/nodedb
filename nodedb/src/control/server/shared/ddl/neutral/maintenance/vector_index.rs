// SPDX-License-Identifier: BUSL-1.1

//! Vector index lifecycle DDL handlers.
//!
//! - `SHOW VECTOR INDEX status ON collection.column` — query live stats from Data Plane
//! - `ALTER VECTOR INDEX ON collection.column SEAL` — force-seal growing segment
//! - `ALTER VECTOR INDEX ON collection.column COMPACT` — tombstone compaction
//!
//! `ALTER VECTOR INDEX ... SET (...)` lives in [`super::vector_index_set`].
//!
//! The `SHOW` result set is all-text columns, so the protocol-neutral
//! [`ShapedRows`] carries `DdlColType::Text` per column and each cell as its
//! `String` form. The Data Plane dispatch paths (`dispatch_to_data_plane`,
//! plan construction, ordering) run here.

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use crate::types::TraceId;
use nodedb_physical::physical_plan::VectorOp;

use super::super::super::result::{DdlError, DdlResult};
use super::collection_column::parse_collection_column;
use super::support::ddl_err;

/// Handle `SHOW VECTOR INDEX status ON collection.column`.
///
/// Dispatches `VectorOp::QueryStats` to the Data Plane, awaits the response,
/// and formats the `VectorIndexStats` payload as a result set.
pub async fn handle_show_vector_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    // Parse: SHOW VECTOR INDEX status ON <collection>.<column>
    // or:   SHOW VECTOR INDEX status ON <collection>
    let (collection, field_name) = parse_collection_column(sql, " ON ")?;
    let tenant_id = identity.tenant_id;
    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &collection).vshard();

    let plan = PhysicalPlan::Vector(VectorOp::QueryStats {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        field_name: field_name.clone(),
    });

    let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    if resp.payload.is_empty() {
        return Err(ddl_err(
            "42P01",
            format!("no vector index found for \"{collection}.{field_name}\""),
        ));
    }

    let stats: nodedb_types::VectorIndexStats = zerompk::from_msgpack(&resp.payload)
        .map_err(|e| DdlError::internal(format!("decode vector stats: {e}")))?;

    let columns = vec!["property".to_string(), "value".to_string()];

    let mut pairs: Vec<(&str, String)> = vec![
        ("dimensions", stats.dimensions.to_string()),
        ("metric", stats.metric.clone()),
        ("index_type", stats.index_type.to_string()),
        ("sealed_segments", stats.sealed_count.to_string()),
        ("building_segments", stats.building_count.to_string()),
        ("growing_vectors", stats.growing_vectors.to_string()),
        ("sealed_vectors", stats.sealed_vectors.to_string()),
        ("live_count", stats.live_count.to_string()),
        ("tombstone_count", stats.tombstone_count.to_string()),
        ("tombstone_ratio", format!("{:.4}", stats.tombstone_ratio)),
        ("quantization", stats.quantization.to_string()),
        (
            "memory_mb",
            format!("{:.1}", stats.memory_bytes as f64 / (1024.0 * 1024.0)),
        ),
        (
            "disk_mb",
            format!("{:.1}", stats.disk_bytes as f64 / (1024.0 * 1024.0)),
        ),
        ("build_in_progress", stats.build_in_progress.to_string()),
        ("builds_queued", stats.builds_queued.to_string()),
        ("builds_completed", stats.builds_completed.to_string()),
        ("builds_failed", stats.builds_failed.to_string()),
        ("hnsw_m", stats.hnsw_m.to_string()),
        ("hnsw_m0", stats.hnsw_m0.to_string()),
        (
            "hnsw_ef_construction",
            stats.hnsw_ef_construction.to_string(),
        ),
        ("seal_threshold", stats.seal_threshold.to_string()),
        ("mmap_segments", stats.mmap_segment_count.to_string()),
    ];
    if let Some(ivf) = &stats.ivf {
        pairs.extend([
            ("ivf_training_threshold", ivf.training_threshold.to_string()),
            ("ivf_trained", ivf.trained.to_string()),
            ("ivf_trained_on", ivf.trained_on.to_string()),
            ("ivf_trained_at_ms", ivf.trained_at_ms.to_string()),
            ("ivf_indexed_vectors", ivf.indexed_vectors.to_string()),
            ("ivf_cells", ivf.cells.to_string()),
            ("ivf_nprobe", ivf.nprobe.to_string()),
        ]);
    }

    let rows: Vec<Map<String, JsonValue>> = pairs
        .into_iter()
        .map(|(prop, val)| {
            let mut row = Map::new();
            row.insert("property".to_string(), JsonValue::String(prop.to_string()));
            row.insert("value".to_string(), JsonValue::String(val));
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// Handle `ALTER VECTOR INDEX ON collection.column SEAL`.
pub async fn handle_alter_vector_index_seal(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (collection, field_name) = parse_collection_column(sql, " ON ")?;
    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &collection,
        "seal a vector index",
    )?;
    let tenant_id = identity.tenant_id;
    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &collection).vshard();

    let plan = PhysicalPlan::Vector(VectorOp::Seal {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        field_name,
    });

    crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    Ok(vec![DdlResult::Status {
        command: "SEAL".to_string(),
        rows_affected: None,
    }])
}

/// Handle `ALTER VECTOR INDEX ON collection.column COMPACT`.
pub async fn handle_alter_vector_index_compact(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (collection, field_name) = parse_collection_column(sql, " ON ")?;
    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &collection,
        "compact a vector index",
    )?;
    let tenant_id = identity.tenant_id;
    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &collection).vshard();

    let plan = PhysicalPlan::Vector(VectorOp::CompactIndex {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        field_name,
    });

    crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    Ok(vec![DdlResult::Status {
        command: "COMPACT".to_string(),
        rows_affected: None,
    }])
}
