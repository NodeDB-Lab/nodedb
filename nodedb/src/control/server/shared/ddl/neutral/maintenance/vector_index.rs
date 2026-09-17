// SPDX-License-Identifier: BUSL-1.1

//! Vector index lifecycle DDL handlers.
//!
//! - `SHOW VECTOR INDEX status ON collection.column` — query live stats from Data Plane
//! - `ALTER VECTOR INDEX ON collection.column SEAL` — force-seal growing segment
//! - `ALTER VECTOR INDEX ON collection.column COMPACT` — tombstone compaction
//!
//! `ALTER VECTOR INDEX ... SET (...)` lives in [`super::vector_index_set`].
//!
//! Ported from the pgwire maintenance handlers. The `SHOW` result set is
//! all-text columns (`text_field`), so the protocol-neutral [`ShapedRows`]
//! carries `DdlColType::Text` per column and each cell as its `String` form —
//! the same bytes `DataRowEncoder::encode_field(&str)` produced. The Data Plane
//! dispatch paths (`dispatch_to_data_plane`, plan construction, ordering) are
//! preserved verbatim.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use crate::types::TraceId;
use nodedb_physical::physical_plan::VectorOp;

use super::super::super::result::{DdlError, DdlResult};
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
    let vshard = crate::types::VShardId::from_collection_in_database(database_id, &collection);

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
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    if resp.payload.is_empty() {
        return Err(ddl_err(
            "42P01",
            format!("no vector index found for \"{collection}.{field_name}\""),
        ));
    }

    let stats: nodedb_types::VectorIndexStats = zerompk::from_msgpack(&resp.payload)
        .map_err(|e| ddl_err("XX000", format!("decode vector stats: {e}")))?;

    let columns = vec!["property".to_string(), "value".to_string()];

    let pairs: Vec<(&str, String)> = vec![
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
        ("hnsw_m", stats.hnsw_m.to_string()),
        ("hnsw_m0", stats.hnsw_m0.to_string()),
        (
            "hnsw_ef_construction",
            stats.hnsw_ef_construction.to_string(),
        ),
        ("seal_threshold", stats.seal_threshold.to_string()),
        ("mmap_segments", stats.mmap_segment_count.to_string()),
    ];

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
    let tenant_id = identity.tenant_id;
    let vshard = crate::types::VShardId::from_collection_in_database(database_id, &collection);

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
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

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
    let tenant_id = identity.tenant_id;
    let vshard = crate::types::VShardId::from_collection_in_database(database_id, &collection);

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
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    Ok(vec![DdlResult::Status {
        command: "COMPACT".to_string(),
        rows_affected: None,
    }])
}

/// Parse `collection.column` or `collection` after a keyword like " ON ".
///
/// Returns `(collection, field_name)`. If no dot, field_name is empty (default field).
pub(super) fn parse_collection_column(
    sql: &str,
    keyword: &str,
) -> Result<(String, String), DdlError> {
    let pos = find_ascii_case_insensitive(sql, keyword)
        .ok_or_else(|| ddl_err("42601", format!("expected '{keyword}' in statement")))?;

    let after = sql[pos + keyword.len()..].trim();
    // Take the next token (ends at space or end of string).
    let token = after
        .split_whitespace()
        .next()
        .ok_or_else(|| ddl_err("42601", "expected collection[.column] after ON"))?
        .to_lowercase();

    if let Some((coll, col)) = token.split_once('.') {
        Ok((coll.to_string(), col.to_string()))
    } else {
        // No dot: default (unnamed) field.
        Ok((token, String::new()))
    }
}
