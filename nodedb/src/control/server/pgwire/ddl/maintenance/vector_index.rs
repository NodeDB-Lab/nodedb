//! Vector index lifecycle DDL handlers.
//!
//! - `SHOW VECTOR INDEX status ON collection.column` — query live stats from Data Plane
//! - `ALTER VECTOR INDEX ON collection.column SEAL` — force-seal growing segment
//! - `ALTER VECTOR INDEX ON collection.column COMPACT` — tombstone compaction
//! - `ALTER VECTOR INDEX ON collection.column SET (m = 32, ef_construction = 400, ...)`

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::VectorOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{sqlstate_error, text_field};

/// Handle `SHOW VECTOR INDEX status ON collection.column`.
///
/// Dispatches `VectorOp::QueryStats` to the Data Plane, awaits the response,
/// and formats the `VectorIndexStats` payload as a pgwire result set.
pub async fn handle_show_vector_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    // Parse: SHOW VECTOR INDEX status ON <collection>.<column>
    // or:   SHOW VECTOR INDEX status ON <collection>
    let (collection, field_name) = parse_collection_column(sql, " ON ")?;
    let tenant_id = identity.tenant_id;
    let vshard = crate::types::VShardId::from_collection(&collection);

    let plan = PhysicalPlan::Vector(VectorOp::QueryStats {
        collection: collection.clone(),
        field_name: field_name.clone(),
    });

    let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    if resp.payload.is_empty() {
        return Err(sqlstate_error(
            "42P01",
            &format!("no vector index found for \"{collection}.{field_name}\""),
        ));
    }

    let stats: nodedb_types::VectorIndexStats = zerompk::from_msgpack(&resp.payload)
        .map_err(|e| sqlstate_error("XX000", &format!("decode vector stats: {e}")))?;

    let schema = Arc::new(vec![text_field("property"), text_field("value")]);

    let rows: Vec<(&str, String)> = vec![
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

    let encoded_rows: Vec<_> = rows
        .into_iter()
        .map(|(prop, val)| {
            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&prop);
            let _ = encoder.encode_field(&val);
            Ok(encoder.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(encoded_rows),
    ))])
}

/// Handle `ALTER VECTOR INDEX ON collection.column SEAL`.
pub async fn handle_alter_vector_index_seal(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, field_name) = parse_collection_column(sql, " ON ")?;
    let tenant_id = identity.tenant_id;
    let vshard = crate::types::VShardId::from_collection(&collection);

    let plan = PhysicalPlan::Vector(VectorOp::Seal {
        collection,
        field_name,
    });

    crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    Ok(vec![Response::Execution(Tag::new("SEAL"))])
}

/// Handle `ALTER VECTOR INDEX ON collection.column COMPACT`.
pub async fn handle_alter_vector_index_compact(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, field_name) = parse_collection_column(sql, " ON ")?;
    let tenant_id = identity.tenant_id;
    let vshard = crate::types::VShardId::from_collection(&collection);

    let plan = PhysicalPlan::Vector(VectorOp::CompactIndex {
        collection,
        field_name,
    });

    crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    Ok(vec![Response::Execution(Tag::new("COMPACT"))])
}

/// Handle `ALTER VECTOR INDEX ON collection.column SET (...)`.
///
/// Supported keys: `m`, `m0`, `ef_construction`, `index_type`, `pq_m`,
/// `ivf_cells`, `ivf_nprobe`. Quantization-shape keys (`index_type`, `pq_m`,
/// `ivf_cells`, `ivf_nprobe`) route through `VectorOp::SetParams`, which
/// updates the stored `IndexConfig` before the collection materializes. HNSW
/// parameter keys (`m`, `m0`, `ef_construction`) route through
/// `VectorOp::Rebuild`, which performs an in-place index rebuild against the
/// already-materialized collection. A single ALTER may specify both groups —
/// they are dispatched independently. Zero / omitted fields preserve the
/// existing stored values (see `execute_set_vector_params`).
pub async fn handle_alter_vector_index_set(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, field_name) = parse_collection_column(sql, " ON ")?;

    // Parse SET (...) parameters.
    let upper = sql.to_uppercase();
    let set_pos = upper.find(" SET ").ok_or_else(|| {
        sqlstate_error(
            "42601",
            "ALTER VECTOR INDEX ... SET (...) requires SET clause",
        )
    })?;
    let params_str = &sql[set_pos + 5..];

    // Strip surrounding parens.
    let inner = params_str
        .trim()
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(params_str.trim());

    let mut m = 0usize;
    let mut m0 = 0usize;
    let mut ef_construction = 0usize;
    let mut index_type: Option<String> = None;
    let mut pq_m = 0usize;
    let mut ivf_cells = 0usize;
    let mut ivf_nprobe = 0usize;

    for pair in inner.split(',') {
        let pair = pair.trim();
        if let Some((key, val)) = pair.split_once('=') {
            let key = key.trim().to_lowercase();
            let val = val.trim().trim_matches('\'').trim_matches('"');
            match key.as_str() {
                "m" => {
                    m = val.parse().map_err(|_| {
                        sqlstate_error("22023", &format!("invalid value for m: {val}"))
                    })?;
                }
                "m0" => {
                    m0 = val.parse().map_err(|_| {
                        sqlstate_error("22023", &format!("invalid value for m0: {val}"))
                    })?;
                }
                "ef_construction" => {
                    ef_construction = val.parse().map_err(|_| {
                        sqlstate_error(
                            "22023",
                            &format!("invalid value for ef_construction: {val}"),
                        )
                    })?;
                }
                "index_type" => {
                    let lower = val.to_lowercase();
                    if !matches!(lower.as_str(), "hnsw" | "hnsw_pq" | "ivf_pq") {
                        return Err(sqlstate_error(
                            "42601",
                            &format!(
                                "unknown index_type '{val}'; supported: hnsw, hnsw_pq, ivf_pq"
                            ),
                        ));
                    }
                    index_type = Some(lower);
                }
                "pq_m" => {
                    pq_m = val.parse().map_err(|_| {
                        sqlstate_error("22023", &format!("invalid value for pq_m: {val}"))
                    })?;
                }
                "ivf_cells" => {
                    ivf_cells = val.parse().map_err(|_| {
                        sqlstate_error("22023", &format!("invalid value for ivf_cells: {val}"))
                    })?;
                }
                "ivf_nprobe" => {
                    ivf_nprobe = val.parse().map_err(|_| {
                        sqlstate_error("22023", &format!("invalid value for ivf_nprobe: {val}"))
                    })?;
                }
                other => {
                    return Err(sqlstate_error(
                        "42601",
                        &format!(
                            "unknown parameter '{other}'; supported: m, m0, ef_construction, \
                             index_type, pq_m, ivf_cells, ivf_nprobe"
                        ),
                    ));
                }
            }
        }
    }

    let has_rebuild = m > 0 || m0 > 0 || ef_construction > 0;
    let has_quantization = index_type.is_some() || pq_m > 0 || ivf_cells > 0 || ivf_nprobe > 0;

    if !has_rebuild && !has_quantization {
        return Err(sqlstate_error(
            "42601",
            "SET clause must specify at least one parameter (m, m0, ef_construction, \
             index_type, pq_m, ivf_cells, ivf_nprobe)",
        ));
    }

    // Default m0 = 2*m when m is provided but m0 is not.
    if m > 0 && m0 == 0 {
        m0 = m * 2;
    }

    let tenant_id = identity.tenant_id;
    let vshard = crate::types::VShardId::from_collection(&collection);

    // Quantization changes route through SetParams (updates stored IndexConfig
    // before the collection materializes). HNSW parameter changes route through
    // Rebuild (in-place index rebuild).
    if has_quantization {
        // Zero / empty = preserve existing stored value. The handler reads the
        // current IndexConfig and only overrides fields that were explicitly set.
        let set_plan = PhysicalPlan::Vector(VectorOp::SetParams {
            collection: collection.clone(),
            m,
            ef_construction,
            metric: String::new(),
            index_type: index_type.unwrap_or_default(),
            pq_m,
            ivf_cells,
            ivf_nprobe,
        });
        crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state, tenant_id, vshard, set_plan, 0,
        )
        .await
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    if has_rebuild {
        let plan = PhysicalPlan::Vector(VectorOp::Rebuild {
            collection,
            field_name,
            m,
            m0,
            ef_construction,
        });

        crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state, tenant_id, vshard, plan, 0,
        )
        .await
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    Ok(vec![Response::Execution(Tag::new("ALTER VECTOR INDEX"))])
}

/// Parse `collection.column` or `collection` after a keyword like " ON ".
///
/// Returns `(collection, field_name)`. If no dot, field_name is empty (default field).
fn parse_collection_column(sql: &str, keyword: &str) -> PgWireResult<(String, String)> {
    let upper = sql.to_uppercase();
    let pos = upper
        .find(keyword)
        .ok_or_else(|| sqlstate_error("42601", &format!("expected '{keyword}' in statement")))?;

    let after = sql[pos + keyword.len()..].trim();
    // Take the next token (ends at space or end of string).
    let token = after
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "expected collection[.column] after ON"))?
        .to_lowercase();

    if let Some((coll, col)) = token.split_once('.') {
        Ok((coll.to_string(), col.to_string()))
    } else {
        // No dot: default (unnamed) field.
        Ok((token, String::new()))
    }
}
