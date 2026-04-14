//! `CREATE VECTOR INDEX` DSL handler.
//!
//! Parses the full quantization-parameter surface advertised in `docs/vectors.md`:
//! INDEX_TYPE (hnsw | hnsw_pq | ivf_pq), PQ_M, IVF_CELLS, IVF_NPROBE. Unknown
//! INDEX_TYPE values are rejected at the DDL layer; invalid combinations
//! (e.g. PQ_M that does not divide DIM) are rejected before reaching the engine.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::VectorOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;

use super::helpers::{find_param_str, find_param_usize};

/// Supported INDEX_TYPE keywords — kept in sync with
/// `nodedb_vector::index_config::IndexType`.
const KNOWN_INDEX_TYPES: &[&str] = &["hnsw", "hnsw_pq", "ivf_pq"];

/// CREATE VECTOR INDEX <name> ON <collection>
///   [METRIC cosine|l2|inner_product|...] [M <m>] [EF_CONSTRUCTION <ef>] [DIM <dim>]
///   [INDEX_TYPE hnsw|hnsw_pq|ivf_pq] [PQ_M <m>] [IVF_CELLS <n>] [IVF_NPROBE <n>]
pub async fn create_vector_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE VECTOR INDEX <name> ON <collection> \
             [METRIC cosine|l2] [M <m>] [EF_CONSTRUCTION <ef>] [DIM <dim>] \
             [INDEX_TYPE hnsw|hnsw_pq|ivf_pq] [PQ_M <m>] [IVF_CELLS <n>] [IVF_NPROBE <n>]",
        ));
    }

    let index_name = parts[3];
    if !parts[4].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after index name"));
    }
    let collection = parts[5];
    let tenant_id = identity.tenant_id;

    let upper_parts: Vec<String> = parts.iter().map(|p| p.to_uppercase()).collect();

    let metric = find_param_str(&upper_parts, "METRIC").unwrap_or_else(|| "COSINE".into());
    let m = find_param_usize(&upper_parts, "M").unwrap_or(16);
    let ef_construction = find_param_usize(&upper_parts, "EF_CONSTRUCTION").unwrap_or(200);
    let dim = find_param_usize(&upper_parts, "DIM").unwrap_or(0);

    // Quantization parameters (advertised in docs/vectors.md).
    let index_type = find_param_str(&upper_parts, "INDEX_TYPE")
        .map(|s| s.to_lowercase())
        .unwrap_or_default();
    let pq_m = find_param_usize(&upper_parts, "PQ_M").unwrap_or(0);
    let ivf_cells = find_param_usize(&upper_parts, "IVF_CELLS").unwrap_or(0);
    let ivf_nprobe = find_param_usize(&upper_parts, "IVF_NPROBE").unwrap_or(0);

    validate_quantization(&index_type, dim, pq_m, ivf_cells, ivf_nprobe)?;

    super::super::owner_propose::propose_owner(
        state,
        "vector_index",
        tenant_id,
        index_name,
        &identity.username,
    )?;

    let vshard = crate::types::VShardId::from_collection(collection);
    let set_params_plan = PhysicalPlan::Vector(VectorOp::SetParams {
        collection: collection.to_string(),
        m,
        ef_construction,
        metric: metric.to_lowercase(),
        index_type: index_type.clone(),
        pq_m,
        ivf_cells,
        ivf_nprobe,
    });
    let _ = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        vshard,
        set_params_plan,
        0,
    )
    .await;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "created vector index '{index_name}' on '{collection}' \
             (metric={metric}, m={m}, ef_construction={ef_construction}, dim={dim}, \
             index_type={}, pq_m={pq_m}, ivf_cells={ivf_cells}, ivf_nprobe={ivf_nprobe})",
            if index_type.is_empty() {
                "hnsw"
            } else {
                &index_type
            }
        ),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE VECTOR INDEX"))])
}

fn validate_quantization(
    index_type: &str,
    dim: usize,
    pq_m: usize,
    ivf_cells: usize,
    ivf_nprobe: usize,
) -> PgWireResult<()> {
    if !index_type.is_empty() && !KNOWN_INDEX_TYPES.contains(&index_type) {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "unknown index_type '{index_type}'; supported: {}",
                KNOWN_INDEX_TYPES.join(", ")
            ),
        ));
    }

    let uses_pq = matches!(index_type, "hnsw_pq" | "ivf_pq");
    if uses_pq && pq_m > 0 && dim > 0 && !dim.is_multiple_of(pq_m) {
        return Err(sqlstate_error(
            "42601",
            &format!("pq_m ({pq_m}) must divide dim ({dim}) evenly"),
        ));
    }

    if !uses_pq && (pq_m > 0 || ivf_cells > 0 || ivf_nprobe > 0) {
        return Err(sqlstate_error(
            "42601",
            "pq_m / ivf_cells / ivf_nprobe require INDEX_TYPE hnsw_pq or ivf_pq",
        ));
    }

    if index_type == "ivf_pq" && ivf_nprobe > 0 && ivf_cells > 0 && ivf_nprobe > ivf_cells {
        return Err(sqlstate_error(
            "42601",
            &format!("ivf_nprobe ({ivf_nprobe}) must not exceed ivf_cells ({ivf_cells})"),
        ));
    }

    Ok(())
}
