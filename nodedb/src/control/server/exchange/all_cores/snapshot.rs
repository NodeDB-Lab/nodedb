// SPDX-License-Identifier: BUSL-1.1

//! Tenant-snapshot fan: dispatch `CreateTenantSnapshot` across all local cores
//! and merge the per-core partial `TenantDataSnapshot` into one blob.

use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};
use nodedb_physical::physical_plan::PhysicalPlan;

use super::dispatch::NodeLevelResult;
use super::fanout::gather_graph_op_all_cores;

/// Tenant-snapshot fan: dispatch `CreateTenantSnapshot` across all local cores,
/// decode each core's partial [`TenantDataSnapshot`], merge them by field
/// concatenation, and re-encode ONE snapshot blob.
///
/// Each core scans only the engine state for the vShards homed on that core, so
/// the per-core snapshots cover DISJOINT key sets — concatenating every `Vec`
/// field requires no dedup, exactly like the BSP/WCC superstep merges. The
/// result is byte-shape-identical to the local `snapshot_self` path's single
/// `TenantDataSnapshot` blob, so backup sections from the local and remote
/// transports converge on the same `from_msgpack::<TenantDataSnapshot>` decode.
/// At 1 core/node this yields the lone core's snapshot unchanged.
pub(super) async fn fan_tenant_snapshot_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    use crate::types::TenantDataSnapshot;

    let responses = gather_graph_op_all_cores(
        state,
        tenant_id,
        database_id,
        plan,
        trace_id,
        None,
        "tenant-snapshot",
    )
    .await?;

    let mut merged = TenantDataSnapshot::default();
    let mut watermark_lsn = Lsn::ZERO;
    for resp in responses {
        if resp.watermark_lsn > watermark_lsn {
            watermark_lsn = resp.watermark_lsn;
        }
        if resp.payload.is_empty() {
            continue;
        }
        let part: TenantDataSnapshot =
            zerompk::from_msgpack(resp.payload.as_ref()).map_err(|e| crate::Error::Codec {
                detail: format!("tenant-snapshot gather: part decode: {e}"),
            })?;
        // Destructure exhaustively so a NEW field added to `TenantDataSnapshot`
        // fails to compile here rather than being silently dropped from the
        // cross-core merge. Every per-core data-bearing section MUST be
        // concatenated — a forgotten field ships an incomplete snapshot and a
        // snapshot-installed follower comes up missing that state.
        let TenantDataSnapshot {
            documents,
            indexes,
            edges,
            vectors,
            kv_tables,
            crdt_state,
            crdt_constraints,
            timeseries,
            flushed_ts_segments,
            columnar_engines,
            vector_params,
            index_configs,
            surrogate_pk,
            tenant_edges,
            group_write_marks,
            documents_versioned,
            indexes_versioned,
        } = part;
        merged.documents.extend(documents);
        merged.indexes.extend(indexes);
        merged.edges.extend(edges);
        merged.vectors.extend(vectors);
        merged.kv_tables.extend(kv_tables);
        merged.crdt_state.extend(crdt_state);
        merged.crdt_constraints.extend(crdt_constraints);
        merged.timeseries.extend(timeseries);
        merged.flushed_ts_segments.extend(flushed_ts_segments);
        merged.columnar_engines.extend(columnar_engines);
        merged.vector_params.extend(vector_params);
        merged.index_configs.extend(index_configs);
        merged.surrogate_pk.extend(surrogate_pk);
        merged.tenant_edges.extend(tenant_edges);
        merged.group_write_marks.extend(group_write_marks);
        merged.documents_versioned.extend(documents_versioned);
        merged.indexes_versioned.extend(indexes_versioned);
    }

    let payload = zerompk::to_msgpack_vec(&merged).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("tenant-snapshot gather: merged encode: {e}"),
    })?;

    Ok(NodeLevelResult {
        payload,
        watermark_lsn,
        read_version_lsn: Lsn::ZERO,
    })
}
