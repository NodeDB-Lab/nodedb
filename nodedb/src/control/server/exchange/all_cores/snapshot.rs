// SPDX-License-Identifier: BUSL-1.1

//! Tenant-snapshot fan: dispatch `CreateTenantSnapshot` across all local cores
//! and merge the per-core partial `TenantDataSnapshot` into one blob.

use std::time::Duration;

use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use super::dispatch::NodeLevelResult;
use super::fanout::gather_every_core;

/// Snapshot `tenant_id` in `database_id` on every local core and return the
/// one merged `TenantDataSnapshot` blob. `arrays` exports every array cell
/// version too.
///
/// Every local caller that snapshots a tenant goes through this function. Each
/// core stores only the collections its vShards home to, so a snapshot taken
/// on one core leaves out every collection homed on another core.
pub(crate) async fn snapshot_tenant_on_local_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    timeout: Duration,
    arrays: bool,
) -> crate::Result<Vec<u8>> {
    let plan = PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
        tenant_id: tenant_id.as_u64(),
        cut_watermark: None,
        cut_capture: None,
        arrays,
    });
    let fan =
        fan_tenant_snapshot_all_cores(state, tenant_id, database_id, plan, TraceId::generate());
    match tokio::time::timeout(timeout, fan).await {
        Ok(result) => result.map(|merged| merged.payload),
        Err(_) => Err(crate::Error::Dispatch {
            detail: format!(
                "tenant snapshot of tenant {} in database {} did not finish within {timeout:?}",
                tenant_id.as_u64(),
                database_id.as_u64()
            ),
        }),
    }
}

/// Tenant-snapshot fan: dispatch `CreateTenantSnapshot` across all local cores,
/// decode each core's partial [`TenantDataSnapshot`], merge them by field
/// concatenation, and re-encode ONE snapshot blob.
///
/// Each core scans only the engine state for the vShards homed on that core, so
/// the per-core snapshots cover disjoint key sets, except a cross-shard edge,
/// which both of its endpoint homes store ([`merge_core_snapshots`]). Every
/// core must answer: a core that fails will leave its collections out of the
/// snapshot, so its error fails the snapshot. At 1 core/node this yields the
/// lone core's snapshot unchanged.
pub(super) async fn fan_tenant_snapshot_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    let responses = gather_every_core(
        state,
        tenant_id,
        database_id,
        plan,
        trace_id,
        "tenant-snapshot",
    )
    .await?;
    merge_core_snapshots(responses)
}

/// Merge every core's partial [`TenantDataSnapshot`] of one tenant into one
/// snapshot blob. The cores cover disjoint key sets, so every field
/// concatenates, except the edges: a cross-shard edge lives on both
/// endpoint homes, so two cores can report one edge version, kept once.
pub(super) fn merge_core_snapshots(
    responses: Vec<crate::bridge::envelope::Response>,
) -> crate::Result<NodeLevelResult> {
    use crate::types::TenantDataSnapshot;

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
            edge_hidden,
            edge_cuts,
            edge_applied,
            tenant_edge_cuts,
            tenant_edge_applied,
            group_write_marks,
            group_proposal_keys,
            group_proposal_keys_complete_from,
            documents_versioned,
            indexes_versioned,
            vector_multi_documents,
            arrays,
            metadata_floor,
            group_cut_index,
            // The group snapshot builder sets the lane state and the Calvin
            // cut on the merged payload. A per-core part carries neither.
            group_event_lane: _,
            group_calvin: _,
            group_redo_streams: _,
        } = part;
        // The group snapshot builder sets the cut on the merged payload. A
        // per-core part carries none.
        merged.group_cut_index = merged.group_cut_index.max(group_cut_index);
        merged.edge_hidden.extend(edge_hidden);
        merged.edge_cuts.extend(edge_cuts);
        merged.edge_applied.extend(edge_applied);
        merged.tenant_edge_cuts.extend(tenant_edge_cuts);
        merged.tenant_edge_applied.extend(tenant_edge_applied);
        merged.documents.extend(documents);
        merged.vector_multi_documents.extend(vector_multi_documents);
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
        merged.group_proposal_keys.extend(group_proposal_keys);
        merged.group_proposal_keys_complete_from = merged
            .group_proposal_keys_complete_from
            .max(group_proposal_keys_complete_from);
        merged.documents_versioned.extend(documents_versioned);
        merged.indexes_versioned.extend(indexes_versioned);
        merged.arrays.extend(arrays);
        merged.metadata_floor = merged.metadata_floor.max(metadata_floor);
    }
    // A cross-shard edge is stored on both endpoint homes under one version
    // key. When the two homes sit on different cores, both cores report it.
    dedup_edges(&mut merged.edges, |(key, _)| key.clone());
    dedup_edges(&mut merged.tenant_edges, |(db, tid, key, _)| {
        (*db, *tid, key.clone())
    });
    dedup_edges(&mut merged.edge_hidden, |(key, _)| key.clone());
    dedup_edges(&mut merged.edge_applied, |(key, _)| key.clone());
    dedup_edges(&mut merged.tenant_edge_applied, |(db, tid, key, _)| {
        (*db, *tid, key.clone())
    });
    // Every core that holds a vShard of the collection records the cut.
    dedup_edges(&mut merged.edge_cuts, Clone::clone);
    dedup_edges(&mut merged.tenant_edge_cuts, Clone::clone);

    let payload = zerompk::to_msgpack_vec(&merged).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("tenant-snapshot gather: merged encode: {e}"),
    })?;

    Ok(NodeLevelResult {
        payload,
        watermark_lsn,
        read_versions: crate::types::ReadVersions::new(),
        not_found: false,
    })
}

/// Keep the first entry of every `identity` in `entries`, in order.
fn dedup_edges<T, K: std::hash::Hash + Eq>(entries: &mut Vec<T>, identity: impl Fn(&T) -> K) {
    let mut seen = std::collections::HashSet::new();
    entries.retain(|entry| seen.insert(identity(entry)));
}

#[cfg(test)]
mod tests {
    use super::dedup_edges;

    #[test]
    fn an_edge_both_homes_report_is_kept_once() {
        let mut edges = vec![
            ("g\0a\0L\0b\x007".to_string(), vec![1]),
            ("g\0a\0L\0c\x007".to_string(), vec![2]),
            ("g\0a\0L\0b\x007".to_string(), vec![1]),
        ];
        dedup_edges(&mut edges, |(key, _)| key.clone());
        assert_eq!(
            edges,
            vec![
                ("g\0a\0L\0b\x007".to_string(), vec![1]),
                ("g\0a\0L\0c\x007".to_string(), vec![2]),
            ]
        );
    }
}
