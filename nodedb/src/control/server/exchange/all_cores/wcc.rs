// SPDX-License-Identifier: BUSL-1.1

//! WCC contraction-round fan: dispatch across all local cores and merge the
//! per-core `WccSuperstepResult` parts by field concatenation.

use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};
use nodedb_physical::physical_plan::{PhysicalPlan, WccSuperstepResult};

use super::dispatch::NodeLevelResult;
use super::fanout::gather_every_core;
use super::read_cut::resolve_plan_cut;

/// WCC contraction-round fan: dispatch to all local cores, decode each core's
/// [`WccSuperstepResult`], merge by field concatenation, and re-encode.
///
/// Owned-node sets are disjoint across cores (each graph node is homed on
/// exactly one core via `VShardId::from_key`), so concatenation requires no
/// dedup. Cross-core edges become ordinary boundary edges (the destination is
/// owned by a sibling core) and are stitched globally by the coordinator.
pub(super) async fn fan_wcc_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    mut plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    // Every core reads the graph at the round's read cut.
    let system_as_of = resolve_plan_cut(state, &mut plan).await?;
    let responses = gather_every_core(state, tenant_id, database_id, plan, trace_id, "wcc").await?;

    let mut parts: Vec<WccSuperstepResult> = Vec::with_capacity(responses.len());
    // The highest watermark any core served the round at, for the
    // coordinator's transaction read-set.
    let mut watermark_lsn = Lsn::ZERO;
    for resp in responses {
        watermark_lsn = watermark_lsn.max(resp.watermark_lsn);
        // An empty payload decodes to WccSuperstepResult::default() (a
        // zero-vertex shard — contributes no labels or boundary edges).
        let part = if resp.payload.is_empty() {
            WccSuperstepResult::default()
        } else {
            zerompk::from_msgpack::<WccSuperstepResult>(resp.payload.as_ref()).map_err(|e| {
                crate::Error::Codec {
                    detail: format!("wcc gather: result decode: {e}"),
                }
            })?
        };
        parts.push(part);
    }

    let mut merged = merge_wcc_results(parts);
    merged.system_as_of = Some(system_as_of);
    let payload = zerompk::to_msgpack_vec(&merged).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wcc gather: merged result encode: {e}"),
    })?;

    Ok(NodeLevelResult {
        payload,
        watermark_lsn,
        read_versions: crate::types::ReadVersions::new(),
        not_found: false,
    })
}

/// Merge per-core [`WccSuperstepResult`] parts by field concatenation.
///
/// Owned-node sets are DISJOINT across cores because `gather_every_core`
/// scopes each core's `owned_vshards` to the vShards homed on that core, so each
/// graph node is owned by exactly one core. Concatenation therefore requires no
/// dedup; cross-core edges already appear as boundary edges (their destination
/// is owned by a sibling core) and are stitched globally by the coordinator.
fn merge_wcc_results(parts: Vec<WccSuperstepResult>) -> WccSuperstepResult {
    let mut out = WccSuperstepResult::default();
    for p in parts {
        out.vertex_count += p.vertex_count;
        out.node_labels.extend(p.node_labels);
        out.boundary_edges.extend(p.boundary_edges);
    }
    out
}
