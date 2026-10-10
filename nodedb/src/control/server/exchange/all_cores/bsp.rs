// SPDX-License-Identifier: BUSL-1.1

//! BSP superstep fan: dispatch across all local cores and merge the per-core
//! `BspSuperstepResult` parts by field concatenation.

use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};
use nodedb_physical::physical_plan::{BspSuperstepResult, PhysicalPlan};

use super::dispatch::NodeLevelResult;
use super::fanout::gather_every_core;
use super::read_cut::resolve_plan_cut;

/// BSP superstep fan: dispatch to all local cores, decode each core's
/// [`BspSuperstepResult`], merge by field concatenation, and re-encode.
///
/// Owned-node sets are disjoint across cores because `gather_every_core`
/// scopes each core's `owned_vshards` to the vShards homed on that core, so each
/// graph node is owned by exactly one core; concatenation therefore requires no
/// dedup.
pub(super) async fn fan_bsp_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    mut plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    // Every core reads the graph at the run's read cut.
    let system_as_of = resolve_plan_cut(state, &mut plan).await?;
    let responses = gather_every_core(state, tenant_id, database_id, plan, trace_id, "bsp").await?;

    let mut parts: Vec<BspSuperstepResult> = Vec::with_capacity(responses.len());
    // The highest watermark any core served the superstep at, for the
    // coordinator's transaction read-set.
    let mut watermark_lsn = Lsn::ZERO;
    for resp in responses {
        watermark_lsn = watermark_lsn.max(resp.watermark_lsn);
        // An empty payload decodes to BspSuperstepResult::default() (a
        // zero-vertex shard — contributes nothing to global_n or the ranks),
        // matching decode_single_result's contract.
        let part = if resp.payload.is_empty() {
            BspSuperstepResult::default()
        } else {
            zerompk::from_msgpack::<BspSuperstepResult>(resp.payload.as_ref()).map_err(|e| {
                crate::Error::Codec {
                    detail: format!("bsp gather: result decode: {e}"),
                }
            })?
        };
        parts.push(part);
    }

    let mut merged = merge_bsp_results(parts);
    merged.system_as_of = Some(system_as_of);
    let payload = zerompk::to_msgpack_vec(&merged).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("bsp gather: merged result encode: {e}"),
    })?;

    Ok(NodeLevelResult {
        payload,
        watermark_lsn,
        read_versions: crate::types::ReadVersions::new(),
        not_found: false,
    })
}

/// Merge per-core [`BspSuperstepResult`] parts by field concatenation.
///
/// Owned-node sets are DISJOINT across cores because `fan_bsp_all_cores` scopes
/// each core's `owned_vshards` to the vShards homed on that core, so each graph
/// node is owned by exactly one core. Concatenation therefore requires no dedup.
fn merge_bsp_results(parts: Vec<BspSuperstepResult>) -> BspSuperstepResult {
    let mut out = BspSuperstepResult::default();
    for p in parts {
        out.local_delta += p.local_delta;
        out.vertex_count += p.vertex_count;
        out.outbound.extend(p.outbound);
        out.node_names.extend(p.node_names);
        out.rank_vec.extend(p.rank_vec);
        // Owned-node sets are DISJOINT across cores (each graph node is homed on
        // exactly one core), so summing per-core dangling sums counts every
        // dangling node exactly once.
        out.dangling_sum += p.dangling_sum;
        // Same disjointness for the count-phase seed-hit tally: each owned node is
        // counted on exactly one core, so per-core seed hits sum cleanly.
        out.seed_hits += p.seed_hits;
    }
    out
}
