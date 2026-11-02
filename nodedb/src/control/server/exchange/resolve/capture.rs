// SPDX-License-Identifier: BUSL-1.1

//! Per-collection read capture for the distributed read resolvers.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::types::ReadShards;

/// One base collection's read observation from a distributed read whose result
/// is materialized on the coordinator.
///
/// Emitted by BOTH the GATHER path (`resolve::exchange` — one capture per base
/// collection under a root `Exchange{Gather}`, including both sides of a
/// gathered `HashJoin`) and the SHUFFLE path (`resolve::shuffle` /
/// `resolve::shuffle_aggregate` — one per join side). Each carries that
/// collection's own bare full-collection scan plan and the vShards the
/// gather/producers read. The record seam re-derives the collection /
/// engine / read key from `scan_plan` (a single-collection scan, so it is NOT
/// collapsed to just the left side the way a `HashJoin` plan is) and records
/// one entry per vShard in `read_vshards`, so commit validates each
/// collection on its own vShard at the snapshot ordinal.
/// This closes the serializability hole where a build-side (or non-left)
/// collection never appeared in the read-set and a concurrent write to it went
/// undetected.
pub struct DistributedReadCapture {
    pub scan_plan: PhysicalPlan,
    pub read_vshards: ReadShards,
}
