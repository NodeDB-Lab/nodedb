// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for array-engine and coordinator-side cluster
//! operations.
//!
//! A permission tree is registered per `(tenant_id, collection)` from the
//! collection's stored tree definition. Arrays live in their own
//! globally-scoped catalog addressed by `ArrayId` — never by a tenant-scoped
//! collection name — so no tree definition in the cache can resolve against an
//! array op, and neither this pass nor the RLS pass has a rule to apply. The
//! matches below stay exhaustive so the day arrays gain a permission surface,
//! every operation forces a decision here.

use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp, ClusterEventOp};

use super::context::PermCtx;

/// Exhaustive over [`ArrayOp`].
pub(super) fn apply_array(_ctx: &PermCtx<'_>, op: &ArrayOp) -> crate::Result<()> {
    match op {
        // No-op: cell reads, addressed by `ArrayId`, which no tree definition
        // can be keyed on.
        ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => Ok(()),

        // No-op: cell writes, array DDL, and storage maintenance.
        ArrayOp::OpenArray { .. }
        | ArrayOp::Put { .. }
        | ArrayOp::Delete { .. }
        | ArrayOp::Flush { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RestoreArrayDrop { .. }
        | ArrayOp::PurgeArrayDrop { .. } => Ok(()),
    }
}

/// Exhaustive over [`ClusterArrayOp`].
pub(super) fn apply_cluster_array(_ctx: &PermCtx<'_>, op: &ClusterArrayOp) -> crate::Result<()> {
    match op {
        // No-op: the coordinator fan-outs of the array reads and writes above,
        // addressed by the same `ArrayId`.
        ClusterArrayOp::Slice { .. }
        | ClusterArrayOp::Agg { .. }
        | ClusterArrayOp::Put { .. }
        | ClusterArrayOp::Delete { .. } => Ok(()),
    }
}

/// Exhaustive over [`ClusterEventOp`].
pub(super) fn apply_cluster_event(_ctx: &PermCtx<'_>, op: &ClusterEventOp) -> crate::Result<()> {
    match op {
        // No-op: a stream consume is addressed by `(stream, group)` and a
        // topic publish by topic name — neither names a collection this pass
        // could resolve a tree definition against. Access to a stream or topic
        // is authorized on the stream/topic object itself.
        ClusterEventOp::ConsumeStream { .. }
        | ClusterEventOp::PublishTopic { .. }
        | ClusterEventOp::TenantWriteMarks { .. } => Ok(()),
    }
}
