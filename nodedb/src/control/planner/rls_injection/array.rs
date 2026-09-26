// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for array-engine and coordinator-side cluster operations.
//!
//! An RLS policy is keyed on `(tenant_id, collection)` and created by
//! `CREATE POLICY … ON <collection>`. Arrays live in their own globally-scoped
//! catalog addressed by `ArrayId` — never by a tenant-scoped collection name —
//! so no policy in the store can resolve against an array op, and neither this
//! pass nor the redaction pass has a rule to apply. The matches below stay
//! exhaustive so the day arrays gain a policy surface, every operation forces
//! a decision here.

use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp, ClusterEventOp};

use super::context::RlsCtx;

/// Exhaustive over [`ArrayOp`].
pub(super) fn inject_array(_ctx: &RlsCtx<'_>, op: &ArrayOp) -> crate::Result<()> {
    match op {
        // No-op: cell reads, addressed by `ArrayId`, which no policy can be
        // keyed on.
        ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => Ok(()),

        // No-op: cell writes, array DDL, and storage maintenance. An array is
        // addressed by `ArrayId` rather than a collection name, and RLS
        // policies are keyed on collections, so no policy — read or write — can
        // name what these touch.
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
pub(super) fn inject_cluster_array(_ctx: &RlsCtx<'_>, op: &ClusterArrayOp) -> crate::Result<()> {
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
pub(super) fn inject_cluster_event(_ctx: &RlsCtx<'_>, op: &ClusterEventOp) -> crate::Result<()> {
    match op {
        // No-op: a stream consume is addressed by `(stream, group)` and a
        // topic publish by topic name — neither names a collection this pass
        // could resolve a policy against. Access to a stream or topic is
        // authorized on the stream/topic object itself.
        ClusterEventOp::ConsumeStream { .. }
        | ClusterEventOp::PublishTopic { .. }
        | ClusterEventOp::TenantWriteMarks { .. } => Ok(()),
    }
}
