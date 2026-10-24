// SPDX-License-Identifier: BUSL-1.1

//! Per-vShard order fence for row writes: document rows and graph edges.
//!
//! A row write journals its rows at one of two instants:
//!
//! - before dispatch, when the plan carries the row (a point put, insert or
//!   delete): the record's LSN is minted right before the enqueue;
//! - after apply, from `Response::write_set`, when the apply decides the row
//!   (an update's post-image, a predicate's row set, a versioned row's
//!   stamp, an edge version's ordinal): the record's LSN is minted once the
//!   response arrives.
//!
//! Replay applies the records in LSN order. For any two writes that touch one
//! row, the order of their records must therefore equal the order the core
//! applied them in. The core applies a vShard's writes in enqueue order, so
//! a write's records must be minted after every record of the writes enqueued
//! before it, and before every record of the writes enqueued after it.
//!
//! A single-row write holds its row's key from before its first mint through
//! its last one: the admission guard, or the keyed order lock this module
//! takes when admission handed out none. Two writes of one row therefore
//! never interleave. A write whose rows no single key covers — a predicate,
//! batch or join write, a truncate, a balance fold, a committed transaction,
//! a point write that folds a materialized-sum target, an edge batch or a
//! cross-home edge — takes the vShard's fence exclusive over the same window.
//! Every other row write takes it shared. No row write of the vShard mints or
//! enqueues while an exclusive holder runs, and an exclusive holder waits for
//! every shared one.
//!
//! The window is held across the Data-Plane round-trip, never across the
//! group-commit fsync: the funnel waits for durability after release.

use std::sync::Arc;

use tokio::sync::{OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::calvin::scheduler::lock_manager::LockKey;
use crate::control::state::SharedState;
use crate::types::VShardId;
use nodedb_physical::physical_plan::{DocumentOp, GraphOp};

use super::lock_keys::{plan_lock_keys, plan_row_key};

/// One fence per vShard.
pub struct WriteOrderFence {
    shards: Box<[Arc<RwLock<()>>]>,
}

impl WriteOrderFence {
    /// A fence for every vShard, none held.
    pub fn new() -> Self {
        Self {
            shards: (0..VShardId::COUNT)
                .map(|_| Arc::new(RwLock::new(())))
                .collect(),
        }
    }

    fn shard(&self, vshard_id: VShardId) -> Arc<RwLock<()>> {
        let index = vshard_id.as_u32() as usize % self.shards.len();
        Arc::clone(&self.shards[index])
    }
}

impl Default for WriteOrderFence {
    fn default() -> Self {
        Self::new()
    }
}

/// The order a document write holds from before its first record through
/// its last one. The guards are held for their `Drop`, which releases the
/// order.
#[derive(Debug, Default)]
pub struct WriteOrder {
    _shared: Option<OwnedRwLockReadGuard<()>>,
    _exclusive: Option<OwnedRwLockWriteGuard<()>>,
    /// The row's keyed order lock, taken when admission held no row key.
    _row_lock: Option<OwnedMutexGuard<()>>,
}

/// Take the order `plan` needs on `vshard_id` before its first record is
/// minted.
///
/// `row_keyed` is whether admission already holds the write's row key. A
/// plan that stores no document row and no edge takes nothing.
pub(crate) async fn order_row_write(
    shared: &SharedState,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
    row_keyed: bool,
) -> WriteOrder {
    let Some(scope) = row_write_scope(plan) else {
        return WriteOrder::default();
    };
    let fence = shared.write_order_fence.shard(vshard_id);
    match scope {
        WriteScope::Row(key) => {
            let row_lock = if row_keyed {
                None
            } else {
                Some(shared.write_order_locks.lock_owned(key).await)
            };
            WriteOrder {
                _shared: Some(fence.read_owned().await),
                _exclusive: None,
                _row_lock: row_lock,
            }
        }
        WriteScope::Rows => WriteOrder {
            _shared: None,
            _exclusive: Some(fence.write_owned().await),
            _row_lock: None,
        },
    }
}

/// Which rows a row write can store.
enum WriteScope {
    /// Exactly the row this key names.
    Row(LockKey),
    /// Rows no single key covers.
    Rows,
}

/// The scope of `plan`'s row writes, `None` for a plan that stores no
/// document row and no edge.
fn row_write_scope(plan: &PhysicalPlan) -> Option<WriteScope> {
    crate::control::server::wal_dispatch::plan_post_apply_redo(plan)?;
    let single_row = match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut {
                resolved_sum_targets,
                ..
            }
            | DocumentOp::PointInsert {
                resolved_sum_targets,
                ..
            }
            | DocumentOp::PointDelete {
                resolved_sum_targets,
                ..
            }
            | DocumentOp::PointUpdate {
                resolved_sum_targets,
                ..
            }
            | DocumentOp::Upsert {
                resolved_sum_targets,
                ..
            },
        ) => resolved_sum_targets.is_empty(),
        PhysicalPlan::Graph(GraphOp::EdgePut { .. } | GraphOp::EdgeDelete { .. }) => true,
        _ => false,
    };
    // A single-home point write names its row by one key, whatever other
    // keys (row id, node pairs) its admission holds beside it.
    let key = if single_row {
        plan_lock_keys(plan).and_then(|_| plan_row_key(plan))
    } else {
        None
    };
    Some(match key {
        Some(key) => WriteScope::Row(key),
        None => WriteScope::Rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    fn point_update(resolved_sum_targets: usize) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            surrogate: Some(Surrogate::new(1)),
            pk_bytes: Vec::new(),
            updates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: (0..resolved_sum_targets)
                .map(|i| {
                    nodedb_physical::physical_plan::ResolvedSumTarget::new(
                        "totals",
                        "k",
                        Surrogate::new(100 + i as u32),
                    )
                })
                .collect(),
            declared_primary_key: None,
        })
    }

    #[test]
    fn a_single_row_write_is_scoped_to_its_row() {
        assert!(matches!(
            row_write_scope(&point_update(0)),
            Some(WriteScope::Row(_))
        ));
    }

    #[test]
    fn a_write_that_folds_a_target_row_spans_rows() {
        assert!(matches!(
            row_write_scope(&point_update(1)),
            Some(WriteScope::Rows)
        ));
    }

    #[test]
    fn an_edge_batch_spans_rows() {
        let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch {
            edges: vec![nodedb_physical::physical_plan::BatchEdge {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "knows"),
                src_id: "a".into(),
                label: "KNOWS".into(),
                dst_id: "b".into(),
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
                write_gate: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }],
        });
        assert!(matches!(row_write_scope(&plan), Some(WriteScope::Rows)));
    }

    #[test]
    fn a_predicate_write_spans_rows() {
        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(matches!(row_write_scope(&plan), Some(WriteScope::Rows)));
    }

    #[test]
    fn a_read_takes_no_order() {
        let plan = PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            field: "f".into(),
        });
        assert!(row_write_scope(&plan).is_none());
    }

    /// An exclusive holder keeps every other write of its vShard out, and a
    /// write of another vShard proceeds.
    #[tokio::test]
    async fn an_exclusive_holder_excludes_its_vshard_only() {
        let fence = WriteOrderFence::new();
        let held = fence.shard(VShardId::new(3)).write_owned().await;
        assert!(fence.shard(VShardId::new(3)).try_read_owned().is_err());
        assert!(fence.shard(VShardId::new(4)).try_read_owned().is_ok());
        drop(held);
        assert!(fence.shard(VShardId::new(3)).try_read_owned().is_ok());
    }
}
