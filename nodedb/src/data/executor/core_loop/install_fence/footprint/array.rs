// SPDX-License-Identifier: BUSL-1.1

//! Footprints of array ops.
//!
//! An array keys by its database and the stored name its writes go by,
//! the name an install arms. An array read reads tiles an install flushes,
//! so it keys the array too. A sync frame, a write with sync provenance,
//! is a task of the array's whole database.

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp};
use nodedb_types::sync::wire::SyncProvenance;

use super::class::{Footprint, Scope};
use super::keys::KeySet;

pub(super) fn array_footprint(op: &ArrayOp, scope: &Scope) -> Footprint {
    match op {
        ArrayOp::Slice { array_id, .. }
        | ArrayOp::Aggregate { array_id, .. }
        | ArrayOp::SurrogateBitmapScan { array_id, .. } => {
            KeySet::new(scope).array(array_id).read()
        }
        ArrayOp::Elementwise { left, right, .. } => {
            KeySet::new(scope).array(left).array(right).read()
        }
        ArrayOp::Put {
            array_id,
            provenance,
            ..
        }
        | ArrayOp::Delete {
            array_id,
            provenance,
            ..
        } => sync_write(array_id, provenance, scope),
        // Opening an array loads its state into the core.
        ArrayOp::OpenArray { array_id, .. }
        | ArrayOp::Flush { array_id, .. }
        | ArrayOp::Compact { array_id, .. }
        | ArrayOp::DropArray { array_id }
        | ArrayOp::PurgeArrayDrop { array_id } => KeySet::new(scope).array(array_id).write(),
        ArrayOp::RekeyArray { array_id, target } => {
            KeySet::new(scope).array(array_id).array(target).write()
        }
    }
}

pub(super) fn cluster_array_footprint(op: &ClusterArrayOp, scope: &Scope) -> Footprint {
    match op {
        ClusterArrayOp::Slice { array_id, .. } | ClusterArrayOp::Agg { array_id, .. } => {
            KeySet::new(scope).array(array_id).read()
        }
        ClusterArrayOp::Elementwise { left, right, .. } => {
            KeySet::new(scope).array(left).array(right).read()
        }
        ClusterArrayOp::Put { array_id, .. }
        | ClusterArrayOp::Delete { array_id, .. }
        | ClusterArrayOp::Flush { array_id, .. }
        | ClusterArrayOp::Compact { array_id, .. } => KeySet::new(scope).array(array_id).write(),
    }
}

/// A write of `array_id`, or of its whole database for a sync frame.
fn sync_write(array_id: &ArrayId, provenance: &Option<SyncProvenance>, scope: &Scope) -> Footprint {
    match provenance {
        Some(_) => scope.whole_database_of(array_id.database_id),
        None => KeySet::new(scope).array(array_id).write(),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_types::TenantId;

    use super::super::keys::tests::{key, scope, write};
    use super::super::plan::plan_footprint;
    use super::*;
    use crate::types::DatabaseId;

    fn class(plan: PhysicalPlan) -> Footprint {
        plan_footprint(&plan, &scope())
    }

    fn array(name: &str) -> ArrayId {
        ArrayId::new(TenantId::new(1), name)
    }

    #[test]
    fn an_array_put_keys_its_array() {
        let put = PhysicalPlan::Array(ArrayOp::Put {
            array_id: array("grid"),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        });
        assert_eq!(class(put), write(vec![key("grid")]));
    }

    #[test]
    fn a_rekey_keys_both_arrays() {
        let rekey = PhysicalPlan::Array(ArrayOp::RekeyArray {
            array_id: array("old"),
            target: array("new"),
        });
        assert_eq!(class(rekey), write(vec![key("old"), key("new")]));
    }

    #[test]
    fn an_array_of_another_database_keys_in_that_database() {
        let other = ArrayId::in_database(TenantId::new(1), DatabaseId::new(7), "arr");
        let drop = PhysicalPlan::Array(ArrayOp::DropArray { array_id: other });
        assert_eq!(
            class(drop),
            write(vec![
                crate::data::executor::core_loop::install_fence::FenceKey::Collection {
                    db: DatabaseId::new(7),
                    name: "7/arr".into(),
                }
            ])
        );
    }

    #[test]
    fn a_cluster_flush_keys_its_array() {
        let flush = PhysicalPlan::ClusterArray(ClusterArrayOp::Flush {
            array_id: array("grid"),
            prefix_bits: 0,
        });
        assert_eq!(class(flush), write(vec![key("grid")]));
    }
}
