// SPDX-License-Identifier: BUSL-1.1

//! The user-data write predicate: the writes a tenant's write marks count.
//!
//! RESTORE's staleness guard refuses an envelope older than the tenant's
//! newest user-data write. A write-class plan that installs schema state on a
//! replica changes no row. The node proposes it on its own, for example the
//! constraint reconcile loop on every boot. Counting it would refuse a restore
//! of a backup that already holds every row the tenant wrote.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{CrdtOp, KvOp};

use super::plan_is_write::plan_is_write;

/// Whether `plan` writes a tenant's rows: a write-class plan that is not a
/// schema install.
pub fn plan_writes_user_data(plan: &PhysicalPlan) -> bool {
    plan_is_write(plan) && !installs_schema(plan)
}

/// A write-class plan that installs or removes schema state on a replica and
/// changes no row.
fn installs_schema(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Crdt(CrdtOp::SetConstraints { .. } | CrdtOp::DropConstraints { .. })
            | PhysicalPlan::Kv(
                KvOp::RegisterIndex { .. }
                    | KvOp::DropIndex { .. }
                    | KvOp::RegisterSortedIndex { .. }
                    | KvOp::DropSortedIndex { .. }
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    fn collection() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "c")
    }

    #[test]
    fn a_constraint_install_is_not_a_user_data_write() {
        let set = PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection: collection(),
            constraint_version: 1,
            constraints: Vec::new(),
        });
        let drop = PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection: collection(),
            constraint_version: 2,
        });
        assert!(plan_is_write(&set), "the install stays write-class");
        assert!(!plan_writes_user_data(&set));
        assert!(!plan_writes_user_data(&drop));
    }

    #[test]
    fn a_kv_put_is_a_user_data_write() {
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: collection(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(plan_writes_user_data(&plan));
    }
}
