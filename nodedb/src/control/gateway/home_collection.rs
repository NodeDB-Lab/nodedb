// SPDX-License-Identifier: BUSL-1.1

//! The collection whose vShard a plan runs on.

use nodedb_physical::physical_plan::{CrdtOp, MetaOp, PhysicalPlan};

use super::version_set::touched_collections;

/// The database-qualified collection whose vShard `plan` runs on. `None` for
/// a plan that names no collection.
///
/// The first collection the plan touches homes it. Two plans add no entry to
/// the version set, yet run on one collection's vShard:
///
/// - A constraint install or removal: its entry rides that collection's
///   data Raft log.
/// - A `REFRESH CONTINUOUS AGGREGATE` wait: the cut it waits for applies on
///   the replicas of its root source's group.
///
/// The gateway router and the node that executes a forwarded plan both home
/// a plan here, so they agree on its vShard.
pub fn home_collection(plan: &PhysicalPlan) -> Option<String> {
    if let PhysicalPlan::Crdt(
        CrdtOp::SetConstraints { collection, .. } | CrdtOp::DropConstraints { collection, .. },
    )
    | PhysicalPlan::Meta(MetaOp::AwaitContinuousAggregateRefresh { collection, .. }) = plan
    {
        return Some(collection.as_str().to_owned());
    }
    touched_collections(plan).into_iter().next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn a_constraint_install_homes_to_its_collection() {
        let set = PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection: QualifiedCollection::new(DatabaseId::new(7), "users"),
            constraint_version: 2,
            constraints: Vec::new(),
        });
        assert_eq!(home_collection(&set).as_deref(), Some("7/users"));
        let drop = PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            constraint_version: 3,
        });
        assert_eq!(home_collection(&drop).as_deref(), Some("users"));
    }

    #[test]
    fn a_refresh_wait_homes_to_its_root_source() {
        let wait = PhysicalPlan::Meta(MetaOp::AwaitContinuousAggregateRefresh {
            collection: QualifiedCollection::new(DatabaseId::new(7), "metrics"),
            name: "metrics_1m".into(),
            refresh_id: 9,
        });
        assert_eq!(home_collection(&wait).as_deref(), Some("7/metrics"));
    }

    #[test]
    fn a_data_plan_homes_to_its_first_collection() {
        let get = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sessions"),
            key: b"k".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        assert_eq!(home_collection(&get).as_deref(), Some("sessions"));
    }
}
