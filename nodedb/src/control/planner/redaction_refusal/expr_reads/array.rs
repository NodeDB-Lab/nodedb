// SPDX-License-Identifier: BUSL-1.1

//! Refusal of an array read under a redaction policy.
//!
//! Column redaction does not cover array attributes. A slice predicate, a
//! polygon zone, a reducer, a group dim, and an elementwise op each consume
//! stored cells on the Data Plane, and the plan names no attribute this pass
//! can check by name. A policy can still name an array: one written before the
//! array existed, or one that races its creation. So a read of such an array
//! is refused whole.

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp, PhysicalPlan};
use nodedb_types::QualifiedCollection;

use super::super::lookup::RefusalCtx;

/// Refuse `plan` when it reads an array one of the requester's roles has a
/// redaction rule on.
pub(super) fn refuse_array_reads(plan: &PhysicalPlan, ctx: &RefusalCtx<'_>) -> crate::Result<()> {
    for array in read_arrays(plan) {
        let qualified = QualifiedCollection::new(array.database_id, &array.name);
        if ctx.collection_is_redacted(qualified.as_str()) {
            return Err(ctx.refusal(format!(
                "array '{}' is named by a redaction policy for this role: reading it is not \
                 permitted, because column redaction does not cover array attributes",
                array.name
            )));
        }
    }
    Ok(())
}

/// The arrays `plan` itself reads stored cells of. Exhaustive over
/// [`PhysicalPlan`], [`ArrayOp`], and [`ClusterArrayOp`].
fn read_arrays(plan: &PhysicalPlan) -> Vec<&ArrayId> {
    match plan {
        PhysicalPlan::Array(op) => match op {
            ArrayOp::Slice { array_id, .. }
            | ArrayOp::Aggregate { array_id, .. }
            | ArrayOp::SurrogateBitmapScan { array_id, .. } => vec![array_id],
            ArrayOp::Elementwise { left, right, .. } => vec![left, right],
            ArrayOp::OpenArray { .. }
            | ArrayOp::Put { .. }
            | ArrayOp::Delete { .. }
            | ArrayOp::Flush { .. }
            | ArrayOp::Compact { .. }
            | ArrayOp::DropArray { .. }
            | ArrayOp::RekeyArray { .. }
            | ArrayOp::PurgeArrayDrop { .. } => Vec::new(),
        },
        PhysicalPlan::ClusterArray(op) => match op {
            ClusterArrayOp::Slice { array_id, .. } | ClusterArrayOp::Agg { array_id, .. } => {
                vec![array_id]
            }
            ClusterArrayOp::Elementwise { left, right, .. } => vec![left, right],
            ClusterArrayOp::Put { .. }
            | ClusterArrayOp::Delete { .. }
            | ClusterArrayOp::Flush { .. }
            | ClusterArrayOp::Compact { .. } => Vec::new(),
        },
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterEvent(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{ArrayBinaryOp, ArrayReducer};
    use nodedb_types::{DatabaseId, SystemTimeScope, TenantId};

    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };

    use super::*;

    const ROLE: &str = "support";

    fn store_with_rule(collection: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: "p".to_string(),
            tenant_id: 1,
            collection: collection.to_string(),
            display_collection: collection.to_string(),
            for_role: ROLE.to_string(),
            rules: vec![RedactionRule {
                field: "secret".to_string(),
                mode: RedactionMode::Mask("***".to_string()),
            }],
        });
        store
    }

    fn grid() -> ArrayId {
        ArrayId::new(TenantId::new(1), "grid")
    }

    fn check(plan: &PhysicalPlan, store: &RedactionStore, role: &str) -> crate::Result<()> {
        let roles = vec![role.to_string()];
        refuse_array_reads(
            plan,
            &RefusalCtx {
                store,
                tenant_id: 1,
                roles: &roles,
                database_id: DatabaseId::DEFAULT,
            },
        )
    }

    fn assert_refused(plan: &PhysicalPlan, store: &RedactionStore) {
        match check(plan, store, ROLE) {
            Err(crate::Error::RejectedAuthz { resource, .. }) => {
                assert!(resource.contains("grid"), "{resource}");
            }
            other => panic!("{plan:?}: expected the refusal, got {other:?}"),
        }
    }

    fn cluster_agg(array_id: ArrayId) -> PhysicalPlan {
        PhysicalPlan::ClusterArray(ClusterArrayOp::Agg {
            array_id,
            attr_idx: 0,
            reducer_msgpack: Vec::new(),
            group_by_dim: -1,
            slice_tile_keys: Vec::new(),
            prefix_bits: 8,
            system_time: SystemTimeScope::Current,
            valid_at_ms: None,
            region_msgpack: Vec::new(),
            physical: false,
        })
    }

    /// `ARRAY_SLICE`, `ARRAY_AGG`, and `ARRAY_ELEMENTWISE` each read the
    /// array's stored cells, on the coordinator op and the core op alike.
    #[test]
    fn every_read_of_an_array_under_a_policy_is_refused() {
        let store = store_with_rule("grid");
        let other = ArrayId::new(TenantId::new(1), "other");
        let plans = [
            cluster_agg(grid()),
            PhysicalPlan::ClusterArray(ClusterArrayOp::Slice {
                array_id: grid(),
                slice_msgpack: Vec::new(),
                attr_projection: Vec::new(),
                limit: 0,
                slice_tile_keys: Vec::new(),
                prefix_bits: 8,
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                zone_msgpack: Vec::new(),
                physical: false,
            }),
            PhysicalPlan::ClusterArray(ClusterArrayOp::Elementwise {
                left: other.clone(),
                right: grid(),
                op: ArrayBinaryOp::Add,
                attr_idx: 0,
                prefix_bits: 8,
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            PhysicalPlan::Array(ArrayOp::Aggregate {
                array_id: grid(),
                attr_idx: 0,
                reducer: ArrayReducer::Sum,
                group_by_dim: -1,
                cell_filter: None,
                return_partial: false,
                shard_bucket: None,
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                region_msgpack: Vec::new(),
                physical: false,
            }),
            PhysicalPlan::Array(ArrayOp::SurrogateBitmapScan {
                array_id: grid(),
                slice_msgpack: Vec::new(),
                system_time: SystemTimeScope::Current,
            }),
        ];
        for plan in &plans {
            assert_refused(plan, &store);
        }
    }

    /// A policy for another role, or on another array, refuses nothing.
    #[test]
    fn a_policy_that_does_not_cover_the_read_refuses_nothing() {
        let store = store_with_rule("grid");
        assert!(check(&cluster_agg(grid()), &store, "analyst").is_ok());
        let other = ArrayId::new(TenantId::new(1), "other");
        assert!(check(&cluster_agg(other), &store, ROLE).is_ok());
    }

    /// A policy keys on the database-qualified name, so the array's own
    /// database selects it.
    #[test]
    fn the_refusal_reads_the_array_database() {
        let qualified = QualifiedCollection::new(DatabaseId::new(7), "grid");
        let store = store_with_rule(qualified.as_str());
        let in_seven = ArrayId::in_database(TenantId::new(1), DatabaseId::new(7), "grid");
        assert_refused(&cluster_agg(in_seven), &store);
        assert!(check(&cluster_agg(grid()), &store, ROLE).is_ok());
    }

    /// A write carries no read of stored cells.
    #[test]
    fn a_write_is_not_a_read() {
        let store = store_with_rule("grid");
        let flush = PhysicalPlan::ClusterArray(ClusterArrayOp::Flush {
            array_id: grid(),
            prefix_bits: 8,
        });
        assert!(check(&flush, &store, ROLE).is_ok());
    }
}
