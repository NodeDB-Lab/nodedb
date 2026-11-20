// SPDX-License-Identifier: Apache-2.0

//! Every collection a set of write plans writes.
//!
//! The Control Plane names them on a transaction's redo record and on its
//! admission keys. The Data Plane names them when it opens a transaction's
//! staging for a plan. A collection a plan only reads is harmless here: a
//! write note on it changes no stored version.

use std::collections::BTreeSet;

use nodedb_array::types::ArrayId;
use nodedb_types::QualifiedCollection;

use super::{GraphOp, MetaOp, PhysicalPlan, PinnedWrite, SpatialOp, VectorOp, document_pin_needs};

/// The written collections of `plans`, sorted and deduplicated.
pub fn written_collections(plans: &[PhysicalPlan]) -> Vec<String> {
    let mut collections: BTreeSet<String> = BTreeSet::new();
    for plan in plans {
        collect_plan(plan, &mut collections);
    }
    collections.into_iter().collect()
}

fn collect_plan(plan: &PhysicalPlan, out: &mut BTreeSet<String>) {
    // Ops whose single target `PhysicalPlan::collection` does not report.
    if let PhysicalPlan::Spatial(
        SpatialOp::Insert { collection, .. } | SpatialOp::Delete { collection, .. },
    )
    | PhysicalPlan::Graph(
        GraphOp::EdgePut { collection, .. } | GraphOp::EdgeDelete { collection, .. },
    )
    | PhysicalPlan::Vector(
        VectorOp::DeleteBySurrogate { collection, .. }
        | VectorOp::SparseInsert { collection, .. }
        | VectorOp::SparseDelete { collection, .. }
        | VectorOp::MultiVectorInsert { collection, .. }
        | VectorOp::MultiVectorDelete { collection, .. },
    ) = plan
    {
        out.insert(collection.to_string());
    } else if let PhysicalPlan::Graph(
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges },
    ) = plan
    {
        out.extend(edges.iter().map(|edge| edge.collection.to_string()));
    } else if let PhysicalPlan::Meta(MetaOp::RestoreRedo(batch)) = plan {
        out.extend(batch.collections.iter().cloned());
        out.extend(batch.arrays.iter().map(|cells| array_name(&cells.array_id)));
    } else if let PhysicalPlan::Array(op) = plan {
        out.insert(array_name(op.primary_array()));
    } else if let PhysicalPlan::ClusterArray(op) = plan {
        out.insert(array_name(op.array_id()));
    } else if let Some(collection) = plan.collection() {
        out.insert(collection.to_string());
    }
}

/// The written collections of `plans` whose writes require a config pin,
/// sorted and deduplicated.
///
/// A document write requires what [`document_pin_needs`] names, the rule a
/// core's install check reads. A balance delta names its target there. A
/// RESTORE batch requires its collections. Its arrays have no config. A KV
/// write requires its collection.
pub fn pin_required_collections(plans: &[PhysicalPlan]) -> Vec<String> {
    let mut collections: BTreeSet<String> = BTreeSet::new();
    for plan in plans {
        if let PhysicalPlan::Document(op) = plan {
            collections.extend(
                document_pin_needs(op)
                    .writes
                    .into_iter()
                    .filter(|(write, _)| write.requires_pin())
                    .map(|(_, name)| name.to_owned()),
            );
        } else if let PhysicalPlan::Meta(MetaOp::RestoreRedo(batch)) = plan {
            collections.extend(batch.collections.iter().cloned());
        } else if let PhysicalPlan::Kv(op) = plan
            && PinnedWrite::KvRows.requires_pin()
            && let Some(collection) = op.collection()
        {
            collections.insert(collection.to_string());
        }
    }
    collections.into_iter().collect()
}

/// The name an array's writes go by here. An `ArrayId` holds the bare name and
/// its database, and every collection here is database-qualified. A write note
/// and a lock key both resolve this form.
fn array_name(array_id: &ArrayId) -> String {
    QualifiedCollection::new(array_id.database_id, &array_id.name)
        .as_str()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::ArrayOp;
    use nodedb_types::{DatabaseId, TenantId};

    /// A write note resolves every name here against its database, so an array
    /// outside the default database names itself qualified.
    #[test]
    fn an_array_write_names_its_database() {
        let array_id = ArrayId::in_database(TenantId::new(1), DatabaseId::new(7), "grid");
        let put = PhysicalPlan::Array(ArrayOp::Put {
            array_id: array_id.clone(),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        });
        let delete = PhysicalPlan::Array(ArrayOp::Delete {
            array_id,
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        });
        assert_eq!(
            written_collections(&[put, delete]),
            vec!["7/grid".to_string()]
        );
    }

    /// The default database stores the bare name.
    #[test]
    fn an_array_write_in_the_default_database_stays_bare() {
        let put = PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "grid"),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        });
        assert_eq!(written_collections(&[put]), vec!["grid".to_string()]);
    }

    #[test]
    fn spatial_writes_name_their_collection_once() {
        let delete = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".into(),
            surrogate: Some(nodedb_types::Surrogate::new(1)),
            provenance: None,
        });
        assert_eq!(
            written_collections(&[delete.clone(), delete]),
            vec!["places".to_string()]
        );
    }

    /// A balance delta folds into its target row, so its target requires a
    /// pin, as a core's install check reads it. A graph edge requires none.
    #[test]
    fn a_balance_delta_requires_its_target_pin() {
        let delta = PhysicalPlan::Document(crate::physical_plan::DocumentOp::ApplyBalanceDelta {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "01".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            column: "balance".into(),
            delta: "5".into(),
            join_column: "id".into(),
            join_value: "a1".into(),
            declared_primary_key: None,
        });
        let edge = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "social"),
            src_id: "alice".into(),
            label: "follows".into(),
            dst_id: "bob".into(),
            properties: Vec::new(),
            src_surrogate: nodedb_types::Surrogate::new(1),
            dst_surrogate: nodedb_types::Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
        });
        assert_eq!(
            pin_required_collections(&[delta, edge]),
            vec!["accounts".to_string()]
        );
    }
}
