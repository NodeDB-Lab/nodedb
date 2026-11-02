// SPDX-License-Identifier: BUSL-1.1

//! The collections and arrays a Calvin transaction's plans name for their
//! incarnation stamps.
//!
//! An array cell write and a RESTORE batch's array cells name the array by
//! its `ArrayId`, as an array of the array catalog. Every other write names
//! user collections of the collection catalog. A collection and an array can
//! share one name in one database, so each name travels with its kind.

use std::collections::BTreeSet;

use nodedb_cluster::calvin::types::IncarnationKind;
use nodedb_physical::physical_plan::{ArrayOp, MetaOp, PhysicalPlan};
use nodedb_types::QualifiedCollection;

/// One name a plan carries for its incarnation stamp, with its kind.
pub(crate) type NamedIncarnation = (String, IncarnationKind);

/// The database-qualified name of `array_id`.
fn array_name(array_id: &nodedb_array::types::ArrayId) -> String {
    QualifiedCollection::new(array_id.database_id, &array_id.name)
        .as_str()
        .to_owned()
}

/// The array whose cells `op` writes. `None` for an op that writes no cell.
fn written_array(op: &ArrayOp) -> Option<&nodedb_array::types::ArrayId> {
    match op {
        ArrayOp::Put { array_id, .. } | ArrayOp::Delete { array_id, .. } => Some(array_id),
        ArrayOp::Flush { .. }
        | ArrayOp::OpenArray { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RekeyArray { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => None,
    }
}

/// Every name `plans` carry for their incarnation stamps, sorted and
/// deduplicated. `collections_of` names the user collections of a plan that
/// writes no array.
pub(crate) fn incarnation_names(
    plans: &[PhysicalPlan],
    collections_of: impl Fn(&PhysicalPlan) -> Vec<String>,
) -> Vec<NamedIncarnation> {
    let mut named: BTreeSet<NamedIncarnation> = BTreeSet::new();
    for plan in plans {
        match plan {
            PhysicalPlan::Array(op) => {
                if let Some(array_id) = written_array(op) {
                    named.insert((array_name(array_id), IncarnationKind::Array));
                }
            }
            // A cluster-fanned array op never reaches a Calvin slice.
            PhysicalPlan::ClusterArray(_) => {}
            PhysicalPlan::Meta(MetaOp::RestoreRedo(batch)) => {
                for collection in &batch.collections {
                    named.insert((collection.clone(), IncarnationKind::Collection));
                }
                for cells in &batch.arrays {
                    named.insert((array_name(&cells.array_id), IncarnationKind::Array));
                }
            }
            PhysicalPlan::Document(_)
            | PhysicalPlan::Kv(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::ClusterEvent(_) => {
                for collection in collections_of(plan) {
                    named.insert((collection, IncarnationKind::Collection));
                }
            }
        }
    }
    named.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::ArrayId;
    use nodedb_physical::physical_plan::{RestoredArrayCells, RestoredRedo};
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;

    fn restore_batch(collections: Vec<String>, array: &str) -> PhysicalPlan {
        PhysicalPlan::Meta(MetaOp::RestoreRedo(Box::new(RestoredRedo {
            vshard: 1,
            rows_redo: Vec::new(),
            rows: Vec::new(),
            kv_keys: Vec::new(),
            edges: Vec::new(),
            node_labels: Vec::new(),
            arrays: vec![RestoredArrayCells {
                array_id: ArrayId::in_database(TenantId::new(1), DatabaseId::new(4), array),
                redo: Vec::new(),
            }],
            columnar_keys: Vec::new(),
            vector_rows: Vec::new(),
            timeseries: Vec::new(),
            collections,
            identities: Vec::new(),
            restore_cut: i64::MAX,
        })))
    }

    /// A restored array and a same-name collection keep two identities.
    #[test]
    fn a_restored_array_names_itself_apart_from_a_same_name_collection() {
        let collection = QualifiedCollection::new(DatabaseId::new(4), "grid")
            .as_str()
            .to_owned();
        let names = incarnation_names(&[restore_batch(vec![collection.clone()], "grid")], |_| {
            Vec::new()
        });
        assert_eq!(
            names,
            vec![
                (collection.clone(), IncarnationKind::Collection),
                (collection, IncarnationKind::Array),
            ]
        );
    }

    /// An array-only batch names no collection.
    #[test]
    fn an_array_batch_names_only_its_array() {
        let names = incarnation_names(&[restore_batch(Vec::new(), "grid")], |_| {
            vec!["never".to_string()]
        });
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].1, IncarnationKind::Array);
    }
}
