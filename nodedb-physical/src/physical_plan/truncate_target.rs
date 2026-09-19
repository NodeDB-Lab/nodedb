// SPDX-License-Identifier: Apache-2.0

//! Which collection a whole-collection `TRUNCATE` plan clears.
//!
//! Kept beside the plan enum rather than inside it so `plan.rs` stays the
//! single declaration of the wire shape and nothing else.

use nodedb_types::QualifiedCollection;

use super::{ColumnarOp, DocumentOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp};

impl PhysicalPlan {
    /// The collection this plan truncates and its `RESTART IDENTITY` flag, or
    /// `None` for every plan that is not a whole-collection truncate. The
    /// match is exhaustive so a new truncate-shaped op forces a decision
    /// here, where sequence restart is applied engine-neutrally.
    pub fn truncate_target(&self) -> Option<(&QualifiedCollection, bool)> {
        match self {
            PhysicalPlan::Document(DocumentOp::Truncate {
                collection,
                restart_identity,
                ..
            })
            | PhysicalPlan::Kv(KvOp::Truncate {
                collection,
                restart_identity,
            })
            | PhysicalPlan::Vector(VectorOp::DirectTruncate {
                collection,
                restart_identity,
                ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::Truncate {
                collection,
                restart_identity,
            })
            | PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection,
                restart_identity,
            }) => Some((collection, *restart_identity)),
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
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::DatabaseId;

    fn coll() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "t")
    }

    #[test]
    fn kv_truncate_reports_collection_and_flag() {
        let plan = PhysicalPlan::Kv(KvOp::Truncate {
            collection: coll(),
            restart_identity: true,
        });
        let (c, restart) = plan.truncate_target().expect("kv truncate target");
        assert_eq!(c.as_str(), "t");
        assert!(restart);
    }

    #[test]
    fn vector_direct_truncate_reports_collection_and_flag() {
        let plan = PhysicalPlan::Vector(VectorOp::DirectTruncate {
            collection: coll(),
            field: "vec".into(),
            restart_identity: false,
        });
        let (c, restart) = plan.truncate_target().expect("vector truncate target");
        assert_eq!(c.as_str(), "t");
        assert!(!restart);
    }

    #[test]
    fn columnar_truncate_reports_collection_and_flag() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Truncate {
            collection: coll(),
            restart_identity: true,
        });
        let (c, restart) = plan.truncate_target().expect("columnar truncate target");
        assert_eq!(c.as_str(), "t");
        assert!(restart);
    }

    #[test]
    fn timeseries_truncate_reports_collection_and_flag() {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: coll(),
            restart_identity: false,
        });
        let (c, restart) = plan.truncate_target().expect("timeseries truncate target");
        assert_eq!(c.as_str(), "t");
        assert!(!restart);
    }

    #[test]
    fn non_truncate_plan_reports_none() {
        let plan = PhysicalPlan::Meta(super::super::MetaOp::Checkpoint);
        assert!(plan.truncate_target().is_none());
    }
}
