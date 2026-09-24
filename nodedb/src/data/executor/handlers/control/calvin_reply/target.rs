// SPDX-License-Identifier: BUSL-1.1

//! What a staged Calvin plan's `RETURNING` clause reports: which
//! collection, which engine's row shape, and whether each row is its image
//! before the plan or after it.

use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, KvOp, PhysicalPlan, ReturningSpec, TimeseriesOp, VectorOp,
};

/// The row shape an engine's `RETURNING` rows take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum RowEngine {
    /// Stored document bodies in the sparse store.
    Document,
    /// CRDT rows: MessagePack bodies in the sparse store in either storage
    /// mode.
    Crdt,
    /// KV values, keyed by their raw key.
    Kv,
    /// Vector-primary payload sidecars.
    Vector,
    /// Columnar rows in schema order.
    Columnar,
    /// Timeseries rows as the raw-scan row emitter renders them.
    Timeseries,
}

/// Which image of a written row `RETURNING` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplyImage {
    /// The row as it was before the plan: a delete reports what it removed.
    Before,
    /// The row as the plan left it.
    After,
}

/// A `RETURNING` plan's reply target.
pub(super) struct ReturningTarget<'a> {
    pub spec: &'a ReturningSpec,
    pub rls_filters: &'a [u8],
    pub collection: &'a str,
    pub engine: RowEngine,
    pub image: ReplyImage,
    /// The vector field a vector-primary delete names. A removed row whose
    /// node is bound but whose sidecar is absent reports an empty sidecar.
    pub vector_field: Option<&'a str>,
}

impl<'a> ReturningTarget<'a> {
    fn new(
        spec: &'a ReturningSpec,
        rls_filters: &'a [u8],
        collection: &'a str,
        engine: RowEngine,
        image: ReplyImage,
    ) -> Self {
        Self {
            spec,
            rls_filters,
            collection,
            engine,
            image,
            vector_field: None,
        }
    }
}

/// The reply target of `plan`, or `None` when it carries no `RETURNING`.
///
/// `UpdateFromJoin` and `Merge` are never Calvin plans: the Control Plane
/// resolves them into point writes, and staging refuses one that arrives.
pub(super) fn returning_target(plan: &PhysicalPlan) -> Option<ReturningTarget<'_>> {
    use ReplyImage::{After, Before};
    match plan {
        PhysicalPlan::Document(op) => match op {
            DocumentOp::PointPut {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | DocumentOp::PointInsert {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | DocumentOp::PointUpdate {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | DocumentOp::BatchInsert {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | DocumentOp::Upsert {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | DocumentOp::BulkUpdate {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            } => Some(ReturningTarget::new(
                spec,
                rls_filters,
                collection.as_str(),
                RowEngine::Document,
                After,
            )),
            DocumentOp::PointDelete {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | DocumentOp::BulkDelete {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            } => Some(ReturningTarget::new(
                spec,
                rls_filters,
                collection.as_str(),
                RowEngine::Document,
                Before,
            )),
            _ => None,
        },
        PhysicalPlan::Kv(op) => match op {
            KvOp::Put {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | KvOp::Insert {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | KvOp::InsertIfAbsent {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | KvOp::InsertOnConflictUpdate {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | KvOp::BatchPut {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | KvOp::FieldSet {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | KvOp::PredicateUpdate {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            } => Some(ReturningTarget::new(
                spec,
                rls_filters,
                collection.as_str(),
                RowEngine::Kv,
                After,
            )),
            KvOp::Delete {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | KvOp::PredicateDelete {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            } => Some(ReturningTarget::new(
                spec,
                rls_filters,
                collection.as_str(),
                RowEngine::Kv,
                Before,
            )),
            _ => None,
        },
        PhysicalPlan::Vector(op) => match op {
            VectorOp::DirectUpsert {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | VectorOp::DirectInsert {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | VectorOp::DirectInsertIfAbsent {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            }
            | VectorOp::DirectUpdate {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            } => Some(ReturningTarget::new(
                spec,
                rls_filters,
                collection.as_str(),
                RowEngine::Vector,
                After,
            )),
            VectorOp::DirectDelete {
                collection,
                field,
                returning: Some(spec),
                rls_filters,
                ..
            } => Some(ReturningTarget {
                vector_field: Some(field.as_str()),
                ..ReturningTarget::new(
                    spec,
                    rls_filters,
                    collection.as_str(),
                    RowEngine::Vector,
                    Before,
                )
            }),
            _ => None,
        },
        PhysicalPlan::Crdt(op) => match op {
            CrdtOp::DocUpsert {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            } => Some(ReturningTarget::new(
                spec,
                rls_filters,
                collection.as_str(),
                RowEngine::Crdt,
                After,
            )),
            CrdtOp::DocDelete {
                collection,
                returning: Some(spec),
                rls_filters,
                ..
            } => Some(ReturningTarget::new(
                spec,
                rls_filters,
                collection.as_str(),
                RowEngine::Crdt,
                Before,
            )),
            _ => None,
        },
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection,
            returning: Some(spec),
            rls_filters,
            ..
        }) => Some(ReturningTarget::new(
            spec,
            rls_filters,
            collection.as_str(),
            RowEngine::Columnar,
            After,
        )),
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            returning: Some(spec),
            rls_filters,
            ..
        }) => Some(ReturningTarget::new(
            spec,
            rls_filters,
            collection.as_str(),
            RowEngine::Timeseries,
            After,
        )),
        // No other plan of these engines carries `RETURNING`.
        PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}
