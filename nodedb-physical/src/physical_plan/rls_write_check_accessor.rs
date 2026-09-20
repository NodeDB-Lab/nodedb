// SPDX-License-Identifier: BUSL-1.1

//! Exhaustive accessors for the RLS write check(s) a [`PhysicalPlan`] carries.
//!
//! RLS injection turns each `PendingInjection` into a real decision after the
//! plan is built; one still carrying `PendingInjection` at the Data Plane
//! means injection was skipped. Exhaustive so a new check field can't be
//! added without updating this match.

use nodedb_types::RlsWriteCheck;

use super::PhysicalPlan;
use super::columnar::ColumnarOp;
use super::document::DocumentOp;
use super::graph::GraphOp;
use super::kv::KvOp;
use super::timeseries::TimeseriesOp;
use super::vector::VectorOp;

impl PhysicalPlan {
    /// The single RLS write check this plan carries, or `None` if it carries
    /// no check or more than one (`KvOp::TransferItem` carries two). A safety
    /// gate needing every slot wants [`PhysicalPlan::rls_write_checks`] instead.
    pub fn sole_rls_write_check(&self) -> Option<&RlsWriteCheck> {
        match self {
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::Update {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::Delete {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::ResolveDml {
                rls_write_check, ..
            })
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                rls_write_check, ..
            })
            | PhysicalPlan::Graph(GraphOp::EdgeDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::PointDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::PointUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::Upsert {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::BulkUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::BulkDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::Merge {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::ResolvedWrite {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Delete {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Expire {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Persist {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::FieldSet {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Incr {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::IncrFloat {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Cas {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::GetSet {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Transfer {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::PredicateUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::PredicateDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::ResolvedWrite {
                rls_write_check, ..
            })
            | PhysicalPlan::Vector(VectorOp::DirectUpsert {
                rls_write_check, ..
            })
            | PhysicalPlan::Vector(VectorOp::DirectDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Vector(VectorOp::DirectUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Vector(VectorOp::ResolvedDirectWrite {
                rls_write_check, ..
            }) => Some(rls_write_check),

            // Carries two checks, not one — see `rls_write_checks`.
            PhysicalPlan::Kv(KvOp::TransferItem { .. }) => None,

            // Read-only: the wrapped op holds the live predicate instead.
            PhysicalPlan::Kv(KvOp::ResolveWrite(_)) => None,

            // Same shape on the document side.
            PhysicalPlan::Document(DocumentOp::ResolveWrite(_)) => None,

            // And on the vector-primary side.
            PhysicalPlan::Vector(VectorOp::ResolveDirectWrite(_)) => None,

            // Not write-class: no `rls_write_check` field at all.
            PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Document(_)
            | PhysicalPlan::Kv(_)
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

    /// Every RLS write check this plan carries. Most write-class ops carry
    /// one; `KvOp::TransferItem` carries two (source + dest); a non-write
    /// plan returns empty. Use over `sole_rls_write_check` whenever every
    /// slot must be checked — e.g. the un-injected-write dispatch guard.
    pub fn rls_write_checks(&self) -> Vec<&RlsWriteCheck> {
        if let PhysicalPlan::Kv(KvOp::TransferItem {
            source_rls_write_check,
            dest_rls_write_check,
            ..
        }) = self
        {
            return vec![source_rls_write_check, dest_rls_write_check];
        }
        // Every other op carries at most one check (`sole_rls_write_check`
        // is itself exhaustive over `PhysicalPlan`).
        self.sole_rls_write_check().into_iter().collect()
    }
}
