// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch decisions about a document operation: whether it mutates (and
//! therefore has to clear the memory-pressure gate), and which temporal slice a
//! scan reads.
//!
//! Separate from the dispatch match itself because both are properties of the
//! op alone — no `CoreLoop` state is consulted — so they are decided once, in
//! one place, rather than re-derived at each call site.

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::SystemTimeScope;

use crate::data::executor::handlers::document::read::DocScanMode;

/// Whether the op mutates stored state.
///
/// Enumerated rather than inferred so a new mutating variant has to be named
/// here to be admitted: a write that slipped through as a read would bypass the
/// engine-pressure gate entirely.
pub(super) fn is_document_write(op: &DocumentOp) -> bool {
    matches!(
        op,
        DocumentOp::PointPut { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::PointUpdate { .. }
            | DocumentOp::PointDelete { .. }
            | DocumentOp::BatchInsert { .. }
            | DocumentOp::BulkUpdate { .. }
            | DocumentOp::BulkDelete { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::ApplyBalanceDelta { .. }
            | DocumentOp::ResolvedWrite { .. }
    )
}

/// Resolve a scan's temporal slice.
///
/// The slice differs ONLY in the fetch stage; sort, computed columns, window
/// functions and DISTINCT are applied by the same downstream pipeline for every
/// mode.
pub(super) fn doc_scan_mode(
    system_time: &SystemTimeScope,
    valid_at_ms: Option<i64>,
) -> DocScanMode {
    if system_time.is_all_versions() {
        DocScanMode::AllVersions { valid_at_ms }
    } else if system_time.is_temporal() || valid_at_ms.is_some() {
        DocScanMode::AsOf {
            system_as_of_ms: system_time.as_of_ms(),
            valid_at_ms,
        }
    } else {
        DocScanMode::Current
    }
}
