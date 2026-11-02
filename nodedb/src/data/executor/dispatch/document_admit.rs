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

/// The engines every document write loads, in the order the pressure gate
/// checks them. Full-text indexing is a side effect of every document write.
pub(super) const DOCUMENT_WRITE_ENGINES: [nodedb_mem::EngineId; 2] = [
    nodedb_mem::EngineId::DocumentSchemaless,
    nodedb_mem::EngineId::Fts,
];

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
            | DocumentOp::BatchUpsert { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::ApplyBalanceDelta { .. }
            | DocumentOp::ResolvedWrite { .. }
    )
}

/// Whether the op adds or removes stored document versions or index entry
/// versions: every write, plus `TRUNCATE` (tombstones) and `DROP INDEX`
/// (removes the field's entries).
pub(super) fn changes_document_history(op: &DocumentOp) -> bool {
    is_document_write(op)
        || matches!(
            op,
            DocumentOp::Truncate { .. } | DocumentOp::DropIndex { .. }
        )
}

/// Resolve a scan's temporal slice.
///
/// The slice differs ONLY in the fetch stage; sort, computed columns, window
/// functions and DISTINCT are applied by the same downstream pipeline for every
/// mode. A system-time cut reads at the scope's ordinal, the domain version
/// keys hold.
pub(super) fn doc_scan_mode(
    system_time: &SystemTimeScope,
    valid_at_ms: Option<i64>,
) -> DocScanMode {
    if system_time.is_all_versions() {
        DocScanMode::AllVersions { valid_at_ms }
    } else if system_time.is_temporal() || valid_at_ms.is_some() {
        DocScanMode::AsOf {
            system_as_of: system_time.as_of_ordinal(),
            valid_at_ms,
            merge_overlay: merges_overlay(system_time, valid_at_ms),
        }
    } else {
        DocScanMode::Current
    }
}

/// True when a read at `system_time` folds the reading transaction's
/// staged writes on top of the versions it selects: a current read or a
/// transaction snapshot read, with no valid-time point. Staged bodies carry
/// no valid-time history, so a valid-time read never merges them.
pub(super) fn merges_overlay(system_time: &SystemTimeScope, valid_at_ms: Option<i64>) -> bool {
    system_time.merges_txn_overlay() && valid_at_ms.is_none()
}

/// The error a single-cut read answers `AS OF SYSTEM TIME NULL` with.
/// `reads` names the read kind, plural.
pub(super) fn all_versions_refused(reads: &str) -> crate::bridge::envelope::ErrorCode {
    crate::bridge::envelope::ErrorCode::Unsupported {
        detail: format!(
            "AS OF SYSTEM TIME NULL (all-versions) is not supported on {reads}; use a table scan"
        ),
    }
}

/// The ordinal a single-cut read reads at: `Some(None)` for the current
/// state, `Some(Some(ordinal))` for a point-in-time scope, and `None` for
/// `AllVersions`, which a single-cut read cannot answer.
pub(super) fn point_cut(system_time: &SystemTimeScope) -> Option<Option<i64>> {
    match system_time {
        SystemTimeScope::Current => Some(None),
        SystemTimeScope::AsOf(_)
        | SystemTimeScope::Pinned { .. }
        | SystemTimeScope::TxnSnapshot { .. } => Some(system_time.as_of_ordinal()),
        SystemTimeScope::AllVersions => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A millisecond cut reads at the last ordinal of its millisecond, and a
    /// pinned cut at its ordinal.
    #[test]
    fn a_cut_reads_at_its_ordinal() {
        let DocScanMode::AsOf {
            system_as_of,
            valid_at_ms,
            merge_overlay,
        } = doc_scan_mode(&SystemTimeScope::AsOf(42), None)
        else {
            panic!("an AS OF scan reads a temporal slice");
        };
        assert_eq!(system_as_of, Some(nodedb_types::ms_to_ordinal_upper(42)));
        assert_eq!(valid_at_ms, None);
        assert!(!merge_overlay);
        let pinned = SystemTimeScope::Pinned {
            ordinal: 42_000_000_007,
            cut_marker: 0,
        };
        let DocScanMode::AsOf { system_as_of, .. } = doc_scan_mode(&pinned, None) else {
            panic!("a pinned scan reads a temporal slice");
        };
        assert_eq!(system_as_of, Some(42_000_000_007));
        assert!(doc_scan_mode(&SystemTimeScope::Current, None).is_current());
    }

    /// A transaction snapshot scan reads at its ordinal and merges the
    /// transaction's staged writes. A pinned scan never merges them.
    #[test]
    fn a_txn_snapshot_scan_reads_at_its_ordinal_and_merges_staged_writes() {
        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: 42_000_000_009,
            cut_marker: 3,
        };
        let mode = doc_scan_mode(&snapshot, None);
        assert!(mode.merges_txn_overlay());
        let DocScanMode::AsOf { system_as_of, .. } = mode else {
            panic!("a snapshot scan reads at an ordinal");
        };
        assert_eq!(system_as_of, Some(42_000_000_009));
        let pinned = SystemTimeScope::Pinned {
            ordinal: 42_000_000_009,
            cut_marker: 3,
        };
        assert!(!doc_scan_mode(&pinned, None).merges_txn_overlay());
        assert!(doc_scan_mode(&SystemTimeScope::Current, None).merges_txn_overlay());
        assert!(!doc_scan_mode(&snapshot, Some(5)).merges_txn_overlay());
        assert_eq!(point_cut(&snapshot), Some(Some(42_000_000_009)));
        assert!(merges_overlay(&snapshot, None));
        assert!(!merges_overlay(&pinned, None));
    }

    #[test]
    fn a_single_cut_read_refuses_every_version() {
        assert_eq!(point_cut(&SystemTimeScope::Current), Some(None));
        assert_eq!(
            point_cut(&SystemTimeScope::Pinned {
                ordinal: 5,
                cut_marker: 1
            }),
            Some(Some(5))
        );
        assert_eq!(point_cut(&SystemTimeScope::AllVersions), None);
    }
}
