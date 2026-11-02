// SPDX-License-Identifier: BUSL-1.1

//! Inputs and outputs of the document scan fetch stage.

use nodedb_types::StorageKey;
use nodedb_types::columnar::schema::StrictSchema;

use crate::bridge::scan_filter::ScanFilter;

/// Which temporal slice of a document collection a scan fetches.
pub(in crate::data::executor) enum DocScanMode {
    /// Newest live version per document. Bitemporal collections read current
    /// state from the versioned store; plain collections from the live table.
    Current,
    /// Newest version per document visible at a system-time cutoff and/or a
    /// valid-time instant (`AS OF SYSTEM TIME` / `AS OF VALID TIME`). The
    /// system-time cutoff is an ordinal, the domain version keys hold.
    AsOf {
        system_as_of: Option<i64>,
        valid_at_ms: Option<i64>,
        /// True for a transaction snapshot read: the reading transaction's
        /// staged writes merge on top of the versions at `system_as_of`.
        merge_overlay: bool,
    },
    /// Every system-time version of every document (`AS OF SYSTEM TIME NULL`
    /// audit log), each row carrying the synthetic `_ts_*` temporal columns.
    AllVersions { valid_at_ms: Option<i64> },
}

impl DocScanMode {
    /// True for a current-time read.
    pub(in crate::data::executor) fn is_current(&self) -> bool {
        matches!(self, DocScanMode::Current)
    }

    /// True when the read folds this transaction's staging overlay onto the
    /// base result: a current-time read and a transaction snapshot read. A
    /// historical read never sees staged writes.
    pub(in crate::data::executor) fn merges_txn_overlay(&self) -> bool {
        match self {
            DocScanMode::Current => true,
            DocScanMode::AsOf { merge_overlay, .. } => *merge_overlay,
            DocScanMode::AllVersions { .. } => false,
        }
    }
}

/// Borrowed inputs for [`crate::data::executor::core_loop::CoreLoop::document_scan_fetch`].
pub(in crate::data::executor) struct DocFetchParams<'a> {
    pub collection: &'a str,
    pub mode: &'a DocScanMode,
    pub limit: usize,
    pub offset: usize,
    pub filter_predicates: &'a [ScanFilter],
    pub strict_schema: Option<&'a StrictSchema>,
    /// The fetch may not stop at `limit`: a downstream ORDER BY or DISTINCT
    /// decides which rows survive, so the first `limit` rows the store happens
    /// to return are not the first `limit` rows of the answer. The fetch is
    /// bounded by the memory budget instead, and the caller surfaces
    /// `ResourcesExhausted` rather than truncating silently.
    pub full_fetch: bool,
}

/// Raw rows plus the schema the downstream should decode them with.
pub(in crate::data::executor) struct FetchedRows {
    pub rows: Vec<(StorageKey, Vec<u8>)>,
    pub effective_schema: Option<StrictSchema>,
    /// The statement's deadline passed while the storage scan was running, so
    /// `rows` holds an arbitrary prefix of the answer. The caller MUST fail the
    /// statement rather than emit these rows — a truncated result set is
    /// indistinguishable from a complete one at the client.
    pub deadline_expired: bool,
}
