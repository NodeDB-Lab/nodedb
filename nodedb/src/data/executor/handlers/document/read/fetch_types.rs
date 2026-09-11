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
    /// valid-time instant (`AS OF SYSTEM TIME` / `AS OF VALID TIME`).
    AsOf {
        system_as_of_ms: Option<i64>,
        valid_at_ms: Option<i64>,
    },
    /// Every system-time version of every document (`AS OF SYSTEM TIME NULL`
    /// audit log), each row carrying the synthetic `_ts_*` temporal columns.
    AllVersions { valid_at_ms: Option<i64> },
}

impl DocScanMode {
    /// The current-time read is the only mode that folds this transaction's
    /// staging overlay onto the base result — temporal reads never see staged
    /// (current-version-only) writes.
    pub(in crate::data::executor) fn is_current(&self) -> bool {
        matches!(self, DocScanMode::Current)
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

/// Parse a fetched row's id back into the storage key it was minted as.
///
/// Every row a document fetch produces is keyed by a rendered surrogate, so
/// a shape that fails to parse names a fetch-pipeline bug, never a row to
/// skip.
pub(super) fn parse_fetched_key(collection: &str, id: &str) -> crate::Result<StorageKey> {
    StorageKey::parse(id).ok_or_else(|| crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!(
            "collection '{collection}' fetched a row whose id is not a valid storage key: '{id}'"
        ),
    })
}

/// Raw rows plus the schema the downstream should decode them with.
pub(in crate::data::executor) struct FetchedRows {
    pub rows: Vec<(String, Vec<u8>)>,
    pub effective_schema: Option<StrictSchema>,
    /// The statement's deadline passed while the storage scan was running, so
    /// `rows` holds an arbitrary prefix of the answer. The caller MUST fail the
    /// statement rather than emit these rows — a truncated result set is
    /// indistinguishable from a complete one at the client.
    pub deadline_expired: bool,
}
