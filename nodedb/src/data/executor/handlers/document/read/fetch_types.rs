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

/// Rows a current-mode fetch produced, typed by [`RowOrigin`].
pub(super) enum Fetched {
    Sparse(Vec<(StorageKey, Vec<u8>)>),
    Foreign(Vec<(String, Vec<u8>)>),
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

/// Which engine keyed the rows a fetch produced. A fetch never mixes the
/// two: the foreign fallback runs only when the sparse store holds nothing
/// for the collection, and an empty fetch always reports `Sparse`.
#[derive(Clone, Copy)]
pub(in crate::data::executor) enum RowOrigin {
    /// Sparse-store rows. Every id is a rendered storage key.
    Sparse,
    /// `scan_collection` fallback rows. A KV row is keyed by its user key and
    /// a columnar row by its `id` column, so the id is the engine's own
    /// identity text, never a storage key, and the body is already a
    /// standard msgpack map.
    Foreign,
}

/// Raw rows plus the schema the downstream should decode them with.
pub(in crate::data::executor) struct FetchedRows {
    pub rows: Vec<(String, Vec<u8>)>,
    pub origin: RowOrigin,
    pub effective_schema: Option<StrictSchema>,
    /// The statement's deadline passed while the storage scan was running, so
    /// `rows` holds an arbitrary prefix of the answer. The caller MUST fail the
    /// statement rather than emit these rows — a truncated result set is
    /// indistinguishable from a complete one at the client.
    pub deadline_expired: bool,
}
