// SPDX-License-Identifier: BUSL-1.1

//! Parameters for a columnar base scan.

use nodedb_types::surrogate_bitmap::SurrogateBitmap;

/// Parameters for a columnar base scan. Bundled as a struct because the
/// raw parameter list exceeds the project's too-many-arguments bound.
pub(in crate::data::executor) struct ColumnarScanParams<'a> {
    pub collection: &'a str,
    pub projection: &'a [String],
    pub limit: usize,
    pub filters: &'a [u8],
    /// The caller's read policy as a MessagePack `Vec<ScanFilter>`, injected
    /// by the planner. Empty when no policy governs the caller. The scan
    /// evaluates it per row after block pruning and `filters`, before
    /// projection, sort, and limit, on the flushed-segment, live-memtable,
    /// and in-transaction overlay rows alike.
    pub rls_filters: &'a [u8],
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    /// Bitemporal system-time selection. `Current` is a current-state read;
    /// `AsOf(ms)` drops rows with `_ts_system > ms`; `AllVersions` emits every
    /// `_ts_system` row ordered ascending (audit log), with the system-time
    /// column projected.
    pub system_time: nodedb_types::SystemTimeScope,
    /// Bitemporal valid-time point: drop rows whose
    /// `[_ts_valid_from, _ts_valid_until)` interval does not contain this
    /// point. `None` skips valid-time filtering entirely.
    pub valid_at_ms: Option<i64>,
    /// Optional cross-engine surrogate prefilter. When `Some`, the scan
    /// skips whole memtable blocks whose surrogate range does not intersect
    /// the bitmap (block boundary) and skips individual rows whose surrogate
    /// is absent from the bitmap (row boundary). `None` = no prefilter.
    pub prefilter: Option<&'a SurrogateBitmap>,
    /// MessagePack-serialized `Vec<ComputedColumn>` for scalar projection
    /// expressions such as JSON arrow operators. Empty slice means no
    /// computed columns are requested.
    pub computed_columns: &'a [u8],
    /// The in-transaction identity of the caller, when the scan is issued
    /// inside `BEGIN..COMMIT`. `Some` gates a post-scan overlay merge
    /// (`merge_overlay_into_columnar_scan`) so the scan observes the
    /// transaction's own staged, not-yet-durable `ColumnarOp::Insert` rows
    /// (read-your-own-writes). `None` for autocommit reads.
    pub txn_id: Option<crate::types::TxnId>,
}
