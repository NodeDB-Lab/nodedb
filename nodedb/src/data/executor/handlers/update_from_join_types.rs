// SPDX-License-Identifier: BUSL-1.1

//! Shared types for `DocumentOp::UpdateFromJoin`: the resolved-row shape both
//! the write pass and the RESOLVE pass consume, and the operation's
//! parameters.

use nodedb_types::Surrogate;

use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec, UpdateValue};

/// One target row matched by the join, with its post-image resolved but not yet
/// written. Produced by [`crate::data::executor::core_loop::CoreLoop::collect_update_from_join_rows`]
/// and consumed by BOTH the write pass and the RESOLVE pass — the single shared
/// classifier so the two cannot diverge on which rows match or what post-image
/// each carries.
pub(in crate::data::executor) struct ResolvedUpdateRow {
    /// Target storage key (hex-encoded surrogate on a surrogate-keyed row).
    pub doc_id: String,
    /// The row's registered surrogate, parsed from `doc_id`. `None` for a
    /// legacy non-surrogate-keyed row.
    pub surrogate: Option<Surrogate>,
    /// Post-image body: strict Binary Tuple for a strict target, MessagePack
    /// for a schemaless target.
    pub body: Vec<u8>,
    /// Pre-update stored bytes (same storage-mode encoding as `body`), read
    /// before any field was mutated. Threaded through to the write pass so it
    /// can emit an `Update` `WriteEvent` carrying the row's real `old_value` —
    /// mirrors `execute_point_update` and `execute_bulk_update`, which both
    /// capture the pre-image before re-encoding.
    pub old_body: Vec<u8>,
    /// Post-image decoded to JSON (generated columns applied), reused by the
    /// write pass to build `RETURNING` rows without re-decoding `body`.
    pub doc: serde_json::Value,
}

/// Parameters for `execute_update_from_join`.
pub(in crate::data::executor) struct UpdateFromJoinParams<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub updates: &'a [(String, UpdateValue)],
    pub target_filter_bytes: &'a [u8],
    pub returning: Option<&'a ReturningSpec>,
    /// RESOLVE-ONLY read pass (Control-Plane COMMIT expander). When `true`, the
    /// handler runs the identical scan/join/assignment/encode pipeline as the
    /// write path but writes NOTHING — no `sparse.put`, no vector re-index, no
    /// write-set, no events — and returns the matched rows as msgpack
    /// `Vec<(doc_id, Option<surrogate_u32>, post_image_body)>` for the expander
    /// to rewrite into concrete `PointPut` ops. `false` = the normal write path.
    pub resolve_only: bool,
    /// Control-Plane-shipped source rows for cross-core `UPDATE ... FROM`. When
    /// `Some`, the source join-map is built from these pre-scanned
    /// `(source_doc_id, raw_stored_source_bytes)` rows instead of a local read
    /// of the source collection (whose vShard may live on a different core).
    /// `None` selects the legacy local-storage read (co-resident / in-txn
    /// buffered replay).
    pub source_rows: Option<&'a [(String, Vec<u8>)]>,
    /// Compiled RLS read policy of the TARGET collection, gating the
    /// `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Compiled RLS write policy of the TARGET collection, gating the PERSIST,
    /// decided per matched row against its post-image. A separate slot from
    /// `rls_filters`: that one bounds what may be shown back, this one bounds
    /// what may be written.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// the matched target rows may touch, resolved on the Control Plane.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// Declared `PRIMARY KEY` column of a schemaless target, `None`
    /// otherwise. `Some` makes `collect_update_from_join_rows`'s post-image
    /// guard run.
    pub declared_primary_key: Option<&'a str>,
}
