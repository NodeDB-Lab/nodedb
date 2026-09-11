// SPDX-License-Identifier: BUSL-1.1

//! Storage-mode-aware `ScanFilter` predicate evaluation on a STORED row body.
//!
//! `ScanFilter::matches_binary` operates on MessagePack. Strict (Binary
//! Tuple) collections store rows in a different wire format, so a predicate
//! evaluated directly against the raw stored bytes of a strict row silently
//! never matches. Every caller that re-checks a scan predicate against a
//! stored body — the base document scan, the bulk DML base scan, and the
//! in-transaction predicate staging handlers (`stage_bulk_update`,
//! `stage_bulk_delete`) — must decode Binary Tuple bodies to MessagePack
//! first. This is the single, shared implementation of that decode-then-match
//! step so the staging handlers (and, ideally, the two base-scan call sites)
//! evaluate strict predicates identically.

use nodedb_query::EvalError;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::strict_format;
use crate::types::TenantId;

use super::CoreLoop;

/// Evaluate `filters` against a stored row `body`, given the row's ALREADY
/// RESOLVED strict schema (`Some` for a strict collection, `None` for
/// schemaless). Callers that check many rows against the same
/// collection/filters (a scan, a bulk staging match) must resolve the schema
/// ONCE outside the per-row loop via [`CoreLoop::resolve_strict_schema`] or
/// [`CoreLoop::strict_aware_matcher`] — this function does no lookup itself,
/// so it never re-pays the `doc_configs` hash lookup per row.
///
/// Returns `Ok(false)` for a strict body that fails to decode against its
/// own schema (a malformed or stale Binary Tuple never matches, mirroring
/// the base scan's behavior). Returns `Err(EvalError::DivisionByZero)` when
/// `filters` contains a `FilterOp::Expr` predicate that divides or takes a
/// modulus by zero — this is the base document scan's WHERE predicate, so
/// the behavior-flip rule applies: the query fails instead of the row being
/// silently excluded.
///
/// `doc_id` is the row's storage key. A schemaless collection with no
/// declared `id` field carries its identity only in that key, never in the
/// body, so the body is matched with `id` injected — the same injection
/// [`super::super::row_shape::sparse_row_to_doc`] applies to a materialized
/// row, so `WHERE id ...` sees the identity a reader of the same row sees. A
/// minted key injects the client-visible decimal identity, not the hex
/// storage key. A strict row already surfaces `id` as a real tuple column, so
/// no injection runs on that arm.
pub(in crate::data::executor) fn matches_with_resolved_schema(
    strict_schema: Option<&StrictSchema>,
    filters: &[ScanFilter],
    doc_id: &str,
    body: &[u8],
) -> Result<bool, EvalError> {
    match strict_schema {
        Some(schema) => match strict_format::binary_tuple_to_msgpack(body, schema) {
            Some(msgpack) => ScanFilter::all_match_binary(filters, &msgpack),
            None => Ok(false),
        },
        None => {
            // `doc_id` comes straight off a store iterator, so only a `&str`
            // is available here, not a `StorageKey`. A value that fails to
            // parse as a minted key is a legacy or user key, taken verbatim.
            let identity = crate::engine::document::store::identity_of(doc_id);
            let with_id =
                nodedb_query::msgpack_scan::inject_str_field(body, "id", identity.as_str());
            ScanFilter::all_match_binary(filters, &with_id)
        }
    }
}

impl CoreLoop {
    /// Resolve `collection`'s strict schema (if any) under tenant `tid`, once.
    /// `Some` for a strict (Binary Tuple) collection, `None` for schemaless
    /// (already MessagePack). Extracted so callers that build a per-row
    /// matcher over a scan resolve this a single time rather than per row.
    pub(in crate::data::executor) fn resolve_strict_schema(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Option<StrictSchema> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        })
    }

    /// Build a reusable `Fn(&str, &[u8]) -> Result<bool, EvalError>` closure
    /// evaluating `filters` against a stored row's `(doc_id, body)`,
    /// resolving `collection`'s strict schema ONCE up front (not per row) and
    /// capturing it in the closure. Suitable for a hot per-row scan loop
    /// directly, or as the fallible half of a Cell-wrapping call pattern —
    /// [`CoreLoop::merge_overlay_into_scan`] actually expects an
    /// *infallible* `&dyn Fn(&str, &[u8]) -> bool`, not this function's own
    /// `Result`-returning output, so callers that feed it into that merge
    /// wrap the closure this function returns in a second, infallible one
    /// that stashes any `Err` into a local `Cell<Option<EvalError>>` and
    /// checks it once the merge call returns — see
    /// `stage_bulk_delete.rs`/`stage_bulk_update.rs`'s `raw_matches` /
    /// `matches` pair for the exact pattern.
    pub(in crate::data::executor) fn strict_aware_matcher<'a>(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &'a [ScanFilter],
    ) -> impl Fn(&str, &[u8]) -> Result<bool, EvalError> + 'a {
        let strict_schema = self.resolve_strict_schema(database_id, tid, collection);
        move |doc_id: &str, body: &[u8]| {
            matches_with_resolved_schema(strict_schema.as_ref(), filters, doc_id, body)
        }
    }
}
