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
/// Returns `false` for a strict body that fails to decode against its own
/// schema (a malformed or stale Binary Tuple never matches, mirroring the
/// base scan's behavior).
pub(in crate::data::executor) fn matches_with_resolved_schema(
    strict_schema: Option<&StrictSchema>,
    filters: &[ScanFilter],
    body: &[u8],
) -> bool {
    match strict_schema {
        Some(schema) => match strict_format::binary_tuple_to_msgpack(body, schema) {
            Some(msgpack) => filters.iter().all(|f| f.matches_binary(&msgpack)),
            None => false,
        },
        None => filters.iter().all(|f| f.matches_binary(body)),
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

    /// Build a reusable `Fn(&[u8]) -> bool` closure evaluating `filters`
    /// against a stored row body, resolving `collection`'s strict schema
    /// ONCE up front (not per row) and capturing it in the closure. Suitable
    /// for passing as the `&dyn Fn(&[u8]) -> bool` predicate
    /// [`CoreLoop::merge_overlay_into_scan`] expects, or for a hot per-row
    /// scan loop.
    pub(in crate::data::executor) fn strict_aware_matcher<'a>(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &'a [ScanFilter],
    ) -> impl Fn(&[u8]) -> bool + 'a {
        let strict_schema = self.resolve_strict_schema(database_id, tid, collection);
        move |body: &[u8]| matches_with_resolved_schema(strict_schema.as_ref(), filters, body)
    }
}
