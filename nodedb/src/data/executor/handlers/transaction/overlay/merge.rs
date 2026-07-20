// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staging overlay into a base scan result, or into a
//! base secondary-index lookup's doc-ID list, so an in-transaction SCAN or
//! `WHERE indexed_field = value` observes the transaction's own uncommitted
//! point writes (read-your-own-writes).
//!
//! The base scan only reads durable rows. This step layers the per-transaction
//! overlay on top: a staged tombstone hides its base row, a staged put replaces
//! the base body (and is re-checked against the scan predicate, since an update
//! may have moved the row out of the result), and a staged put for a surrogate
//! absent from the base set is appended when it satisfies the predicate. The
//! `seen` set keeps additions from duplicating rows already present.
//!
//! The secondary-index lookup path needs the same shape of fix but a
//! different mechanism: the `INDEXES` table is never staged (only body
//! storage is), so `merge_overlay_into_index_lookup` decodes each candidate's
//! staged body and re-extracts the indexed field via the same
//! `extract_index_values` the write path uses, rather than re-checking a
//! generic predicate closure directly on the indexed term. It additionally
//! re-checks any compound-predicate residual (the WHERE conjuncts beyond the
//! indexed field) via the same schema-aware `ScanFilter` evaluator the base
//! scan overlay merge uses, so a staged row can't satisfy the indexed term
//! alone and skip the rest of the WHERE clause.
//!
//! Current-version only: temporal (`AS OF` / valid-at) scans never call this,
//! because staged bodies represent the current version alone. Staged put bodies
//! are encoded identically to base-stored bodies (same canonicalization /
//! Binary Tuple form), so the caller's decode + scan predicate + projection
//! apply to them unchanged.

use std::collections::HashSet;

use nodedb_types::Surrogate;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::filter_match::matches_with_resolved_schema;
use crate::data::executor::handlers::transaction::overlay::{Staged, StagedTtl};
use crate::engine::document::store::{extract_index_values, surrogate_to_doc_id};
use crate::engine::kv::current_ms;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Inputs for [`CoreLoop::merge_overlay_into_index_lookup`].
///
/// Bundles the lookup identity (`coll_key`, `path`, `value`) with the
/// index-write semantics needed to re-derive whether a staged body still
/// matches (`is_array`, `case_insensitive`) and the transaction to merge
/// (`txn_id`), so the merge takes one parameter rather than a long positional
/// list.
///
/// `residual` / `strict_schema` carry the compound-predicate leftover: when
/// the query is `WHERE indexed_field = value AND other_field = other_value`,
/// the planner resolves the indexed equality but ships the rest of the
/// conjunction as post-filters on the `IndexedFetch` physical op (see
/// `nodedb-sql`'s `try_document_index_lookup`). The base (committed) doc IDs
/// carry no such re-check today for this path, but a staged Put must not
/// bypass it: a row the overlay adds or keeps only because it matches the
/// indexed term, while failing the residual, must be excluded exactly like a
/// residual-failing row is excluded from a base scan
/// ([`CoreLoop::merge_overlay_into_scan`]'s `matches` contract). Empty
/// `residual` (no compound predicate, or the id-only `IndexLookup` op which
/// carries no filters at all) makes the residual check a no-op.
pub(in crate::data::executor) struct IndexOverlayMergeParams<'a> {
    pub txn_id: TxnId,
    pub coll_key: &'a (DatabaseId, TenantId, String),
    pub path: &'a str,
    pub value: &'a str,
    pub is_array: bool,
    pub case_insensitive: bool,
    pub residual: &'a [ScanFilter],
    pub strict_schema: Option<&'a StrictSchema>,
}

impl CoreLoop {
    /// Merge the overlay for `txn_id` into `rows` (base scan `(hex_row_key,
    /// body)` pairs). `matches` is the SAME predicate the base scan applied,
    /// evaluated on a stored body (Binary Tuple for strict, MessagePack for
    /// schemaless). No-op when the transaction has no overlay entries.
    pub(in crate::data::executor) fn merge_overlay_into_scan(
        &self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        rows: &mut Vec<(String, Vec<u8>)>,
        matches: &dyn Fn(&[u8]) -> bool,
    ) {
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };

        // Surrogates already represented in the base result. Additions consult
        // this to avoid re-adding a row that base already carries (or that the
        // retain pass has just superseded in place).
        let mut seen: HashSet<u32> = rows
            .iter()
            .filter_map(|(k, _)| u32::from_str_radix(k, 16).ok())
            .collect();

        // Base-minus-superseded: a single in-place pass. Drop tombstoned rows,
        // replace put-superseded bodies and re-check the predicate, keep the
        // rest untouched.
        rows.retain_mut(|(row_key, body)| {
            let Ok(surrogate) = u32::from_str_radix(row_key, 16) else {
                return true;
            };
            match overlay.get(coll_key, surrogate) {
                Some(Staged::Tombstone) => false,
                Some(Staged::Put(staged_body)) => {
                    *body = staged_body.clone();
                    matches(body)
                }
                None => true,
            }
        });

        // Overlay additions: staged puts for surrogates the base scan did not
        // return. A tombstone for a surrogate absent from base hides nothing.
        for (surrogate, staged) in overlay.iter_for_collection(coll_key) {
            if seen.contains(&surrogate) {
                continue;
            }
            match staged {
                Staged::Put(body) => {
                    if matches(body) {
                        rows.push((surrogate_to_doc_id(Surrogate::new(surrogate)), body.clone()));
                        seen.insert(surrogate);
                    }
                }
                Staged::Tombstone => {}
            }
        }
    }

    /// Fold a transaction's staging overlay into a KV scan result's `(key,
    /// value)` pairs, so an in-transaction KV `SCAN` observes the
    /// transaction's own uncommitted point writes (read-your-own-writes).
    ///
    /// Unlike [`merge_overlay_into_scan`](Self::merge_overlay_into_scan),
    /// whose row identity is the Document scan's hex-surrogate row key, a
    /// KV row's scan identity is its raw key bytes -- so this merges by
    /// [`hex_key`](super::super::stage_write::hex_key) identity instead,
    /// via [`TxnOverlay::iter_doc_entries_for_collection`] and
    /// [`unhex_key`](super::super::stage_write::unhex_key) to recover the
    /// raw key bytes for a staged addition. `matches` is the SAME predicate
    /// the base KV scan applied, evaluated on the value bytes.
    pub(in crate::data::executor) fn merge_kv_overlay_into_scan(
        &self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        rows: &mut Vec<(Vec<u8>, Vec<u8>)>,
        matches: &dyn Fn(&[u8]) -> bool,
    ) {
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };

        let mut seen: HashSet<String> = rows
            .iter()
            .map(|(key, _)| super::super::stage_write::hex_key(key))
            .collect();

        let now_ms = current_ms();
        let staged_expired = |doc_id: &str| -> bool {
            matches!(
                overlay.get_ttl_by_doc_id(coll_key, doc_id),
                Some(StagedTtl::ExpireAt(t)) if t <= now_ms
            )
        };

        rows.retain_mut(|(key, value)| {
            let doc_id = super::super::stage_write::hex_key(key);
            // A staged EXPIRE with an already-past instant hides the row from
            // an in-transaction scan -- independent of whether the row's
            // VALUE was also staged this transaction (an `Expire` on a
            // base-only row stages only the TTL delta, never a
            // `Staged::Put`).
            if staged_expired(&doc_id) {
                return false;
            }
            match overlay.get_by_doc_id(coll_key, &doc_id) {
                Some(Staged::Tombstone) => false,
                Some(Staged::Put(staged_value)) => {
                    *value = staged_value.clone();
                    matches(value)
                }
                None => true,
            }
        });

        for (doc_id, staged) in overlay.iter_doc_entries_for_collection(coll_key) {
            if seen.contains(doc_id) {
                continue;
            }
            if let Staged::Put(value) = staged
                && !staged_expired(doc_id)
                && matches(value)
                && let Some(key) = super::super::stage_write::unhex_key(doc_id)
            {
                rows.push((key, value.clone()));
                seen.insert(doc_id.to_string());
            }
        }
    }

    /// Fold a transaction's staging overlay into a base secondary-index
    /// lookup's doc-ID list, so an in-transaction `WHERE indexed_field =
    /// value` observes the transaction's own uncommitted point writes.
    ///
    /// The `INDEXES` table (plain or versioned) is never staged — only body
    /// storage is. So a staged write that changes whether a document
    /// satisfies `path == value` must be resolved by decoding the
    /// document's staged body and re-extracting `path`, using the exact
    /// same [`extract_index_values`] the index write path uses. `decode`
    /// turns stored bytes (MessagePack or, for strict collections, Binary
    /// Tuple already normalized by the caller) into a `serde_json::Value`;
    /// a body that fails to decode never matches. `case_insensitive`
    /// mirrors the index's own COLLATE NOCASE fold (values lowercased
    /// before comparison on both sides, matching the write-path convention
    /// in `execute_backfill_index` / the dual-write index maintenance).
    ///
    /// - Base-minus-superseded: a tombstoned row is dropped; a put re-checks
    ///   the staged body against `path == value` (an update may have moved
    ///   the row off the indexed value) and drops it if it no longer matches.
    /// - Overlay additions: staged puts for rows the base index lookup did
    ///   not return are appended when their staged body satisfies
    ///   `path == value` — this is what makes a staged insert or an
    ///   update-into-the-value visible.
    ///
    /// Works for both the plain-index and versioned-index (bitemporal)
    /// lookups: bitemporal staged bodies are current-version-only (same as
    /// the scan overlay merge), matching `versioned_index_lookup_as_of`'s
    /// own current-version + tombstone-aware semantics.
    ///
    /// Identity note: `DocumentEngine::index_lookup` returns each match's
    /// storage key, which is the hex surrogate (`surrogate_to_doc_id`), NOT
    /// the user-visible primary key — the secondary index stores the row's
    /// hex-surrogate storage key as its document_id component. So this path
    /// keys the overlay by surrogate exactly like `merge_overlay_into_scan`:
    /// parse each base doc_id as hex to a surrogate, consult
    /// `overlay.get(coll_key, surrogate)`, and append additions as
    /// `surrogate_to_doc_id(..)` hex — the same identity the base list and the
    /// handler's body fetch use. (The overlay's `doc_id_to_surrogate` map is
    /// keyed by the PK, so `get_by_doc_id` would never match a hex key here.)
    pub(in crate::data::executor) fn merge_overlay_into_index_lookup(
        &self,
        params: IndexOverlayMergeParams<'_>,
        doc_ids: &mut Vec<String>,
        decode: &dyn Fn(&[u8]) -> Option<serde_json::Value>,
    ) {
        let IndexOverlayMergeParams {
            txn_id,
            coll_key,
            path,
            value,
            is_array,
            case_insensitive,
            residual,
            strict_schema,
        } = params;
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };

        let normalize = |s: String| -> String {
            if case_insensitive {
                s.to_lowercase()
            } else {
                s
            }
        };
        let target = normalize(value.to_string());
        let value_matches = |body: &[u8]| -> bool {
            let Some(doc) = decode(body) else {
                return false;
            };
            extract_index_values(&doc, path, is_array)
                .into_iter()
                .any(|v| normalize(v) == target)
        };
        // The compound-predicate leftover (e.g. the `other_col = 'y'` half of
        // `indexed_col = 'x' AND other_col = 'y'`) — re-checked on the RAW
        // stored body via the same schema-aware evaluator the base scan
        // overlay merge uses, so a staged Put that satisfies the indexed term
        // but not the residual is excluded exactly like it would be from a
        // base scan result.
        let residual_matches = |body: &[u8]| -> bool {
            residual.is_empty() || matches_with_resolved_schema(strict_schema, residual, body)
        };

        // Base doc IDs are hex surrogates; track their surrogates so additions
        // don't re-append a row the base index lookup already returned.
        let mut seen: HashSet<u32> = doc_ids
            .iter()
            .filter_map(|id| u32::from_str_radix(id, 16).ok())
            .collect();

        // Base-minus-superseded: resolve each base hex doc_id to its surrogate
        // and consult the overlay. A tombstone drops it; a staged put re-checks
        // whether the new body still equals the lookup value (an update may
        // have moved the row off the indexed value); no overlay entry — or an
        // unparseable key — keeps it as-is.
        doc_ids.retain(|doc_id| {
            let Ok(surrogate) = u32::from_str_radix(doc_id, 16) else {
                return true;
            };
            match overlay.get(coll_key, surrogate) {
                Some(Staged::Tombstone) => false,
                Some(Staged::Put(body)) => value_matches(body) && residual_matches(body),
                None => true,
            }
        });

        // Overlay additions: staged puts for surrogates the base lookup did
        // not return, appended as hex `surrogate_to_doc_id(..)` when their
        // staged body matches the lookup value — this surfaces a staged insert
        // or an update-into-the-value.
        for (surrogate, staged) in overlay.iter_for_collection(coll_key) {
            if seen.contains(&surrogate) {
                continue;
            }
            match staged {
                Staged::Put(body) => {
                    if value_matches(body) && residual_matches(body) {
                        doc_ids.push(surrogate_to_doc_id(Surrogate::new(surrogate)));
                        seen.insert(surrogate);
                    }
                }
                Staged::Tombstone => {}
            }
        }
    }

    /// Resolve the current body for a hex-surrogate `doc_id` in `coll_key`,
    /// preferring the transaction's staged overlay over base storage.
    ///
    /// Used by the index-lookup handlers after [`merge_overlay_into_index_lookup`]:
    /// a row that was added by the merge (a staged insert/update) or whose
    /// base body was superseded by a staged update has no correct body in base
    /// storage — the staged `Put` bytes are the only current representation.
    /// `doc_id` is the hex-surrogate storage key the index lookup returned, so
    /// the overlay is consulted by surrogate (`get`), matching the identity
    /// the merge used — `get_by_doc_id` is keyed by the PK and would not match.
    /// Returns `None` for a staged tombstone. Falls back to the lazy `base`
    /// closure (skipped whenever the overlay already has the answer) when the
    /// key is unparseable or the surrogate has no staged mutation.
    pub(in crate::data::executor) fn overlay_or_base_body(
        &self,
        txn_id: Option<TxnId>,
        coll_key: &(DatabaseId, TenantId, String),
        doc_id: &str,
        base: impl FnOnce() -> crate::Result<Option<Vec<u8>>>,
    ) -> crate::Result<Option<Vec<u8>>> {
        if let Some(txn_id) = txn_id {
            // Read-your-own-writes refreshes the lease (see the reaper).
            self.touch_overlay(txn_id);
            if let Some(overlay) = self.txn_overlays.get(&txn_id)
                && let Ok(surrogate) = u32::from_str_radix(doc_id, 16)
            {
                match overlay.get(coll_key, surrogate) {
                    Some(Staged::Put(body)) => return Ok(Some(body.clone())),
                    Some(Staged::Tombstone) => return Ok(None),
                    None => {}
                }
            }
        }
        base()
    }

    /// Look up the declared `is_array` / `case_insensitive` modifiers for an
    /// index path, so the overlay merge folds a staged body's field values
    /// using the exact same extraction/fold the index write path applied.
    /// Defaults to `(false, false)` when the collection has no registered
    /// config or no matching index path — the merge still runs (comparing
    /// unfolded scalar values), it just cannot special-case an array field
    /// or a case-insensitive collation it doesn't know about.
    pub(in crate::data::executor) fn index_path_flags(
        &self,
        config_key: &(DatabaseId, TenantId, String),
        path: &str,
    ) -> (bool, bool) {
        self.doc_configs
            .get(config_key)
            .and_then(|config| config.index_paths.iter().find(|ip| ip.path == path))
            .map_or((false, false), |ip| (ip.is_array, ip.case_insensitive))
    }

    /// Decode a stored document body (base or staged) into JSON using the
    /// collection's registered storage mode, for overlay-merge field
    /// re-extraction. Returns `None` when the collection has no registered
    /// config or the bytes fail to decode under that mode.
    pub(in crate::data::executor) fn decode_indexed_body(
        &self,
        config_key: &(DatabaseId, TenantId, String),
        body: &[u8],
    ) -> Option<serde_json::Value> {
        let config = self.doc_configs.get(config_key)?;
        self.decode_stored_document(config, body)
    }
}
