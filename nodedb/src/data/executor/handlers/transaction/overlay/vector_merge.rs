// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staging overlay into a base vector search result, so
//! an in-transaction `vector_search(...)` / kNN query observes the
//! transaction's own uncommitted document writes (read-your-own-writes for
//! Vector).
//!
//! A vector is a FIELD on a document, not a standalone stageable write: the
//! HNSW/IVF index update is an inline side effect of the document write
//! (`apply_point_put_vector_indexes`, called from
//! `handlers/point/apply_put/core.rs::apply_point_put`), the same shape as
//! FTS indexing (see `fts_merge.rs`). There is therefore no staged vector
//! insert to read at query time -- instead this merge re-reads the
//! transaction's already-staged DOCUMENT BODIES (held in [`TxnOverlay`] by
//! the ordinary document `PointInsert`/`PointPut` staging path), extracts
//! the named vector field, and re-scores it against the query vector with
//! the exact same distance function (`nodedb_vector::distance::distance`)
//! and metric the base HNSW/IVF search used, so a staged hit's distance is
//! directly comparable to base-search distances -- the merge is a plain
//! ascending sort + truncate (see `nodedb_vector::distance::distance`:
//! `InnerProduct` is pre-negated so smaller-is-better holds for every
//! metric).

use std::collections::HashMap;

use nodedb_types::{PayloadAtom, Surrogate, SurrogateBitmap, value::Value};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::response_codec::VectorSearchHit;
use crate::engine::vector::distance::DistanceMetric;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Inputs for [`CoreLoop::merge_vector_overlay_into_search`]. Bundled to
/// stay within the project's too-many-arguments bound.
pub(in crate::data::executor) struct VectorMergeParams<'a> {
    pub txn_id: TxnId,
    pub database_id: DatabaseId,
    pub tid: TenantId,
    pub collection: &'a str,
    pub field_name: &'a str,
    pub query_vector: &'a [f32],
    pub metric: DistanceMetric,
    pub top_k: usize,
    pub filter_bitmap: Option<&'a SurrogateBitmap>,
    pub payload_filters: &'a [PayloadAtom],
}

/// Extract `field` from a decoded staged document as a numeric vector.
/// Returns `None` when the field is absent or is not a JSON array of
/// numbers -- such a row is skipped rather than fabricating a fake hit.
fn extract_vector_field(doc: &serde_json::Value, field: &str) -> Option<Vec<f32>> {
    let arr = doc.get(field)?.as_array()?;
    arr.iter().map(json_number_as_f32).collect()
}

/// Coerce one array element to `f32`. Document storage round-trips a vector
/// component as a JSON number when it is integral (e.g. `0.0` -> `0`) but as a
/// JSON string when it carries a fractional part (e.g. `0.1` -> `"0.1"`), so
/// both encodings must be accepted or a staged vector silently fails to merge.
fn json_number_as_f32(v: &serde_json::Value) -> Option<f32> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
        .map(|f| f as f32)
}

/// Evaluate a single [`PayloadAtom`] directly against a staged document's
/// decoded JSON fields, mirroring the semantics `PayloadIndexSet::pre_filter`
/// applies to the durable bitmap indexes -- there is no existing evaluator
/// that runs a `PayloadAtom` against a raw JSON object (the durable path
/// only evaluates against bitmap indexes), so this is a small,
/// self-contained evaluator built from `serde_json::Value` comparisons
/// rather than a new bitmap index.
fn payload_atom_matches(atom: &PayloadAtom, doc: &serde_json::Value) -> bool {
    match atom {
        PayloadAtom::Eq(field, expected) => doc
            .get(field)
            .is_some_and(|actual| json_value_eq(actual, expected)),
        PayloadAtom::In(field, values) => {
            let Some(actual) = doc.get(field) else {
                return false;
            };
            values.iter().any(|v| json_value_eq(actual, v))
        }
        PayloadAtom::Range {
            field,
            low,
            low_inclusive,
            high,
            high_inclusive,
        } => {
            let Some(actual) = doc.get(field).and_then(serde_json::Value::as_f64) else {
                return false;
            };
            let above_low = match low {
                None => true,
                Some(low) => {
                    let low = value_as_f64(low).unwrap_or(f64::NEG_INFINITY);
                    if actual > low {
                        true
                    } else {
                        actual == low && *low_inclusive
                    }
                }
            };
            let below_high = match high {
                None => true,
                Some(high) => {
                    let high = value_as_f64(high).unwrap_or(f64::INFINITY);
                    if actual < high {
                        true
                    } else {
                        actual == high && *high_inclusive
                    }
                }
            };
            above_low && below_high
        }
        // `PayloadAtom` is `#[non_exhaustive]`: a future atom kind this
        // evaluator does not yet understand conservatively fails to match,
        // so a staged document is excluded rather than surfaced past a
        // filter it may not satisfy (a missed staged hit is safer than a
        // wrong one).
        _ => false,
    }
}

/// Compare a decoded JSON field value against a `nodedb_types::Value`
/// filter literal for equality.
fn json_value_eq(actual: &serde_json::Value, expected: &Value) -> bool {
    match expected {
        Value::Integer(i) => actual.as_i64() == Some(*i),
        Value::Float(f) => actual.as_f64() == Some(*f),
        Value::String(s) => actual.as_str() == Some(s.as_str()),
        Value::Bool(b) => actual.as_bool() == Some(*b),
        _ => false,
    }
}

/// Extract a numeric value from a `nodedb_types::Value` filter literal.
fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    }
}

/// After a `Vec::remove(idx)` shifts every later element left by one, shift
/// every recorded index greater than `idx` in `seen` to match.
fn reindex_after_removal(seen: &mut HashMap<u32, usize>, removed_idx: usize) {
    for idx in seen.values_mut() {
        if *idx > removed_idx {
            *idx -= 1;
        }
    }
}

impl CoreLoop {
    /// Merge staged document writes described by `params` into `hits` (base
    /// HNSW/IVF search hits, `id` already resolved to surrogate where
    /// possible), re-scoring staged puts that carry the queried vector field
    /// against `params.query_vector` and removing staged tombstones, then
    /// re-sorting ascending by distance (smaller = closer for every metric
    /// -- see module doc) and truncating to `params.top_k`.
    ///
    /// A staged put is skipped (contributes no entry, and is removed if
    /// already present from base) when: its body cannot be decoded under
    /// the collection's registered storage mode, it has no `field_name`
    /// field or that field is not a numeric array, its dimensionality does
    /// not match `params.query_vector`, it is excluded by
    /// `params.filter_bitmap`, or it fails `params.payload_filters`. A
    /// staged put that also appears in `hits` (from base) is re-scored and
    /// replaces the base entry's distance/body (an in-transaction re-insert
    /// should reflect the latest write).
    pub(in crate::data::executor) fn merge_vector_overlay_into_search(
        &self,
        params: VectorMergeParams<'_>,
        hits: &mut Vec<VectorSearchHit>,
    ) {
        let VectorMergeParams {
            txn_id,
            database_id,
            tid,
            collection,
            field_name,
            query_vector,
            metric,
            top_k,
            filter_bitmap,
            payload_filters,
        } = params;
        let coll_key = (database_id, tid, collection.to_string());
        let config_key = (database_id, tid, collection.to_string());

        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        if let Some(overlay) = self.txn_overlays.get(&txn_id) {
            let mut seen: HashMap<u32, usize> = hits
                .iter()
                .enumerate()
                .map(|(idx, h)| (h.id, idx))
                .collect();

            for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
                match staged {
                    Staged::Tombstone => {
                        if let Some(idx) = seen.remove(&surrogate) {
                            hits.remove(idx);
                            reindex_after_removal(&mut seen, idx);
                        }
                    }
                    Staged::Put(body) => {
                        let Some(doc) = self.decode_indexed_body(&config_key, body) else {
                            continue;
                        };
                        let Some(vector) = extract_vector_field(&doc, field_name) else {
                            continue;
                        };
                        if vector.len() != query_vector.len() {
                            continue;
                        }
                        let passes_filter_bitmap =
                            filter_bitmap.is_none_or(|fb| fb.contains(Surrogate::new(surrogate)));
                        let passes_payload = payload_filters.is_empty()
                            || payload_filters
                                .iter()
                                .all(|atom| payload_atom_matches(atom, &doc));
                        if !passes_filter_bitmap || !passes_payload {
                            if let Some(idx) = seen.remove(&surrogate) {
                                hits.remove(idx);
                                reindex_after_removal(&mut seen, idx);
                            }
                            continue;
                        }

                        let dist = nodedb_vector::distance::distance(query_vector, &vector, metric);
                        match seen.get(&surrogate).copied() {
                            Some(idx) => {
                                hits[idx].distance = dist;
                                hits[idx].body = Some(body.clone());
                            }
                            None => {
                                seen.insert(surrogate, hits.len());
                                hits.push(VectorSearchHit {
                                    id: surrogate,
                                    distance: dist,
                                    doc_id: None,
                                    body: Some(body.clone()),
                                });
                            }
                        }
                    }
                }
            }
        }

        hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        hits.truncate(top_k);
    }
}
