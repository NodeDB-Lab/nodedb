// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staged vector-primary rows into the reads of that
//! collection: a base vector search, a sidecar scan, and a sidecar point
//! read (read-your-own-writes for a `primary='vector'` collection).
//!
//! A staged vector-primary row is a [`StagedVectorRow`]: the vector the
//! HNSW node carries at COMMIT plus the exact sidecar bytes the sparse
//! store holds afterwards. The search merge ranks the staged vector with
//! `nodedb_vector::distance::distance` under the search's metric, the same
//! function the base search scored with, so a staged hit's distance sorts
//! against base distances directly. The scan and point-read merges hand
//! the sidecar to the same converter a committed sidecar goes through.

use std::collections::HashMap;

use nodedb_types::{StorageKey, Surrogate};

use super::StagedVectorRow;
use super::merge::merge_staged_rows;
use super::vector_merge::{VectorMergeParams, payload_atom_matches, reindex_after_removal};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::hybrid_key::HybridFusionKey;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::response_codec::VectorSearchHit;
use crate::data::executor::scan_normalize::{sparse_body_to_msgpack, sparse_row_to_doc};
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::types::{DatabaseId, TenantId, TxnId};

/// The sidecar bytes of a staged vector-primary put, as the sparse store
/// holds them. A point read hands these to the sidecar converter exactly
/// like a committed row's bytes.
pub(in crate::data::executor) fn staged_vector_sidecar(body: &[u8]) -> crate::Result<Vec<u8>> {
    Ok(StagedVectorRow::from_bytes(body)?.sidecar)
}

/// The shape a scan holds its sidecar rows in when the overlay is merged.
#[derive(Clone, Copy)]
pub(in crate::data::executor) enum SidecarRowShape {
    /// Stored sidecar bytes; the caller normalizes every row afterwards.
    Stored,
    /// Standard msgpack with the row identity injected, the shape the scan
    /// fetch stage produces for every committed sidecar row.
    Normalized,
}

/// The scan-row body of a staged vector-primary put in `shape`.
fn staged_vector_scan_body(
    shape: SidecarRowShape,
    key: &StorageKey,
    body: &[u8],
) -> crate::Result<Vec<u8>> {
    let sidecar = staged_vector_sidecar(body)?;
    match shape {
        SidecarRowShape::Stored => Ok(sidecar),
        SidecarRowShape::Normalized => {
            let (_, normalized) =
                sparse_row_to_doc(key, &sidecar, SparseBodyFormatRef::VectorSidecar);
            Ok(normalized)
        }
    }
}

impl CoreLoop {
    /// Merge the overlay for `txn_id` into `rows` (base sidecar scan rows in
    /// `shape`). `matches` is the SAME predicate the base scan applied to a
    /// row of that shape.
    pub(in crate::data::executor) fn merge_vector_primary_overlay_into_scan(
        &self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        shape: SidecarRowShape,
        rows: &mut Vec<(StorageKey, Vec<u8>)>,
        matches: &dyn Fn(&StorageKey, &[u8]) -> bool,
    ) -> crate::Result<()> {
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(());
        };
        merge_staged_rows(overlay, coll_key, rows, matches, &|key, body| {
            staged_vector_scan_body(shape, key, body)
        })
    }

    /// Merge staged vector-primary rows described by `params` into `hits`
    /// (base HNSW hits), ranking each staged vector by true distance under
    /// `params.metric`, hiding staged tombstones, then re-sorting ascending
    /// and truncating to `params.top_k`.
    ///
    /// A staged put is skipped (and removed from `hits` when base returned
    /// it) when its dimensionality differs from the query, when
    /// `params.filter_bitmap` excludes its surrogate, or when its sidecar
    /// fails `params.payload_filters`. A staged body that will not decode
    /// fails the search: the merge exists so a transaction sees its own
    /// writes, and a silently dropped row defeats that.
    pub(in crate::data::executor) fn merge_vector_primary_overlay_into_search(
        &self,
        params: VectorMergeParams<'_>,
        hits: &mut Vec<VectorSearchHit>,
    ) -> crate::Result<()> {
        let VectorMergeParams {
            txn_id,
            database_id,
            tid,
            collection,
            field_name: _,
            query_vector,
            metric,
            top_k,
            filter_bitmap,
            payload_filters,
        } = params;
        let coll_key = (database_id, tid, collection.to_string());

        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        if let Some(overlay) = self.txn_overlays.get(&txn_id) {
            // A staged TRUNCATE hides every base hit; staged puts re-enter below.
            if overlay.is_truncated(&coll_key) {
                hits.clear();
            }
            let mut seen: HashMap<HybridFusionKey, usize> = hits
                .iter()
                .enumerate()
                .map(|(idx, h)| (h.id, idx))
                .collect();
            for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
                let key = HybridFusionKey::for_surrogate(Surrogate::new(surrogate));
                let Staged::Put(body) = staged else {
                    drop_hit(hits, &mut seen, &key);
                    continue;
                };
                let row = StagedVectorRow::from_bytes(body)?;
                if row.vector.len() != query_vector.len() {
                    drop_hit(hits, &mut seen, &key);
                    continue;
                }
                let normalized =
                    sparse_body_to_msgpack(&row.sidecar, SparseBodyFormatRef::VectorSidecar)
                        .into_owned();
                let passes_bitmap =
                    filter_bitmap.is_none_or(|fb| fb.contains(Surrogate::new(surrogate)));
                if !passes_bitmap || !sidecar_passes_payload_filters(&normalized, payload_filters)?
                {
                    drop_hit(hits, &mut seen, &key);
                    continue;
                }

                let dist = nodedb_vector::distance::distance(query_vector, &row.vector, metric);
                match seen.get(&key).copied() {
                    Some(idx) => {
                        hits[idx].distance = dist;
                        hits[idx].body = Some(normalized);
                    }
                    None => {
                        seen.insert(key, hits.len());
                        hits.push(VectorSearchHit {
                            id: key,
                            distance: dist,
                            doc_id: None,
                            body: Some(normalized),
                        });
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
        Ok(())
    }
}

/// Remove the hit `key` names, when base returned one, and keep `seen`'s
/// indexes aligned with the shortened `hits`.
fn drop_hit(
    hits: &mut Vec<VectorSearchHit>,
    seen: &mut HashMap<HybridFusionKey, usize>,
    key: &HybridFusionKey,
) {
    if let Some(idx) = seen.remove(key) {
        hits.remove(idx);
        reindex_after_removal(seen, idx);
    }
}

/// Whether a normalized sidecar satisfies every payload atom. Sidecar
/// field names are lower-cased at write time, so an atom's field is folded
/// the same way before lookup, matching the bitmap pre-filter.
fn sidecar_passes_payload_filters(
    normalized: &[u8],
    payload_filters: &[nodedb_types::PayloadAtom],
) -> crate::Result<bool> {
    if payload_filters.is_empty() {
        return Ok(true);
    }
    let doc: serde_json::Value =
        nodedb_types::json_from_msgpack(normalized).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("staged vector-primary sidecar decode: {e}"),
        })?;
    Ok(payload_filters
        .iter()
        .all(|atom| payload_atom_matches(atom, &|name| doc.get(name.to_ascii_lowercase()))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{PayloadAtom, Value};

    fn sidecar(owner: &str, score: i64) -> Vec<u8> {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String("r1".into()));
        fields.insert("owner".to_string(), Value::String(owner.into()));
        fields.insert("score".to_string(), Value::Integer(score));
        zerompk::to_msgpack_vec(&fields).expect("encode sidecar")
    }

    fn normalized(owner: &str, score: i64) -> Vec<u8> {
        sparse_body_to_msgpack(&sidecar(owner, score), SparseBodyFormatRef::VectorSidecar)
            .into_owned()
    }

    #[test]
    fn payload_filters_fold_field_case_and_read_sidecar_values() {
        let body = normalized("alice", 7);
        let eq = [PayloadAtom::Eq(
            "Owner".into(),
            Value::String("alice".into()),
        )];
        assert!(sidecar_passes_payload_filters(&body, &eq).expect("eval"));
        let miss = [PayloadAtom::Eq("owner".into(), Value::String("bob".into()))];
        assert!(!sidecar_passes_payload_filters(&body, &miss).expect("eval"));
        let range = [PayloadAtom::Range {
            field: "score".into(),
            low: Some(Value::Integer(5)),
            low_inclusive: true,
            high: None,
            high_inclusive: false,
        }];
        assert!(sidecar_passes_payload_filters(&body, &range).expect("eval"));
        assert!(sidecar_passes_payload_filters(&body, &[]).expect("empty filter admits"));
    }

    #[test]
    fn staged_scan_body_is_the_normalized_sidecar() {
        let stored_sidecar = sidecar("carol", 1);
        let staged = StagedVectorRow {
            vector: vec![1.0, 0.0],
            sidecar: stored_sidecar.clone(),
        }
        .to_bytes()
        .expect("encode");
        let key = StorageKey::for_surrogate(Surrogate::new(9));
        let body =
            staged_vector_scan_body(SidecarRowShape::Normalized, &key, &staged).expect("decode");
        let doc: serde_json::Value = nodedb_types::json_from_msgpack(&body).expect("json");
        assert_eq!(doc.get("owner").and_then(|v| v.as_str()), Some("carol"));
        assert_eq!(doc.get("id").and_then(|v| v.as_str()), Some("r1"));
        assert_eq!(
            staged_vector_scan_body(SidecarRowShape::Stored, &key, &staged).expect("decode"),
            stored_sidecar,
            "the stored shape is the sidecar byte-for-byte"
        );
        assert!(staged_vector_scan_body(SidecarRowShape::Normalized, &key, &[0xc1]).is_err());
    }
}
