// SPDX-License-Identifier: BUSL-1.1

//! Vector search parameter types and shared helper functions.
//!
//! DP emits each hit's `id` as its `HybridFusionKey`: `Bound` when the local
//! HNSW node resolves to a surrogate, `Headless` otherwise. `doc_id` is
//! always `None` from DP; the Control Plane fills it via the catalog
//! at the response boundary.

use roaring::RoaringBitmap;
use tracing::warn;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::collection::VectorCollection;
use crate::engine::vector::distance::DistanceMetric;

/// Build a search hit from raw search result data. `id` is the
/// `HybridFusionKey` the local HNSW node resolves to: `Bound` to its
/// surrogate's storage key when present, else `Headless` (so headless rows
/// still round-trip).
pub(super) fn build_search_hit(
    collection: Option<&VectorCollection>,
    local_id: u32,
    distance: f32,
) -> super::super::response_codec::VectorSearchHit {
    super::super::response_codec::VectorSearchHit {
        id: vector_leg_key(collection, local_id),
        distance,
        doc_id: None,
        body: None,
    }
}

/// The RRF-fusion key of one vector-search leg hit.
///
/// A hit whose local HNSW id resolves to a bound surrogate fuses under that
/// surrogate's storage key, the key space the text and graph legs use. A
/// headless hit (no surrogate binding) is `HybridFusionKey::Headless`: it
/// fuses with nothing, so a local id is never misread as a global surrogate.
/// Shared by the autocommit hybrid / triple handlers and the
/// transaction-overlay splice so both paths derive identical keys.
pub(super) fn vector_leg_key(
    collection: Option<&VectorCollection>,
    local_id: u32,
) -> super::hybrid_key::HybridFusionKey {
    collection
        .and_then(|c| c.get_surrogate(local_id))
        .map(super::hybrid_key::HybridFusionKey::for_surrogate)
        .unwrap_or(super::hybrid_key::HybridFusionKey::Headless(local_id))
}

/// Translate a `SurrogateBitmap` (keyed by global surrogate IDs) into a
/// `RoaringBitmap` keyed by the collection's global vector IDs.
///
/// The HNSW search layer checks candidate eligibility by testing whether
/// `(local_node_id + segment_base_id)` is present in the bitmap. Global
/// vector IDs are the collection's own monotonic counter, distinct from
/// surrogate IDs allocated by the Control Plane. Using surrogate IDs
/// directly would silently pass or reject wrong nodes.
///
/// Surrogates without a recorded local mapping (e.g. headless inserts) are
/// omitted from the result bitmap — they would never match anyway.
pub(super) fn surrogate_bitmap_to_global_ids(
    collection: &VectorCollection,
    surrogate_bm: &nodedb_types::SurrogateBitmap,
) -> RoaringBitmap {
    let mut local_bm = RoaringBitmap::new();
    for surrogate in surrogate_bm.iter() {
        if let Some(&global_id) = collection.surrogate_to_local.get(&surrogate) {
            local_bm.insert(global_id);
        }
    }
    local_bm
}

/// Encode search hits and return response.
pub(super) fn encode_hits_response(
    core: &CoreLoop,
    task: &ExecutionTask,
    hits: &Vec<super::super::response_codec::VectorSearchHit>,
) -> Response {
    match super::super::response_codec::encode(hits) {
        Ok(payload) => core.response_with_payload(task, payload),
        Err(e) => {
            warn!(core = core.core_id, error = %e, "vector search serialization failed");
            core.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            )
        }
    }
}

/// Parameters for vector search.
pub(in crate::data::executor) struct VectorSearchParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: usize,
    pub ef_search: usize,
    /// Per-query distance metric (from SQL operator). Overrides the
    /// collection-configured metric at search time.
    pub metric: DistanceMetric,
    pub filter_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    pub field_name: &'a str,
    /// RLS post-candidate filters. Applied after HNSW/IVF returns candidates.
    pub rls_filters: &'a [u8],
    /// Cross-engine prefilter sub-plan: when `Some`, executed locally and
    /// its output rows materialized into a `SurrogateBitmap` that is
    /// intersected with `filter_bitmap` before HNSW search.
    pub inline_prefilter_plan: Option<&'a crate::bridge::envelope::PhysicalPlan>,
    /// ANN tuning knobs from the SQL caller.
    pub ann_options: &'a nodedb_types::VectorAnnOptions,
    /// Projection fast-path: when `true` and RLS is inactive, skip document
    /// body fetch and return only `{id, distance}` per hit.
    pub skip_payload_fetch: bool,
    /// Payload bitmap pre-filter atoms (Eq / In / Range) for vector-primary
    /// collections. The handler ANDs all atoms and intersects the resulting
    /// bitmap with the HNSW candidate set before walking. Empty = no
    /// payload pre-filter.
    pub payload_filters: &'a [nodedb_types::PayloadAtom],
}

/// Parameters for multi-vector search (all named fields, RRF fusion).
pub(in crate::data::executor) struct VectorMultiSearchParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: usize,
    pub ef_search: usize,
    pub filter_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    /// RLS post-candidate filters (evaluated per-candidate after RRF fusion).
    pub rls_filters: &'a [u8],
}

/// Maximum allowed ef_search value. Prevents DoS via unbounded beam width.
pub(super) const MAX_EF_SEARCH: usize = 8192;

/// Compute effective ef parameter for HNSW search.
pub(super) fn effective_ef(ef_search: usize, top_k: usize) -> usize {
    if ef_search > 0 {
        ef_search.max(top_k).min(MAX_EF_SEARCH)
    } else {
        top_k.saturating_mul(4).clamp(64, MAX_EF_SEARCH)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{Surrogate, SurrogateBitmap};

    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::hnsw::HnswParams;

    use super::super::hybrid_key::HybridFusionKey;
    use super::{surrogate_bitmap_to_global_ids, vector_leg_key};

    /// Build a `VectorCollection` with `n` vectors of dimension 1.
    /// Vector `i` is `[i as f32]` and is bound to `Surrogate(i as u32 + 1)`
    /// (surrogates are 1-based to distinguish them from local IDs).
    fn make_collection_with_surrogates(n: usize) -> VectorCollection {
        let mut coll = VectorCollection::new(1, HnswParams::default());
        for i in 0..n {
            let surrogate = Surrogate(i as u32 + 1);
            coll.insert_with_surrogate(vec![i as f32], surrogate);
        }
        coll
    }

    /// Verify the oversample-based fetch_k arithmetic in isolation.
    /// oversample=3, top_k=10, no RLS → fetch_k = 30.
    /// oversample=3, top_k=10, RLS active → fetch_k = max(10*2*3, 20) = 60.
    #[test]
    fn oversample_fetch_k_arithmetic() {
        let top_k: usize = 10;

        // No RLS, oversample=3.
        let oversample: usize = 3;
        let fetch_k_no_rls = top_k.saturating_mul(oversample);
        assert_eq!(
            fetch_k_no_rls, 30,
            "no-RLS oversample=3 fetch_k should be 30"
        );

        // RLS active, oversample=3.
        let rls_active = true;
        let fetch_k_rls = if rls_active {
            top_k.saturating_mul(2).saturating_mul(oversample).max(20)
        } else {
            top_k.saturating_mul(oversample)
        };
        assert_eq!(fetch_k_rls, 60, "RLS oversample=3 fetch_k should be 60");

        // oversample=1 (default) → no change from baseline.
        let oversample_default: usize = 1;
        let fetch_k_default = top_k.saturating_mul(oversample_default);
        assert_eq!(
            fetch_k_default, 10,
            "oversample=1 fetch_k should equal top_k"
        );
    }

    #[test]
    fn surrogate_bitmap_translates_to_correct_global_ids() {
        let coll = make_collection_with_surrogates(10);

        // Allow only surrogates 1, 3, 5 (global vector IDs 0, 2, 4).
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate(1));
        bm.insert(Surrogate(3));
        bm.insert(Surrogate(5));

        let local_bm = surrogate_bitmap_to_global_ids(&coll, &bm);

        assert!(local_bm.contains(0), "Surrogate(1) → global_id 0");
        assert!(local_bm.contains(2), "Surrogate(3) → global_id 2");
        assert!(local_bm.contains(4), "Surrogate(5) → global_id 4");
        assert!(
            !local_bm.contains(1),
            "Surrogate(2) not in bitmap → global_id 1 absent"
        );
        assert!(
            !local_bm.contains(3),
            "Surrogate(4) not in bitmap → global_id 3 absent"
        );
        assert_eq!(local_bm.len(), 3);
    }

    #[test]
    fn non_member_surrogates_never_appear_in_search_results() {
        // Insert 20 vectors, bind each to a unique surrogate.
        let coll = make_collection_with_surrogates(20);

        // Permit only even surrogates: Surrogate(2), Surrogate(4), ..., Surrogate(20).
        // Corresponding global IDs: 1, 3, ..., 19.
        let mut surrogate_bm = SurrogateBitmap::new();
        for i in (2u32..=20).step_by(2) {
            surrogate_bm.insert(Surrogate(i));
        }

        // Translate to local IDs and serialise for HNSW.
        let local_bm = surrogate_bitmap_to_global_ids(&coll, &surrogate_bm);
        let mut buf = Vec::new();
        local_bm.serialize_into(&mut buf).unwrap();

        // Search for nearest neighbours — all results must be even surrogates.
        let results = coll.search_with_bitmap_bytes(&[10.0], 5, 64, &buf);

        assert!(!results.is_empty(), "expected at least one result");
        for r in &results {
            // `r.id` is the global vector ID; the bound surrogate is global_id + 1.
            let surrogate = Surrogate(r.id + 1);
            assert!(
                surrogate_bm.contains(surrogate),
                "result surrogate {:?} (global_id={}) is not in the filter bitmap",
                surrogate,
                r.id
            );
        }
    }

    /// The hybrid-fusion key derivation must yield `Headless` for a vector hit
    /// with no surrogate binding and `Bound` for a bound hit. A headless row's
    /// raw local id is never misread as a global surrogate, so it can never
    /// fuse with a real FTS hit. This is the single shared helper both the
    /// committed hybrid path and the transaction-overlay splice use.
    #[test]
    fn vector_leg_key_headless_emits_sentinel_bound_emits_surrogate() {
        // No collection at all → always headless.
        assert_eq!(vector_leg_key(None, 7), HybridFusionKey::Headless(7));

        let coll = make_collection_with_surrogates(3);
        // Local id 0 is bound to Surrogate(1): resolves to that surrogate's
        // storage key (never the sentinel).
        let bound = vector_leg_key(Some(&coll), 0);
        assert_eq!(
            bound,
            HybridFusionKey::for_surrogate(Surrogate(1)),
            "a bound hit must resolve to its surrogate's storage key"
        );
        assert_eq!(bound.to_string(), "00000001");
        // A local id with no surrogate binding in this collection → headless.
        assert_eq!(
            vector_leg_key(Some(&coll), 999),
            HybridFusionKey::Headless(999)
        );
    }

    #[test]
    fn empty_surrogate_bitmap_returns_empty_results() {
        let coll = make_collection_with_surrogates(10);
        let empty_bm = SurrogateBitmap::new();

        let local_bm = surrogate_bitmap_to_global_ids(&coll, &empty_bm);
        let mut buf = Vec::new();
        local_bm.serialize_into(&mut buf).unwrap();

        let results = coll.search_with_bitmap_bytes(&[5.0], 5, 64, &buf);
        assert!(results.is_empty(), "empty bitmap should yield no results");
    }
}
