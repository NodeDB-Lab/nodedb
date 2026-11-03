// SPDX-License-Identifier: Apache-2.0

//! VectorCollection search: multi-segment merging with SQ8 reranking. A
//! trained IVF-PQ index answers beside the segments under global ids.
//!
//! The `search_with_payload_filter` method wires payload bitmap pre-filtering
//! into the search path. When all referenced fields in the predicate are
//! indexed, the bitmap is built and passed to `search_with_bitmap_bytes`.
//! When any field is un-indexed, the search falls back to the full unfiltered
//! path and lets the caller apply post-filtering — the un-indexed predicate is
//! never silently dropped.

use roaring::RoaringBitmap;

use crate::distance::{DistanceMetric, distance};
use crate::error::{VectorError, check_dim};
use crate::hnsw::SearchResult;
use crate::hnsw::search::decode_filter_bitmap;

use super::lifecycle::VectorCollection;
use super::payload_index::FilterPredicate;
use super::segment::SealedSegment;

/// Score a single candidate via the SQ8 codec, using the metric-appropriate
/// asymmetric distance.
#[inline]
fn sq8_score(
    codec: &crate::quantize::sq8::Sq8Codec,
    query: &[f32],
    encoded: &[u8],
    metric: DistanceMetric,
) -> f32 {
    match metric {
        DistanceMetric::Cosine => codec.asymmetric_cosine(query, encoded),
        DistanceMetric::InnerProduct => codec.asymmetric_ip(query, encoded),
        // L2 (and all other metrics that don't have a specialized asymmetric
        // form yet) fall back to squared L2 — correct for ordering when the
        // metric is L2 and a reasonable proxy otherwise since we rerank with
        // exact FP32 below.
        _ => codec.asymmetric_l2(query, encoded),
    }
}

/// Candidate-generation + rerank for a sealed segment that has a quantized
/// codec attached. Generates a widened candidate pool via HNSW, re-scores
/// candidates using the quantized codec (this is where SQ8/PQ actually pay
/// off — the FP32 vectors need not be resident), and reranks the top
/// `top_k` via exact FP32 distance from mmap or index storage.
fn quantized_search(
    seg: &SealedSegment,
    query: &[f32],
    top_k: usize,
    ef: usize,
    metric: DistanceMetric,
) -> Result<Vec<SearchResult>, VectorError> {
    let rerank_k = top_k.saturating_mul(3).max(20);
    let hnsw_candidates = seg.index.search(query, rerank_k, ef)?;

    // Phase 1: rank candidates by quantized distance.
    let mut scored: Vec<(u32, f32)> = if let Some((codec, codes)) = &seg.pq {
        let table = codec.build_distance_table(query)?;
        let m = codec.m;
        hnsw_candidates
            .into_iter()
            .filter_map(|r| {
                let start = (r.id as usize).checked_mul(m)?;
                let end = start.checked_add(m)?;
                let slice = codes.get(start..end)?;
                Some((r.id, codec.asymmetric_distance(&table, slice)))
            })
            .collect()
    } else if let Some((codec, data)) = &seg.sq8 {
        let dim = codec.dim();
        hnsw_candidates
            .into_iter()
            .filter_map(|r| {
                let start = (r.id as usize).checked_mul(dim)?;
                let end = start.checked_add(dim)?;
                let slice = data.get(start..end)?;
                Some((r.id, sq8_score(codec, query, slice, metric)))
            })
            .collect()
    } else {
        hnsw_candidates
            .into_iter()
            .map(|r| (r.id, r.distance))
            .collect()
    };
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    // Keep only the most promising candidates for FP32 rerank.
    let keep = rerank_k.min(scored.len());
    scored.truncate(keep);

    // Prefetch FP32 vectors for reranking.
    if let Some(mmap) = &seg.mmap_vectors {
        let ids: Vec<u32> = scored.iter().map(|&(id, _)| id).collect();
        mmap.prefetch_batch(&ids);
    }

    // Phase 2: rerank with exact FP32.
    let mut reranked: Vec<SearchResult> = scored
        .into_iter()
        .filter_map(|(id, _)| {
            let v = if let Some(mmap) = &seg.mmap_vectors {
                mmap.get_vector(id)?
            } else {
                seg.index.get_vector(id)?
            };
            Some(SearchResult {
                id,
                distance: distance(query, v, metric),
            })
        })
        .collect();
    reranked.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    reranked.truncate(top_k);
    Ok(reranked)
}

/// Search one sealed segment: through its quantized codec when it has one,
/// else its HNSW graph. A codec pass that exceeds the memory budget falls
/// back to the HNSW graph, which answers the same query from FP32 vectors.
/// Every other error fails the search.
fn search_sealed(
    seg: &SealedSegment,
    query: &[f32],
    top_k: usize,
    ef: usize,
    metric: DistanceMetric,
) -> Result<Vec<SearchResult>, VectorError> {
    if seg.pq.is_none() && seg.sq8.is_none() {
        return seg.index.search(query, top_k, ef);
    }
    match quantized_search(seg, query, top_k, ef, metric) {
        Ok(results) => Ok(results),
        Err(VectorError::BudgetExhausted(e)) => {
            tracing::warn!(error = %e, "quantized search over budget; searching the HNSW graph");
            seg.index.search(query, top_k, ef)
        }
        Err(e) => Err(e),
    }
}

/// Search one sealed segment under a filter that holds global ids.
///
/// A sparse filter is answered by scoring its members in the segment
/// exactly. A beam search of width `ef` under a filter holds `ef` matches
/// only after it visits about `ef * len / members` nodes, and it scores up to
/// `m0` neighbours of each. Scoring the members once costs less whenever
/// `members^2 <= ef * len * m0`. Distances are in `metric`.
fn search_sealed_filtered(
    seg: &SealedSegment,
    query: &[f32],
    top_k: usize,
    ef: usize,
    filter: &RoaringBitmap,
    metric: DistanceMetric,
) -> Result<Vec<SearchResult>, VectorError> {
    let len = seg.index.len();
    let lo = seg.base_id;
    let hi = lo.saturating_add(u32::try_from(len).unwrap_or(u32::MAX));
    let members = usize::try_from(filter.range_cardinality(lo..hi)).unwrap_or(usize::MAX);
    let beam_cost = ef.saturating_mul(len).saturating_mul(seg.index.params.m0);
    if members.saturating_mul(members) > beam_cost {
        let mut results =
            seg.index
                .search_filtered_offset(query, top_k, ef, filter, seg.base_id)?;
        // Rerank with the requested metric using the stored FP32 vector.
        for r in &mut results {
            if let Some(v) = seg.index.get_vector(r.id) {
                r.distance = distance(query, v, metric);
            }
        }
        return Ok(results);
    }
    let mut scored: Vec<SearchResult> = Vec::with_capacity(members);
    for global in filter.range(lo..hi) {
        let id = global - lo;
        if seg.index.is_deleted(id) {
            continue;
        }
        if let Some(v) = seg.index.get_vector_or_backing(id) {
            scored.push(SearchResult {
                id,
                distance: distance(query, &v, metric),
            });
        }
    }
    if top_k > 0 && scored.len() > top_k {
        scored.select_nth_unstable_by(top_k - 1, |a, b| a.distance.total_cmp(&b.distance));
        scored.truncate(top_k);
    }
    scored.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance));
    Ok(scored)
}

/// Shift segment-local result ids to global ids and append them.
pub(super) fn push_shifted(all: &mut Vec<SearchResult>, results: Vec<SearchResult>, base_id: u32) {
    all.extend(results.into_iter().map(|mut r| {
        r.id += base_id;
        r
    }));
}

/// Order merged results by distance and keep the `top_k` nearest.
pub(super) fn finish(mut all: Vec<SearchResult>, top_k: usize) -> Vec<SearchResult> {
    all.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    all.truncate(top_k);
    all
}

impl VectorCollection {
    /// Search across all segments, merging results by distance.
    ///
    /// A query without the collection dimension fails with
    /// [`VectorError::DimensionMismatch`] before any segment is read.
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
    ) -> Result<Vec<SearchResult>, VectorError> {
        check_dim(self.dim, query.len())?;
        let mut all: Vec<SearchResult> = Vec::new();

        // Codec-dispatch fast path: a collection-level HnswCodecIndex (RaBitQ
        // or BBQ) answers for the sealed segments; the growing and building
        // segments are read by brute force beside it.
        if let Some(ref dispatch) = self.codec_dispatch {
            all.extend(
                dispatch
                    .search(query, top_k, ef)?
                    .into_iter()
                    .map(|r| SearchResult {
                        id: r.id,
                        distance: r.distance,
                    }),
            );
        } else {
            for seg in &self.sealed {
                let results = search_sealed(seg, query, top_k, ef, self.params.metric)?;
                push_shifted(&mut all, results, seg.base_id);
            }
        }
        if let Some(ivf) = &self.ivf {
            all.extend(ivf.search(query, top_k)?);
        }

        push_shifted(
            &mut all,
            self.growing.search(query, top_k)?,
            self.growing_base_id,
        );
        for seg in &self.building {
            push_shifted(&mut all, seg.flat.search(query, top_k)?, seg.base_id);
        }
        Ok(finish(all, top_k))
    }

    /// Search across all segments using an explicit metric override.
    ///
    /// For sealed segments with quantized codecs, the metric override is applied
    /// during candidate reranking. Growing and building segments apply it exactly
    /// via brute-force. The HNSW graph structure was built with the collection
    /// metric; using a different metric affects the scoring but not graph traversal.
    /// A codec-dispatch index scores with the collection metric.
    pub fn search_with_metric(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>, VectorError> {
        check_dim(self.dim, query.len())?;
        let mut all: Vec<SearchResult> = Vec::new();

        if let Some(ref dispatch) = self.codec_dispatch {
            all.extend(
                dispatch
                    .search(query, top_k, ef)?
                    .into_iter()
                    .map(|r| SearchResult {
                        id: r.id,
                        distance: r.distance,
                    }),
            );
        } else {
            for seg in &self.sealed {
                let results = search_sealed(seg, query, top_k, ef, metric)?;
                push_shifted(&mut all, results, seg.base_id);
            }
        }
        if let Some(ivf) = &self.ivf {
            all.extend(ivf.search_with(query, top_k, metric, None)?);
        }

        push_shifted(
            &mut all,
            self.growing.search_with_metric(query, top_k, metric)?,
            self.growing_base_id,
        );
        for seg in &self.building {
            push_shifted(
                &mut all,
                seg.flat.search_with_metric(query, top_k, metric)?,
                seg.base_id,
            );
        }
        Ok(finish(all, top_k))
    }

    /// Search with a pre-filter bitmap (byte-array format) and explicit metric override.
    ///
    /// Bitmap bytes that do not decode fail with
    /// [`VectorError::InvalidFilterBitmap`].
    pub fn search_with_bitmap_bytes_and_metric(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        bitmap: &[u8],
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>, VectorError> {
        check_dim(self.dim, query.len())?;
        let mut all: Vec<SearchResult> = Vec::new();
        let filter = decode_filter_bitmap(bitmap)?;

        if let Some(ivf) = &self.ivf {
            all.extend(ivf.search_with(query, top_k, metric, Some(&filter))?);
        }
        push_shifted(
            &mut all,
            self.growing.search_filtered_offset_with_metric(
                query,
                top_k,
                bitmap,
                self.growing_base_id,
                metric,
            )?,
            self.growing_base_id,
        );

        for seg in &self.sealed {
            let results = search_sealed_filtered(seg, query, top_k, ef, &filter, metric)?;
            push_shifted(&mut all, results, seg.base_id);
        }

        for seg in &self.building {
            push_shifted(
                &mut all,
                seg.flat.search_filtered_offset_with_metric(
                    query,
                    top_k,
                    bitmap,
                    seg.base_id,
                    metric,
                )?,
                seg.base_id,
            );
        }
        Ok(finish(all, top_k))
    }

    /// Search with a pre-filter bitmap (byte-array format).
    ///
    /// Bitmap bytes that do not decode fail with
    /// [`VectorError::InvalidFilterBitmap`].
    pub fn search_with_bitmap_bytes(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        bitmap: &[u8],
    ) -> Result<Vec<SearchResult>, VectorError> {
        check_dim(self.dim, query.len())?;
        let mut all: Vec<SearchResult> = Vec::new();
        let filter = decode_filter_bitmap(bitmap)?;

        if let Some(ivf) = &self.ivf {
            all.extend(ivf.search_with(query, top_k, self.params.metric, Some(&filter))?);
        }
        push_shifted(
            &mut all,
            self.growing
                .search_filtered_offset(query, top_k, bitmap, self.growing_base_id)?,
            self.growing_base_id,
        );
        for seg in &self.sealed {
            let results =
                search_sealed_filtered(seg, query, top_k, ef, &filter, self.params.metric)?;
            push_shifted(&mut all, results, seg.base_id);
        }
        for seg in &self.building {
            push_shifted(
                &mut all,
                seg.flat
                    .search_filtered_offset(query, top_k, bitmap, seg.base_id)?,
                seg.base_id,
            );
        }
        Ok(finish(all, top_k))
    }

    /// Search with a structured payload predicate.
    ///
    /// If `predicate` is fully covered by indexed fields (all leaf fields have
    /// a bitmap index), the bitmap is built and HNSW traversal uses it as a
    /// pre-filter.
    ///
    /// If any field in `predicate` is un-indexed, the method returns
    /// `(results, false)` where `false` signals that the predicate was NOT
    /// applied and the caller must apply it as a post-filter. This guarantees
    /// the un-indexed predicate is never silently dropped.
    ///
    /// Returns `(results, filter_was_applied)`.
    pub fn search_with_payload_filter(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        predicate: &FilterPredicate,
    ) -> Result<(Vec<SearchResult>, bool), VectorError> {
        match self.payload.pre_filter(predicate) {
            Some(bm) => {
                // Serialize the bitmap to the byte format expected by
                // `search_with_bitmap_bytes`.
                let mut bm_bytes = Vec::new();
                if bm.serialize_into(&mut bm_bytes).is_ok() {
                    let results = self.search_with_bitmap_bytes(query, top_k, ef, &bm_bytes)?;
                    Ok((results, true))
                } else {
                    // Serialization failure: unfiltered search, and the
                    // caller applies the predicate as a post-filter.
                    Ok((self.search(query, top_k, ef)?, false))
                }
            }
            None => {
                // Un-indexed field present: full scan, caller must post-filter.
                Ok((self.search(query, top_k, ef)?, false))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::collection::lifecycle::VectorCollection;
    use crate::collection::segment::DEFAULT_SEAL_THRESHOLD;
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};
    use crate::test_support::test_memory;

    fn make_collection() -> VectorCollection {
        VectorCollection::new(
            3,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        )
    }

    #[test]
    fn insert_and_search() {
        let mut coll = make_collection();
        for i in 0..100u32 {
            coll.insert(vec![i as f32, 0.0, 0.0]).unwrap();
        }
        assert_eq!(coll.len(), 100);
        let results = coll.search(&[50.0, 0.0, 0.0], 3, 64).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 50);
    }

    #[test]
    fn seal_moves_to_building() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..DEFAULT_SEAL_THRESHOLD {
            coll.insert(vec![i as f32, 0.0]).unwrap();
        }
        assert!(coll.needs_seal());

        let req = coll.seal("test_key").unwrap();
        assert_eq!(req.vectors.len(), DEFAULT_SEAL_THRESHOLD);
        assert_eq!(coll.building.len(), 1);
        assert_eq!(coll.growing.len(), 0);

        let results = coll.search(&[100.0, 0.0], 1, 64).unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn complete_build_promotes_to_sealed() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..100 {
            coll.insert(vec![i as f32, 0.0]).unwrap();
        }
        let req = coll.seal("test").unwrap();

        let mut index = HnswIndex::new(req.dim, req.params);
        for v in &req.vectors {
            index.insert(v.clone()).unwrap();
        }
        coll.complete_build(req.segment_id, index, test_memory());

        assert_eq!(coll.building.len(), 0);
        assert_eq!(coll.sealed.len(), 1);

        let results = coll.search(&[50.0, 0.0], 3, 64).unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn multi_segment_search_merges() {
        let mut coll = VectorCollection::new(
            2,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );

        for i in 0..100 {
            coll.insert(vec![i as f32, 0.0]).unwrap();
        }
        let req = coll.seal("test").unwrap();
        let mut idx = HnswIndex::new(2, req.params);
        for v in &req.vectors {
            idx.insert(v.clone()).unwrap();
        }
        coll.complete_build(req.segment_id, idx, test_memory());

        for i in 100..200 {
            coll.insert(vec![i as f32, 0.0]).unwrap();
        }

        let results = coll.search(&[150.0, 0.0], 3, 64).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 150);
    }

    #[test]
    fn delete_across_segments() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..10 {
            coll.insert(vec![i as f32, 0.0]).unwrap();
        }
        assert!(coll.delete(5));
        assert_eq!(coll.live_count(), 9);

        let results = coll.search(&[5.0, 0.0], 10, 64).unwrap();
        assert!(results.iter().all(|r| r.id != 5));
    }

    /// Build a sealed HNSW segment from `n` vectors of `dim=2`, where vector `i`
    /// is `[i as f32, 0.0]`. Returns the collection with one sealed segment.
    fn make_sealed_collection(n: usize) -> VectorCollection {
        let mut coll = VectorCollection::new(
            2,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );
        for i in 0..n {
            coll.insert(vec![i as f32, 0.0]).unwrap();
        }
        let req = coll.seal("seg").unwrap();
        let mut idx = HnswIndex::new(req.dim, req.params);
        for v in &req.vectors {
            idx.insert(v.clone()).unwrap();
        }
        coll.complete_build(req.segment_id, idx, test_memory());
        coll
    }

    /// Attach SQ8 quantization to the first sealed segment of `coll`.
    fn attach_sq8(coll: &mut VectorCollection) {
        use crate::quantize::sq8::Sq8Codec;

        let sealed = &mut coll.sealed[0];
        let dim = sealed.index.dim();
        let n = sealed.index.len();
        let vecs: Vec<Vec<f32>> = (0..n)
            .filter_map(|i| sealed.index.get_vector(i as u32).map(|v| v.to_vec()))
            .collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = Sq8Codec::calibrate(&refs, dim).unwrap();
        let sq8_data: Vec<u8> = vecs.iter().flat_map(|v| codec.quantize(v)).collect();
        sealed.sq8 = Some((codec, sq8_data));
        sealed.remeasure();
    }

    #[test]
    fn sq8_search_returns_correct_nearest_neighbor() {
        let mut coll = make_sealed_collection(200);
        attach_sq8(&mut coll);

        let results = coll.search(&[100.0, 0.0], 5, 64).unwrap();
        assert!(!results.is_empty(), "expected non-empty results");
        assert_eq!(
            results[0].id, 100,
            "nearest neighbor of [100,0] should be id=100, got id={}",
            results[0].id
        );
    }

    #[test]
    fn sq8_search_recall_matches_hnsw() {
        // Build two identical collections — one without SQ8, one with.
        let coll_plain = make_sealed_collection(500);
        let mut coll_sq8 = make_sealed_collection(500);
        attach_sq8(&mut coll_sq8);

        let query = [250.0f32, 0.0];
        let top_k = 5;

        let plain_results = coll_plain.search(&query, top_k, 64).unwrap();
        let sq8_results = coll_sq8.search(&query, top_k, 64).unwrap();

        let plain_ids: std::collections::HashSet<u32> =
            plain_results.iter().map(|r| r.id).collect();
        let sq8_ids: std::collections::HashSet<u32> = sq8_results.iter().map(|r| r.id).collect();

        let overlap = plain_ids.intersection(&sq8_ids).count();
        assert!(
            overlap >= 4,
            "SQ8 recall too low: {overlap}/5 results matched plain HNSW (need >=4)"
        );
    }

    /// A collection whose first 50 vectors (`[i, 0, 0, 0]`) sit in one
    /// sealed segment, with an empty growing segment.
    fn sealed_dim4_collection() -> VectorCollection {
        let mut coll = VectorCollection::new(
            4,
            HnswParams {
                metric: DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
                ..HnswParams::default()
            },
        );
        for i in 0u32..50 {
            coll.insert(vec![i as f32, 0.0, 0.0, 0.0]).unwrap();
        }
        let req = coll.seal("codec").unwrap();
        let mut idx = HnswIndex::new(req.dim, req.params);
        for v in &req.vectors {
            idx.insert(v.clone()).unwrap();
        }
        coll.complete_build(req.segment_id, idx, test_memory());
        coll
    }

    #[test]
    fn codec_dispatch_bbq_search_returns_results_and_stats_report_bbq() {
        let mut coll = sealed_dim4_collection();

        // Build the collection-level BBQ dispatch index over the sealed vectors.
        let dispatch = coll.build_codec_dispatch("bbq").unwrap();
        assert!(
            dispatch.is_some(),
            "build_codec_dispatch(bbq) should return Some"
        );

        // Query near id=25.
        let query = [25.0f32, 0.0, 0.0, 0.0];
        let results = coll.search(&query, 5, 32).unwrap();
        assert!(
            !results.is_empty(),
            "BBQ codec-dispatch search should return results"
        );

        // Stats should report Bbq quantization.
        let stats = coll.stats();
        assert_eq!(
            stats.quantization,
            nodedb_types::VectorIndexQuantization::Bbq,
            "stats quantization should be Bbq after build_codec_dispatch(bbq)"
        );
    }

    #[test]
    fn codec_dispatch_rabitq_search_non_empty() {
        let mut coll = sealed_dim4_collection();
        coll.build_codec_dispatch("rabitq").unwrap().unwrap();

        let results = coll.search(&[10.0, 0.0, 0.0, 0.0], 3, 32).unwrap();
        assert!(
            !results.is_empty(),
            "RaBitQ dispatch search should return results"
        );

        let stats = coll.stats();
        assert_eq!(
            stats.quantization,
            nodedb_types::VectorIndexQuantization::RaBitQ
        );
    }

    /// The codec index covers the sealed segments under their global ids;
    /// the growing segment is read beside it. A growing vector is found under
    /// its own id, once.
    #[test]
    fn codec_dispatch_keeps_global_ids_and_reads_growing_once() {
        let mut coll = sealed_dim4_collection();
        coll.build_codec_dispatch("bbq").unwrap().unwrap();
        let far = coll.insert(vec![1000.0, 0.0, 0.0, 0.0]).unwrap();
        assert_eq!(far, 50, "the first growing vector takes the next global id");

        let results = coll.search(&[1000.0, 0.0, 0.0, 0.0], 3, 32).unwrap();
        assert_eq!(results[0].id, far);
        assert_eq!(
            results.iter().filter(|r| r.id == far).count(),
            1,
            "{results:?}"
        );
        assert!(results.iter().all(|r| r.id <= far), "{results:?}");
    }

    /// Every collection search entry point refuses a query of the wrong
    /// dimension, on each segment kind and on the codec-dispatch path.
    #[test]
    fn wrong_dimension_query_is_a_typed_error() {
        use crate::error::VectorError;
        let mut coll = sealed_dim4_collection();
        coll.insert(vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        let bytes = {
            let bm: roaring::RoaringBitmap = (0..51u32).collect();
            let mut out = Vec::new();
            bm.serialize_into(&mut out).unwrap();
            out
        };
        let short = [1.0_f32, 0.0];
        let check = |coll: &VectorCollection| {
            for result in [
                coll.search(&short, 3, 32),
                coll.search_with_metric(&short, 3, 32, DistanceMetric::Cosine),
                coll.search_with_bitmap_bytes(&short, 3, 32, &bytes),
                coll.search_with_bitmap_bytes_and_metric(&short, 3, 32, &bytes, DistanceMetric::L2),
            ] {
                assert!(
                    matches!(
                        result,
                        Err(VectorError::DimensionMismatch {
                            expected: 4,
                            got: 2
                        })
                    ),
                    "{result:?}"
                );
            }
        };
        check(&coll);
        coll.build_codec_dispatch("rabitq").unwrap().unwrap();
        check(&coll);
        assert!(matches!(
            coll.insert(vec![1.0; 3]),
            Err(VectorError::DimensionMismatch {
                expected: 4,
                got: 3
            })
        ));
    }

    #[test]
    fn sq8_search_does_not_scan_all_vectors() {
        // This test validates correctness of the SQ8 search path for a large
        // segment. The bug being guarded against is an O(N) linear scan instead
        // of graph-guided traversal: the fix must use HNSW with SQ8 as the
        // distance function. Correctness (correct nearest neighbor) is the
        // invariant that must be preserved when the implementation changes.
        let mut coll = make_sealed_collection(2000);
        attach_sq8(&mut coll);

        let results = coll.search(&[1000.0, 0.0], 5, 64).unwrap();
        assert!(!results.is_empty(), "expected non-empty results");
        assert_eq!(
            results[0].id, 1000,
            "nearest neighbor of [1000,0] should be id=1000, got id={}",
            results[0].id
        );
    }

    fn bitmap_bytes(ids: impl IntoIterator<Item = u32>) -> Vec<u8> {
        let bitmap: roaring::RoaringBitmap = ids.into_iter().collect();
        let mut out = Vec::new();
        bitmap.serialize_into(&mut out).unwrap();
        out
    }

    fn ids(results: &[crate::hnsw::SearchResult]) -> Vec<u32> {
        results.iter().map(|r| r.id).collect()
    }

    /// A filter with few members ranks exactly those members, nearest first,
    /// and never returns an id outside the filter.
    #[test]
    fn sparse_filter_on_sealed_segment_ranks_members_exactly() {
        let coll = make_sealed_collection(2000);
        let bitmap = bitmap_bytes([10, 700, 701, 1500, 1999]);
        let results = coll
            .search_with_bitmap_bytes_and_metric(&[705.0, 0.0], 3, 64, &bitmap, DistanceMetric::L2)
            .unwrap();
        assert_eq!(ids(&results), vec![701, 700, 10]);
        assert_eq!(results[0].distance, 16.0);
    }

    /// A deleted member of a sparse filter is not returned.
    #[test]
    fn sparse_filter_on_sealed_segment_skips_deleted_members() {
        let mut coll = make_sealed_collection(2000);
        assert!(coll.delete(701));
        let bitmap = bitmap_bytes([700, 701, 1500]);
        let results = coll
            .search_with_bitmap_bytes_and_metric(&[705.0, 0.0], 2, 64, &bitmap, DistanceMetric::L2)
            .unwrap();
        assert_eq!(ids(&results), vec![700, 1500]);
    }

    /// A filter holding half the segment, searched with a narrow beam, still
    /// returns the nearest member.
    #[test]
    fn dense_filter_on_sealed_segment_returns_nearest_member() {
        let coll = make_sealed_collection(2000);
        let bitmap = bitmap_bytes((0..2000).step_by(2));
        let results = coll
            .search_with_bitmap_bytes_and_metric(&[1001.0, 0.0], 1, 1, &bitmap, DistanceMetric::L2)
            .unwrap();
        assert!(results[0].id == 1000 || results[0].id == 1002);
    }

    /// The index-metric variant answers a sparse filter the same way.
    #[test]
    fn sparse_filter_without_metric_ranks_members_exactly() {
        let coll = make_sealed_collection(2000);
        let bitmap = bitmap_bytes([10, 700, 701, 1500, 1999]);
        let results = coll
            .search_with_bitmap_bytes(&[705.0, 0.0], 3, 64, &bitmap)
            .unwrap();
        assert_eq!(ids(&results), vec![701, 700, 10]);
    }
}
