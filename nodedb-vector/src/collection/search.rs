// SPDX-License-Identifier: Apache-2.0

//! VectorCollection search: multi-segment merging. Each sealed segment is
//! searched through its codec (`search_sealed`). A trained IVF-PQ index
//! answers beside the segments under global ids. Filtered searches rank
//! exact vectors.
//!
//! The `search_with_payload_filter` method wires payload bitmap pre-filtering
//! into the search path. When all referenced fields in the predicate are
//! indexed, the bitmap is built and passed to `search_with_bitmap_bytes`.
//! When any field is un-indexed, the search falls back to the full unfiltered
//! path and lets the caller apply post-filtering — the un-indexed predicate is
//! never silently dropped.

use crate::distance::DistanceMetric;
use crate::error::{VectorError, check_dim};
use crate::hnsw::SearchResult;
use crate::hnsw::search::decode_filter_bitmap;
use crate::rerank::SearchBreadth;

use super::lifecycle::VectorCollection;
use super::payload_index::FilterPredicate;
use super::search_sealed::{search_sealed, search_sealed_filtered};

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
    /// Search across all segments, merging results by distance. `breadth`
    /// is the HNSW beam width and the rerank oversample; a plain `usize`
    /// beam takes the default oversample.
    ///
    /// A query without the collection dimension fails with
    /// [`VectorError::DimensionMismatch`] before any segment is read.
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        breadth: impl Into<SearchBreadth>,
    ) -> Result<Vec<SearchResult>, VectorError> {
        let breadth = breadth.into();
        check_dim(self.dim, query.len())?;
        let mut all: Vec<SearchResult> = Vec::new();

        for seg in &self.sealed {
            let results = search_sealed(seg, query, top_k, breadth, self.params.metric)?;
            push_shifted(&mut all, results, seg.base_id);
        }
        if let Some(ivf) = &self.ivf {
            all.extend(ivf.search(query, top_k, breadth.oversample)?);
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
    /// Every segment returns exact distances in `metric`: sealed segments
    /// rerank their candidates under it, and the growing and building
    /// segments scan under it. The HNSW graphs were built with the collection
    /// metric, so the override changes scoring, not traversal.
    pub fn search_with_metric(
        &self,
        query: &[f32],
        top_k: usize,
        breadth: impl Into<SearchBreadth>,
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>, VectorError> {
        let breadth = breadth.into();
        check_dim(self.dim, query.len())?;
        let mut all: Vec<SearchResult> = Vec::new();

        for seg in &self.sealed {
            let results = search_sealed(seg, query, top_k, breadth, metric)?;
            push_shifted(&mut all, results, seg.base_id);
        }
        if let Some(ivf) = &self.ivf {
            all.extend(ivf.search_with(query, top_k, breadth.oversample, metric, None)?);
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
        breadth: impl Into<SearchBreadth>,
        bitmap: &[u8],
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>, VectorError> {
        let breadth = breadth.into();
        check_dim(self.dim, query.len())?;
        let mut all: Vec<SearchResult> = Vec::new();
        let filter = decode_filter_bitmap(bitmap)?;

        if let Some(ivf) = &self.ivf {
            all.extend(ivf.search_with(query, top_k, breadth.oversample, metric, Some(&filter))?);
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
            let results =
                search_sealed_filtered(seg, query, top_k, breadth.ef_search, &filter, metric)?;
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
        breadth: impl Into<SearchBreadth>,
        bitmap: &[u8],
    ) -> Result<Vec<SearchResult>, VectorError> {
        let breadth = breadth.into();
        check_dim(self.dim, query.len())?;
        let mut all: Vec<SearchResult> = Vec::new();
        let filter = decode_filter_bitmap(bitmap)?;

        if let Some(ivf) = &self.ivf {
            all.extend(ivf.search_with(
                query,
                top_k,
                breadth.oversample,
                self.params.metric,
                Some(&filter),
            )?);
        }
        push_shifted(
            &mut all,
            self.growing
                .search_filtered_offset(query, top_k, bitmap, self.growing_base_id)?,
            self.growing_base_id,
        );
        for seg in &self.sealed {
            let results = search_sealed_filtered(
                seg,
                query,
                top_k,
                breadth.ef_search,
                &filter,
                self.params.metric,
            )?;
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
        breadth: impl Into<SearchBreadth>,
        predicate: &FilterPredicate,
    ) -> Result<(Vec<SearchResult>, bool), VectorError> {
        let breadth = breadth.into();
        match self.payload.pre_filter(predicate) {
            Some(bm) => {
                // Serialize the bitmap to the byte format expected by
                // `search_with_bitmap_bytes`.
                let mut bm_bytes = Vec::new();
                if bm.serialize_into(&mut bm_bytes).is_ok() {
                    let results =
                        self.search_with_bitmap_bytes(query, top_k, breadth, &bm_bytes)?;
                    Ok((results, true))
                } else {
                    // Serialization failure: unfiltered search, and the
                    // caller applies the predicate as a post-filter.
                    Ok((self.search(query, top_k, breadth)?, false))
                }
            }
            None => {
                // Un-indexed field present: full scan, caller must post-filter.
                Ok((self.search(query, top_k, breadth)?, false))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::collection::lifecycle::VectorCollection;
    use crate::collection::segment::DEFAULT_SEAL_THRESHOLD;
    use crate::collection::segment_codec::SegmentCodec;
    use crate::distance::DistanceMetric;
    use crate::hnsw::HnswParams;
    use crate::rerank::SearchBreadth;
    use crate::test_support::{build_now, test_memory};
    use nodedb_types::{VectorIndexQuantization, VectorQuantization};

    /// A breadth whose rerank pool covers a whole 50-vector segment, so the
    /// tests below check ids and exact distances, not codec recall.
    const WIDE: SearchBreadth = SearchBreadth {
        ef_search: 64,
        oversample: 20,
    };

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

        let req = coll.seal("test_key", test_memory()).unwrap();
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
        let req = coll.seal("test", test_memory()).unwrap();
        assert!(
            coll.complete_build(req.segment_id, build_now(&req))
                .installed()
        );

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
        let req = coll.seal("test", test_memory()).unwrap();
        assert!(
            coll.complete_build(req.segment_id, build_now(&req))
                .installed()
        );

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
    /// is `[i as f32, 0.0]`, under quantization `q`. Returns the collection
    /// with one sealed segment.
    fn sealed_collection_with(n: usize, q: VectorQuantization) -> VectorCollection {
        let mut coll = VectorCollection::new(
            2,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );
        coll.set_quantization(q);
        for i in 0..n {
            coll.insert(vec![i as f32, 0.0]).unwrap();
        }
        let req = coll.seal("seg", test_memory()).unwrap();
        assert!(
            coll.complete_build(req.segment_id, build_now(&req))
                .installed()
        );
        coll
    }

    /// [`sealed_collection_with`] without a codec.
    fn make_sealed_collection(n: usize) -> VectorCollection {
        sealed_collection_with(n, VectorQuantization::None)
    }

    #[test]
    fn sq8_search_returns_correct_nearest_neighbor() {
        let coll = sealed_collection_with(200, VectorQuantization::Sq8);
        assert!(matches!(coll.sealed[0].codec, SegmentCodec::Sq8 { .. }));

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
        let coll_sq8 = sealed_collection_with(500, VectorQuantization::Sq8);

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
    /// sealed segment carrying the codec `q` selects, with an empty growing
    /// segment.
    fn sealed_dim4_collection(q: VectorQuantization) -> VectorCollection {
        let mut coll = VectorCollection::new(
            4,
            HnswParams {
                metric: DistanceMetric::L2,
                m: 8,
                ef_construction: 50,
                ..HnswParams::default()
            },
        );
        coll.set_quantization(q);
        for i in 0u32..50 {
            coll.insert(vec![i as f32, 0.0, 0.0, 0.0]).unwrap();
        }
        let req = coll.seal("codec", test_memory()).unwrap();
        assert!(
            coll.complete_build(req.segment_id, build_now(&req))
                .installed()
        );
        coll
    }

    #[test]
    fn bbq_segment_search_returns_exact_distances_and_stats_report_bbq() {
        let coll = sealed_dim4_collection(VectorQuantization::Bbq);
        assert!(matches!(coll.sealed[0].codec, SegmentCodec::Graph(_)));

        let results = coll.search(&[25.0f32, 0.0, 0.0, 0.0], 5, WIDE).unwrap();
        assert_eq!(results[0].id, 25);
        assert_eq!(results[0].distance, 0.0, "the rerank scores exactly");
        assert_eq!(coll.stats().quantization, VectorIndexQuantization::Bbq);
    }

    #[test]
    fn rabitq_segment_search_non_empty() {
        let coll = sealed_dim4_collection(VectorQuantization::RaBitQ);
        let results = coll.search(&[10.0, 0.0, 0.0, 0.0], 3, WIDE).unwrap();
        assert_eq!(results[0].id, 10);
        assert_eq!(coll.stats().quantization, VectorIndexQuantization::RaBitQ);
    }

    /// A graph codec covers its sealed segment under local ids, shifted to
    /// global ids; the growing segment is read beside it. A growing vector
    /// is found under its own id, once.
    #[test]
    fn graph_codec_keeps_global_ids_and_reads_growing_once() {
        let mut coll = sealed_dim4_collection(VectorQuantization::Bbq);
        let far = coll.insert(vec![1000.0, 0.0, 0.0, 0.0]).unwrap();
        assert_eq!(far, 50, "the first growing vector takes the next global id");

        let results = coll.search(&[1000.0, 0.0, 0.0, 0.0], 3, WIDE).unwrap();
        assert_eq!(results[0].id, far);
        assert_eq!(
            results.iter().filter(|r| r.id == far).count(),
            1,
            "{results:?}"
        );
        assert!(results.iter().all(|r| r.id <= far), "{results:?}");
    }

    /// A graph codec skips a deleted row, and finds it again once undeleted.
    #[test]
    fn graph_codec_follows_delete_and_undelete() {
        let mut coll = sealed_dim4_collection(VectorQuantization::RaBitQ);
        let query = [20.0f32, 0.0, 0.0, 0.0];
        assert!(coll.delete_inner(20));
        let hits = coll.search(&query, 5, WIDE).unwrap();
        assert!(hits.iter().all(|r| r.id != 20), "{hits:?}");
        assert!(coll.undelete(20));
        assert_eq!(coll.search(&query, 1, WIDE).unwrap()[0].id, 20);
    }

    /// Every collection search entry point refuses a query of the wrong
    /// dimension, on each segment kind and on a graph-codec segment.
    #[test]
    fn wrong_dimension_query_is_a_typed_error() {
        use crate::error::VectorError;
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
        for q in [VectorQuantization::None, VectorQuantization::RaBitQ] {
            let mut coll = sealed_dim4_collection(q);
            coll.insert(vec![1.0, 0.0, 0.0, 0.0]).unwrap();
            check(&coll);
            assert!(matches!(
                coll.insert(vec![1.0; 3]),
                Err(VectorError::DimensionMismatch {
                    expected: 4,
                    got: 3
                })
            ));
        }
    }

    #[test]
    fn sq8_search_does_not_scan_all_vectors() {
        // This test validates correctness of the SQ8 search path for a large
        // segment. The bug being guarded against is an O(N) linear scan instead
        // of graph-guided traversal: the fix must use HNSW with SQ8 as the
        // distance function. Correctness (correct nearest neighbor) is the
        // invariant that must be preserved when the implementation changes.
        let coll = sealed_collection_with(2000, VectorQuantization::Sq8);

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
