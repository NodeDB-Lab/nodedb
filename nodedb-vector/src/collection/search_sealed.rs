// SPDX-License-Identifier: Apache-2.0

//! Searching one sealed segment through the codec it carries.
//!
//! Every codec path reads a pool of `top_k × oversample` candidates and
//! reranks them by exact distance under the requested metric, so the
//! distances a sealed segment returns are always exact. An SQ8 / PQ segment
//! reads a wider beam from its HNSW graph and cuts it to the pool by code
//! distance.

use roaring::RoaringBitmap;

use crate::distance::{DistanceMetric, distance};
use crate::error::VectorError;
use crate::hnsw::SearchResult;
use crate::rerank::SearchBreadth;

use super::lifecycle_insert_ops::sealed_vector_ref;
use super::segment::SealedSegment;
use super::segment_codec::SegmentCodec;

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

/// Rank `ids` of `seg` by exact distance under `metric`; keep `top_k`.
/// A candidate whose vector cannot be read fails the search.
fn exact_rerank(
    seg: &SealedSegment,
    query: &[f32],
    ids: Vec<u32>,
    top_k: usize,
    metric: DistanceMetric,
) -> Result<Vec<SearchResult>, VectorError> {
    if let Some(mmap) = &seg.mmap_vectors {
        mmap.prefetch_batch(&ids);
    }
    let mut ranked = Vec::with_capacity(ids.len());
    for id in ids {
        let v = sealed_vector_ref(seg, id).ok_or(VectorError::VectorUnavailable {
            id: seg.base_id + id,
        })?;
        ranked.push(SearchResult {
            id,
            distance: distance(query, &v, metric),
        });
    }
    ranked.sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance));
    ranked.truncate(top_k);
    Ok(ranked)
}

/// The code row of local node `id`, `width` bytes per row.
fn code_row(codes: &[u8], width: usize, id: u32, base_id: u32) -> Result<&[u8], VectorError> {
    let start = (id as usize).saturating_mul(width);
    codes
        .get(start..start.saturating_add(width))
        .ok_or(VectorError::VectorUnavailable { id: base_id + id })
}

/// The code rows of an SQ8 or PQ segment.
#[derive(Clone, Copy)]
enum CodeRows<'a> {
    Sq8 {
        codec: &'a crate::quantize::sq8::Sq8Codec,
        codes: &'a [u8],
    },
    Pq {
        codec: &'a crate::quantize::pq::PqCodec,
        codes: &'a [u8],
    },
}

/// SQ8 / PQ code stage: the graph yields `breadth.beam(top_k)` candidates,
/// `ef_search` widened to at least the rerank pool. The code distance cuts
/// them to the `breadth.rerank_pool(top_k)` code-nearest, the ids the exact
/// rerank reads.
fn code_stage(
    seg: &SealedSegment,
    rows: CodeRows<'_>,
    query: &[f32],
    top_k: usize,
    breadth: SearchBreadth,
    metric: DistanceMetric,
) -> Result<Vec<u32>, VectorError> {
    let beam = breadth.beam(top_k);
    let candidates = seg.index.search(query, beam, beam)?;
    let mut scored: Vec<(u32, f32)> = Vec::with_capacity(candidates.len());
    match rows {
        CodeRows::Pq { codec, codes } => {
            let table = codec.build_distance_table(query)?;
            for r in candidates {
                let code = code_row(codes, codec.m, r.id, seg.base_id)?;
                scored.push((r.id, codec.asymmetric_distance(&table, code)));
            }
        }
        CodeRows::Sq8 { codec, codes } => {
            for r in candidates {
                let code = code_row(codes, codec.dim(), r.id, seg.base_id)?;
                scored.push((r.id, sq8_score(codec, query, code, metric)));
            }
        }
    }
    let pool = breadth.rerank_pool(top_k);
    if scored.len() > pool {
        scored.select_nth_unstable_by(pool, |a, b| a.1.total_cmp(&b.1));
        scored.truncate(pool);
    }
    Ok(scored.into_iter().map(|(id, _)| id).collect())
}

/// SQ8 / PQ segment: the code stage picks the rerank pool and the exact
/// rerank keeps `top_k`. A code stage over the memory budget falls back to
/// the HNSW graph, which answers from the stored vectors.
fn quantized_search(
    seg: &SealedSegment,
    rows: CodeRows<'_>,
    query: &[f32],
    top_k: usize,
    breadth: SearchBreadth,
    metric: DistanceMetric,
) -> Result<Vec<SearchResult>, VectorError> {
    let ids = match code_stage(seg, rows, query, top_k, breadth, metric) {
        Ok(ids) => ids,
        Err(VectorError::BudgetExhausted(e)) => {
            tracing::warn!(error = %e, "quantized search over budget; searching the HNSW graph");
            seg.index
                .search(query, top_k, breadth.ef_search)?
                .into_iter()
                .map(|r| r.id)
                .collect()
        }
        Err(e) => return Err(e),
    };
    exact_rerank(seg, query, ids, top_k, metric)
}

/// Search one sealed segment through the codec it carries. Distances are
/// exact under `metric`. An SQ8 / PQ pass that exceeds the memory budget
/// falls back to the HNSW graph, which answers from the stored vectors.
/// Every other error fails the search.
pub(super) fn search_sealed(
    seg: &SealedSegment,
    query: &[f32],
    top_k: usize,
    breadth: SearchBreadth,
    metric: DistanceMetric,
) -> Result<Vec<SearchResult>, VectorError> {
    match &seg.codec {
        SegmentCodec::None => {
            let candidates = seg.index.search(query, top_k, breadth.ef_search)?;
            if metric == seg.index.params.metric {
                return Ok(candidates);
            }
            exact_rerank(
                seg,
                query,
                candidates.into_iter().map(|r| r.id).collect(),
                top_k,
                metric,
            )
        }
        SegmentCodec::Graph(graph) => {
            let candidates =
                graph.search(query, breadth.rerank_pool(top_k), breadth.beam(top_k))?;
            exact_rerank(
                seg,
                query,
                candidates.into_iter().map(|c| c.id).collect(),
                top_k,
                metric,
            )
        }
        SegmentCodec::Sq8 { codec, codes } => quantized_search(
            seg,
            CodeRows::Sq8 { codec, codes },
            query,
            top_k,
            breadth,
            metric,
        ),
        SegmentCodec::Pq { codec, codes } => quantized_search(
            seg,
            CodeRows::Pq { codec, codes },
            query,
            top_k,
            breadth,
            metric,
        ),
    }
}

/// Search one sealed segment under a filter that holds global ids.
///
/// A sparse filter is answered by scoring its members in the segment
/// exactly. A beam search of width `ef` under a filter holds `ef` matches
/// only after it visits about `ef * len / members` nodes, and it scores up to
/// `m0` neighbours of each. Scoring the members once costs less whenever
/// `members^2 <= ef * len * m0`. Distances are exact in `metric`.
pub(super) fn search_sealed_filtered(
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
        // Rescore with the requested metric from the stored vector.
        for r in &mut results {
            if let Some(v) = sealed_vector_ref(seg, r.id) {
                r.distance = distance(query, &v, metric);
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
        if let Some(v) = sealed_vector_ref(seg, id) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::lifecycle::VectorCollection;
    use crate::hnsw::HnswParams;
    use crate::test_support::{build_now, seeded_vectors, test_memory};

    #[test]
    fn the_code_stage_cuts_the_beam_to_the_code_nearest_pool() {
        let params = HnswParams {
            metric: DistanceMetric::L2,
            ..HnswParams::default()
        };
        let mut coll = VectorCollection::with_seal_threshold(8, params, 200);
        for v in seeded_vectors(200, 8, 7) {
            coll.insert(v).unwrap();
        }
        let req = coll.seal("k", test_memory()).unwrap();
        assert!(
            coll.complete_build(req.segment_id, build_now(&req))
                .installed()
        );
        let seg = &coll.sealed[0];
        let SegmentCodec::Sq8 { codec, codes } = &seg.codec else {
            panic!("the default plan seals an SQ8 segment");
        };
        let breadth = SearchBreadth {
            ef_search: 64,
            oversample: 2,
        };
        let query = seeded_vectors(1, 8, 99).remove(0);
        let rows = CodeRows::Sq8 { codec, codes };

        let ids = code_stage(seg, rows, &query, 5, breadth, DistanceMetric::L2).unwrap();

        let pool = breadth.rerank_pool(5);
        assert_eq!(ids.len(), pool, "the exact rerank reads the pool only");
        let beam = breadth.beam(5);
        let candidates = seg.index.search(&query, beam, beam).unwrap();
        assert!(candidates.len() > pool, "the graph yields a wider beam");
        let mut by_code: Vec<(u32, f32)> = candidates
            .iter()
            .map(|r| {
                let code = code_row(codes, codec.dim(), r.id, seg.base_id).unwrap();
                (r.id, sq8_score(codec, &query, code, DistanceMetric::L2))
            })
            .collect();
        by_code.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
        let mut want: Vec<u32> = by_code.iter().take(pool).map(|(id, _)| *id).collect();
        let mut got = ids.clone();
        want.sort_unstable();
        got.sort_unstable();
        assert_eq!(got, want, "the code distance picks the pool");

        let hits = search_sealed(seg, &query, 5, breadth, DistanceMetric::L2).unwrap();
        assert_eq!(hits.len(), 5);
        for hit in hits {
            let v = sealed_vector_ref(seg, hit.id).unwrap();
            assert_eq!(hit.distance, distance(&query, &v, DistanceMetric::L2));
        }
    }
}
