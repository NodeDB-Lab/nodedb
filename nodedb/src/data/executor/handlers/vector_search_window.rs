// SPDX-License-Identifier: BUSL-1.1

//! Candidate window of a vector search.
//!
//! A search with a residual row filter (`rls_filters`: the statement's
//! `WHERE` conjuncts and any read policy) ranks more candidates than it
//! returns. The filter runs over the attached bodies and keeps the `top_k`
//! nearest survivors. A fixed over-fetch returns fewer than `top_k` rows
//! whenever the nearest candidates fail the filter. The window therefore
//! widens until `top_k` candidates pass the filter or the index holds no
//! further candidate, so the filter narrows the candidates before the top-k
//! cut.

use std::collections::HashMap;

use nodedb_query::scan_filter::ScanFilter;

use super::super::response_codec::VectorSearchHit;
use super::vector_search::{build_search_hit, effective_ef};
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::vector::collection::VectorCollection;
use crate::engine::vector::distance::DistanceMetric;

/// One ranking request against a vector index.
pub(super) struct SearchWindow<'a> {
    pub collection: &'a VectorCollection,
    pub query_vector: &'a [f32],
    pub ef_search: usize,
    /// Rerank candidates a codec stage keeps per requested row.
    pub oversample: u8,
    pub metric: DistanceMetric,
    /// Serialized candidate bitmap in the index's node-id space. A current
    /// search reads it.
    pub bitmap: Option<&'a [u8]>,
    /// The system-time ordinal a search at a cut reads, with its candidate
    /// node ids at the cut. `None` ranks the live nodes.
    pub cut: Option<(i64, Option<&'a roaring::RoaringBitmap>)>,
}

/// Where the bodies of ranked candidates are read from.
pub(super) struct BodySource<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// Attach each candidate's body to its hit.
    pub attach: bool,
    /// The system-time ordinal the bodies are read at. `None` reads the
    /// current version.
    pub at: Option<i64>,
    /// When set, a candidate carries only these fields of its body: the ones
    /// the residual filter reads. `None` attaches whole bodies.
    pub filter_columns: Option<&'a [String]>,
}

/// The ranked hits and the window width that produced them.
pub(super) struct WindowHits {
    pub hits: Vec<VectorSearchHit>,
    pub fetch_k: usize,
}

/// Candidates a first window asks for per wanted hit when the index scans
/// flat. A flat scan ranks every vector whatever the window, so the width
/// costs only the bodies it attaches, and one pass covers a filter that
/// passes one candidate in this many.
const FLAT_WINDOW_PER_HIT: usize = 20;

/// Candidates a first window asks for per wanted hit when a residual filter
/// runs over the bodies.
pub(super) const RESIDUAL_WINDOW_PER_HIT: usize = 2;

/// Smallest first window of a search with a residual filter.
pub(super) const MIN_RESIDUAL_WINDOW: usize = 20;

/// The first window of a search for `top_k` hits. A residual filter starts
/// it wider, and `rank_window` widens it until `top_k` candidates pass.
/// Rerank breadth is the collection's concern, so the window ignores
/// `oversample`.
pub(super) fn first_window(top_k: usize, has_residual: bool) -> usize {
    if has_residual {
        top_k
            .saturating_mul(RESIDUAL_WINDOW_PER_HIT)
            .max(MIN_RESIDUAL_WINDOW)
    } else {
        top_k
    }
}

impl CoreLoop {
    /// Rank `fetch_k` candidates and attach their bodies.
    ///
    /// With a non-empty `residual` and no candidate bitmap, the first window
    /// is at least as wide as the search beam, since the beam ranks that many
    /// candidates whatever the window. The window then widens until `top_k`
    /// hits pass it or the index returns fewer candidates than the window
    /// asked for. A hit passes when it has a body and every filter matches
    /// that body. The next width follows the pass rate seen so far, and a hit
    /// already ranked keeps the body it was given, so a widening ranks each
    /// candidate's body once.
    pub(super) fn rank_window(
        &self,
        window: &SearchWindow<'_>,
        bodies: &BodySource<'_>,
        residual: &[ScanFilter],
        top_k: usize,
        mut fetch_k: usize,
    ) -> crate::Result<WindowHits> {
        // A search at a cut can read ended nodes, so every node held bounds
        // its window.
        let live = match window.cut {
            None => window.collection.live_count(),
            Some(_) => window.collection.len(),
        };
        let crs = self.crs_context();
        if !residual.is_empty() {
            if window.bitmap.is_none() {
                fetch_k = fetch_k.max(effective_ef(window.ef_search, fetch_k));
            }
            if window.collection.scans_flat_only() {
                fetch_k = fetch_k.max(top_k.saturating_mul(FLAT_WINDOW_PER_HIT));
            }
        }
        let mut ranked_before: HashMap<u32, VectorSearchHit> = HashMap::new();
        loop {
            let breadth = nodedb_vector::rerank::SearchBreadth {
                ef_search: effective_ef(window.ef_search, fetch_k),
                oversample: window.oversample,
            };
            let ranked = match (window.cut, window.bitmap) {
                (Some((at, allow)), _) => window.collection.search_at(
                    window.query_vector,
                    fetch_k,
                    breadth,
                    window.metric,
                    allow,
                    at,
                ),
                (None, Some(bitmap)) => window.collection.search_with_bitmap_bytes_and_metric(
                    window.query_vector,
                    fetch_k,
                    breadth,
                    bitmap,
                    window.metric,
                ),
                (None, None) => window.collection.search_with_metric(
                    window.query_vector,
                    fetch_k,
                    breadth,
                    window.metric,
                ),
            }
            .map_err(crate::Error::from)?;
            let exhausted = ranked.len() < fetch_k || fetch_k >= live;
            let mut hits = Vec::with_capacity(ranked.len());
            for r in &ranked {
                let hit = match ranked_before.remove(&r.id) {
                    Some(hit) => hit,
                    None => {
                        let built = build_search_hit(Some(window.collection), r.id, r.distance);
                        match bodies.filter_columns {
                            Some(columns) => self.attach_filter_body(
                                bodies.database_id,
                                bodies.tid,
                                bodies.collection,
                                columns,
                                bodies.at,
                                built,
                            )?,
                            None => self.attach_body(
                                bodies.database_id,
                                bodies.tid,
                                bodies.collection,
                                bodies.attach,
                                bodies.at,
                                built,
                            )?,
                        }
                    }
                };
                hits.push(hit);
            }
            if residual.is_empty() || exhausted {
                return Ok(WindowHits { hits, fetch_k });
            }
            let passed = passing(&hits, residual, &crs);
            if passed >= top_k {
                return Ok(WindowHits { hits, fetch_k });
            }
            fetch_k = wider_window(fetch_k, hits.len(), passed, top_k);
            ranked_before = ranked.iter().map(|r| r.id).zip(hits).collect();
        }
    }
}

/// The width of the next window: the one at which the pass rate seen so far
/// yields `top_k` hits, with half again as much headroom. A window that
/// passed nothing doubles. Growth is at most eightfold per step and always
/// at least one candidate.
fn wider_window(fetch_k: usize, ranked: usize, passed: usize, top_k: usize) -> usize {
    let grown = if passed == 0 {
        fetch_k.saturating_mul(2)
    } else {
        ranked
            .saturating_mul(top_k)
            .div_ceil(passed)
            .saturating_mul(3)
            / 2
    };
    grown
        .min(fetch_k.saturating_mul(8))
        .max(fetch_k.saturating_add(1))
}

/// Keep the hits whose body passes `residual`, nearest first, cut to `top_k`.
/// The Control Plane applies the same filter and cut to the merged hits of
/// every core, so a core that drops its failing hits sends it no fewer rows
/// than it keeps.
pub(super) fn keep_passing(
    hits: &mut Vec<VectorSearchHit>,
    residual: &[ScanFilter],
    crs: &nodedb_crs::CrsContext<'_>,
    top_k: usize,
) {
    hits.retain(|hit| {
        hit.body
            .as_deref()
            .is_some_and(|body| ScanFilter::all_match_binary(residual, body, crs).unwrap_or(false))
    });
    hits.truncate(top_k);
}

/// Hits the Control Plane keeps under `residual`. A filter that fails to
/// evaluate against a body drops that hit there, so it does not count here.
pub(super) fn passing(
    hits: &[VectorSearchHit],
    residual: &[ScanFilter],
    crs: &nodedb_crs::CrsContext<'_>,
) -> usize {
    hits.iter()
        .filter(|hit| {
            hit.body.as_deref().is_some_and(|body| {
                ScanFilter::all_match_binary(residual, body, crs).unwrap_or(false)
            })
        })
        .count()
}
