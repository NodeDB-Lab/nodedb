// SPDX-License-Identifier: BUSL-1.1

//! Candidate window of a vector search.
//!
//! A search with a residual row filter (`rls_filters`: the statement's
//! `WHERE` conjuncts and any read policy) ranks more candidates than it
//! returns. The Control Plane evaluates the filter over the attached bodies
//! and keeps the `top_k` nearest survivors. A fixed over-fetch returns fewer
//! than `top_k` rows whenever the nearest candidates fail the filter. The
//! window therefore doubles until `top_k` candidates pass the filter or the
//! index holds no further candidate, so the filter narrows the candidates
//! before the top-k cut.

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
    pub metric: DistanceMetric,
    /// Serialized candidate bitmap in the index's node-id space.
    pub bitmap: Option<&'a [u8]>,
}

/// Where the bodies of ranked candidates are read from.
pub(super) struct BodySource<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// Attach each candidate's body to its hit.
    pub attach: bool,
}

/// The ranked hits and the window width that produced them.
pub(super) struct WindowHits {
    pub hits: Vec<VectorSearchHit>,
    pub fetch_k: usize,
}

impl CoreLoop {
    /// Rank `fetch_k` candidates and attach their bodies.
    ///
    /// With a non-empty `residual`, the window doubles until `top_k` hits
    /// pass it or the index returns fewer candidates than the window asked
    /// for. A hit passes when the Control Plane will keep it: it has a body
    /// and every filter matches that body. The hits are not filtered here.
    pub(super) fn rank_window(
        &self,
        window: &SearchWindow<'_>,
        bodies: &BodySource<'_>,
        residual: &[ScanFilter],
        top_k: usize,
        mut fetch_k: usize,
    ) -> crate::Result<WindowHits> {
        let live = window.collection.live_count();
        loop {
            let ef = effective_ef(window.ef_search, fetch_k);
            let ranked = match window.bitmap {
                Some(bitmap) => window.collection.search_with_bitmap_bytes_and_metric(
                    window.query_vector,
                    fetch_k,
                    ef,
                    bitmap,
                    window.metric,
                ),
                None => window.collection.search_with_metric(
                    window.query_vector,
                    fetch_k,
                    ef,
                    window.metric,
                ),
            }
            .map_err(crate::Error::from)?;
            let exhausted = ranked.len() < fetch_k || fetch_k >= live;
            let hits = ranked
                .iter()
                .map(|r| build_search_hit(Some(window.collection), r.id, r.distance))
                .map(|hit| {
                    self.attach_body(
                        bodies.database_id,
                        bodies.tid,
                        bodies.collection,
                        bodies.attach,
                        hit,
                    )
                })
                .collect::<crate::Result<Vec<_>>>()?;
            if residual.is_empty() || exhausted || passing(&hits, residual) >= top_k {
                return Ok(WindowHits { hits, fetch_k });
            }
            fetch_k = fetch_k.saturating_mul(2);
        }
    }
}

/// Hits the Control Plane keeps under `residual`. A filter that fails to
/// evaluate against a body drops that hit there, so it does not count here.
fn passing(hits: &[VectorSearchHit], residual: &[ScanFilter]) -> usize {
    hits.iter()
        .filter(|hit| {
            hit.body
                .as_deref()
                .is_some_and(|body| ScanFilter::all_match_binary(residual, body).unwrap_or(false))
        })
        .count()
}
