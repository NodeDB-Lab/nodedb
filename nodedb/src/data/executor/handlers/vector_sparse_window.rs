// SPDX-License-Identifier: BUSL-1.1

//! Candidate window of a sparse vector search.
//!
//! A sparse search with a residual row filter (the statement's `WHERE`
//! conjuncts) ranks more candidates than it returns, as a dense vector search
//! does. Each candidate carries its body. The Control Plane evaluates the
//! filter over the bodies and keeps the `top_k` highest-scoring survivors.
//! The window doubles until `top_k` candidates pass the filter or the index
//! holds no further candidate, so the filter narrows the candidates before
//! the top-k cut and a matching row is never cut off by a fixed window.

use nodedb_query::scan_filter::ScanFilter;

use super::super::response_codec::VectorSearchHit;
use super::hybrid_key::HybridFusionKey;
use super::vector_search_window::passing;
use crate::engine::vector::sparse::SparseSearchResult;

/// The first window of a filtered search: room for `top_k` survivors when
/// some candidates fail the filter.
fn initial_window(top_k: usize, residual: &[ScanFilter]) -> usize {
    if residual.is_empty() {
        top_k
    } else {
        top_k.saturating_mul(2).max(20)
    }
}

/// The hit a ranked sparse result becomes. The index keys documents by
/// their hex surrogate, so the hit `id` is that surrogate's storage key.
/// A `doc_id` that is not a hex surrogate falls back to the internal id.
pub(super) fn sparse_hit(result: &SparseSearchResult) -> VectorSearchHit {
    let id = result
        .doc_id
        .as_deref()
        .and_then(nodedb_types::StorageKey::parse)
        .map(HybridFusionKey::Bound)
        .unwrap_or(HybridFusionKey::Headless(result.internal_id));
    VectorSearchHit {
        id,
        distance: result.score,
        doc_id: None,
        body: None,
    }
}

/// Rank sparse candidates for `top_k` survivors of `residual`.
///
/// `rank(k)` returns the `k` highest-scoring candidates, best first.
/// `attach` reads a candidate's body. Every candidate carries its body, so
/// the projection can read the row's columns. Without a residual the first
/// `top_k` candidates return as they rank. With one, the window doubles
/// until `top_k` candidates pass or `rank` returns fewer than the window
/// asked for. The hits are not filtered here: the Control Plane applies the
/// filter and the cut.
pub(super) fn rank_sparse_window<R, A>(
    top_k: usize,
    residual: &[ScanFilter],
    crs: &nodedb_crs::CrsContext<'_>,
    rank: R,
    attach: A,
) -> crate::Result<Vec<VectorSearchHit>>
where
    R: Fn(usize) -> Vec<SparseSearchResult>,
    A: Fn(VectorSearchHit) -> crate::Result<VectorSearchHit>,
{
    let mut fetch_k = initial_window(top_k, residual);
    loop {
        let ranked = rank(fetch_k);
        let exhausted = ranked.len() < fetch_k;
        let hits: Vec<VectorSearchHit> = ranked
            .iter()
            .map(|result| attach(sparse_hit(result)))
            .collect::<crate::Result<_>>()?;
        if residual.is_empty() || exhausted || passing(&hits, residual, crs) >= top_k {
            return Ok(hits);
        }
        fetch_k = fetch_k.saturating_mul(2);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use nodedb_query::scan_filter::FilterOp;
    use nodedb_types::Value;

    use super::*;

    /// Candidates `0..count`, scored highest first. Candidates below
    /// `keep_from` are in category `skip`, the rest in `keep`.
    struct Index {
        count: u32,
        keep_from: u32,
    }

    impl Index {
        fn rank(&self, k: usize) -> Vec<SparseSearchResult> {
            (0..self.count)
                .take(k)
                .map(|i| SparseSearchResult {
                    internal_id: i,
                    score: 1000.0 - i as f32,
                    doc_id: None,
                })
                .collect()
        }

        fn body(&self, hit: VectorSearchHit) -> crate::Result<VectorSearchHit> {
            let HybridFusionKey::Headless(i) = &hit.id else {
                panic!("test hits are headless");
            };
            let category = if *i < self.keep_from { "skip" } else { "keep" };
            let mut map = std::collections::HashMap::new();
            map.insert("category".to_string(), Value::String(category.into()));
            let body = nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode body");
            Ok(VectorSearchHit {
                body: Some(body),
                ..hit
            })
        }
    }

    fn keep_filter() -> Vec<ScanFilter> {
        vec![ScanFilter {
            field: "category".into(),
            op: FilterOp::Eq,
            value: Value::String("keep".into()),
            clauses: Vec::new(),
            expr: None,
        }]
    }

    /// The nearest candidates all fail the filter and outnumber the first
    /// window. The window widens until `top_k` matching candidates rank.
    #[test]
    fn the_window_widens_past_candidates_that_fail_the_filter() {
        let index = Index {
            count: 43,
            keep_from: 40,
        };
        let windows = Cell::new(0);
        let filter = keep_filter();
        let hits = rank_sparse_window(
            2,
            &filter,
            &nodedb_crs::CrsContext::builtin_only(),
            |k| {
                windows.set(windows.get() + 1);
                index.rank(k)
            },
            |hit| index.body(hit),
        )
        .expect("rank");
        assert!(windows.get() > 1, "the first window held no match");
        assert!(
            passing(&hits, &filter, &nodedb_crs::CrsContext::builtin_only()) >= 2,
            "two matching candidates rank in the final window"
        );
    }

    /// Fewer matches than `top_k` stop the widening once the index runs out.
    #[test]
    fn the_window_stops_when_the_index_is_exhausted() {
        let index = Index {
            count: 30,
            keep_from: 29,
        };
        let filter = keep_filter();
        let hits = rank_sparse_window(
            5,
            &filter,
            &nodedb_crs::CrsContext::builtin_only(),
            |k| index.rank(k),
            |hit| index.body(hit),
        )
        .expect("rank");
        assert_eq!(hits.len(), 30, "every candidate ranked");
        assert_eq!(
            passing(&hits, &filter, &nodedb_crs::CrsContext::builtin_only()),
            1
        );
    }

    /// Without a filter the search ranks `top_k` in one window, and each hit
    /// carries its body for the projection.
    #[test]
    fn an_unfiltered_search_ranks_top_k_with_bodies() {
        let index = Index {
            count: 10,
            keep_from: 0,
        };
        let windows = Cell::new(0);
        let hits = rank_sparse_window(
            3,
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
            |k| {
                windows.set(windows.get() + 1);
                index.rank(k)
            },
            |hit| index.body(hit),
        )
        .expect("rank");
        assert_eq!(windows.get(), 1);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().all(|hit| hit.body.is_some()));
    }
}
