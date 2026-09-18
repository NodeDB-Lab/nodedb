// SPDX-License-Identifier: Apache-2.0

//! Search-cell classification for plan variants.

use super::variants::SqlPlan;

impl SqlPlan {
    /// Whether this plan's rows carry the search cells `distance` and
    /// `_surrogate`.
    ///
    /// The three variants here lower to the vector-hit row shape
    /// (`classify_hit_shape` → `HitShape::Vector`, over `VectorOp::Search |
    /// MultiSearch | SparseSearch | MultiVectorScoreSearch`): the engine emits
    /// `{id: <surrogate>, distance, …}` rows, and the post-process arm resolves
    /// the surrogate to the user primary key.
    ///
    /// Hybrid fusion rows (`HybridSearch`, `HybridSearchTriple`) classify as
    /// `HitShape::Hybrid` and carry a `doc_id` plus a score alias instead, so
    /// they are not search-cell plans.
    pub fn carries_search_cells(&self) -> bool {
        matches!(
            self,
            SqlPlan::VectorSearch { .. }
                | SqlPlan::SparseSearch { .. }
                | SqlPlan::MultiVectorSearch { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal::TemporalScope;
    use crate::types::*;

    fn scan() -> SqlPlan {
        SqlPlan::Scan {
            collection: "c".into(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: TemporalScope::default(),
        }
    }

    #[test]
    fn only_the_three_vector_hit_variants_carry_the_cells() {
        assert!(
            SqlPlan::VectorSearch {
                collection: "c".into(),
                field: "e".into(),
                query_vector: vec![0.1],
                top_k: 1,
                ef_search: 2,
                metric: DistanceMetric::Cosine,
                filters: Vec::new(),
                array_prefilter: None,
                ann_options: VectorAnnOptions::default(),
                skip_payload_fetch: false,
                payload_filters: Vec::new(),
                projection: Vec::new(),
            }
            .carries_search_cells()
        );
        assert!(
            SqlPlan::SparseSearch {
                collection: "c".into(),
                field: "t".into(),
                query_entries: vec![(3, 1.0)],
                top_k: 1,
                projection: Vec::new(),
            }
            .carries_search_cells()
        );
        assert!(
            SqlPlan::MultiVectorSearch {
                collection: "c".into(),
                query_vector: vec![0.1],
                top_k: 1,
                ef_search: 2,
                projection: Vec::new(),
            }
            .carries_search_cells()
        );
        assert!(!scan().carries_search_cells());
    }
}
