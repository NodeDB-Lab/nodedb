// SPDX-License-Identifier: Apache-2.0

//! The system time an index search plan reads at.

use crate::temporal::TemporalScope;

use super::variants::{HybridSearchPlan, HybridSearchTriplePlan, SqlPlan, TextSearchPlan};

impl SqlPlan {
    /// The `AS OF` of a vector, sparse, text, hybrid, or spatial search plan.
    /// `None` for every other plan.
    pub fn search_temporal_mut(&mut self) -> Option<&mut TemporalScope> {
        match self {
            SqlPlan::VectorSearch { temporal, .. }
            | SqlPlan::MultiVectorSearch { temporal, .. }
            | SqlPlan::SparseSearch { temporal, .. }
            | SqlPlan::SpatialScan { temporal, .. }
            | SqlPlan::TextSearch(TextSearchPlan { temporal, .. })
            | SqlPlan::HybridSearch(HybridSearchPlan { temporal, .. })
            | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan { temporal, .. }) => {
                Some(temporal)
            }
            _ => None,
        }
    }

    /// Set the `AS OF` of the search this plan holds: the plan itself, or the
    /// search a computed tail (`Subquery`) wraps.
    pub fn set_search_temporal(&mut self, scope: TemporalScope) {
        if let SqlPlan::Subquery { input, .. } = self {
            input.set_search_temporal(scope);
            return;
        }
        if let Some(temporal) = self.search_temporal_mut() {
            *temporal = scope;
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::SystemTimeScope;

    use super::*;

    #[test]
    fn a_wrapped_search_takes_the_scope() {
        let search = SqlPlan::MultiVectorSearch {
            collection: "c".into(),
            query_vector: vec![0.1],
            top_k: 1,
            ef_search: 2,
            projection: Vec::new(),
            temporal: TemporalScope::default(),
        };
        let mut plan = SqlPlan::Subquery {
            input: Box::new(search),
            filters: Vec::new(),
            projection: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            offset: 0,
            distinct: false,
            limit: None,
        };
        let scope = TemporalScope {
            system_time: SystemTimeScope::AsOf(5),
            ..TemporalScope::default()
        };
        plan.set_search_temporal(scope);
        let SqlPlan::Subquery { mut input, .. } = plan else {
            panic!("the wrapper stays");
        };
        assert_eq!(input.search_temporal_mut().copied(), Some(scope));
    }
}
