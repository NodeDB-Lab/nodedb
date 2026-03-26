//! Global IDF computation for distributed BM25 text search.
//!
//! BM25 scoring depends on IDF (Inverse Document Frequency) — how rare a
//! term is across the ENTIRE corpus. When documents are sharded, each shard
//! only knows its local DF. Scores from different shards are incomparable
//! without global IDF.
//!
//! Two-phase scatter-gather:
//! 1. **Phase 1 (DF collection)**: Ask all shards for local document
//!    frequencies and total doc counts for the search terms.
//! 2. **Coordinator**: Compute global IDF from aggregated DFs.
//! 3. **Phase 2 (Scored search)**: Send global IDF to all shards. Each
//!    shard computes BM25 with the shared IDF, returns its local top-K.
//! 4. **Coordinator**: Merge-sort by BM25 score, return global top-K.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Per-shard document frequency report (Phase 1 response).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardDfReport {
    pub shard_id: u16,
    /// Total documents on this shard.
    pub total_docs: u64,
    /// Per-term document frequency: `term → count of docs containing term`.
    pub term_dfs: HashMap<String, u64>,
}

/// Global IDF values computed from all shard DF reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalIdf {
    /// Total documents across all shards.
    pub total_docs: u64,
    /// Per-term IDF: `term → idf_score`.
    pub term_idfs: HashMap<String, f64>,
}

/// A scored search hit from a shard (Phase 2 response).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredHit {
    pub doc_id: String,
    pub bm25_score: f64,
    pub shard_id: u16,
}

/// Coordinator for 2-phase distributed BM25.
pub struct GlobalIdfCoordinator {
    /// Search terms for this query.
    terms: Vec<String>,
    /// Collected DF reports from shards.
    df_reports: Vec<ShardDfReport>,
    /// Number of shards expected.
    expected_shards: usize,
    /// Computed global IDF (available after Phase 1 complete).
    global_idf: Option<GlobalIdf>,
}

impl GlobalIdfCoordinator {
    pub fn new(terms: Vec<String>, expected_shards: usize) -> Self {
        Self {
            terms,
            df_reports: Vec::with_capacity(expected_shards),
            expected_shards,
            global_idf: None,
        }
    }

    // -- Phase 1: Collect DFs --

    /// Record a shard's DF report.
    pub fn add_df_report(&mut self, report: ShardDfReport) {
        self.df_reports.push(report);
    }

    /// Whether all shards have reported Phase 1.
    pub fn phase1_complete(&self) -> bool {
        self.df_reports.len() >= self.expected_shards
    }

    /// Compute global IDF from all shard DF reports.
    ///
    /// Call this after `phase1_complete()` returns true.
    /// Uses the standard BM25 IDF formula:
    /// `idf(t) = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)`
    /// where N = total docs, df(t) = docs containing term t.
    pub fn compute_global_idf(&mut self) -> &GlobalIdf {
        let total_docs: u64 = self.df_reports.iter().map(|r| r.total_docs).sum();

        let mut global_dfs: HashMap<String, u64> = HashMap::new();
        for report in &self.df_reports {
            for (term, &df) in &report.term_dfs {
                *global_dfs.entry(term.clone()).or_insert(0) += df;
            }
        }

        let mut term_idfs = HashMap::new();
        let n = total_docs as f64;
        for term in &self.terms {
            let df = *global_dfs.get(term).unwrap_or(&0) as f64;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            term_idfs.insert(term.clone(), idf);
        }

        self.global_idf = Some(GlobalIdf {
            total_docs,
            term_idfs,
        });
        // Safety: we just assigned Some above.
        match &self.global_idf {
            Some(idf) => idf,
            None => unreachable!(),
        }
    }

    /// Get the computed global IDF (None if Phase 1 not complete).
    pub fn global_idf(&self) -> Option<&GlobalIdf> {
        self.global_idf.as_ref()
    }

    // -- Phase 2: Merge scored results --

    /// Merge scored hits from all shards, return global top-K by BM25 score.
    pub fn merge_scored_hits(shard_results: &[Vec<ScoredHit>], top_k: usize) -> Vec<ScoredHit> {
        let mut all_hits: Vec<ScoredHit> = shard_results
            .iter()
            .flat_map(|r| r.iter().cloned())
            .collect();
        all_hits.sort_by(|a, b| {
            b.bm25_score
                .partial_cmp(&a.bm25_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all_hits.truncate(top_k);
        all_hits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_idf_two_shards() {
        let mut coord = GlobalIdfCoordinator::new(vec!["rust".into(), "database".into()], 2);

        coord.add_df_report(ShardDfReport {
            shard_id: 0,
            total_docs: 1000,
            term_dfs: HashMap::from([("rust".into(), 50), ("database".into(), 200)]),
        });
        coord.add_df_report(ShardDfReport {
            shard_id: 1,
            total_docs: 1000,
            term_dfs: HashMap::from([("rust".into(), 30), ("database".into(), 300)]),
        });

        assert!(coord.phase1_complete());
        let idf = coord.compute_global_idf();

        assert_eq!(idf.total_docs, 2000);
        // "rust": df=80, N=2000 → idf = ln((2000-80+0.5)/(80+0.5)+1) ≈ 3.2
        assert!(idf.term_idfs["rust"] > 3.0);
        // "database": df=500, N=2000 → idf = ln((2000-500+0.5)/(500+0.5)+1) ≈ 1.4
        assert!(idf.term_idfs["database"] > 1.0);
        assert!(idf.term_idfs["database"] < idf.term_idfs["rust"]); // "rust" is rarer.
    }

    #[test]
    fn merge_scored_hits() {
        let shard_a = vec![
            ScoredHit {
                doc_id: "a1".into(),
                bm25_score: 5.0,
                shard_id: 0,
            },
            ScoredHit {
                doc_id: "a2".into(),
                bm25_score: 3.0,
                shard_id: 0,
            },
        ];
        let shard_b = vec![
            ScoredHit {
                doc_id: "b1".into(),
                bm25_score: 4.5,
                shard_id: 1,
            },
            ScoredHit {
                doc_id: "b2".into(),
                bm25_score: 2.0,
                shard_id: 1,
            },
        ];

        let merged = GlobalIdfCoordinator::merge_scored_hits(&[shard_a, shard_b], 3);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].doc_id, "a1"); // score 5.0
        assert_eq!(merged[1].doc_id, "b1"); // score 4.5
        assert_eq!(merged[2].doc_id, "a2"); // score 3.0
    }

    #[test]
    fn rare_term_has_higher_idf() {
        let mut coord = GlobalIdfCoordinator::new(vec!["rare".into(), "common".into()], 1);
        coord.add_df_report(ShardDfReport {
            shard_id: 0,
            total_docs: 10_000,
            term_dfs: HashMap::from([("rare".into(), 5), ("common".into(), 9000)]),
        });
        let idf = coord.compute_global_idf();
        assert!(idf.term_idfs["rare"] > idf.term_idfs["common"]);
    }
}
