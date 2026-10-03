// SPDX-License-Identifier: Apache-2.0

//! Fuzzy term candidates for the FtsIndex.

use crate::backend::FtsBackend;
use crate::fuzzy;
use crate::index::FtsIndex;
use crate::lsm::query as lsm_query;
use crate::scope::IndexScope;

impl<B: FtsBackend> FtsIndex<B> {
    /// Terms within fuzzy distance of `query_term`, best first. The
    /// vocabulary is every term of the index plus `extra_vocabulary` (the
    /// tokens of a transaction's staged documents).
    pub(crate) fn fuzzy_candidates(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        query_term: &str,
        extra_vocabulary: &[&str],
    ) -> Result<Vec<String>, B::Error> {
        let mut all_terms =
            lsm_query::collect_all_terms(&self.backend, database_id, tid, index, self.memtable())?;
        all_terms.extend(self.backend.collection_terms(database_id, tid, index)?);
        all_terms.extend(extra_vocabulary.iter().map(|t| (*t).to_string()));
        all_terms.sort();
        all_terms.dedup();

        Ok(
            fuzzy::fuzzy_match(query_term, all_terms.iter().map(String::as_str))
                .into_iter()
                .map(|(term, _dist)| term.to_string())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::test_support::test_governor;
    use nodedb_types::Surrogate;

    const DB: u64 = 0;
    const T: u64 = 1;

    #[test]
    fn fuzzy_candidates_find_close_term() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", Surrogate(1), "distributed database systems")
            .unwrap();

        let candidates = idx
            .fuzzy_candidates(DB, T, "docs".into(), "databse", &[])
            .unwrap();
        assert!(!candidates.is_empty(), "should find a fuzzy match");
    }

    #[test]
    fn fuzzy_candidates_no_match() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", Surrogate(1), "hello world")
            .unwrap();

        let candidates = idx
            .fuzzy_candidates(DB, T, "docs".into(), "zzzzzzz", &[])
            .unwrap();
        assert!(candidates.is_empty());
    }

    #[test]
    fn fuzzy_candidates_include_extra_vocabulary() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        let candidates = idx
            .fuzzy_candidates(DB, T, "docs".into(), "databse", &["databas"])
            .unwrap();
        assert_eq!(candidates, vec!["databas".to_string()]);
    }
}
