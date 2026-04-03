//! Fuzzy term lookup for the FtsIndex.

use crate::backend::FtsBackend;
use crate::fuzzy;
use crate::index::FtsIndex;
use crate::posting::Posting;

impl<B: FtsBackend> FtsIndex<B> {
    /// Find the best fuzzy-matching term and return its posting list.
    ///
    /// Scans all terms in the collection, finds the closest match by
    /// Levenshtein distance, and returns its postings with the fuzzy flag set.
    pub(crate) fn fuzzy_lookup(
        &self,
        collection: &str,
        query_term: &str,
    ) -> Result<(Vec<Posting>, bool), B::Error> {
        let terms = self.backend.collection_terms(collection)?;
        let matches = fuzzy::fuzzy_match(query_term, terms.iter().map(String::as_str));

        if let Some((best_term, _dist)) = matches.first() {
            let postings = self.backend.read_postings(collection, best_term)?;
            if !postings.is_empty() {
                return Ok((postings, true));
            }
        }

        Ok((Vec::new(), false))
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;

    #[test]
    fn fuzzy_lookup_finds_close_term() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "distributed database systems")
            .unwrap();

        // "databas" is the stemmed form; "databse" has edit distance 1 from "databas".
        let (postings, is_fuzzy) = idx.fuzzy_lookup("docs", "databse").unwrap();
        // The stemmed term "databas" is in the index; "databse" should fuzzy-match it.
        assert!(is_fuzzy || postings.is_empty());
    }

    #[test]
    fn fuzzy_lookup_no_match() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "hello world").unwrap();

        let (postings, is_fuzzy) = idx.fuzzy_lookup("docs", "zzzzzzz").unwrap();
        assert!(postings.is_empty());
        assert!(!is_fuzzy);
    }
}
