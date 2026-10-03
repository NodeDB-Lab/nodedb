// SPDX-License-Identifier: Apache-2.0

//! Per-collection analyzer configuration stored in backend metadata.
//!
//! Uses structural meta blobs of the collection's whole-document index
//! (`IndexScope::document(collection)`), shared by every field index:
//! - `subkey = "analyzer"` → analyzer name (e.g. "german", "standard")
//! - `subkey = "language"` → lang code (e.g. "de", "ja")
//! - `subkey = "fuzzy"` → `"1"` when the collection defaults to fuzzy matching
//!
//! Applied automatically at both index time and query time.

use crate::analyzer::language::stemmer::{LanguageAnalyzer, NoStemAnalyzer};
use crate::analyzer::pipeline::{TextAnalyzer, analyze};
use crate::analyzer::standard::StandardAnalyzer;
use crate::backend::FtsBackend;
use crate::index::FtsIndex;
use crate::scope::IndexScope;

impl<B: FtsBackend> FtsIndex<B> {
    /// Set the analyzer for a collection. Persists to backend metadata.
    pub fn set_collection_analyzer(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        analyzer_name: &str,
    ) -> Result<(), B::Error> {
        self.backend.write_meta(
            database_id,
            tid,
            IndexScope::document(collection),
            "analyzer",
            analyzer_name.as_bytes(),
        )
    }

    /// Set the language for a collection. Persists to backend metadata.
    pub fn set_collection_language(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        lang_code: &str,
    ) -> Result<(), B::Error> {
        self.backend.write_meta(
            database_id,
            tid,
            IndexScope::document(collection),
            "language",
            lang_code.as_bytes(),
        )
    }

    /// Set whether searches over this collection fall back to fuzzy
    /// (Levenshtein) matching by default. Persists to backend metadata.
    pub fn set_collection_fuzzy(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        fuzzy: bool,
    ) -> Result<(), B::Error> {
        self.backend.write_meta(
            database_id,
            tid,
            IndexScope::document(collection),
            "fuzzy",
            if fuzzy { b"1" } else { b"0" },
        )
    }

    /// Whether this collection defaults to fuzzy matching. `false` when unset.
    pub fn get_collection_fuzzy(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Result<bool, B::Error> {
        Ok(self
            .backend
            .read_meta(database_id, tid, IndexScope::document(collection), "fuzzy")?
            .is_some_and(|bytes| bytes.as_slice() == b"1"))
    }

    /// Get the configured analyzer name for a collection.
    pub fn get_collection_analyzer(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Result<Option<String>, B::Error> {
        match self.backend.read_meta(
            database_id,
            tid,
            IndexScope::document(collection),
            "analyzer",
        )? {
            Some(bytes) => Ok(std::str::from_utf8(&bytes).ok().map(String::from)),
            None => Ok(None),
        }
    }

    /// Get the configured language for a collection.
    pub fn get_collection_language(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Result<Option<String>, B::Error> {
        match self.backend.read_meta(
            database_id,
            tid,
            IndexScope::document(collection),
            "language",
        )? {
            Some(bytes) => Ok(std::str::from_utf8(&bytes).ok().map(String::from)),
            None => Ok(None),
        }
    }

    /// Analyze text using the collection's configured analyzer.
    ///
    /// Falls back to the standard English analyzer if no analyzer is configured.
    pub fn analyze_for_collection(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        text: &str,
    ) -> Result<Vec<String>, B::Error> {
        let analyzer_name = self.get_collection_analyzer(database_id, tid, collection)?;
        match analyzer_name.as_deref() {
            Some(name) => Ok(resolve_analyzer(name).analyze(text)),
            None => Ok(analyze(text)),
        }
    }

    /// Tokenize text WITHOUT stemming for fuzzy matching.
    pub fn tokenize_raw_for_collection(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        text: &str,
    ) -> Result<Vec<String>, B::Error> {
        let lang = self.get_collection_language(database_id, tid, collection)?;
        let lang_code = lang.as_deref().unwrap_or("en");
        let stop_list = crate::analyzer::language::stop_words::stop_words(lang_code);
        Ok(crate::analyzer::pipeline::tokenize_raw(
            text, lang_code, stop_list,
        ))
    }
}

/// Whether `name` resolves to a real analyzer.
///
/// [`resolve_analyzer`] falls back to the standard analyzer for anything it
/// does not know, which silently turns a misspelled analyzer name into a
/// different analyzer. Callers that accept a name from a user — DDL, config —
/// gate on this first so the fallback is only ever reached for a name that
/// was already checked. The two must stay in step: every arm below has a
/// matching arm there.
pub fn analyzer_exists(name: &str) -> bool {
    name == "standard"
        || LanguageAnalyzer::new(name).is_some()
        || NoStemAnalyzer::new(name).is_some()
}

/// Resolve an analyzer name to a `Box<dyn TextAnalyzer>`.
fn resolve_analyzer(name: &str) -> Box<dyn TextAnalyzer> {
    match name {
        "standard" => Box::new(StandardAnalyzer),
        _ => {
            if let Some(a) = LanguageAnalyzer::new(name) {
                Box::new(a)
            } else if let Some(a) = NoStemAnalyzer::new(name) {
                Box::new(a)
            } else {
                Box::new(StandardAnalyzer)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::test_support::test_governor;

    const DB: u64 = 0;
    const T: u64 = 1;

    #[test]
    fn default_analyzer() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        let tokens = idx
            .analyze_for_collection(DB, T, "col", "The quick brown fox")
            .unwrap();
        assert!(tokens.contains(&"quick".to_string()));
        assert!(!tokens.contains(&"the".to_string()));
    }

    #[test]
    fn configured_german_analyzer() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.set_collection_analyzer(DB, T, "col", "german").unwrap();

        let tokens = idx
            .analyze_for_collection(DB, T, "col", "Die Datenbanken sind schnell")
            .unwrap();
        assert!(!tokens.iter().any(|t| t == "die" || t == "sind"));
        assert!(!tokens.is_empty());
    }

    #[test]
    fn configured_hindi_no_stem() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.set_collection_analyzer(DB, T, "col", "hindi").unwrap();

        let tokens = idx
            .analyze_for_collection(DB, T, "col", "यह एक परीक्षा है")
            .unwrap();
        assert!(!tokens.iter().any(|t| t == "यह" || t == "है"));
    }

    #[test]
    fn analyzer_persists() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.set_collection_analyzer(DB, T, "col", "french").unwrap();
        idx.set_collection_language(DB, T, "col", "fr").unwrap();

        assert_eq!(
            idx.get_collection_analyzer(DB, T, "col")
                .unwrap()
                .as_deref(),
            Some("french")
        );
        assert_eq!(
            idx.get_collection_language(DB, T, "col")
                .unwrap()
                .as_deref(),
            Some("fr")
        );
    }
}
