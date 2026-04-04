//! Per-collection analyzer configuration stored in backend metadata.
//!
//! Schema:
//! - `"{collection}:analyzer" → analyzer_name` (e.g., "german", "cjk_bigram")
//! - `"{collection}:language" → lang_code` (e.g., "de", "ja")
//!
//! Applied automatically at both index time and query time.

use crate::analyzer::language::stemmer::{LanguageAnalyzer, NoStemAnalyzer};
use crate::analyzer::pipeline::{TextAnalyzer, analyze};
use crate::analyzer::standard::StandardAnalyzer;
use crate::backend::FtsBackend;
use crate::index::FtsIndex;

impl<B: FtsBackend> FtsIndex<B> {
    /// Set the analyzer for a collection. Persists to backend metadata.
    pub fn set_collection_analyzer(
        &self,
        collection: &str,
        analyzer_name: &str,
    ) -> Result<(), B::Error> {
        let key = format!("{collection}:analyzer");
        self.backend.write_meta(&key, analyzer_name.as_bytes())
    }

    /// Set the language for a collection. Persists to backend metadata.
    pub fn set_collection_language(
        &self,
        collection: &str,
        lang_code: &str,
    ) -> Result<(), B::Error> {
        let key = format!("{collection}:language");
        self.backend.write_meta(&key, lang_code.as_bytes())
    }

    /// Get the configured analyzer name for a collection.
    pub fn get_collection_analyzer(&self, collection: &str) -> Result<Option<String>, B::Error> {
        let key = format!("{collection}:analyzer");
        match self.backend.read_meta(&key)? {
            Some(bytes) => Ok(std::str::from_utf8(&bytes).ok().map(String::from)),
            None => Ok(None),
        }
    }

    /// Get the configured language for a collection.
    pub fn get_collection_language(&self, collection: &str) -> Result<Option<String>, B::Error> {
        let key = format!("{collection}:language");
        match self.backend.read_meta(&key)? {
            Some(bytes) => Ok(std::str::from_utf8(&bytes).ok().map(String::from)),
            None => Ok(None),
        }
    }

    /// Analyze text using the collection's configured analyzer.
    ///
    /// Falls back to the standard English analyzer if no analyzer is configured.
    pub fn analyze_for_collection(
        &self,
        collection: &str,
        text: &str,
    ) -> Result<Vec<String>, B::Error> {
        let analyzer_name = self.get_collection_analyzer(collection)?;
        match analyzer_name.as_deref() {
            Some(name) => Ok(resolve_analyzer(name).analyze(text)),
            None => Ok(analyze(text)),
        }
    }
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

    #[test]
    fn default_analyzer() {
        let idx = FtsIndex::new(MemoryBackend::new());
        let tokens = idx
            .analyze_for_collection("col", "The quick brown fox")
            .unwrap();
        assert!(tokens.contains(&"quick".to_string()));
        assert!(!tokens.contains(&"the".to_string()));
    }

    #[test]
    fn configured_german_analyzer() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.set_collection_analyzer("col", "german").unwrap();

        let tokens = idx
            .analyze_for_collection("col", "Die Datenbanken sind schnell")
            .unwrap();
        assert!(!tokens.iter().any(|t| t == "die" || t == "sind"));
        assert!(!tokens.is_empty());
    }

    #[test]
    fn configured_hindi_no_stem() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.set_collection_analyzer("col", "hindi").unwrap();

        let tokens = idx.analyze_for_collection("col", "यह एक परीक्षा है").unwrap();
        assert!(!tokens.iter().any(|t| t == "यह" || t == "है"));
    }

    #[test]
    fn analyzer_persists() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.set_collection_analyzer("col", "french").unwrap();
        idx.set_collection_language("col", "fr").unwrap();

        assert_eq!(
            idx.get_collection_analyzer("col").unwrap().as_deref(),
            Some("french")
        );
        assert_eq!(
            idx.get_collection_language("col").unwrap().as_deref(),
            Some("fr")
        );
    }
}
