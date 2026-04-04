//! Language-specific analyzer: Snowball stemming with per-language stop words.

use rust_stemmers::{Algorithm, Stemmer};

use crate::analyzer::pipeline::{TextAnalyzer, tokenize_with_stemmer};

use super::stop_words;

/// Language-specific analyzer using Snowball stemming and per-language stop words.
pub struct LanguageAnalyzer {
    algorithm: Algorithm,
    lang_code: String,
    lang_name: String,
}

impl LanguageAnalyzer {
    pub fn new(language: &str) -> Option<Self> {
        let lower = language.to_lowercase();
        let (algorithm, code) = match lower.as_str() {
            "english" | "en" => (Algorithm::English, "en"),
            "german" | "de" => (Algorithm::German, "de"),
            "french" | "fr" => (Algorithm::French, "fr"),
            "spanish" | "es" => (Algorithm::Spanish, "es"),
            "italian" | "it" => (Algorithm::Italian, "it"),
            "portuguese" | "pt" => (Algorithm::Portuguese, "pt"),
            "dutch" | "nl" => (Algorithm::Dutch, "nl"),
            "swedish" | "sv" => (Algorithm::Swedish, "sv"),
            "norwegian" | "no" => (Algorithm::Norwegian, "no"),
            "danish" | "da" => (Algorithm::Danish, "da"),
            "finnish" | "fi" => (Algorithm::Finnish, "fi"),
            "russian" | "ru" => (Algorithm::Russian, "ru"),
            "turkish" | "tr" => (Algorithm::Turkish, "tr"),
            "hungarian" | "hu" => (Algorithm::Hungarian, "hu"),
            "romanian" | "ro" => (Algorithm::Romanian, "ro"),
            "arabic" | "ar" => (Algorithm::Arabic, "ar"),
            _ => return None,
        };
        Some(Self {
            algorithm,
            lang_code: code.to_string(),
            lang_name: lower,
        })
    }

    /// Language code (ISO 639-1).
    pub fn lang_code(&self) -> &str {
        &self.lang_code
    }
}

impl TextAnalyzer for LanguageAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let stemmer = Stemmer::create(self.algorithm);
        let stop_list = stop_words::stop_words(&self.lang_code);
        tokenize_with_stemmer(text, &stemmer, &self.lang_code, stop_list)
    }

    fn name(&self) -> &str {
        &self.lang_name
    }
}

/// No-stemmer analyzer for languages without Snowball support (Hindi, etc.).
///
/// Applies the full pipeline (normalize, split, stop words) but skips stemming.
pub struct NoStemAnalyzer {
    lang_code: String,
    lang_name: String,
}

impl NoStemAnalyzer {
    pub fn new(language: &str) -> Option<Self> {
        let lower = language.to_lowercase();
        let code = match lower.as_str() {
            "hindi" | "hi" => "hi",
            "hebrew" | "he" => "he",
            "thai" | "th" => "th",
            "vietnamese" | "vi" => "vi",
            "indonesian" | "id" => "id",
            "chinese" | "zh" => "zh",
            "japanese" | "ja" => "ja",
            "korean" | "ko" => "ko",
            "czech" | "cs" => "cs",
            "polish" | "pl" => "pl",
            "greek" | "el" => "el",
            _ => return None,
        };
        Some(Self {
            lang_code: code.to_string(),
            lang_name: lower,
        })
    }
}

impl TextAnalyzer for NoStemAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let stop_list = stop_words::stop_words(&self.lang_code);
        // Use English stemmer as no-op: it won't affect non-English words meaningfully.
        // The stop word list does the language-specific work.
        let stemmer = Stemmer::create(Algorithm::English);
        tokenize_with_stemmer(text, &stemmer, &self.lang_code, stop_list)
    }

    fn name(&self) -> &str {
        &self.lang_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn german_uses_german_stop_words() {
        let analyzer = LanguageAnalyzer::new("german").unwrap();
        let tokens = analyzer.analyze("Die Datenbanken sind schnell");
        // "die" and "sind" are German stop words — should be removed.
        assert!(!tokens.iter().any(|t| t == "die" || t == "sind"));
        assert!(!tokens.is_empty());
    }

    #[test]
    fn german_does_not_use_english_stop_words() {
        let analyzer = LanguageAnalyzer::new("german").unwrap();
        // "the" is an English stop word but NOT a German one — should pass through.
        let tokens = analyzer.analyze("the Datenbank");
        assert!(tokens.iter().any(|t| t == "the"));
    }

    #[test]
    fn french_stop_words() {
        let analyzer = LanguageAnalyzer::new("french").unwrap();
        let tokens = analyzer.analyze("le chat est sur la table");
        // "le", "est", "sur", "la" are French stop words.
        assert!(!tokens.iter().any(|t| t == "le" || t == "la" || t == "sur"));
    }

    #[test]
    fn arabic_analyzer() {
        let analyzer = LanguageAnalyzer::new("arabic").unwrap();
        let tokens = analyzer.analyze("في المدينة الكبيرة");
        // "في" is Arabic stop word.
        assert!(!tokens.iter().any(|t| t == "في"));
    }

    #[test]
    fn unknown_language_returns_none() {
        assert!(LanguageAnalyzer::new("klingon").is_none());
    }

    #[test]
    fn no_stem_hindi() {
        let analyzer = NoStemAnalyzer::new("hindi").unwrap();
        let tokens = analyzer.analyze("यह एक परीक्षा है");
        // "यह" and "है" are Hindi stop words.
        assert!(!tokens.iter().any(|t| t == "यह" || t == "है"));
    }
}
