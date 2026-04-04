//! Per-language stop word lists with O(log n) binary search lookup.
//!
//! All lists are compiled-in `static` sorted arrays (~50KB total).
//! Dispatch by ISO 639-1 code or full language name.

mod asian;
mod eastern_european;
mod european;
mod semitic;

/// Get the stop word list for a language (ISO 639-1 code or full name).
///
/// Returns an empty slice for unknown languages (no stop words removed).
pub fn stop_words(lang: &str) -> &'static [&'static str] {
    match lang {
        "en" | "english" => european::ENGLISH,
        "de" | "german" => european::GERMAN,
        "fr" | "french" => european::FRENCH,
        "es" | "spanish" => european::SPANISH,
        "it" | "italian" => european::ITALIAN,
        "pt" | "portuguese" => european::PORTUGUESE,
        "nl" | "dutch" => european::DUTCH,
        "sv" | "swedish" => european::SWEDISH,
        "no" | "norwegian" => european::NORWEGIAN,
        "da" | "danish" => european::DANISH,
        "fi" | "finnish" => european::FINNISH,
        "ro" | "romanian" => european::ROMANIAN,
        "ru" | "russian" => eastern_european::RUSSIAN,
        "tr" | "turkish" => eastern_european::TURKISH,
        "hu" | "hungarian" => eastern_european::HUNGARIAN,
        "cs" | "czech" => eastern_european::CZECH,
        "pl" | "polish" => eastern_european::POLISH,
        "el" | "greek" => eastern_european::GREEK,
        "ar" | "arabic" => semitic::ARABIC,
        "he" | "hebrew" => semitic::HEBREW,
        "hi" | "hindi" => asian::HINDI,
        "zh" | "chinese" => asian::CHINESE,
        "ja" | "japanese" => asian::JAPANESE,
        "ko" | "korean" => asian::KOREAN,
        "th" | "thai" => asian::THAI,
        "vi" | "vietnamese" => asian::VIETNAMESE,
        "id" | "indonesian" => asian::INDONESIAN,
        _ => &[],
    }
}

/// Check if a word is a stop word for the given language.
///
/// Uses binary search on the sorted static list for O(log n) lookup.
pub fn is_stop_word(lang: &str, word: &str) -> bool {
    stop_words(lang).binary_search(&word).is_ok()
}

/// Check if a word is an English stop word (convenience function).
pub fn is_stop_word_en(word: &str) -> bool {
    european::ENGLISH.binary_search(&word).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn english_stop_words() {
        assert!(is_stop_word("en", "the"));
        assert!(is_stop_word("english", "and"));
        assert!(!is_stop_word("en", "database"));
    }

    #[test]
    fn german_stop_words() {
        assert!(is_stop_word("de", "und"));
        assert!(is_stop_word("de", "der"));
        assert!(is_stop_word("de", "die"));
        assert!(!is_stop_word("de", "the")); // English stop word not in German list.
    }

    #[test]
    fn french_stop_words() {
        assert!(is_stop_word("fr", "le"));
        assert!(is_stop_word("fr", "les"));
        assert!(!is_stop_word("fr", "database"));
    }

    #[test]
    fn russian_stop_words() {
        assert!(is_stop_word("ru", "и"));
        assert!(is_stop_word("ru", "не"));
    }

    #[test]
    fn arabic_stop_words() {
        assert!(is_stop_word("ar", "في"));
        assert!(is_stop_word("ar", "من"));
    }

    #[test]
    fn unknown_language_returns_empty() {
        assert!(stop_words("klingon").is_empty());
        assert!(!is_stop_word("klingon", "anything"));
    }

    #[test]
    fn all_lists_sorted() {
        let langs = [
            "en", "de", "fr", "es", "it", "pt", "nl", "sv", "no", "da", "fi", "ro", "ru", "tr",
            "hu", "cs", "pl", "el", "ar", "he", "hi", "zh", "ja", "ko", "th", "vi", "id",
        ];
        for lang in &langs {
            let list = stop_words(lang);
            for window in list.windows(2) {
                assert!(
                    window[0] <= window[1],
                    "lang={lang}: '{}'  > '{}'",
                    window[0],
                    window[1]
                );
            }
        }
    }
}
