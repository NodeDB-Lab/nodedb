//! Feature-gated dictionary-based segmentation dispatch.
//!
//! When the corresponding feature is enabled, uses dictionary segmentation
//! instead of bigrams for that language. Falls back to bigram when the
//! feature is disabled.
//!
//! Feature gates:
//! - `lang-ja`: lindera with IPADIC for Japanese
//! - `lang-zh`: currently falls back to CJK bigrams (see Cargo.toml)
//! - `lang-ko`: lindera with ko-dic for Korean
//! - `lang-th`: icu_segmenter for Thai

use super::bigram::tokenize_cjk;

/// Segment text using the best available method for the given language.
///
/// Falls back to CJK bigrams if no dictionary is available.
pub fn segment(text: &str, lang: &str) -> Vec<String> {
    match lang {
        "ja" | "japanese" => segment_japanese(text),
        "zh" | "chinese" => segment_chinese(text),
        "ko" | "korean" => segment_korean(text),
        "th" | "thai" => segment_thai(text),
        _ => tokenize_cjk(text),
    }
}

/// Japanese segmentation: lindera/IPADIC when `lang-ja` is enabled, bigrams otherwise.
fn segment_japanese(text: &str) -> Vec<String> {
    #[cfg(feature = "lang-ja")]
    {
        lindera_segment(text, "ipadic")
    }
    #[cfg(not(feature = "lang-ja"))]
    {
        tokenize_cjk(text)
    }
}

/// Chinese segmentation: CJK bigrams (dictionary segmentation temporarily disabled).
fn segment_chinese(text: &str) -> Vec<String> {
    tokenize_cjk(text)
}

/// Korean segmentation: lindera/ko-dic when `lang-ko` is enabled, bigrams otherwise.
fn segment_korean(text: &str) -> Vec<String> {
    #[cfg(feature = "lang-ko")]
    {
        lindera_segment(text, "ko-dic")
    }
    #[cfg(not(feature = "lang-ko"))]
    {
        tokenize_cjk(text)
    }
}

/// Thai segmentation: icu_segmenter when `lang-th` is enabled, bigrams otherwise.
fn segment_thai(text: &str) -> Vec<String> {
    #[cfg(feature = "lang-th")]
    {
        icu_segment_thai(text)
    }
    #[cfg(not(feature = "lang-th"))]
    {
        // Thai bigram fallback (same strategy as CJK).
        tokenize_cjk(text)
    }
}

// ── Feature-gated implementations ──────────────────────────────────

#[cfg(feature = "lang-ja")]
fn lindera_segment(text: &str, _dict: &str) -> Vec<String> {
    use lindera::tokenizer::TokenizerBuilder;
    let Ok(tokenizer) = TokenizerBuilder::new().and_then(|b| b.build()) else {
        return tokenize_cjk(text);
    };
    let Ok(tokens) = tokenizer.tokenize(text) else {
        return tokenize_cjk(text);
    };
    tokens
        .into_iter()
        .map(|t| t.surface.to_string())
        .filter(|t: &String| t.len() > 1 || t.chars().next().is_some_and(super::script::is_cjk))
        .collect()
}

#[cfg(feature = "lang-th")]
fn icu_segment_thai(text: &str) -> Vec<String> {
    use icu_segmenter::WordSegmenter;
    let segmenter = WordSegmenter::new_auto();
    let breakpoints: Vec<usize> = segmenter.segment_str(text).collect();
    let mut words = Vec::new();
    for window in breakpoints.windows(2) {
        let word = &text[window[0]..window[1]];
        if !word.trim().is_empty() {
            words.push(word.to_string());
        }
    }
    words
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigrams_chinese() {
        let tokens = segment("全文検索", "zh");
        assert_eq!(tokens, vec!["全文", "文検", "検索"]);
    }

    #[test]
    #[cfg(not(feature = "lang-ja"))]
    fn fallback_to_bigrams_japanese() {
        let tokens = segment("東京タワー", "ja");
        assert!(!tokens.is_empty());
    }

    #[test]
    #[cfg(feature = "lang-ja")]
    fn dictionary_segmentation_japanese() {
        let tokens = segment("東京タワー", "ja");
        assert!(!tokens.is_empty());
    }

    #[test]
    #[cfg(not(feature = "lang-ko"))]
    fn fallback_to_bigrams_korean() {
        let tokens = segment("한국어", "ko");
        assert!(!tokens.is_empty());
    }

    #[test]
    #[cfg(feature = "lang-ko")]
    fn dictionary_segmentation_korean() {
        let tokens = segment("한국어", "ko");
        assert!(!tokens.is_empty());
    }

    #[test]
    fn unknown_lang_fallback() {
        let tokens = segment("全文検索", "unknown");
        assert_eq!(tokens, vec!["全文", "文検", "検索"]);
    }
}
