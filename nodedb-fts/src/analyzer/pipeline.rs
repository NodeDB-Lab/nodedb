//! Core text analysis trait and the default English analysis pipeline.
//!
//! Pipeline stages (applied at both index time and query time):
//! 1. Unicode NFD normalization + strip combining marks
//! 2. Lowercase
//! 3. Split on non-alphanumeric boundaries (preserving hyphens within words)
//! 4. For CJK segments: generate bigrams (or dictionary segmentation if enabled)
//! 5. For non-CJK: filter empty tokens and single characters
//! 6. Remove stop words (language-specific)
//! 7. Snowball stemming

use rust_stemmers::{Algorithm, Stemmer};
use unicode_normalization::UnicodeNormalization;

use super::language::cjk::{bigram, script};
use super::language::stop_words;

/// Text analyzer trait: transforms raw text into searchable tokens.
///
/// Implementations must produce the same tokens for equivalent text at
/// both index time and query time (deterministic).
pub trait TextAnalyzer: Send + Sync {
    /// Analyze text into tokens.
    fn analyze(&self, text: &str) -> Vec<String>;

    /// Analyzer name (for serialization and config).
    fn name(&self) -> &str;
}

/// Analyze text into searchable tokens using the standard English analyzer.
///
/// Applies the full pipeline: normalize → lowercase → split → filter →
/// stop words → stem. Used for both indexing and querying.
pub fn analyze(text: &str) -> Vec<String> {
    let stemmer = Stemmer::create(Algorithm::English);
    let en_stops = stop_words::stop_words("en");
    tokenize_with_stemmer(text, &stemmer, "en", en_stops)
}

/// Shared tokenization pipeline used by both the standard `analyze()` function
/// and `LanguageAnalyzer`. Normalizes Unicode, splits on word boundaries,
/// routes CJK text to bigram tokenizer, removes stop words, and stems.
pub(crate) fn tokenize_with_stemmer(
    text: &str,
    stemmer: &Stemmer,
    lang: &str,
    stop_list: &[&str],
) -> Vec<String> {
    // Stage 1-2: Normalize and lowercase.
    // Process char-by-char: for CJK/Hangul characters, preserve as-is (lowercased).
    // For others, apply NFD + strip combining marks to handle diacritics (café → cafe).
    let mut normalized = String::with_capacity(text.len());
    for c in text.chars() {
        if script::is_cjk(c) || script::is_hangul_jamo(c) || script::is_thai(c) {
            // CJK/Hangul/Thai: preserve character, just lowercase.
            for lc in c.to_lowercase() {
                normalized.push(lc);
            }
        } else {
            // Latin/Cyrillic/etc: NFD decompose, strip combining marks, lowercase.
            for decomposed in c.nfd() {
                if unicode_normalization::char::is_combining_mark(decomposed) {
                    continue;
                }
                for lc in decomposed.to_lowercase() {
                    normalized.push(lc);
                }
            }
        }
    }

    let mut tokens = Vec::new();

    // Stage 3: Split on non-alphanumeric boundaries.
    // Keep hyphens and underscores within words.
    for word in normalized.split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_') {
        let trimmed = word.trim_matches(|c: char| c == '-' || c == '_');
        if trimmed.is_empty() {
            continue;
        }

        // Stage 4: Check for CJK content — split into CJK and non-CJK runs.
        if trimmed.chars().any(script::needs_segmentation) {
            // Split into alternating CJK and non-CJK runs within the word.
            let chars: Vec<char> = trimmed.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                let is_seg = script::needs_segmentation(chars[i]);
                let run_end = chars[i..]
                    .iter()
                    .position(|&c| script::needs_segmentation(c) != is_seg)
                    .map_or(chars.len(), |p| i + p);

                let run: String = chars[i..run_end].iter().collect();
                if is_seg {
                    // CJK run → bigram/segmentation.
                    let cjk_tokens = if matches!(lang, "ja" | "zh" | "ko" | "th") {
                        super::language::cjk::segmenter::segment(&run, lang)
                    } else {
                        bigram::tokenize_cjk(&run)
                    };
                    for token in cjk_tokens {
                        if !token.is_empty() && !is_stop_word_in_list(&token, stop_list) {
                            tokens.push(token);
                        }
                    }
                } else if run.len() > 1 && !is_stop_word_in_list(&run, stop_list) {
                    // Non-CJK run → stem.
                    let stemmed = stemmer.stem(&run);
                    if !stemmed.is_empty() {
                        tokens.push(stemmed.into_owned());
                    }
                }
                i = run_end;
            }
            continue;
        }

        // Stage 5: Filter single characters (non-CJK path).
        if trimmed.len() <= 1 {
            continue;
        }

        // Stage 6: Stop word removal.
        if is_stop_word_in_list(trimmed, stop_list) {
            continue;
        }

        // Stage 7: Snowball stemming.
        let stemmed = stemmer.stem(trimmed);
        if !stemmed.is_empty() {
            tokens.push(stemmed.into_owned());
        }
    }

    tokens
}

/// Check if a word is in a sorted stop word list via binary search.
fn is_stop_word_in_list(word: &str, list: &[&str]) -> bool {
    list.binary_search(&word).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_analysis() {
        let tokens = analyze("The quick Brown FOX jumped over the lazy dog");
        assert!(tokens.contains(&"quick".to_string()));
        assert!(tokens.contains(&"brown".to_string()));
        assert!(tokens.contains(&"fox".to_string()));
        assert!(tokens.contains(&"jump".to_string()));
        assert!(tokens.contains(&"lazi".to_string()));
        assert!(tokens.contains(&"dog".to_string()));
        assert!(!tokens.contains(&"the".to_string()));
    }

    #[test]
    fn stop_words_removed() {
        let tokens = analyze("this is a test of the system");
        assert_eq!(tokens, vec!["test", "system"]);
    }

    #[test]
    fn stemming_works() {
        let tokens = analyze("running databases distributed systems");
        assert!(tokens.contains(&"run".to_string()));
        assert!(tokens.contains(&"databas".to_string()));
        assert!(tokens.contains(&"distribut".to_string()));
        assert!(tokens.contains(&"system".to_string()));
    }

    #[test]
    fn unicode_normalization() {
        let tokens = analyze("cafe\u{0301}");
        assert_eq!(tokens, vec!["cafe"]);
    }

    #[test]
    fn hyphenated_words_preserved() {
        let tokens = analyze("e-mail real-time");
        assert!(tokens.contains(&"e-mail".to_string()) || tokens.contains(&"email".to_string()));
    }

    #[test]
    fn empty_and_single_char_filtered() {
        let tokens = analyze("I a x  ");
        assert!(tokens.is_empty());
    }

    #[test]
    fn cjk_routed_to_bigrams() {
        let tokens = analyze("全文検索 system");
        // CJK bigrams + "system" stemmed.
        assert!(tokens.contains(&"全文".to_string()));
        assert!(tokens.contains(&"文検".to_string()));
        assert!(tokens.contains(&"検索".to_string()));
        assert!(tokens.contains(&"system".to_string()));
    }

    #[test]
    fn mixed_cjk_latin() {
        let tokens = analyze("Tokyo東京Tower");
        // "tokyo" gets stemmed, "東京" generates bigram (single pair), "tower" stemmed.
        assert!(tokens.iter().any(|t| t == "tokyo"));
        assert!(tokens.iter().any(|t| t == "tower"));
    }

    #[test]
    fn korean_text_bigrams() {
        let tokens = analyze("한국어 검색");
        assert!(tokens.contains(&"한국".to_string()));
        assert!(tokens.contains(&"국어".to_string()));
    }
}
