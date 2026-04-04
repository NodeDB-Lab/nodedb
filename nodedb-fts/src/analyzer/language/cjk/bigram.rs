//! CJK character bigram tokenizer (Elasticsearch `cjk_analyzer` approach).
//!
//! Generates overlapping character bigrams for CJK text.
//! "全文検索" → ["全文", "文検", "検索"]
//!
//! High recall, acceptable precision, zero external dictionary dependency.
//! This is the always-available fallback when dictionary-based segmentation
//! is not enabled via feature gates.

use super::script;

/// Generate overlapping character bigrams from CJK text.
///
/// Only generates bigrams between characters that are both CJK.
/// Non-CJK characters break the bigram chain.
pub fn cjk_bigrams(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut bigrams = Vec::new();

    let mut i = 0;
    while i + 1 < chars.len() {
        if script::is_cjk(chars[i]) && script::is_cjk(chars[i + 1]) {
            let mut s = String::with_capacity(8);
            s.push(chars[i]);
            s.push(chars[i + 1]);
            bigrams.push(s);
            i += 1;
        } else {
            i += 1;
        }
    }

    bigrams
}

/// Tokenize a text segment that contains CJK characters.
///
/// Splits into runs of CJK characters and generates bigrams for each run.
/// Single CJK characters (no bigram possible) are emitted as unigrams.
pub fn tokenize_cjk(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut run_start = None;

    for (i, &c) in chars.iter().enumerate() {
        if script::is_cjk(c) {
            if run_start.is_none() {
                run_start = Some(i);
            }
        } else if let Some(start) = run_start {
            emit_cjk_run(&chars[start..i], &mut tokens);
            run_start = None;
        }
    }
    // Flush trailing CJK run.
    if let Some(start) = run_start {
        emit_cjk_run(&chars[start..], &mut tokens);
    }

    tokens
}

/// Emit bigrams (or unigram for single char) from a CJK character run.
fn emit_cjk_run(run: &[char], out: &mut Vec<String>) {
    if run.len() == 1 {
        out.push(run[0].to_string());
        return;
    }
    for window in run.windows(2) {
        let mut s = String::with_capacity(8);
        s.push(window[0]);
        s.push(window[1]);
        out.push(s);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chinese_bigrams() {
        let tokens = cjk_bigrams("全文検索");
        assert_eq!(tokens, vec!["全文", "文検", "検索"]);
    }

    #[test]
    fn japanese_mixed() {
        // "東京タワー" (Tokyo Tower) — mixed kanji + katakana, all CJK.
        let tokens = cjk_bigrams("東京タワー");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0], "東京");
        assert_eq!(tokens[1], "京タ");
    }

    #[test]
    fn korean_bigrams() {
        let tokens = cjk_bigrams("한국어");
        assert_eq!(tokens, vec!["한국", "국어"]);
    }

    #[test]
    fn single_char_no_bigram() {
        assert!(cjk_bigrams("中").is_empty());
    }

    #[test]
    fn mixed_cjk_latin() {
        // CJK broken by Latin — no cross-script bigrams.
        let tokens = cjk_bigrams("中a文");
        assert!(tokens.is_empty());
    }

    #[test]
    fn tokenize_cjk_mixed() {
        let tokens = tokenize_cjk("hello全文検索world");
        // "全文検索" → 3 bigrams. "hello" and "world" are not CJK, ignored.
        assert_eq!(tokens, vec!["全文", "文検", "検索"]);
    }

    #[test]
    fn tokenize_cjk_single_char() {
        let tokens = tokenize_cjk("中");
        assert_eq!(tokens, vec!["中"]); // Unigram fallback.
    }

    #[test]
    fn empty_input() {
        assert!(cjk_bigrams("").is_empty());
        assert!(tokenize_cjk("").is_empty());
    }

    #[test]
    fn all_latin() {
        assert!(tokenize_cjk("hello world").is_empty());
    }
}
