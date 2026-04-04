//! Unicode script detection for CJK and other non-Latin scripts.
//!
//! Routes text segments to the appropriate tokenization path:
//! - CJK codepoints → bigram tokenizer (or dictionary segmentation if enabled)
//! - Latin/Cyrillic/Arabic/Devanagari → standard whitespace/boundary splitting

/// Check if a character is a CJK ideograph (Chinese/Japanese kanji).
///
/// Covers CJK Unified Ideographs and common extensions.
pub fn is_cjk_ideograph(c: char) -> bool {
    matches!(c,
        '\u{4E00}'..='\u{9FFF}'   // CJK Unified Ideographs
        | '\u{3400}'..='\u{4DBF}' // CJK Extension A
        | '\u{20000}'..='\u{2A6DF}' // CJK Extension B
        | '\u{2A700}'..='\u{2B73F}' // CJK Extension C
        | '\u{2B740}'..='\u{2B81F}' // CJK Extension D
        | '\u{F900}'..='\u{FAFF}'  // CJK Compatibility Ideographs
    )
}

/// Check if a character is Japanese Hiragana.
pub fn is_hiragana(c: char) -> bool {
    ('\u{3040}'..='\u{309F}').contains(&c)
}

/// Check if a character is Japanese Katakana.
pub fn is_katakana(c: char) -> bool {
    ('\u{30A0}'..='\u{30FF}').contains(&c)
}

/// Check if a character is Korean Hangul syllable.
pub fn is_hangul(c: char) -> bool {
    ('\u{AC00}'..='\u{D7AF}').contains(&c)
}

/// Check if a character is Hangul Jamo (consonant/vowel components).
pub fn is_hangul_jamo(c: char) -> bool {
    matches!(c,
        '\u{1100}'..='\u{11FF}'   // Hangul Jamo
        | '\u{3130}'..='\u{318F}' // Hangul Compatibility Jamo
    )
}

/// Check if a character belongs to any CJK script that needs special tokenization.
///
/// Returns true for CJK ideographs, Hiragana, Katakana, and Hangul.
pub fn is_cjk(c: char) -> bool {
    is_cjk_ideograph(c) || is_hiragana(c) || is_katakana(c) || is_hangul(c)
}

/// Check if a character is Thai script.
pub fn is_thai(c: char) -> bool {
    ('\u{0E00}'..='\u{0E7F}').contains(&c)
}

/// Check if a character belongs to a script that has no whitespace word boundaries
/// and requires special segmentation (CJK or Thai/Lao/Khmer).
pub fn needs_segmentation(c: char) -> bool {
    is_cjk(c) || is_thai(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cjk_detection() {
        assert!(is_cjk_ideograph('中'));
        assert!(is_cjk_ideograph('全'));
        assert!(!is_cjk_ideograph('a'));
        assert!(!is_cjk_ideograph('Ω'));
    }

    #[test]
    fn hiragana_detection() {
        assert!(is_hiragana('あ'));
        assert!(is_hiragana('ん'));
        assert!(!is_hiragana('ア'));
    }

    #[test]
    fn katakana_detection() {
        assert!(is_katakana('ア'));
        assert!(is_katakana('ン'));
        assert!(!is_katakana('あ'));
    }

    #[test]
    fn hangul_detection() {
        assert!(is_hangul('한'));
        assert!(is_hangul('글'));
        assert!(!is_hangul('a'));
    }

    #[test]
    fn cjk_combined() {
        assert!(is_cjk('中')); // Chinese
        assert!(is_cjk('あ')); // Hiragana
        assert!(is_cjk('ア')); // Katakana
        assert!(is_cjk('한')); // Hangul
        assert!(!is_cjk('a')); // Latin
        assert!(!is_cjk('α')); // Greek
    }

    #[test]
    fn thai_detection() {
        assert!(is_thai('ก'));
        assert!(is_thai('ๆ'));
        assert!(!is_thai('a'));
    }
}
