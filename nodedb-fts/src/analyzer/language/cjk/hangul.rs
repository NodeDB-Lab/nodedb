//! Hangul syllable decomposition into Jamo (consonant/vowel components).
//!
//! Korean Hangul syllables (U+AC00-U+D7AF) are composed of:
//! - Leading consonant (초성, choseong): 19 possible
//! - Vowel (중성, jungseong): 21 possible
//! - Optional trailing consonant (종성, jongseong): 28 possible (0 = none)
//!
//! Decomposition enables morphological matching: "한" → "ㅎ" + "ㅏ" + "ㄴ".

/// Base offset for Hangul syllables.
const HANGUL_BASE: u32 = 0xAC00;
/// Number of trailing consonants (including none).
const JONGSEONG_COUNT: u32 = 28;
/// Number of vowels.
const JUNGSEONG_COUNT: u32 = 21;

/// Leading consonant (choseong) Jamo characters.
static CHOSEONG: &[char] = &[
    'ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ',
    'ㅌ', 'ㅍ', 'ㅎ',
];

/// Vowel (jungseong) Jamo characters.
static JUNGSEONG: &[char] = &[
    'ㅏ', 'ㅐ', 'ㅑ', 'ㅒ', 'ㅓ', 'ㅔ', 'ㅕ', 'ㅖ', 'ㅗ', 'ㅘ', 'ㅙ', 'ㅚ', 'ㅛ', 'ㅜ', 'ㅝ', 'ㅞ',
    'ㅟ', 'ㅠ', 'ㅡ', 'ㅢ', 'ㅣ',
];

/// Trailing consonant (jongseong) Jamo characters. Index 0 = no trailing consonant.
static JONGSEONG: &[Option<char>] = &[
    None,
    Some('ㄱ'),
    Some('ㄲ'),
    Some('ㄳ'),
    Some('ㄴ'),
    Some('ㄵ'),
    Some('ㄶ'),
    Some('ㄷ'),
    Some('ㄹ'),
    Some('ㄺ'),
    Some('ㄻ'),
    Some('ㄼ'),
    Some('ㄽ'),
    Some('ㄾ'),
    Some('ㄿ'),
    Some('ㅀ'),
    Some('ㅁ'),
    Some('ㅂ'),
    Some('ㅄ'),
    Some('ㅅ'),
    Some('ㅆ'),
    Some('ㅇ'),
    Some('ㅈ'),
    Some('ㅊ'),
    Some('ㅋ'),
    Some('ㅌ'),
    Some('ㅍ'),
    Some('ㅎ'),
];

/// Decompose a Hangul syllable character into its Jamo components.
///
/// Returns `None` if the character is not a Hangul syllable (U+AC00-U+D7AF).
pub fn decompose(c: char) -> Option<(char, char, Option<char>)> {
    let code = c as u32;
    if !(0xAC00..=0xD7AF).contains(&code) {
        return None;
    }

    let offset = code - HANGUL_BASE;
    let choseong_idx = offset / (JUNGSEONG_COUNT * JONGSEONG_COUNT);
    let jungseong_idx = (offset % (JUNGSEONG_COUNT * JONGSEONG_COUNT)) / JONGSEONG_COUNT;
    let jongseong_idx = offset % JONGSEONG_COUNT;

    let leading = *CHOSEONG.get(choseong_idx as usize)?;
    let vowel = *JUNGSEONG.get(jungseong_idx as usize)?;
    let trailing = JONGSEONG.get(jongseong_idx as usize).copied().flatten();

    Some((leading, vowel, trailing))
}

/// Decompose all Hangul syllables in a string to their Jamo components.
///
/// Non-Hangul characters are passed through unchanged.
pub fn decompose_string(text: &str) -> String {
    let mut result = String::with_capacity(text.len() * 3);
    for c in text.chars() {
        if let Some((leading, vowel, trailing)) = decompose(c) {
            result.push(leading);
            result.push(vowel);
            if let Some(t) = trailing {
                result.push(t);
            }
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decompose_basic() {
        // 한 = ㅎ + ㅏ + ㄴ
        let (l, v, t) = decompose('한').unwrap();
        assert_eq!(l, 'ㅎ');
        assert_eq!(v, 'ㅏ');
        assert_eq!(t, Some('ㄴ'));
    }

    #[test]
    fn decompose_no_trailing() {
        // 가 = ㄱ + ㅏ (no trailing consonant)
        let (l, v, t) = decompose('가').unwrap();
        assert_eq!(l, 'ㄱ');
        assert_eq!(v, 'ㅏ');
        assert_eq!(t, None);
    }

    #[test]
    fn decompose_non_hangul() {
        assert!(decompose('a').is_none());
        assert!(decompose('中').is_none());
    }

    #[test]
    fn decompose_string_test() {
        let result = decompose_string("한글");
        // 한 = ㅎㅏㄴ, 글 = ㄱㅡㄹ
        assert_eq!(result, "ㅎㅏㄴㄱㅡㄹ");
    }

    #[test]
    fn decompose_string_mixed() {
        let result = decompose_string("hello한글world");
        assert_eq!(result, "helloㅎㅏㄴㄱㅡㄹworld");
    }
}
