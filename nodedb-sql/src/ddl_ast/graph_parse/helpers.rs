// SPDX-License-Identifier: Apache-2.0

use super::tokenizer::Tok;
use crate::error::SqlError;

pub(super) fn find_keyword(toks: &[Tok<'_>], keyword: &str) -> Option<usize> {
    toks.iter()
        .position(|t| matches!(t, Tok::Word(w) if w.eq_ignore_ascii_case(keyword)))
}

pub(super) fn quoted_after(toks: &[Tok<'_>], keyword: &str) -> Option<String> {
    let pos = find_keyword(toks, keyword)?;
    match toks.get(pos + 1)? {
        Tok::Quoted(s) => Some(s.clone().into_owned()),
        Tok::Word(w) => Some((*w).to_string()),
        Tok::Object(_) => None,
    }
}

/// Extract a brace-balanced object literal (`{…}`, braces included) that
/// immediately follows `keyword`. Used for `PERSONALIZATION {…}` in
/// `GRAPH ALGO`. Returns `None` when the keyword is absent or is not followed
/// by an object token.
pub(super) fn word_after(toks: &[Tok<'_>], keyword: &str) -> Option<String> {
    let pos = find_keyword(toks, keyword)?;
    if let Tok::Word(w) = toks.get(pos + 1)? {
        Some((*w).to_string())
    } else {
        None
    }
}

pub(super) fn usize_after(toks: &[Tok<'_>], keyword: &str) -> Option<usize> {
    word_after(toks, keyword)?.parse().ok()
}

/// Extract the two consecutive float tokens that follow `keyword`.
///
/// The tokenizer strips `(` and `)`, so `RRF_K (60.0, 35.0)` becomes the
/// token sequence `[Word("RRF_K"), Word("60.0"), Word("35.0")]`. This helper
/// reads both values without requiring the caller to know about that stripping.
pub(super) fn float_pair_after(toks: &[Tok<'_>], keyword: &str) -> Option<(f64, f64)> {
    let pos = find_keyword(toks, keyword)?;
    let k1 = match toks.get(pos + 1)? {
        Tok::Word(w) => w.parse::<f64>().ok()?,
        _ => return None,
    };
    let k2 = match toks.get(pos + 2)? {
        Tok::Word(w) => w.parse::<f64>().ok()?,
        _ => return None,
    };
    Some((k1, k2))
}

/// Extract three consecutive float tokens that follow `keyword`.
///
/// The tokenizer strips `(` and `)`, so `RRF_K (60.0, 35.0, 50.0)` becomes
/// `[Word("RRF_K"), Word("60.0"), Word("35.0"), Word("50.0")]`. Returns `None`
/// if fewer than three float tokens follow the keyword.
pub(super) fn float_triple_after(toks: &[Tok<'_>], keyword: &str) -> Option<(f64, f64, f64)> {
    let pos = find_keyword(toks, keyword)?;
    let k1 = match toks.get(pos + 1)? {
        Tok::Word(w) => w.parse::<f64>().ok()?,
        _ => return None,
    };
    let k2 = match toks.get(pos + 2)? {
        Tok::Word(w) => w.parse::<f64>().ok()?,
        _ => return None,
    };
    let k3 = match toks.get(pos + 3)? {
        Tok::Word(w) => w.parse::<f64>().ok()?,
        _ => return None,
    };
    Some((k1, k2, k3))
}

/// A required clause was absent.
pub(super) fn missing_clause(statement: &str, clause: &str) -> SqlError {
    SqlError::Parse {
        detail: format!("{statement} requires {clause}"),
    }
}

/// Extract `ARRAY[f1, f2, …]` that appears after `keyword` in raw SQL.
///
/// Used for `QUERY ARRAY[…]` in `GRAPH RAG FUSION` where the vector payload
/// cannot be tokenized as keyword-value pairs. Searches for the first `[`
/// after the keyword and collects comma-separated f32 values up to `]`.
pub(super) fn array_floats_after(sql: &str, keyword: &str) -> Option<Vec<f32>> {
    let upper = sql.to_ascii_uppercase();
    let kw_pos = upper.find(keyword)?;
    let after_kw = &sql[kw_pos + keyword.len()..];
    let bracket_start = after_kw.find('[').map(|i| i + 1)?;
    let bracket_end = after_kw[bracket_start..]
        .find(']')
        .map(|i| i + bracket_start)?;
    let content = &after_kw[bracket_start..bracket_end];
    let floats: Vec<f32> = content
        .split(',')
        .filter_map(|s| s.trim().parse::<f32>().ok())
        .collect();
    if floats.is_empty() {
        None
    } else {
        Some(floats)
    }
}
