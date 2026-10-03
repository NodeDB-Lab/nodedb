// SPDX-License-Identifier: Apache-2.0

use super::{
    super::statement::{GraphDirection, GraphProperties},
    tokenizer::Tok,
};
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

/// Collect the run of quoted tokens at the head of `toks`.
///
/// The run ends at the first unquoted token. The tokenizer drops `,`, `(`
/// and `)`, so `'a', 'b'` and `('a', 'b')` give the same run.
fn quoted_run(toks: &[Tok<'_>]) -> Vec<String> {
    toks.iter()
        .map_while(|t| match t {
            Tok::Quoted(s) => Some(s.clone().into_owned()),
            _ => None,
        })
        .collect()
}

pub(super) fn quoted_list_after(toks: &[Tok<'_>], keyword: &str) -> Vec<String> {
    find_keyword(toks, keyword)
        .map(|pos| quoted_run(&toks[pos + 1..]))
        .unwrap_or_default()
}

/// Read an optional `LABEL '<label>'[, '<label>' ...]` clause.
///
/// An omitted clause returns an empty set, which keeps every edge. A present
/// clause needs one or more quoted labels. A bare word is refused: a quoted
/// list ends at the first unquoted token.
pub(super) fn label_set_after(toks: &[Tok<'_>], statement: &str) -> Result<Vec<String>, SqlError> {
    let Some(pos) = find_keyword(toks, "LABEL") else {
        return Ok(Vec::new());
    };
    let labels = quoted_run(&toks[pos + 1..]);
    if labels.is_empty() {
        return Err(SqlError::Parse {
            detail: format!("{statement} LABEL needs one or more quoted labels, as LABEL 'a', 'b'"),
        });
    }
    Ok(labels)
}

/// Extract a brace-balanced object literal (`{…}`, braces included) that
/// immediately follows `keyword`. Used for `PERSONALIZATION {…}` in
/// `GRAPH ALGO`. Returns `None` when the keyword is absent or is not followed
/// by an object token.
pub(super) fn object_after(toks: &[Tok<'_>], keyword: &str) -> Option<String> {
    let pos = find_keyword(toks, keyword)?;
    match toks.get(pos + 1)? {
        Tok::Object(s) => Some((*s).to_string()),
        _ => None,
    }
}

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

pub(super) fn float_after(toks: &[Tok<'_>], keyword: &str) -> Option<f64> {
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

/// Read a `DIRECTION` clause.
///
/// An omitted clause defaults to `out`. A value outside the vocabulary is
/// refused by name — defaulting it would answer a question the caller did
/// not ask, and `DIRECTION INBOUND` is indistinguishable from `DIRECTION
/// BANANA` once both have become `out`.
pub(super) fn direction_after(toks: &[Tok<'_>]) -> Result<GraphDirection, SqlError> {
    let Some(word) = word_after(toks, "DIRECTION") else {
        return Ok(GraphDirection::Out);
    };
    match word.to_ascii_uppercase().as_str() {
        "IN" => Ok(GraphDirection::In),
        "OUT" => Ok(GraphDirection::Out),
        "BOTH" => Ok(GraphDirection::Both),
        _ => Err(SqlError::Parse {
            detail: format!("DIRECTION must be one of in, out, both — found '{word}'"),
        }),
    }
}

/// Read an optional numeric clause, refusing a value that is present but
/// not a number. `Ok(None)` means the clause was omitted, so the caller
/// applies its own default; it never means "the value was unreadable".
pub(super) fn usize_after_checked(
    toks: &[Tok<'_>],
    keyword: &str,
) -> Result<Option<usize>, SqlError> {
    let Some(word) = word_after(toks, keyword) else {
        return Ok(None);
    };
    word.parse().map(Some).map_err(|_| SqlError::Parse {
        detail: format!("{keyword} must be a non-negative integer — found '{word}'"),
    })
}

/// A required clause was absent.
pub(super) fn missing_clause(statement: &str, clause: &str) -> SqlError {
    SqlError::Parse {
        detail: format!("{statement} requires {clause}"),
    }
}

pub(super) fn extract_properties(toks: &[Tok<'_>]) -> GraphProperties {
    let Some(pos) = find_keyword(toks, "PROPERTIES") else {
        return GraphProperties::None;
    };
    match toks.get(pos + 1) {
        Some(Tok::Object(obj_str)) => GraphProperties::Object((*obj_str).to_string()),
        Some(Tok::Quoted(s)) => GraphProperties::Quoted(s.clone().into_owned()),
        _ => GraphProperties::None,
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
