//! Parsing helpers shared across the tree-ops DDL entry points.

use pgwire::error::PgWireResult;

use crate::control::server::pgwire::types::sqlstate_error;

/// Largest accepted value for tree MAX_DEPTH. Prevents a single
/// statement from saturating `cross_core_bfs` with an unbounded
/// fan-out per hop.
pub(super) const TREE_MAX_DEPTH_CAP: usize = 1024;

/// Parse `(parent_col -> id_col)` from CREATE GRAPH INDEX DDL.
pub(super) fn parse_edge_columns(sql: &str) -> PgWireResult<(String, String)> {
    let paren_start = sql
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "missing (parent_col -> id_col)"))?;
    let paren_end = sql
        .rfind(')')
        .ok_or_else(|| sqlstate_error("42601", "missing closing ')'"))?;
    let inner = &sql[paren_start + 1..paren_end];

    // Split on -> or →
    let (parent, child) = if let Some(pos) = inner.find("->") {
        (&inner[..pos], &inner[pos + 2..])
    } else if let Some(pos) = inner.find('→') {
        (&inner[..pos], &inner[pos + '→'.len_utf8()..])
    } else {
        return Err(sqlstate_error(
            "42601",
            "edge definition requires '->' or '→' between columns",
        ));
    };

    let parent = parent.trim().to_lowercase();
    let child = child.trim().to_lowercase();

    if parent.is_empty() || child.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "both parent and child columns required in (parent -> child)",
        ));
    }

    Ok((parent, child))
}

/// Extract function arguments from `FUNC_NAME(arg1, arg2, arg3)`.
pub(super) fn extract_function_args<'a>(
    upper: &str,
    original: &'a str,
    func_name: &str,
) -> PgWireResult<Vec<&'a str>> {
    let pos = upper
        .find(func_name)
        .ok_or_else(|| sqlstate_error("42601", &format!("missing {func_name}")))?;
    let after = &original[pos + func_name.len()..];
    let paren_start = after
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", &format!("{func_name} requires (...) arguments")))?;
    let paren_end = after
        .find(')')
        .ok_or_else(|| sqlstate_error("42601", "missing closing ')'"))?;
    let inner = &after[paren_start + 1..paren_end];
    Ok(inner.split(',').collect())
}

/// Extract a number after a keyword and clamp to `TREE_MAX_DEPTH_CAP`.
///
/// Returns `Ok(None)` when the keyword is absent; the caller supplies
/// its own default. Rejects out-of-range values with SQLSTATE 22023
/// instead of forwarding them to the BFS loop.
pub(super) fn extract_number_after(upper: &str, keyword: &str) -> PgWireResult<Option<usize>> {
    let Some(pos) = upper.find(keyword) else {
        return Ok(None);
    };
    let after = upper[pos + keyword.len()..].trim_start();
    let Some(tok) = after.split_whitespace().next() else {
        return Ok(None);
    };
    let v: usize = tok.parse().map_err(|_| {
        sqlstate_error(
            "22023",
            &format!("{keyword} must be a non-negative integer"),
        )
    })?;
    if v > TREE_MAX_DEPTH_CAP {
        return Err(sqlstate_error(
            "22023",
            &format!("{keyword} {v} exceeds maximum allowed value {TREE_MAX_DEPTH_CAP}"),
        ));
    }
    Ok(Some(v))
}

/// Convert a JSON value to Decimal for summation.
///
/// Non-numeric values (null, objects, arrays, unparseable strings, NaN, Infinity)
/// are treated as zero — TREE_SUM skips them silently.
pub(super) fn json_to_decimal(v: &serde_json::Value) -> rust_decimal::Decimal {
    if let Some(i) = v.as_i64() {
        rust_decimal::Decimal::from(i)
    } else if let Some(f) = v.as_f64() {
        rust_decimal::Decimal::try_from(f).unwrap_or(rust_decimal::Decimal::ZERO)
    } else if let Some(s) = v.as_str() {
        s.parse().unwrap_or(rust_decimal::Decimal::ZERO)
    } else {
        rust_decimal::Decimal::ZERO
    }
}
