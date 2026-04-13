//! Extract dependency references from a function body.
//!
//! Scans the body SQL for function calls that match other
//! user-defined functions. Collection references in subqueries
//! (e.g. `SELECT x FROM users`) are not extracted here — they
//! are resolved at query time via the schema provider and
//! protected by collection-level permissions + RLS.

use crate::control::security::catalog::StoredFunction;
use crate::control::security::catalog::dependencies::Dependency;

pub fn extract_dependencies(func: &StoredFunction) -> Vec<Dependency> {
    // Simple regex-free scan: look for `identifier(` patterns
    // that aren't SQL keywords or parameter names. A more robust
    // approach would parse the body into an AST and walk it, but
    // the validate step already did that. For now, we extract
    // function names from the body by finding word(...) patterns.
    let body = func.body_sql.to_lowercase();
    let param_names: std::collections::HashSet<&str> =
        func.parameters.iter().map(|p| p.name.as_str()).collect();

    let mut deps = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &body[start..i];
            let mut j = i;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len()
                && bytes[j] == b'('
                && !is_sql_keyword(word)
                && !param_names.contains(word)
            {
                deps.push(Dependency {
                    target_type: "function".into(),
                    target_name: word.to_string(),
                });
            }
        } else {
            i += 1;
        }
    }

    deps.sort_by(|a, b| a.target_name.cmp(&b.target_name));
    deps.dedup_by(|a, b| a.target_name == b.target_name && a.target_type == b.target_type);
    deps
}

fn is_sql_keyword(word: &str) -> bool {
    matches!(
        word,
        "select"
            | "from"
            | "where"
            | "and"
            | "or"
            | "not"
            | "in"
            | "is"
            | "null"
            | "true"
            | "false"
            | "as"
            | "case"
            | "when"
            | "then"
            | "else"
            | "end"
            | "between"
            | "like"
            | "exists"
            | "cast"
            | "coalesce"
            | "nullif"
            | "if"
            | "limit"
            | "order"
            | "by"
            | "asc"
            | "desc"
            | "group"
            | "having"
            | "union"
            | "all"
            | "distinct"
            | "join"
            | "on"
            | "left"
            | "right"
            | "inner"
            | "outer"
            | "cross"
            | "values"
    )
}
