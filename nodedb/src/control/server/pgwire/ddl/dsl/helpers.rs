//! Shared parameter-extraction helpers for DSL handlers.

pub(super) fn extract_param(upper: &str, name: &str) -> Option<usize> {
    let idx = upper.find(name)?;
    let rest = &upper[idx + name.len()..];
    rest.split(|c: char| !c.is_ascii_digit())
        .find(|s| !s.is_empty())
        .and_then(|s| s.parse().ok())
}

pub(super) fn extract_string_param(sql: &str, name: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let idx = upper.find(name)?;
    let rest = &sql[idx + name.len()..];
    let rest = rest.trim();
    if rest.starts_with('\'') || rest.starts_with('"') {
        let quote = rest.chars().next()?;
        let end = rest[1..].find(quote)?;
        Some(rest[1..end + 1].to_string())
    } else {
        rest.split_whitespace().next().map(|s| s.to_string())
    }
}

pub(super) fn find_param_str(upper_parts: &[String], name: &str) -> Option<String> {
    let idx = upper_parts.iter().position(|p| p == name)?;
    upper_parts.get(idx + 1).cloned()
}

pub(super) fn find_param_usize(upper_parts: &[String], name: &str) -> Option<usize> {
    let idx = upper_parts.iter().position(|p| p == name)?;
    upper_parts
        .get(idx + 1)
        .and_then(|s| s.parse::<usize>().ok())
}
