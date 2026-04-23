//! Shared parameter-extraction helpers for DSL handlers.

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
