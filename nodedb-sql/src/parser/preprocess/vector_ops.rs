//! Rewrite pgvector's `<->` distance operator into a `vector_distance()`
//! function call that standard sqlparser can parse.

/// Rewrite all occurrences of `expr <-> expr` to `vector_distance(expr, expr)`.
///
/// Handles: `column_name <-> ARRAY[...]`, `column <-> $param`, etc.
/// Returns `None` if no valid `<->` patterns are found.
pub(super) fn rewrite_arrow_distance(sql: &str) -> Option<String> {
    let mut result = String::with_capacity(sql.len());
    let mut remaining = sql;
    let mut found = false;

    while let Some(arrow_pos) = remaining.find("<->") {
        let before = &remaining[..arrow_pos];
        let left = extract_left_operand(before)?;
        let left_start = arrow_pos - left.len();

        let after = &remaining[arrow_pos + 3..];
        let (right, right_len) = extract_right_operand(after.trim_start())?;
        let ws_skip = after.len() - after.trim_start().len();

        result.push_str(&remaining[..left_start]);
        result.push_str(&format!("vector_distance({left}, {right})"));
        remaining = &remaining[arrow_pos + 3 + ws_skip + right_len..];
        found = true;
    }

    if !found {
        return None;
    }

    result.push_str(remaining);
    Some(result)
}

/// Extract the left operand before `<->`: a column name or dotted path.
fn extract_left_operand(before: &str) -> Option<String> {
    let trimmed = before.trim_end();
    let start = trimmed
        .rfind(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
        .map(|p| p + 1)
        .unwrap_or(0);
    let ident = &trimmed[start..];
    if ident.is_empty() {
        return None;
    }
    Some(ident.to_string())
}

/// Extract the right operand after `<->`: ARRAY[...], $param, or identifier.
/// Returns (operand_text, consumed_length).
fn extract_right_operand(after: &str) -> Option<(String, usize)> {
    let trimmed = after.trim_start();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("ARRAY[") {
        let mut depth = 0;
        for (i, c) in trimmed.char_indices() {
            match c {
                '[' => depth += 1,
                ']' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some((trimmed[..=i].to_string(), i + 1));
                    }
                }
                _ => {}
            }
        }
        None
    } else if trimmed.starts_with('$') {
        let end = trimmed
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '$')
            .unwrap_or(trimmed.len());
        Some((trimmed[..end].to_string(), end))
    } else {
        let end = trimmed
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
            .unwrap_or(trimmed.len());
        if end == 0 {
            return None;
        }
        Some((trimmed[..end].to_string(), end))
    }
}
