/// SQL placeholder scanning and rewriting utilities.
///
/// Provides context-aware `$N` placeholder detection that correctly skips
/// string literals, quoted identifiers, comments, and dollar-quoted blocks.
/// Locate all `$N` placeholders in SQL, returning `(start, end, index)` triples.
///
/// Skips placeholders inside single-quoted strings, double-quoted identifiers,
/// line comments (`--`), block comments (`/* */`), and dollar-quoted strings.
pub(crate) fn placeholder_ranges(sql: &str) -> Vec<(usize, usize, usize)> {
    let bytes = sql.as_bytes();
    let mut ranges = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_single_quoted(bytes, i),
            b'"' => i = skip_double_quoted(bytes, i),
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                i = skip_line_comment(bytes, i);
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i = skip_block_comment(bytes, i);
            }
            b'$' => {
                if let Some(next) = skip_dollar_quoted(bytes, i) {
                    i = next;
                    continue;
                }
                let start_digits = i + 1;
                let mut j = start_digits;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > start_digits
                    && let Ok(idx) = sql[start_digits..j].parse::<usize>()
                {
                    ranges.push((i, j, idx));
                    i = j;
                    continue;
                }
                i += 1;
            }
            _ => i += 1,
        }
    }

    ranges
}

/// Rewrite `$N` placeholders with the corresponding replacement strings.
///
/// Placeholders are 1-indexed: `$1` maps to `replacements[0]`, etc.
/// Placeholders without a matching replacement are left as-is.
pub(crate) fn rewrite_sql_placeholders(sql: &str, replacements: &[String]) -> String {
    let ranges = placeholder_ranges(sql);
    if ranges.is_empty() {
        return sql.to_owned();
    }

    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0usize;
    for (start, end, idx) in ranges {
        out.push_str(&sql[cursor..start]);
        if let Some(replacement) = idx.checked_sub(1).and_then(|i| replacements.get(i)) {
            out.push_str(replacement);
        } else {
            out.push_str(&sql[start..end]);
        }
        cursor = end;
    }
    out.push_str(&sql[cursor..]);
    out
}

fn skip_single_quoted(bytes: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

fn skip_double_quoted(bytes: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

fn skip_line_comment(bytes: &[u8], mut i: usize) -> usize {
    i += 2;
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

fn skip_block_comment(bytes: &[u8], mut i: usize) -> usize {
    i += 2;
    while i + 1 < bytes.len() {
        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            return i + 2;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_dollar_quoted(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    if j >= bytes.len() || bytes[j] != b'$' {
        return None;
    }
    let tag = &bytes[i..=j];
    let mut k = j + 1;
    while k + tag.len() <= bytes.len() {
        if &bytes[k..k + tag.len()] == tag {
            return Some(k + tag.len());
        }
        k += 1;
    }
    Some(bytes.len())
}
