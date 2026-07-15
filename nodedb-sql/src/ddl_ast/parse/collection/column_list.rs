// SPDX-License-Identifier: Apache-2.0

//! Parse the parenthesised column list in CREATE COLLECTION / CREATE TABLE.

use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_case_insensitive;

/// Find the byte offset of the closing paren that matches the first `(` in
/// `body` (depth-aware, so nested parens like `VECTOR(128)` are handled).
///
/// Returns `None` when there is no column list (or it is unterminated).
/// This is the single source of truth for the column-list boundary — any
/// other consumer that needs to know "where does the column list end"
/// (e.g. the trailing `ENGINE = <name>` suffix scan) MUST call this instead
/// of re-deriving the boundary with a naive `find(')')`.
pub(super) fn find_column_list_paren_end(body: &str) -> Option<usize> {
    let paren_start = body.find('(')?;

    let mut depth = 0usize;
    for (i, b) in body.bytes().enumerate().skip(paren_start) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Extract `(name, type)` pairs from the first parenthesised column list
/// in `body` (the text after the collection name). Returns an empty Vec
/// when no column list is present or parsing fails.
///
/// Handles nested parens for types like `VECTOR(128)`.
pub(super) fn extract_column_pairs(body: &str) -> Result<Vec<(String, String)>, SqlError> {
    let paren_start = match body.find('(') {
        Some(p) => p,
        None => return Ok(Vec::new()),
    };

    let paren_end = match find_column_list_paren_end(body) {
        Some(p) => p,
        None => return Ok(Vec::new()),
    };

    let inner = &body[paren_start + 1..paren_end];
    let upper_inner = inner.to_uppercase();

    // If this looks like a WITH clause rather than a column list, skip.
    // WITH clauses start with known option keywords like ENGINE, PROFILE,
    // VECTOR_FIELD, PARTITION_BY, etc.
    if is_with_clause_inner(&upper_inner) {
        return Ok(Vec::new());
    }

    split_column_pairs(inner)
}

/// Heuristic: does the first token in the paren body look like a WITH-clause
/// key rather than a column name+type?
fn is_with_clause_inner(upper_inner: &str) -> bool {
    let first_tok = upper_inner.split_whitespace().next().unwrap_or("");
    matches!(
        first_tok.trim_end_matches(['=', '\'']),
        "ENGINE"
            | "PROFILE"
            | "VECTOR_FIELD"
            | "PARTITION_BY"
            | "DIM"
            | "METRIC"
            | "PAYLOAD_INDEXES"
            | "APPEND_ONLY"
            | "HASH_CHAIN"
            | "BITEMPORAL"
    )
}

/// Split the interior of a column-list paren into `(name, type)` pairs.
/// Uses top-level comma splitting (respects nested parens for VECTOR(n)).
fn split_column_pairs(inner: &str) -> Result<Vec<(String, String)>, SqlError> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;

    for (i, c) in inner.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => {
                let token = inner[start..i].trim();
                if !token.is_empty()
                    && let Some(pair) = parse_col_token(token)?
                {
                    pairs.push(pair);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = inner[start..].trim();
    if !last.is_empty()
        && let Some(pair) = parse_col_token(last)?
    {
        pairs.push(pair);
    }
    Ok(pairs)
}

/// Parse a single column token like `"id BIGINT NOT NULL"` into `(name, type_str)`.
///
/// Captures only the name and bare type (including generic `VECTOR(128)`);
/// skips constraint keywords. Returns `Err` if the column name is a reserved
/// identifier and `None` for constraint-only clauses that should be skipped.
fn parse_col_token(token: &str) -> Result<Option<(String, String)>, SqlError> {
    use crate::reserved::check_identifier;

    let mut toks = token.split_whitespace();
    let raw_name = match toks.next() {
        None => return Ok(None),
        Some(s) => s,
    };

    // Reject unsupported SQL constraint keywords with typed errors and migration hints.
    let upper_name = raw_name.to_uppercase();
    match upper_name.as_str() {
        "PRIMARY" => {
            // Table-level `PRIMARY KEY (col)` clause: the column name is not present here,
            // so we cannot wire `is_pk` on a specific column. Reject with a hint to use the
            // inline form instead, which `parse_column_type_str_full` already handles.
            return Err(SqlError::UnsupportedConstraint {
                feature: "PRIMARY KEY".to_string(),
                hint: "use the inline form on the column instead: \
                       `<colname> <TYPE> PRIMARY KEY`"
                    .to_string(),
            });
        }
        "UNIQUE" => {
            return Err(SqlError::UnsupportedConstraint {
                feature: "UNIQUE constraint".to_string(),
                hint: "use a UNIQUE secondary index: \
                       CREATE INDEX ... ON collection (field) UNIQUE"
                    .to_string(),
            });
        }
        "CHECK" => {
            return Err(SqlError::UnsupportedConstraint {
                feature: "CHECK constraint".to_string(),
                hint: "CHECK constraints are unsupported; enforce in application code \
                       or use a typed function in INSERT"
                    .to_string(),
            });
        }
        "FOREIGN" => {
            return Err(SqlError::UnsupportedConstraint {
                feature: "FOREIGN KEY constraint".to_string(),
                hint: "FOREIGN KEY enforcement is unsupported; \
                       enforce in application code"
                    .to_string(),
            });
        }
        "REFERENCES" => {
            return Err(SqlError::UnsupportedConstraint {
                feature: "REFERENCES constraint".to_string(),
                hint: "FOREIGN KEY enforcement is unsupported; \
                       enforce in application code"
                    .to_string(),
            });
        }
        "CONSTRAINT" => {
            // Named constraint: peek at the next token to determine kind.
            let mut rest = toks.clone();
            let _constraint_name = rest.next(); // skip the constraint name
            let kind_tok = rest.next().map(|t| t.to_uppercase()).unwrap_or_default();
            let (feature, hint) = match kind_tok.as_str() {
                "PRIMARY" => (
                    "CONSTRAINT ... PRIMARY KEY".to_string(),
                    "use the inline form on the column instead: \
                     `<colname> <TYPE> PRIMARY KEY`"
                        .to_string(),
                ),
                "UNIQUE" => (
                    "CONSTRAINT ... UNIQUE".to_string(),
                    "use a UNIQUE secondary index: \
                     CREATE INDEX ... ON collection (field) UNIQUE"
                        .to_string(),
                ),
                "CHECK" => (
                    "CONSTRAINT ... CHECK".to_string(),
                    "CHECK constraints are unsupported; enforce in application code \
                     or use a typed function in INSERT"
                        .to_string(),
                ),
                "FOREIGN" => (
                    "CONSTRAINT ... FOREIGN KEY".to_string(),
                    "FOREIGN KEY enforcement is unsupported; \
                     enforce in application code"
                        .to_string(),
                ),
                _ => (
                    format!("CONSTRAINT {}", kind_tok),
                    "named constraints are unsupported; \
                     use NodeDB-native enforcement (indexes, typeguards)"
                        .to_string(),
                ),
            };
            return Err(SqlError::UnsupportedConstraint { feature, hint });
        }
        _ => {}
    }

    // Validate that the column name is not a reserved identifier.
    let name = check_identifier(raw_name)?;

    // Collect the column definition (bare type + modifiers like NOT NULL, DEFAULT expr,
    // TIME_KEY, SPATIAL_INDEX). Downstream builders (build_strict_schema,
    // build_kv_collection_type, etc.) each strip to the bare type as needed via
    // parse_column_type_str.
    //
    // Inline constraint keywords (PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY, REFERENCES,
    // CONSTRAINT) appearing after the type are rejected with typed errors — they are
    // never silently absorbed into the type string.
    let mut type_parts: Vec<&str> = Vec::new();
    let mut in_paren = false;
    let mut hit_generated = false;
    for t in toks {
        let upper_t = t.to_uppercase();
        let stripped = upper_t.trim_end_matches(['(', ')', ',']);
        // GENERATED is NOT stopped here: we pass the raw text through so that
        // `build_strict_schema` can detect and store the generated expression.
        if !in_paren && stripped == "GENERATED" {
            hit_generated = true;
            // Stop the word-by-word iteration here.  We will append the original
            // raw text from "GENERATED" onwards below, preserving spaces inside
            // expressions like GENERATED ALWAYS AS ('café' || city).
            break;
        }
        // Reject inline constraint keywords — same error family as table-level constraints.
        // Note: "PRIMARY" (inline `col TYPE PRIMARY KEY`) is intentionally NOT rejected here;
        // it flows through to `parse_column_type_str_full` which extracts `is_pk` correctly.
        if !in_paren {
            match stripped {
                "UNIQUE" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "UNIQUE constraint".to_string(),
                        hint: "use a UNIQUE secondary index: \
                               CREATE INDEX ... ON collection (field) UNIQUE"
                            .to_string(),
                    });
                }
                "CHECK" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "CHECK constraint".to_string(),
                        hint: "CHECK constraints are unsupported; enforce in application code \
                               or use a typed function in INSERT"
                            .to_string(),
                    });
                }
                "FOREIGN" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "FOREIGN KEY constraint".to_string(),
                        hint: "FOREIGN KEY enforcement is unsupported; \
                               enforce in application code"
                            .to_string(),
                    });
                }
                "REFERENCES" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "REFERENCES constraint".to_string(),
                        hint: "FOREIGN KEY enforcement is unsupported; \
                               enforce in application code"
                            .to_string(),
                    });
                }
                "CONSTRAINT" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "CONSTRAINT clause".to_string(),
                        hint: "named constraints are unsupported; \
                               use NodeDB-native enforcement (indexes, typeguards)"
                            .to_string(),
                    });
                }
                _ => {}
            }
        }
        if t.contains('(') {
            in_paren = true;
        }
        if t.contains(')') {
            in_paren = false;
        }
        type_parts.push(t);
    }

    if type_parts.is_empty() {
        return Ok(None);
    }

    let mut type_str = type_parts.join(" ");

    // When GENERATED ALWAYS AS was found, append the remainder of the original
    // token text verbatim so that downstream builders can parse the expression.
    if hit_generated && let Some(gen_pos) = find_ascii_case_insensitive(token, "GENERATED") {
        type_str.push(' ');
        type_str.push_str(token[gen_pos..].trim());
    }

    Ok(Some((name, type_str)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_clause_after_unicode_type_preserves_original_offsets() {
        let parsed = parse_col_token("slug CUSTOMﬀﬀ GENERATED ALWAYS AS (lower(name))")
            .expect("column should parse")
            .expect("column definition should be present");
        assert_eq!(parsed.0, "slug");
        assert_eq!(parsed.1, "CUSTOMﬀﬀ GENERATED ALWAYS AS (lower(name))");
    }
}
