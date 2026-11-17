// SPDX-License-Identifier: BUSL-1.1

//! The `collection[.column]` target of `SHOW VECTOR INDEX ... ON` and
//! `ALTER VECTOR INDEX ON`.
//!
//! Each part follows the SQL identifier rule that `CREATE VECTOR INDEX`
//! applies: a bare part lowercases, a double-quoted part keeps its case. The
//! dot splits the target only outside quotes, so `"my.coll".emb` names
//! collection `my.coll`.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use super::super::super::result::DdlError;
use super::super::super::sql_parse::parse_ident_token;
use super::support::ddl_err;

/// Parse the `collection[.column]` target after `keyword`, such as `" ON "`.
///
/// Returns `(collection, field_name)`. A target with no column names the
/// default field, and `field_name` is empty.
pub(super) fn parse_collection_column(
    sql: &str,
    keyword: &str,
) -> Result<(String, String), DdlError> {
    let pos = find_ascii_case_insensitive(sql, keyword)
        .ok_or_else(|| ddl_err("42601", format!("expected '{keyword}' in statement")))?;
    let after = sql[pos + keyword.len()..].trim_start();

    let (collection_raw, rest) = split_part(after)?;
    if collection_raw.is_empty() {
        return Err(ddl_err("42601", "expected collection[.column] after ON"));
    }
    let collection = parse_ident_token(collection_raw)?;
    let Some(column_text) = rest.strip_prefix('.') else {
        return Ok((collection, String::new()));
    };
    let (column_raw, rest) = split_part(column_text)?;
    if column_raw.is_empty() {
        return Err(ddl_err(
            "42601",
            format!("expected a column name after '{collection_raw}.'"),
        ));
    }
    if rest.starts_with('.') {
        return Err(ddl_err(
            "42601",
            format!("'{collection_raw}.{column_raw}.' names more than a collection and a column"),
        ));
    }
    Ok((collection, parse_ident_token(column_raw)?))
}

/// Split one identifier part off the front of `text`: a double-quoted part
/// up to its closing quote, with `""` as an escaped quote, or a bare part up
/// to whitespace, `.`, `(` or `;`. Returns the raw part and the rest.
fn split_part(text: &str) -> Result<(&str, &str), DdlError> {
    if let Some(inner) = text.strip_prefix('"') {
        let mut chars = inner.char_indices().peekable();
        while let Some((i, ch)) = chars.next() {
            if ch != '"' {
                continue;
            }
            if matches!(chars.peek(), Some((_, '"'))) {
                chars.next();
                continue;
            }
            // `i` indexes `inner`, one byte past the opening quote.
            let end = i + 2;
            return Ok((&text[..end], &text[end..]));
        }
        return Err(ddl_err(
            "42601",
            format!("unterminated quoted identifier: {text}"),
        ));
    }
    let end = text
        .find(|c: char| c.is_whitespace() || matches!(c, '.' | '(' | ';'))
        .unwrap_or(text.len());
    Ok((&text[..end], &text[end..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(sql: &str) -> Result<(String, String), DdlError> {
        parse_collection_column(sql, " ON ")
    }

    #[test]
    fn bare_parts_lowercase() {
        assert_eq!(
            target("ALTER VECTOR INDEX ON Docs.Emb SEAL").unwrap(),
            ("docs".to_string(), "emb".to_string())
        );
        assert_eq!(
            target("SHOW VECTOR INDEX status ON Docs").unwrap(),
            ("docs".to_string(), String::new())
        );
    }

    #[test]
    fn quoted_parts_keep_case_and_their_dots() {
        assert_eq!(
            target("ALTER VECTOR INDEX ON \"Docs\".\"Emb\" SET (m = 8)").unwrap(),
            ("Docs".to_string(), "Emb".to_string())
        );
        assert_eq!(
            target("ALTER VECTOR INDEX ON \"my.docs\".emb COMPACT").unwrap(),
            ("my.docs".to_string(), "emb".to_string())
        );
        assert_eq!(
            target("SHOW VECTOR INDEX status ON \"My Docs\"").unwrap(),
            ("My Docs".to_string(), String::new())
        );
    }

    #[test]
    fn malformed_targets_are_errors() {
        assert!(target("ALTER VECTOR INDEX ON \"docs SEAL").is_err());
        assert!(target("ALTER VECTOR INDEX ON docs. SEAL").is_err());
        assert!(target("ALTER VECTOR INDEX ON a.b.c SEAL").is_err());
        assert!(target("ALTER VECTOR INDEX ON \"\".emb SEAL").is_err());
        assert!(target("ALTER VECTOR INDEX SEAL").is_err());
    }
}
