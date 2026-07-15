// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER/SHOW RETENTION POLICY.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::{NodedbStatement, PolicyStmt};
use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_case_insensitive;

/// Extract the value for a SET key from the SQL string.
/// Returns a quoted string's inner content or an unquoted token.
fn extract_set_value(sql: &str, key: &str) -> Option<String> {
    let pos = find_ascii_case_insensitive(sql, key)?;
    let after = sql[pos + key.len()..].trim_start();
    let after = after.strip_prefix('=').unwrap_or(after).trim_start();
    if let Some(inner) = after.strip_prefix('\'') {
        let end = inner.find('\'')?;
        Some(inner[..end].to_string())
    } else {
        after.split_whitespace().next().map(|s| s.to_string())
    }
}

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE RETENTION POLICY ") {
            return Ok(Some(parse_create_retention_policy(upper, trimmed)));
        }
        if upper.starts_with("DROP RETENTION POLICY ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "POLICY") {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::Policy(
                PolicyStmt::DropRetentionPolicy { name, if_exists },
            )));
        }
        if upper.starts_with("ALTER RETENTION POLICY ") {
            // ALTER RETENTION POLICY <name> [ON <collection>] ENABLE | DISABLE | SET <key> = <value>
            let name = parts.get(3).map(|s| s.to_lowercase()).unwrap_or_default();
            let action_idx = if parts
                .get(4)
                .map(|s| s.eq_ignore_ascii_case("ON"))
                .unwrap_or(false)
            {
                6
            } else {
                4
            };
            let action = parts
                .get(action_idx)
                .map(|s| s.to_uppercase())
                .unwrap_or_default();
            let (set_key, set_value) = if action == "SET" {
                let key = parts
                    .get(action_idx + 1)
                    .map(|s| s.to_uppercase())
                    .unwrap_or_default();
                let val = extract_set_value(trimmed, &key);
                (Some(key), val)
            } else {
                (None, None)
            };
            return Ok(Some(NodedbStatement::Policy(
                PolicyStmt::AlterRetentionPolicy {
                    name,
                    action,
                    set_key,
                    set_value,
                },
            )));
        }
        if upper.starts_with("SHOW RETENTION POLIC") {
            return Ok(Some(NodedbStatement::Policy(
                PolicyStmt::ShowRetentionPolicies,
            )));
        }
        let _ = parts;
        Ok(None)
    })()
    .transpose()
}

/// Structural extraction for `CREATE RETENTION POLICY`.
///
/// Extracts name, collection, raw body (between outer parens), and
/// optional EVAL_INTERVAL string. The handler converts the body to
/// RetentionPolicyDef.
fn parse_create_retention_policy(_upper: &str, trimmed: &str) -> NodedbStatement {
    // Syntax: CREATE RETENTION POLICY <name> ON <collection> ( <body> ) [WITH (EVAL_INTERVAL = '<val>')]
    let prefix = "CREATE RETENTION POLICY ";
    let rest = &trimmed[prefix.len()..];
    let name = rest.split_whitespace().next().unwrap_or("").to_lowercase();

    let collection = find_ascii_case_insensitive(rest, " ON ")
        .map(|pos| {
            rest[pos + 4..]
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_lowercase()
        })
        .unwrap_or_default();

    // Body = everything between the first '(' and its matching ')'
    let body_raw = extract_outer_parens(trimmed).unwrap_or_default();

    // EVAL_INTERVAL from trailing WITH clause
    let eval_interval_raw = extract_eval_interval(trimmed);

    NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
        name,
        collection,
        body_raw,
        eval_interval_raw,
    })
}

/// Extract content between the first outer-level `(` and matching `)`.
fn extract_outer_parens(s: &str) -> Option<String> {
    let start = s.find('(')?;
    let mut depth = 0i32;
    let mut end = 0usize;
    for (i, ch) in s[start..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return None;
    }
    Some(s[start + 1..end].trim().to_string())
}

/// Extract EVAL_INTERVAL value from WITH clause (if present).
fn extract_eval_interval(trimmed: &str) -> Option<String> {
    // Find trailing WITH ( after the main body parens.
    // The main body is the first outer-paren group; WITH comes after.
    let body_end = {
        let start = trimmed.find('(')?;
        let mut depth = 0i32;
        let mut end = 0usize;
        for (i, ch) in trimmed[start..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = start + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        end
    };

    let after_body = &trimmed[body_end..];
    let with_pos = find_ascii_case_insensitive(after_body, "WITH")?;
    let after_with = &trimmed[body_end + with_pos + 4..].trim_start();
    let inner = after_with
        .strip_prefix('(')
        .and_then(|s| s.split_once(')'))
        .map(|(i, _)| i)?;

    for pair in inner.split(',') {
        if let Some((k, v)) = pair.split_once('=')
            && k.trim().to_uppercase() == "EVAL_INTERVAL"
        {
            return Some(v.trim().trim_matches('\'').trim_matches('"').to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_retention_name_preserves_original_offsets() {
        let sql = "CREATE RETENTION POLICY rpﬀﬀ ON metrics (RAW RETAIN '7d')";
        let upper = sql.to_uppercase();
        if let NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            name,
            collection,
            body_raw,
            ..
        }) = parse_create_retention_policy(&upper, sql)
        {
            assert_eq!(name, "rpﬀﬀ");
            assert_eq!(collection, "metrics");
            assert_eq!(body_raw, "RAW RETAIN '7d'");
        } else {
            panic!("expected CreateRetentionPolicy");
        }
    }

    #[test]
    fn set_value_after_unicode_text_preserves_original_offsets() {
        assert_eq!(
            extract_set_value(
                "ALTER RETENTION POLICY rpﬀﬀ SET INTERVAL = '1h'",
                "INTERVAL"
            ),
            Some("1h".to_string())
        );
    }

    #[test]
    fn parse_basic_retention_policy() {
        let sql = "CREATE RETENTION POLICY rp1 ON metrics (RAW RETAIN '7d')";
        let upper = sql.to_uppercase();
        if let NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            name,
            collection,
            body_raw,
            eval_interval_raw,
        }) = parse_create_retention_policy(&upper, sql)
        {
            assert_eq!(name, "rp1");
            assert_eq!(collection, "metrics");
            assert!(body_raw.contains("RAW RETAIN"));
            assert!(eval_interval_raw.is_none());
        } else {
            panic!("expected CreateRetentionPolicy");
        }
    }

    #[test]
    fn parse_with_eval_interval() {
        let sql =
            "CREATE RETENTION POLICY rp2 ON ts (RAW RETAIN '30d') WITH (EVAL_INTERVAL = '1h')";
        let upper = sql.to_uppercase();
        if let NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            eval_interval_raw,
            ..
        }) = parse_create_retention_policy(&upper, sql)
        {
            assert_eq!(eval_interval_raw.as_deref(), Some("1h"));
        } else {
            panic!("expected CreateRetentionPolicy");
        }
    }
}
