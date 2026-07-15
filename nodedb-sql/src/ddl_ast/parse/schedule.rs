// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER/SHOW SCHEDULE + SHOW SCHEDULE HISTORY.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::{AutomationStmt, NodedbStatement};
use crate::error::SqlError;
use crate::parser::preprocess::lex::{find_ascii_case_insensitive, rfind_ascii_case_insensitive};

/// Extract the content between the first pair of single quotes after
/// "CRON" in the SQL string.
fn extract_quoted_cron(sql: &str) -> Option<String> {
    let cron_idx = find_ascii_case_insensitive(sql, "CRON")?;
    let rest = &sql[cron_idx + 4..];
    let start = rest.find('\'')?;
    let inner = &rest[start + 1..];
    let mut i = 0;
    let bytes = inner.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            return Some(inner[..i].replace("''", "'"));
        }
        i += 1;
    }
    None
}

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE SCHEDULE ") {
            return Ok(Some(parse_create_schedule(upper, trimmed)));
        }
        if upper.starts_with("DROP SCHEDULE ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "SCHEDULE") {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::Automation(
                AutomationStmt::DropSchedule { name, if_exists },
            )));
        }
        if upper.starts_with("ALTER SCHEDULE ") {
            // ALTER SCHEDULE <name> ENABLE | DISABLE | SET CRON '<expr>'
            let name = parts.get(2).map(|s| s.to_lowercase()).unwrap_or_default();
            let action = parts.get(3).map(|s| s.to_uppercase()).unwrap_or_default();
            // When action is "SET CRON", extract the quoted cron expression.
            let cron_expr = if action == "SET" {
                extract_quoted_cron(trimmed)
            } else {
                None
            };
            return Ok(Some(NodedbStatement::Automation(
                AutomationStmt::AlterSchedule {
                    name,
                    action,
                    cron_expr,
                },
            )));
        }
        if upper.starts_with("SHOW SCHEDULE HISTORY ") {
            let name = match parts.get(3) {
                None => return Ok(None),
                Some(s) => s.to_string(),
            };
            return Ok(Some(NodedbStatement::Automation(
                AutomationStmt::ShowScheduleHistory { name },
            )));
        }
        if upper == "SHOW SCHEDULES" || upper.starts_with("SHOW SCHEDULES") {
            return Ok(Some(NodedbStatement::Automation(
                AutomationStmt::ShowSchedules,
            )));
        }
        Ok(None)
    })()
    .transpose()
}

/// Structural extraction for `CREATE SCHEDULE`.
///
/// Extracts name, cron_expr, body_sql, scope, missed_policy, allow_overlap
/// as primitive types. The handler converts scope/missed_policy strings to
/// their respective enum variants.
fn parse_create_schedule(upper: &str, trimmed: &str) -> NodedbStatement {
    let prefix = "CREATE SCHEDULE ";
    let rest = &trimmed[prefix.len()..];
    let _rest_upper = &upper[prefix.len()..];
    let tokens: Vec<&str> = rest.split_whitespace().collect();

    let name = tokens.first().map(|s| s.to_lowercase()).unwrap_or_default();

    // Extract cron expression (quoted or 5-field unquoted).
    let cron_expr = extract_cron_expr_str(rest).unwrap_or_default();

    // scope: SCOPE LOCAL → "LOCAL", default "NORMAL"
    let scope = if upper.contains(" SCOPE LOCAL") {
        "LOCAL".to_string()
    } else {
        "NORMAL".to_string()
    };

    // allow_overlap: default true; WITH (ALLOW_OVERLAP = false) → false
    let mut allow_overlap = true;
    let mut missed_policy = "SKIP".to_string();

    if let Some(with_pos) = find_ascii_case_insensitive(trimmed, " WITH ") {
        let after_with = &trimmed[with_pos + 6..];
        if let Some(inner) = after_with
            .trim()
            .strip_prefix('(')
            .and_then(|s| s.split_once(')'))
            .map(|(i, _)| i)
        {
            for opt in inner.split(',') {
                let opt = opt.trim();
                if let Some((key, val)) = opt.split_once('=') {
                    let key = key.trim().to_uppercase();
                    let val = val.trim().trim_matches('\'').trim_matches('"');
                    match key.as_str() {
                        "ALLOW_OVERLAP" => {
                            allow_overlap = !val.eq_ignore_ascii_case("false");
                        }
                        "MISSED" => {
                            missed_policy = val.to_uppercase();
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    // Body SQL: everything after the last " AS " keyword.
    let body_sql = rfind_ascii_case_insensitive(trimmed, " AS ")
        .map(|pos| trimmed[pos + 4..].trim().to_string())
        .unwrap_or_default();

    NodedbStatement::Automation(AutomationStmt::CreateSchedule {
        name,
        cron_expr,
        body_sql,
        scope,
        missed_policy,
        allow_overlap,
    })
}

/// Extract cron expression from rest (after "CREATE SCHEDULE <name> ").
fn extract_cron_expr_str(rest: &str) -> Option<String> {
    let cron_pos = find_ascii_case_insensitive(rest, "CRON")?;
    let after_cron = rest[cron_pos + 4..].trim_start();

    // Quoted form: 'expr'
    if let Some(after_quote) = after_cron.strip_prefix('\'')
        && let Some(end) = after_quote.find('\'')
    {
        return Some(after_quote[..end].to_string());
    }

    // Unquoted form: 5 whitespace tokens
    let tokens: Vec<&str> = after_cron.split_whitespace().collect();
    if tokens.len() >= 5 {
        return Some(format!(
            "{} {} {} {} {}",
            tokens[0], tokens[1], tokens[2], tokens[3], tokens[4]
        ));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> NodedbStatement {
        let upper = sql.to_uppercase();
        parse_create_schedule(&upper, sql)
    }

    #[test]
    fn unicode_schedule_name_preserves_original_offsets() {
        let sql = "CREATE SCHEDULE cleanupﬀﬀ CRON '0 0 * * *' AS BEGIN DELETE FROM old; END";
        if let NodedbStatement::Automation(AutomationStmt::CreateSchedule {
            name,
            cron_expr,
            body_sql,
            ..
        }) = create(sql)
        {
            assert_eq!(name, "cleanupﬀﬀ");
            assert_eq!(cron_expr, "0 0 * * *");
            assert_eq!(body_sql, "BEGIN DELETE FROM old; END");
        } else {
            panic!("expected CreateSchedule");
        }
    }

    #[test]
    fn basic_schedule() {
        let sql = "CREATE SCHEDULE cleanup CRON '0 0 * * *' AS BEGIN DELETE FROM old; END";
        if let NodedbStatement::Automation(AutomationStmt::CreateSchedule {
            name,
            cron_expr,
            body_sql,
            scope,
            allow_overlap,
            ..
        }) = create(sql)
        {
            assert_eq!(name, "cleanup");
            assert_eq!(cron_expr, "0 0 * * *");
            assert!(body_sql.contains("DELETE FROM old"));
            assert_eq!(scope, "NORMAL");
            assert!(allow_overlap);
        } else {
            panic!("expected CreateSchedule");
        }
    }

    #[test]
    fn scope_local() {
        let sql = "CREATE SCHEDULE gc CRON '0 * * * *' SCOPE LOCAL AS BEGIN RETURN; END";
        if let NodedbStatement::Automation(AutomationStmt::CreateSchedule { scope, .. }) =
            create(sql)
        {
            assert_eq!(scope, "LOCAL");
        } else {
            panic!("expected CreateSchedule");
        }
    }

    #[test]
    fn with_options() {
        let sql = "CREATE SCHEDULE agg CRON '*/5 * * * *' \
                   WITH (ALLOW_OVERLAP = false, MISSED = CATCH_UP) \
                   AS BEGIN INSERT INTO stats SELECT 1; END";
        if let NodedbStatement::Automation(AutomationStmt::CreateSchedule {
            allow_overlap,
            missed_policy,
            ..
        }) = create(sql)
        {
            assert!(!allow_overlap);
            assert_eq!(missed_policy, "CATCH_UP");
        } else {
            panic!("expected CreateSchedule");
        }
    }
}
