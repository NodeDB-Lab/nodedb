// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER/SHOW TRIGGER.

use super::helpers::{extract_after_keyword, extract_name_after_if_exists};
use crate::ddl_ast::statement::{AutomationStmt, NodedbStatement};
use crate::error::SqlError;
use crate::parser::preprocess::lex::{find_ascii_case_insensitive_from, find_ascii_keyword};

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE ") && upper.contains("TRIGGER ") {
            return Ok(Some(parse_create_trigger(upper, trimmed)));
        }
        if upper.starts_with("DROP TRIGGER ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "TRIGGER") {
                None => return Ok(None),
                Some(r) => r?,
            };
            let collection = extract_after_keyword(parts, "ON").unwrap_or_default();
            return Ok(Some(NodedbStatement::Automation(
                AutomationStmt::DropTrigger {
                    name,
                    collection,
                    if_exists,
                },
            )));
        }
        if upper.starts_with("ALTER TRIGGER ") {
            // ALTER TRIGGER <name> ENABLE|DISABLE|OWNER TO <new_owner>
            let name = parts.get(2).map(|s| s.to_lowercase()).unwrap_or_default();
            let action = parts.get(3).map(|s| s.to_uppercase()).unwrap_or_default();
            // When action is "OWNER", "TO" is at index 4 and new_owner at index 5.
            let new_owner = if action == "OWNER" {
                parts.get(5).map(|s| s.trim_end_matches(';').to_string())
            } else {
                None
            };
            return Ok(Some(NodedbStatement::Automation(
                AutomationStmt::AlterTrigger {
                    name,
                    action,
                    new_owner,
                },
            )));
        }
        if upper.starts_with("SHOW TRIGGERS") {
            let collection = if upper.starts_with("SHOW TRIGGERS ON ") {
                parts.get(3).map(|s| s.to_string())
            } else {
                None
            };
            return Ok(Some(NodedbStatement::Automation(
                AutomationStmt::ShowTriggers { collection },
            )));
        }
        Ok(None)
    })()
    .transpose()
}

/// Structural extraction for `CREATE [OR REPLACE] [SYNC|DEFERRED] TRIGGER`.
///
/// Extracts all fields as primitive types. The handler converts timing/events/
/// granularity strings to their respective enums.
fn parse_create_trigger(upper: &str, trimmed: &str) -> NodedbStatement {
    let (or_replace, execution_mode, rest_original) = strip_create_trigger_prefix(upper, trimmed);

    let (header_original, body_sql) = split_header_and_body(rest_original);

    let tokens_orig: Vec<&str> = header_original.split_whitespace().collect();
    let tokens_upper: Vec<String> = tokens_orig.iter().map(|t| t.to_uppercase()).collect();

    let name = tokens_orig
        .first()
        .map(|s| s.to_lowercase())
        .unwrap_or_default();

    let mut i = 1usize;

    let timing = parse_timing_token(&tokens_upper, &mut i);
    let (events_insert, events_update, events_delete) = parse_event_tokens(&tokens_upper, &mut i);

    if i < tokens_upper.len() && tokens_upper[i] == "ON" {
        i += 1;
    }
    let collection = tokens_orig
        .get(i)
        .map(|s| s.to_lowercase())
        .unwrap_or_default();
    i += 1;

    let granularity = parse_granularity_tokens(&tokens_upper, &mut i);
    let when_condition = extract_when_condition(&header_original, &tokens_upper, &mut i);
    let priority = parse_priority_token(&tokens_orig, &tokens_upper, &mut i);
    let security = parse_security_token(&tokens_upper, &mut i);

    NodedbStatement::Automation(AutomationStmt::CreateTrigger {
        or_replace,
        execution_mode,
        name,
        timing,
        events_insert,
        events_update,
        events_delete,
        collection,
        granularity,
        when_condition,
        priority,
        security,
        body_sql,
    })
}

/// Strip `CREATE [OR REPLACE] [SYNC|DEFERRED] TRIGGER ` prefix.
/// Returns `(or_replace, execution_mode, original_rest)`.
fn strip_create_trigger_prefix<'a>(upper: &str, original: &'a str) -> (bool, String, &'a str) {
    const PREFIXES: &[(&str, bool, &str)] = &[
        ("CREATE OR REPLACE SYNC TRIGGER ", true, "SYNC"),
        ("CREATE OR REPLACE DEFERRED TRIGGER ", true, "DEFERRED"),
        ("CREATE OR REPLACE TRIGGER ", true, "ASYNC"),
        ("CREATE SYNC TRIGGER ", false, "SYNC"),
        ("CREATE DEFERRED TRIGGER ", false, "DEFERRED"),
        ("CREATE TRIGGER ", false, "ASYNC"),
    ];
    for &(prefix, or_replace, mode) in PREFIXES {
        if upper.starts_with(prefix) {
            let rest = &original[prefix.len()..];
            return (or_replace, mode.to_string(), rest);
        }
    }
    (false, "ASYNC".to_string(), original)
}

/// Split trigger SQL into `(header_original, body_sql)`.
/// Handles both `$$ ... $$` and `BEGIN ... END` body forms.
fn split_header_and_body(rest: &str) -> (String, String) {
    // Try $$ ... $$ first
    if let Some(first) = rest.find("$$") {
        let inner_start = first + 2;
        if let Some(second) = rest[inner_start..].find("$$") {
            let header = rest[..first].trim().to_string();
            let body = rest[inner_start..inner_start + second].trim().to_string();
            return (header, body);
        }
    }

    // Find BEGIN keyword not inside quotes
    let bytes = rest.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < bytes.len() && bytes[i] == b'\'' {
                        i += 1;
                    }
                    break;
                }
                i += 1;
            }
            continue;
        }
        if find_ascii_case_insensitive_from(rest, "BEGIN", i) == Some(i) {
            let before_ok =
                i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
            let after_ok = i + 5 >= bytes.len()
                || (!bytes[i + 5].is_ascii_alphanumeric() && bytes[i + 5] != b'_');
            if before_ok && after_ok {
                let header = rest[..i].trim().to_string();
                let body = rest[i..].trim().to_string();
                return (header, body);
            }
        }
        i += 1;
    }
    (rest.to_string(), String::new())
}

fn parse_timing_token(tokens: &[String], i: &mut usize) -> String {
    if *i >= tokens.len() {
        return "AFTER".to_string();
    }
    match tokens[*i].as_str() {
        "BEFORE" => {
            *i += 1;
            "BEFORE".to_string()
        }
        "AFTER" => {
            *i += 1;
            "AFTER".to_string()
        }
        "INSTEAD" => {
            *i += 1;
            if *i < tokens.len() && tokens[*i] == "OF" {
                *i += 1;
            }
            "INSTEAD OF".to_string()
        }
        _ => "AFTER".to_string(),
    }
}

fn parse_event_tokens(tokens: &[String], i: &mut usize) -> (bool, bool, bool) {
    let (mut insert, mut update, mut delete) = (false, false, false);
    loop {
        if *i >= tokens.len() {
            break;
        }
        match tokens[*i].as_str() {
            "INSERT" => {
                insert = true;
                *i += 1;
            }
            "UPDATE" => {
                update = true;
                *i += 1;
            }
            "DELETE" => {
                delete = true;
                *i += 1;
            }
            "OR" => {
                *i += 1;
            }
            _ => break,
        }
    }
    (insert, update, delete)
}

fn parse_granularity_tokens(tokens: &[String], i: &mut usize) -> String {
    if *i + 2 < tokens.len() && tokens[*i] == "FOR" && tokens[*i + 1] == "EACH" {
        *i += 2;
        let g = tokens[*i].clone();
        *i += 1;
        g
    } else {
        "ROW".to_string()
    }
}

fn extract_when_condition(
    header_original: &str,
    tokens_upper: &[String],
    i: &mut usize,
) -> Option<String> {
    if *i >= tokens_upper.len() || tokens_upper[*i] != "WHEN" {
        return None;
    }
    *i += 1;

    let when_pos = find_ascii_keyword(header_original, "WHEN")?;
    let after_when = header_original[when_pos + 4..].trim_start();
    if !after_when.starts_with('(') {
        return None;
    }
    let mut depth = 0i32;
    let mut end = 0usize;
    for (j, ch) in after_when.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = j;
                    break;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return None;
    }
    let condition = after_when[1..end].trim().to_string();
    while *i < tokens_upper.len() && tokens_upper[*i] != "PRIORITY" {
        *i += 1;
    }
    Some(condition)
}

fn parse_priority_token(tokens_orig: &[&str], tokens_upper: &[String], i: &mut usize) -> i32 {
    if *i >= tokens_upper.len() || tokens_upper[*i] != "PRIORITY" {
        return 0;
    }
    *i += 1;
    if let Some(val) = tokens_orig.get(*i).and_then(|s| s.parse::<i32>().ok()) {
        *i += 1;
        val
    } else {
        0
    }
}

fn parse_security_token(tokens_upper: &[String], i: &mut usize) -> String {
    if *i >= tokens_upper.len() || tokens_upper[*i] != "SECURITY" {
        return "INVOKER".to_string();
    }
    *i += 1;
    if *i >= tokens_upper.len() {
        return "INVOKER".to_string();
    }
    let mode = tokens_upper[*i].clone();
    *i += 1;
    mode
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> NodedbStatement {
        let upper = sql.to_uppercase();
        parse_create_trigger(&upper, sql)
    }

    #[test]
    fn unicode_trigger_name_before_when_preserves_original_offsets() {
        let sql = "CREATE TRIGGER tﬀﬀ AFTER INSERT ON orders FOR EACH ROW WHEN (NEW.status = 'active') BEGIN RETURN; END";
        if let NodedbStatement::Automation(AutomationStmt::CreateTrigger {
            name,
            when_condition,
            ..
        }) = create(sql)
        {
            assert_eq!(name, "tﬀﬀ");
            assert_eq!(when_condition.as_deref(), Some("NEW.status = 'active'"));
        } else {
            panic!("expected CreateTrigger");
        }
    }

    #[test]
    fn unicode_text_before_begin_preserves_body_boundary() {
        let (header, body) = split_header_and_body(
            "t AFTER INSERT ON orders WHEN (NEW.note = 'ﬀﬀ') BEGIN RETURN; END",
        );
        assert_eq!(header, "t AFTER INSERT ON orders WHEN (NEW.note = 'ﬀﬀ')");
        assert_eq!(body, "BEGIN RETURN; END");
    }

    #[test]
    fn basic_after_insert() {
        let sql = "CREATE TRIGGER audit AFTER INSERT ON orders FOR EACH ROW BEGIN RETURN; END";
        if let NodedbStatement::Automation(AutomationStmt::CreateTrigger {
            name,
            timing,
            events_insert,
            collection,
            granularity,
            ..
        }) = create(sql)
        {
            assert_eq!(name, "audit");
            assert_eq!(timing, "AFTER");
            assert!(events_insert);
            assert_eq!(collection, "orders");
            assert_eq!(granularity, "ROW");
        } else {
            panic!("expected CreateTrigger");
        }
    }

    #[test]
    fn or_replace_sync() {
        let sql = "CREATE OR REPLACE SYNC TRIGGER t AFTER INSERT ON c FOR EACH ROW BEGIN END";
        if let NodedbStatement::Automation(AutomationStmt::CreateTrigger {
            or_replace,
            execution_mode,
            ..
        }) = create(sql)
        {
            assert!(or_replace);
            assert_eq!(execution_mode, "SYNC");
        } else {
            panic!("expected CreateTrigger");
        }
    }

    #[test]
    fn multi_event() {
        let sql = "CREATE TRIGGER t AFTER INSERT OR UPDATE OR DELETE ON c FOR EACH ROW BEGIN END";
        if let NodedbStatement::Automation(AutomationStmt::CreateTrigger {
            events_insert,
            events_update,
            events_delete,
            ..
        }) = create(sql)
        {
            assert!(events_insert);
            assert!(events_update);
            assert!(events_delete);
        } else {
            panic!("expected CreateTrigger");
        }
    }

    #[test]
    fn statement_granularity() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON c FOR EACH STATEMENT BEGIN END";
        if let NodedbStatement::Automation(AutomationStmt::CreateTrigger { granularity, .. }) =
            create(sql)
        {
            assert_eq!(granularity, "STATEMENT");
        } else {
            panic!("expected CreateTrigger");
        }
    }

    #[test]
    fn with_priority() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON c FOR EACH ROW PRIORITY 10 BEGIN END";
        if let NodedbStatement::Automation(AutomationStmt::CreateTrigger { priority, .. }) =
            create(sql)
        {
            assert_eq!(priority, 10);
        } else {
            panic!("expected CreateTrigger");
        }
    }
}
