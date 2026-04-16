//! CREATE TRIGGER SQL parser.

use pgwire::error::PgWireResult;

use crate::control::security::catalog::trigger_types::*;

use super::super::super::types::sqlstate_error;

pub(super) struct ParsedCreateTrigger {
    pub or_replace: bool,
    pub name: String,
    pub timing: TriggerTiming,
    pub events: TriggerEvents,
    pub collection: String,
    pub granularity: TriggerGranularity,
    pub when_condition: Option<String>,
    pub priority: i32,
    pub execution_mode: TriggerExecutionMode,
    pub security: TriggerSecurity,
    pub body_sql: String,
}

/// Syntax:
/// ```text
/// CREATE [OR REPLACE] [SYNC | DEFERRED] TRIGGER <name> ...
/// ```
/// Default execution mode is ASYNC (omit the keyword).
pub(super) fn parse_create_trigger(sql: &str) -> PgWireResult<ParsedCreateTrigger> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    // Parse prefix: CREATE [OR REPLACE] [SYNC|DEFERRED] TRIGGER
    let (or_replace, execution_mode, rest) = parse_prefix(&upper, trimmed)?;

    // Support both `BEGIN ... END` and `$$ BEGIN ... END; $$` body syntax.
    // Dollar-quoting is needed when psql sends the SQL (psql splits on `;`).
    let (header, body_sql) = if let Some((h, b)) = extract_dollar_quoted_body(rest) {
        (h, b)
    } else {
        let begin_pos = find_begin_pos(rest)
            .ok_or_else(|| sqlstate_error("42601", "trigger body must start with BEGIN or $$"))?;
        let header = rest[..begin_pos].trim();
        let body_sql = rest[begin_pos..].trim().to_string();
        (header, body_sql)
    };

    let tokens: Vec<&str> = header.split_whitespace().collect();
    if tokens.is_empty() {
        return Err(sqlstate_error("42601", "trigger name required"));
    }

    let name = tokens[0].to_lowercase();
    let mut i = 1;

    let timing = parse_timing(&tokens, &mut i)?;
    let events = parse_events(&tokens, &mut i)?;

    if i >= tokens.len() || !tokens[i].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON <collection>"));
    }
    i += 1;
    if i >= tokens.len() {
        return Err(sqlstate_error("42601", "expected collection name after ON"));
    }
    let collection = tokens[i].to_lowercase();
    i += 1;

    let granularity = parse_granularity(&tokens, &mut i)?;
    let when_condition = parse_when_clause(header, &tokens, &mut i)?;
    let priority = parse_priority(&tokens, &mut i)?;
    let security = parse_security(&tokens, &mut i)?;

    Ok(ParsedCreateTrigger {
        or_replace,
        name,
        timing,
        events,
        collection,
        granularity,
        when_condition,
        priority,
        execution_mode,
        security,
        body_sql,
    })
}

/// Parse `CREATE [OR REPLACE] [SYNC|DEFERRED] TRIGGER` prefix.
/// Returns `(or_replace, execution_mode, rest_of_sql)`.
fn parse_prefix<'a>(
    upper: &str,
    original: &'a str,
) -> PgWireResult<(bool, TriggerExecutionMode, &'a str)> {
    // Try all valid prefixes from most specific to least (no heap allocations).
    const PREFIXES: &[(&str, bool, TriggerExecutionMode)] = &[
        (
            "CREATE OR REPLACE SYNC TRIGGER ",
            true,
            TriggerExecutionMode::Sync,
        ),
        (
            "CREATE OR REPLACE DEFERRED TRIGGER ",
            true,
            TriggerExecutionMode::Deferred,
        ),
        (
            "CREATE OR REPLACE TRIGGER ",
            true,
            TriggerExecutionMode::Async,
        ),
        ("CREATE SYNC TRIGGER ", false, TriggerExecutionMode::Sync),
        (
            "CREATE DEFERRED TRIGGER ",
            false,
            TriggerExecutionMode::Deferred,
        ),
        ("CREATE TRIGGER ", false, TriggerExecutionMode::Async),
    ];
    for &(prefix, or_replace, mode) in PREFIXES {
        if upper.starts_with(prefix) {
            return Ok((or_replace, mode, &original[prefix.len()..]));
        }
    }
    Err(sqlstate_error(
        "42601",
        "expected CREATE [SYNC|DEFERRED] TRIGGER",
    ))
}

fn parse_timing(tokens: &[&str], i: &mut usize) -> PgWireResult<TriggerTiming> {
    if *i >= tokens.len() {
        return Err(sqlstate_error(
            "42601",
            "expected BEFORE, AFTER, or INSTEAD OF",
        ));
    }
    let t = tokens[*i].to_uppercase();
    match t.as_str() {
        "BEFORE" => {
            *i += 1;
            Ok(TriggerTiming::Before)
        }
        "AFTER" => {
            *i += 1;
            Ok(TriggerTiming::After)
        }
        "INSTEAD" => {
            *i += 1;
            if *i < tokens.len() && tokens[*i].eq_ignore_ascii_case("OF") {
                *i += 1;
            }
            Ok(TriggerTiming::InsteadOf)
        }
        _ => Err(sqlstate_error(
            "42601",
            &format!("expected BEFORE/AFTER/INSTEAD OF, got '{t}'"),
        )),
    }
}

fn parse_events(tokens: &[&str], i: &mut usize) -> PgWireResult<TriggerEvents> {
    let mut events = TriggerEvents {
        on_insert: false,
        on_update: false,
        on_delete: false,
    };

    if *i >= tokens.len() {
        return Err(sqlstate_error(
            "42601",
            "expected INSERT, UPDATE, or DELETE",
        ));
    }

    loop {
        if *i >= tokens.len() {
            break;
        }
        let t = tokens[*i].to_uppercase();
        match t.as_str() {
            "INSERT" => {
                events.on_insert = true;
                *i += 1;
            }
            "UPDATE" => {
                events.on_update = true;
                *i += 1;
            }
            "DELETE" => {
                events.on_delete = true;
                *i += 1;
            }
            "OR" => {
                *i += 1;
            }
            _ => break,
        }
    }

    if !events.on_insert && !events.on_update && !events.on_delete {
        return Err(sqlstate_error("42601", "at least one event required"));
    }
    Ok(events)
}

fn parse_granularity(tokens: &[&str], i: &mut usize) -> PgWireResult<TriggerGranularity> {
    if *i + 2 >= tokens.len()
        || !tokens[*i].eq_ignore_ascii_case("FOR")
        || !tokens[*i + 1].eq_ignore_ascii_case("EACH")
    {
        return Err(sqlstate_error(
            "42601",
            "expected FOR EACH ROW or FOR EACH STATEMENT",
        ));
    }
    *i += 2;
    let g = tokens[*i].to_uppercase();
    *i += 1;
    match g.as_str() {
        "ROW" => Ok(TriggerGranularity::Row),
        "STATEMENT" => Ok(TriggerGranularity::Statement),
        _ => Err(sqlstate_error(
            "42601",
            &format!("expected ROW or STATEMENT, got '{g}'"),
        )),
    }
}

fn parse_when_clause(header: &str, tokens: &[&str], i: &mut usize) -> PgWireResult<Option<String>> {
    if *i >= tokens.len() || !tokens[*i].eq_ignore_ascii_case("WHEN") {
        return Ok(None);
    }
    *i += 1;

    let when_pos = header.to_uppercase().find("WHEN").unwrap_or(0);
    let after_when = header[when_pos + 4..].trim_start();
    if !after_when.starts_with('(') {
        return Err(sqlstate_error(
            "42601",
            "WHEN condition must be in parentheses",
        ));
    }
    let mut depth = 0i32;
    let mut end = 0;
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
        return Err(sqlstate_error("42601", "unmatched '(' in WHEN clause"));
    }
    let condition = after_when[1..end].trim().to_string();

    while *i < tokens.len() && !tokens[*i].eq_ignore_ascii_case("PRIORITY") {
        *i += 1;
    }

    Ok(Some(condition))
}

fn parse_priority(tokens: &[&str], i: &mut usize) -> PgWireResult<i32> {
    if *i >= tokens.len() || !tokens[*i].eq_ignore_ascii_case("PRIORITY") {
        return Ok(0);
    }
    *i += 1;
    if *i >= tokens.len() {
        return Err(sqlstate_error("42601", "expected number after PRIORITY"));
    }
    let val: i32 = tokens[*i]
        .parse()
        .map_err(|_| sqlstate_error("42601", &format!("invalid priority: '{}'", tokens[*i])))?;
    *i += 1;
    Ok(val)
}

/// Parse optional `SECURITY INVOKER` or `SECURITY DEFINER` clause.
/// Defaults to INVOKER if not specified.
fn parse_security(tokens: &[&str], i: &mut usize) -> PgWireResult<TriggerSecurity> {
    if *i >= tokens.len() || !tokens[*i].eq_ignore_ascii_case("SECURITY") {
        return Ok(TriggerSecurity::Invoker);
    }
    *i += 1;
    if *i >= tokens.len() {
        return Err(sqlstate_error(
            "42601",
            "expected INVOKER or DEFINER after SECURITY",
        ));
    }
    let mode = tokens[*i].to_uppercase();
    *i += 1;
    match mode.as_str() {
        "INVOKER" => Ok(TriggerSecurity::Invoker),
        "DEFINER" => Ok(TriggerSecurity::Definer),
        _ => Err(sqlstate_error(
            "42601",
            &format!("expected INVOKER or DEFINER, got '{mode}'"),
        )),
    }
}

/// Extract trigger body from `$$ ... $$` dollar-quoted syntax.
/// Returns `(header, body_sql)` where body_sql includes `BEGIN ... END`.
fn extract_dollar_quoted_body(s: &str) -> Option<(&str, String)> {
    let first = s.find("$$")?;
    let inner_start = first + 2;
    let second = s[inner_start..].find("$$")?;
    let header = s[..first].trim();
    let body = s[inner_start..inner_start + second].trim().to_string();
    Some((header, body))
}

fn find_begin_pos(s: &str) -> Option<usize> {
    let upper = s.to_uppercase();
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        // Skip single-quoted string literals so that 'BEGIN' inside a
        // string (e.g. in a WHEN clause) is not mistaken for the body start.
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    i += 1;
                    // Handle '' escape.
                    if i < bytes.len() && bytes[i] == b'\'' {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            continue;
        }

        // Check for "BEGIN" keyword at this position.
        if i + 5 <= bytes.len() && &upper[i..i + 5] == "BEGIN" {
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
            let after_pos = i + 5;
            let after_ok = after_pos >= bytes.len()
                || !bytes[after_pos].is_ascii_alphanumeric() && bytes[after_pos] != b'_';
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_after_insert() {
        let sql = "CREATE TRIGGER audit_insert \
                    AFTER INSERT ON orders \
                    FOR EACH ROW \
                    BEGIN INSERT INTO audit (id) VALUES (NEW.id); END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert_eq!(parsed.name, "audit_insert");
        assert_eq!(parsed.timing, TriggerTiming::After);
        assert!(parsed.events.on_insert);
        assert!(!parsed.events.on_update);
        assert_eq!(parsed.collection, "orders");
        assert_eq!(parsed.granularity, TriggerGranularity::Row);
        assert!(parsed.when_condition.is_none());
        assert_eq!(parsed.priority, 0);
    }

    #[test]
    fn parse_multi_event() {
        let sql = "CREATE TRIGGER t AFTER INSERT OR UPDATE OR DELETE ON c \
                    FOR EACH ROW BEGIN RETURN; END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert!(parsed.events.on_insert);
        assert!(parsed.events.on_update);
        assert!(parsed.events.on_delete);
    }

    #[test]
    fn parse_before_with_when() {
        let sql = "CREATE TRIGGER validate BEFORE INSERT ON orders \
                    FOR EACH ROW WHEN (NEW.total > 0) BEGIN RETURN; END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert_eq!(parsed.timing, TriggerTiming::Before);
        assert_eq!(parsed.when_condition.as_deref(), Some("NEW.total > 0"));
    }

    #[test]
    fn parse_with_priority() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON c \
                    FOR EACH ROW PRIORITY 10 BEGIN RETURN; END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert_eq!(parsed.priority, 10);
    }

    #[test]
    fn parse_or_replace() {
        let sql = "CREATE OR REPLACE TRIGGER t AFTER INSERT ON c \
                    FOR EACH ROW BEGIN RETURN; END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert!(parsed.or_replace);
    }

    #[test]
    fn parse_statement_level() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON c \
                    FOR EACH STATEMENT BEGIN RETURN; END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert_eq!(parsed.granularity, TriggerGranularity::Statement);
    }

    #[test]
    fn parse_sync_trigger() {
        let sql = "CREATE SYNC TRIGGER t AFTER INSERT ON c \
                    FOR EACH ROW BEGIN RETURN; END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert_eq!(parsed.execution_mode, TriggerExecutionMode::Sync);
        assert_eq!(parsed.name, "t");
    }

    #[test]
    fn parse_deferred_trigger() {
        let sql = "CREATE DEFERRED TRIGGER t AFTER INSERT ON c \
                    FOR EACH ROW BEGIN RETURN; END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert_eq!(parsed.execution_mode, TriggerExecutionMode::Deferred);
    }

    #[test]
    fn parse_default_is_async() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON c \
                    FOR EACH ROW BEGIN RETURN; END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert_eq!(parsed.execution_mode, TriggerExecutionMode::Async);
    }

    #[test]
    fn parse_or_replace_sync() {
        let sql = "CREATE OR REPLACE SYNC TRIGGER t AFTER INSERT ON c \
                    FOR EACH ROW BEGIN RETURN; END";
        let parsed = parse_create_trigger(sql).unwrap();
        assert!(parsed.or_replace);
        assert_eq!(parsed.execution_mode, TriggerExecutionMode::Sync);
    }
}
