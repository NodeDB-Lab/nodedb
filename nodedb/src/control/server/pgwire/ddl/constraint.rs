//! DDL handlers for state transition constraints and transition check predicates.
//!
//! Syntax:
//! ```sql
//! ALTER COLLECTION invoices ADD CONSTRAINT status_flow
//!     ON COLUMN status TRANSITIONS (
//!         'draft' -> 'submitted',
//!         'submitted' -> 'approved' BY ROLE 'manager',
//!         'approved' -> 'posted' BY ROLE 'accountant'
//!     );
//!
//! ALTER COLLECTION journal_entries ADD TRANSITION CHECK no_edit_after_seal (
//!     OLD.sealed = FALSE OR (
//!         NEW.amount = OLD.amount AND NEW.account_id = OLD.account_id
//!     )
//! );
//!
//! DROP CONSTRAINT status_flow ON invoices;
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::catalog::types::{StateTransitionDef, TransitionRule};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Handle `ALTER COLLECTION x ADD CONSTRAINT name ON COLUMN col TRANSITIONS (...)`.
pub fn add_state_constraint(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let upper = sql.to_uppercase();

    // ALTER COLLECTION <coll> ADD CONSTRAINT <name> ON COLUMN <col> TRANSITIONS (...)
    let coll_name = parts
        .get(2)
        .ok_or_else(|| err("42601", "missing collection name"))?
        .to_lowercase();

    let constraint_name = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("CONSTRAINT"))
        .and_then(|i| parts.get(i + 1))
        .ok_or_else(|| err("42601", "missing constraint name after CONSTRAINT"))?
        .to_lowercase();

    let column = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))
        .and_then(|i| parts.get(i + 1))
        .ok_or_else(|| err("42601", "missing column name after ON COLUMN"))?
        .to_lowercase();

    let transitions = parse_transitions(&upper)?;

    let def = StateTransitionDef {
        name: constraint_name.clone(),
        column,
        transitions,
    };

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    // Check for duplicate constraint name.
    if coll
        .state_constraints
        .iter()
        .any(|c| c.name == constraint_name)
    {
        return Err(err(
            "42710",
            &format!("constraint '{constraint_name}' already exists"),
        ));
    }

    coll.state_constraints.push(def);
    catalog
        .put_collection(&coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}

/// Handle `ALTER COLLECTION x ADD TRANSITION CHECK name (predicate)`.
pub fn add_transition_check(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // ALTER COLLECTION <coll> ADD TRANSITION CHECK <name> (predicate)
    let coll_name = parts
        .get(2)
        .ok_or_else(|| err("42601", "missing collection name"))?
        .to_lowercase();

    // Find the check name: token after "CHECK"
    let check_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("CHECK"))
        .ok_or_else(|| err("42601", "missing CHECK keyword"))?;
    let check_name = parts
        .get(check_idx + 1)
        .ok_or_else(|| err("42601", "missing name after TRANSITION CHECK"))?
        .to_lowercase()
        .trim_matches('(')
        .to_string();

    // Extract the predicate expression between parentheses.
    let predicate_expr = extract_parenthesized_predicate(sql)?;
    let parsed = parse_transition_predicate(&predicate_expr)?;

    let def = crate::control::security::catalog::types::TransitionCheckDef {
        name: check_name.clone(),
        predicate: parsed,
    };

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if coll.transition_checks.iter().any(|c| c.name == check_name) {
        return Err(err(
            "42710",
            &format!("transition check '{check_name}' already exists"),
        ));
    }

    coll.transition_checks.push(def);
    catalog
        .put_collection(&coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}

/// Handle `DROP CONSTRAINT name ON collection`.
pub fn drop_constraint(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    // DROP CONSTRAINT <name> ON <collection>
    let constraint_name = parts
        .get(2)
        .ok_or_else(|| err("42601", "missing constraint name"))?
        .to_lowercase();

    let coll_name = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("ON"))
        .and_then(|i| parts.get(i + 1))
        .ok_or_else(|| err("42601", "DROP CONSTRAINT requires ON <collection>"))?
        .to_lowercase();

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    let before_state = coll.state_constraints.len();
    let before_check = coll.transition_checks.len();

    coll.state_constraints.retain(|c| c.name != constraint_name);
    coll.transition_checks.retain(|c| c.name != constraint_name);

    if coll.state_constraints.len() == before_state && coll.transition_checks.len() == before_check
    {
        return Err(err(
            "42704",
            &format!("constraint '{constraint_name}' not found on {coll_name}"),
        ));
    }

    catalog
        .put_collection(&coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("DROP CONSTRAINT"))])
}

// ── Parsing helpers ──

/// Parse transition rules from `TRANSITIONS ('a' -> 'b', 'b' -> 'c' BY ROLE 'role', ...)`.
fn parse_transitions(upper: &str) -> PgWireResult<Vec<TransitionRule>> {
    let start = upper
        .find("TRANSITIONS")
        .ok_or_else(|| err("42601", "missing TRANSITIONS keyword"))?;
    let after = &upper[start + "TRANSITIONS".len()..];
    let paren_start = after
        .find('(')
        .ok_or_else(|| err("42601", "TRANSITIONS requires (...)"))?;
    let paren_end = after
        .rfind(')')
        .ok_or_else(|| err("42601", "missing closing ')' in TRANSITIONS"))?;
    let inner = &after[paren_start + 1..paren_end];

    let mut rules = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let rule = parse_single_transition(part)?;
        rules.push(rule);
    }

    if rules.is_empty() {
        return Err(err("42601", "TRANSITIONS must define at least one rule"));
    }

    Ok(rules)
}

/// Parse `'from' -> 'to'` or `'from' -> 'to' BY ROLE 'role'`.
fn parse_single_transition(s: &str) -> PgWireResult<TransitionRule> {
    // Split on arrow: -> or →
    let (from_part, rest) = if let Some(pos) = s.find("->") {
        (&s[..pos], &s[pos + 2..])
    } else if let Some(pos) = s.find("→") {
        (&s[..pos], &s[pos + "→".len()..])
    } else {
        return Err(err(
            "42601",
            &format!("transition rule must contain '->' or '→': '{s}'"),
        ));
    };

    let from = from_part
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    // Check for BY ROLE suffix.
    let (to, required_role) = if let Some(by_pos) = rest.find("BY ROLE") {
        let to = rest[..by_pos]
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        let role = rest[by_pos + "BY ROLE".len()..]
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        (to, Some(role))
    } else {
        let to = rest.trim().trim_matches('\'').trim_matches('"').to_string();
        (to, None)
    };

    if from.is_empty() || to.is_empty() {
        return Err(err("42601", "transition rule: from and to values required"));
    }

    Ok(TransitionRule {
        from,
        to,
        required_role,
    })
}

/// Extract the parenthesized predicate from a TRANSITION CHECK DDL statement.
///
/// Finds the first `(` after `CHECK name` and the matching closing `)`.
fn extract_parenthesized_predicate(sql: &str) -> PgWireResult<String> {
    let upper = sql.to_uppercase();
    let check_pos = upper
        .find("CHECK")
        .ok_or_else(|| err("42601", "missing CHECK keyword"))?;
    let after_check = &sql[check_pos + 5..]; // skip "CHECK"

    // Skip the check name, then find the opening '('.
    let paren_start = after_check
        .find('(')
        .ok_or_else(|| err("42601", "TRANSITION CHECK requires (predicate)"))?;

    // Find the matching closing ')' with nesting support.
    let body = &after_check[paren_start + 1..];
    let mut depth = 1;
    let mut end = 0;
    for (i, ch) in body.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(err("42601", "unmatched parentheses in TRANSITION CHECK"));
    }

    Ok(body[..end].trim().to_string())
}

/// Parse a transition check predicate string into a SqlExpr.
///
/// Supports simple comparisons with `OLD.column` and `NEW.column` references,
/// connected by `AND` / `OR`. This is a simplified parser for the common patterns:
/// - `OLD.sealed = FALSE`
/// - `NEW.amount >= OLD.amount`
/// - `OLD.sealed = FALSE OR (NEW.amount = OLD.amount AND NEW.status = OLD.status)`
fn parse_transition_predicate(s: &str) -> PgWireResult<crate::bridge::expr_eval::SqlExpr> {
    use crate::bridge::expr_eval::{BinaryOp, SqlExpr};

    let s = s.trim();
    if s.is_empty() {
        return Err(err("42601", "empty predicate"));
    }

    // Split on OR (lowest precedence).
    if let Some((left, right)) = split_top_level(s, " OR ") {
        let l = parse_transition_predicate(left)?;
        let r = parse_transition_predicate(right)?;
        return Ok(SqlExpr::BinaryOp {
            left: Box::new(l),
            op: BinaryOp::Or,
            right: Box::new(r),
        });
    }

    // Split on AND.
    if let Some((left, right)) = split_top_level(s, " AND ") {
        let l = parse_transition_predicate(left)?;
        let r = parse_transition_predicate(right)?;
        return Ok(SqlExpr::BinaryOp {
            left: Box::new(l),
            op: BinaryOp::And,
            right: Box::new(r),
        });
    }

    // Strip outer parens.
    if s.starts_with('(') && s.ends_with(')') {
        let inner = &s[1..s.len() - 1];
        // Verify the parens are matching (not just first/last chars).
        let mut depth = 0i32;
        let mut all_inner = true;
        for (i, ch) in inner.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth < 0 {
                        all_inner = false;
                        break;
                    }
                }
                _ => {}
            }
            let _ = i;
        }
        if all_inner && depth == 0 {
            return parse_transition_predicate(inner);
        }
    }

    // Parse a simple comparison: expr op expr
    parse_simple_comparison(s)
}

/// Parse a simple comparison like `OLD.sealed = FALSE` or `NEW.amount >= OLD.amount`.
fn parse_simple_comparison(s: &str) -> PgWireResult<crate::bridge::expr_eval::SqlExpr> {
    use crate::bridge::expr_eval::{BinaryOp, SqlExpr};

    // Try operators from longest to shortest to avoid prefix matching.
    for (op_str, op) in &[
        ("!=", BinaryOp::NotEq),
        ("<>", BinaryOp::NotEq),
        (">=", BinaryOp::GtEq),
        ("<=", BinaryOp::LtEq),
        ("=", BinaryOp::Eq),
        (">", BinaryOp::Gt),
        ("<", BinaryOp::Lt),
    ] {
        if let Some((left, right)) = split_on_operator(s, op_str) {
            let l = parse_value_ref(left.trim())?;
            let r = parse_value_ref(right.trim())?;
            return Ok(SqlExpr::BinaryOp {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            });
        }
    }

    Err(err(
        "42601",
        &format!("cannot parse transition predicate term: '{s}'"),
    ))
}

/// Parse a value reference: `OLD.column`, `NEW.column`, `column`, literal.
fn parse_value_ref(s: &str) -> PgWireResult<crate::bridge::expr_eval::SqlExpr> {
    use crate::bridge::expr_eval::SqlExpr;

    let upper = s.to_uppercase();
    if let Some(col) = upper.strip_prefix("OLD.") {
        return Ok(SqlExpr::OldColumn(col.to_lowercase()));
    }
    if let Some(col) = upper.strip_prefix("NEW.") {
        return Ok(SqlExpr::Column(col.to_lowercase()));
    }

    // Literal: TRUE, FALSE, NULL, quoted string, number.
    if upper == "TRUE" {
        return Ok(SqlExpr::Literal(serde_json::Value::Bool(true)));
    }
    if upper == "FALSE" {
        return Ok(SqlExpr::Literal(serde_json::Value::Bool(false)));
    }
    if upper == "NULL" {
        return Ok(SqlExpr::Literal(serde_json::Value::Null));
    }
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        let inner = &s[1..s.len() - 1];
        return Ok(SqlExpr::Literal(serde_json::Value::String(
            inner.to_string(),
        )));
    }
    if let Ok(n) = s.parse::<f64>() {
        return Ok(SqlExpr::Literal(serde_json::json!(n)));
    }

    // Bare column name (resolves against NEW document).
    Ok(SqlExpr::Column(s.to_lowercase()))
}

/// Split a string at the top-level occurrence of `sep` (respecting parentheses).
fn split_top_level<'a>(s: &'a str, sep: &str) -> Option<(&'a str, &'a str)> {
    let upper = s.to_uppercase();
    let mut depth = 0i32;
    let sep_upper = sep.to_uppercase();
    let mut i = 0;
    while i < upper.len() {
        let ch = upper.as_bytes()[i];
        match ch {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && upper[i..].starts_with(&sep_upper) {
            return Some((&s[..i], &s[i + sep.len()..]));
        }
        i += 1;
    }
    None
}

/// Split on comparison operator, avoiding >= <= != <>.
fn split_on_operator<'a>(s: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    let mut start = 0;
    while let Some(pos) = s[start..].find(op) {
        let abs_pos = start + pos;
        // Avoid matching >= when looking for > , or <= when looking for <.
        if op == "=" && abs_pos > 0 {
            let prev = s.as_bytes()[abs_pos - 1];
            if prev == b'>' || prev == b'<' || prev == b'!' {
                start = abs_pos + op.len();
                continue;
            }
        }
        if op == ">" && abs_pos + 1 < s.len() && s.as_bytes()[abs_pos + 1] == b'=' {
            start = abs_pos + 2;
            continue;
        }
        if op == "<" && abs_pos + 1 < s.len() {
            let next = s.as_bytes()[abs_pos + 1];
            if next == b'=' || next == b'>' {
                start = abs_pos + 2;
                continue;
            }
        }
        return Some((&s[..abs_pos], &s[abs_pos + op.len()..]));
    }
    None
}

fn err(code: &str, msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        msg.to_owned(),
    )))
}
