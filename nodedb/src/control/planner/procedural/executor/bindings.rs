// SPDX-License-Identifier: BUSL-1.1

//! Row variable bindings for trigger execution.
//!
//! Provides NEW/OLD row references and statement-level variables (TG_OP, etc.)
//! that are substituted into SQL text before planning.
//!
//! Uses `nodedb_types::Value` internally instead of `serde_json::Value` for
//! fewer heap allocations. JSON values are converted at the API boundary.

use std::collections::HashMap;

use nodedb_types::Value;

use super::sql_bytes::skip_ascii_whitespace;

/// Row bindings available during trigger/procedure body execution.
#[derive(Debug, Clone)]
pub struct RowBindings {
    /// NEW row fields (INSERT/UPDATE). None for DELETE.
    new_row: Option<HashMap<String, Value>>,
    /// OLD row fields (UPDATE/DELETE). None for INSERT.
    old_row: Option<HashMap<String, Value>>,
    /// DML operation name: "INSERT", "UPDATE", "DELETE".
    tg_op: String,
    /// Collection name.
    tg_table_name: String,
    /// Trigger timing: "BEFORE" or "AFTER".
    tg_when: String,
    /// Additional variables (loop variables, procedure parameters).
    variables: HashMap<String, String>,
}

impl RowBindings {
    /// Create empty bindings (for stored procedure execution — no NEW/OLD).
    pub fn empty() -> Self {
        Self {
            new_row: None,
            old_row: None,
            tg_op: String::new(),
            tg_table_name: String::new(),
            tg_when: String::new(),
            variables: HashMap::new(),
        }
    }

    /// Create bindings with procedure parameter values substituted.
    pub fn with_params(params: HashMap<String, String>) -> Self {
        // Normalize in a stable source-key order so malformed case-colliding
        // input maps cannot make the selected binding depend on HashMap order.
        let mut params = params.into_iter().collect::<Vec<_>>();
        params.sort_by(|(left, _), (right, _)| left.cmp(right));
        let variables = params
            .into_iter()
            .map(|(name, value)| (name.to_lowercase(), value))
            .collect();
        Self {
            new_row: None,
            old_row: None,
            tg_op: String::new(),
            tg_table_name: String::new(),
            tg_when: String::new(),
            variables,
        }
    }

    /// Create a copy with an additional variable binding (for loop variables).
    pub fn with_variable(&self, name: &str, value: &str) -> Self {
        let mut copy = self.clone();
        copy.variables
            .insert(name.to_lowercase(), value.to_string());
        copy
    }

    /// Create a copy with replaced NEW row fields (for BEFORE trigger chaining).
    ///
    /// Used when a BEFORE trigger modifies the NEW row and subsequent triggers
    /// in the chain need to see the updated values.
    pub fn with_new_row(&self, new_row: HashMap<String, Value>) -> Self {
        let mut copy = self.clone();
        copy.new_row = Some(new_row);
        copy
    }

    /// Create bindings for a BEFORE INSERT trigger.
    pub fn before_insert(collection: &str, new_row: HashMap<String, Value>) -> Self {
        Self {
            new_row: Some(new_row),
            old_row: None,
            tg_op: "INSERT".into(),
            tg_table_name: collection.into(),
            tg_when: "BEFORE".into(),
            variables: HashMap::new(),
        }
    }

    /// Create bindings for a BEFORE UPDATE trigger.
    pub fn before_update(
        collection: &str,
        old_row: HashMap<String, Value>,
        new_row: HashMap<String, Value>,
    ) -> Self {
        Self {
            new_row: Some(new_row),
            old_row: Some(old_row),
            tg_op: "UPDATE".into(),
            tg_table_name: collection.into(),
            tg_when: "BEFORE".into(),
            variables: HashMap::new(),
        }
    }

    /// Create bindings for a BEFORE DELETE trigger.
    pub fn before_delete(collection: &str, old_row: HashMap<String, Value>) -> Self {
        Self {
            new_row: None,
            old_row: Some(old_row),
            tg_op: "DELETE".into(),
            tg_table_name: collection.into(),
            tg_when: "BEFORE".into(),
            variables: HashMap::new(),
        }
    }

    /// Create bindings for an AFTER INSERT trigger.
    pub fn after_insert(collection: &str, new_row: HashMap<String, Value>) -> Self {
        Self {
            new_row: Some(new_row),
            old_row: None,
            tg_op: "INSERT".into(),
            tg_table_name: collection.into(),
            tg_when: "AFTER".into(),
            variables: HashMap::new(),
        }
    }

    /// Create bindings for an AFTER UPDATE trigger.
    pub fn after_update(
        collection: &str,
        old_row: HashMap<String, Value>,
        new_row: HashMap<String, Value>,
    ) -> Self {
        Self {
            new_row: Some(new_row),
            old_row: Some(old_row),
            tg_op: "UPDATE".into(),
            tg_table_name: collection.into(),
            tg_when: "AFTER".into(),
            variables: HashMap::new(),
        }
    }

    /// Create bindings for a STATEMENT-level trigger (no NEW/OLD row).
    pub fn statement(collection: &str, tg_op: &str) -> Self {
        Self {
            new_row: None,
            old_row: None,
            tg_op: tg_op.into(),
            tg_table_name: collection.into(),
            tg_when: "AFTER".into(),
            variables: HashMap::new(),
        }
    }

    /// Create bindings for an AFTER DELETE trigger.
    pub fn after_delete(collection: &str, old_row: HashMap<String, Value>) -> Self {
        Self {
            new_row: None,
            old_row: Some(old_row),
            tg_op: "DELETE".into(),
            tg_table_name: collection.into(),
            tg_when: "AFTER".into(),
            variables: HashMap::new(),
        }
    }

    /// Substitute NEW.field, OLD.field, TG_OP, TG_TABLE_NAME, TG_WHEN in SQL text.
    ///
    /// Replaces:
    /// - `NEW.field_name` → SQL literal of the field value
    /// - `OLD.field_name` → SQL literal of the field value
    /// - `TG_OP` → 'INSERT' / 'UPDATE' / 'DELETE'
    /// - `TG_TABLE_NAME` → 'collection_name'
    /// - `TG_WHEN` → 'BEFORE' / 'AFTER'
    /// - `COALESCE(NEW.field, OLD.field)` handled naturally since each is replaced
    pub fn substitute(&self, sql: &str) -> String {
        SqlBindingScanner {
            bindings: self,
            sql,
        }
        .scan()
    }
}

/// Single-pass lexical scanner for trigger/procedure bindings.
///
/// Only unquoted ASCII identifiers are candidates.  Replacement precedence is
/// row references, then user variables, then reserved TG variables; inserted
/// replacement text is copied directly and is never scanned again.
struct SqlBindingScanner<'a> {
    bindings: &'a RowBindings,
    sql: &'a str,
}

impl SqlBindingScanner<'_> {
    fn scan(&self) -> String {
        let bytes = self.sql.as_bytes();
        let mut output = String::with_capacity(self.sql.len());
        let mut cursor = 0;
        while cursor < bytes.len() {
            let opaque_end = match bytes[cursor] {
                b'\'' => Some(consume_quoted(bytes, cursor, b'\'')),
                b'"' => Some(consume_quoted(bytes, cursor, b'"')),
                b'-' if bytes.get(cursor + 1) == Some(&b'-') => {
                    Some(consume_line_comment(bytes, cursor))
                }
                b'/' if bytes.get(cursor + 1) == Some(&b'*') => {
                    Some(consume_block_comment(bytes, cursor))
                }
                b'$' => consume_dollar_quote(self.sql, cursor),
                _ => None,
            };
            if let Some(end) = opaque_end {
                output.push_str(&self.sql[cursor..end]);
                cursor = end;
                continue;
            }

            if self.sql[cursor..]
                .chars()
                .next()
                .is_some_and(is_identifier_start)
            {
                let token_end = consume_identifier(self.sql, cursor);
                if let Some((replacement, end)) = self.qualified_replacement(cursor, token_end) {
                    push_replacement(&mut output, &replacement);
                    cursor = end;
                    continue;
                }
                let token = &self.sql[cursor..token_end];
                if let Some(replacement) = self.standalone_replacement(token) {
                    push_replacement(&mut output, &replacement);
                } else {
                    output.push_str(token);
                }
                cursor = token_end;
                continue;
            }

            let char_len = self.sql[cursor..].chars().next().map_or(1, char::len_utf8);
            output.push_str(&self.sql[cursor..cursor + char_len]);
            cursor += char_len;
        }
        output
    }

    fn qualified_replacement(&self, start: usize, qualifier_end: usize) -> Option<(String, usize)> {
        let qualifier = &self.sql[start..qualifier_end];
        let row = if qualifier.eq_ignore_ascii_case("NEW") {
            self.bindings.new_row.as_ref()?
        } else if qualifier.eq_ignore_ascii_case("OLD") {
            self.bindings.old_row.as_ref()?
        } else {
            return None;
        };
        let bytes = self.sql.as_bytes();
        let dot = skip_ascii_whitespace(bytes, qualifier_end);
        if bytes.get(dot) != Some(&b'.') {
            return None;
        }
        let field_start = skip_ascii_whitespace(bytes, dot + 1);
        if !self.sql[field_start..]
            .chars()
            .next()
            .is_some_and(is_identifier_start)
        {
            return None;
        }
        let field_end = consume_identifier(self.sql, field_start);
        let field = &self.sql[field_start..field_end];
        let value = row.get(field).or_else(|| {
            let folded = field.to_lowercase();
            row.iter()
                .filter(|(name, _)| name.to_lowercase() == folded)
                .min_by(|(left, _), (right, _)| left.cmp(right))
                .map(|(_, value)| value)
        })?;
        Some((value.to_sql_literal(), field_end))
    }

    fn standalone_replacement(&self, token: &str) -> Option<String> {
        if let Some(value) = self.bindings.variables.get(&token.to_lowercase()) {
            return Some(value.clone());
        }
        let value = if token.eq_ignore_ascii_case("TG_OP") {
            &self.bindings.tg_op
        } else if token.eq_ignore_ascii_case("TG_TABLE_NAME") {
            &self.bindings.tg_table_name
        } else if token.eq_ignore_ascii_case("TG_WHEN") {
            &self.bindings.tg_when
        } else {
            return None;
        };
        Some(Value::String(value.clone()).to_sql_literal())
    }
}

/// Append a binding's `replacement` text to `output`. A negative value is
/// wrapped in parentheses, so it never joins a `-` before it into `--`,
/// which starts a comment, and stays one value next to any operator.
fn push_replacement(output: &mut String, replacement: &str) {
    if replacement.starts_with('-') {
        output.push('(');
        output.push_str(replacement);
        output.push(')');
    } else {
        output.push_str(replacement);
    }
}

fn consume_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        if bytes[cursor] == quote {
            if bytes.get(cursor + 1) == Some(&quote) {
                cursor += 2;
            } else {
                return cursor + 1;
            }
        } else {
            cursor += 1;
        }
    }
    bytes.len()
}

fn consume_dollar_quote(sql: &str, start: usize) -> Option<usize> {
    let rest = &sql[start..];
    let close = rest[1..].find('$')? + 1;
    let tag = &rest[..=close];
    let tag_body = &tag[1..tag.len() - 1];
    if !tag_body.is_empty()
        && (!tag_body
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_alphabetic())
            || !tag_body.chars().all(|ch| ch == '_' || ch.is_alphanumeric()))
    {
        return None;
    }
    let body_start = start + tag.len();
    Some(
        sql[body_start..]
            .find(tag)
            .map_or(sql.len(), |offset| body_start + offset + tag.len()),
    )
}

fn consume_line_comment(bytes: &[u8], start: usize) -> usize {
    bytes[start..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map_or(bytes.len(), |offset| start + offset)
}

fn consume_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut cursor = start + 2;
    let mut depth = 1usize;
    while cursor + 1 < bytes.len() {
        match (bytes[cursor], bytes[cursor + 1]) {
            (b'/', b'*') => {
                depth += 1;
                cursor += 2;
            }
            (b'*', b'/') => {
                depth -= 1;
                cursor += 2;
                if depth == 0 {
                    return cursor;
                }
            }
            _ => cursor += 1,
        }
    }
    bytes.len()
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_alphabetic()
}

fn consume_identifier(input: &str, start: usize) -> usize {
    input[start..]
        .char_indices()
        .find_map(|(offset, ch)| {
            (offset > 0 && !(ch == '_' || ch == '$' || ch.is_alphanumeric()))
                .then_some(start + offset)
        })
        .unwrap_or(input.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scanner_preserves_unicode_and_replaces_unquoted_references() {
        let mut row = HashMap::new();
        row.insert("id".into(), Value::Integer(42));
        let bindings = RowBindings::after_insert("orders", row);
        assert_eq!(
            bindings.substitute("SELECT 'ﬀﬀ雪', NEW.id, new.ID"),
            "SELECT 'ﬀﬀ雪', 42, 42"
        );
    }

    #[test]
    fn scanner_leaves_literals_identifiers_and_comments_opaque() {
        let mut row = HashMap::new();
        row.insert("id".into(), Value::Integer(7));
        let bindings = RowBindings::after_insert("orders", row);
        let sql = "SELECT 'NEW.id '' TG_OP', \"NEW.id\", $$NEW.id TG_OP$$, $tag$NEW.id TG_OP$tag$, NEW.id -- NEW.id TG_OP\n/* outer NEW.id /* inner TG_OP */ NEW.id */ TG_OP";
        assert_eq!(
            bindings.substitute(sql),
            "SELECT 'NEW.id '' TG_OP', \"NEW.id\", $$NEW.id TG_OP$$, $tag$NEW.id TG_OP$tag$, 7 -- NEW.id TG_OP\n/* outer NEW.id /* inner TG_OP */ NEW.id */ 'INSERT'"
        );
    }

    #[test]
    fn scanner_enforces_boundaries_case_and_spaced_references() {
        let mut row = HashMap::new();
        row.insert("id".into(), Value::Integer(9));
        let bindings = RowBindings::after_insert("orders", row).with_variable("loop_var", "17");
        assert_eq!(
            bindings
                .substitute("NEW . id, old.id, XNEW.id, NEW.identifier, LOOP_VAR, loop_var_suffix"),
            "9, old.id, XNEW.id, NEW.identifier, 17, loop_var_suffix"
        );
    }

    #[test]
    fn row_field_resolution_is_exact_first_and_folded_deterministic() {
        let row = HashMap::from([
            ("id".to_string(), Value::Integer(1)),
            ("ID".to_string(), Value::Integer(2)),
            ("É".to_string(), Value::Integer(3)),
        ]);
        let bindings = RowBindings::after_insert("orders", row);
        assert_eq!(
            bindings.substitute("NEW.id, NEW.ID, NEW.Id, NEW.é"),
            "1, 2, 2, 3"
        );
    }

    #[test]
    fn scanner_escapes_tg_values_and_never_rescans_replacements() {
        let bindings = RowBindings::statement("o'reilly", "INSERT").with_variable("value", "TG_OP");
        assert_eq!(
            bindings.substitute("TG_TABLE_NAME, value, TG_OP"),
            "'o''reilly', TG_OP, 'INSERT'"
        );
    }

    #[test]
    fn variable_names_are_case_normalized_deterministically() {
        let params = HashMap::from([
            ("i".to_string(), "parameter".to_string()),
            ("ID".to_string(), "upper".to_string()),
            ("id".to_string(), "lower".to_string()),
            ("É".to_string(), "unicode".to_string()),
        ]);
        let bindings = RowBindings::with_params(params).with_variable("I", "loop");
        assert_eq!(
            bindings.substitute("i, I, id, ID, é, É"),
            "loop, loop, lower, lower, unicode, unicode"
        );
    }

    /// A negative field after a `-` never forms `--`, which would turn the
    /// rest of the line into a comment.
    #[test]
    fn a_negative_field_never_forms_a_comment() {
        let mut row = HashMap::new();
        row.insert("delta".into(), Value::Integer(-5));
        let bindings = RowBindings::after_insert("orders", row);
        let result = bindings.substitute("UPDATE t SET n = n -NEW.delta WHERE id = 1");
        assert_eq!(result, "UPDATE t SET n = n -(-5) WHERE id = 1");
    }

    #[test]
    fn substitute_new_fields() {
        let mut row = HashMap::new();
        row.insert("id".into(), Value::String("ord-1".into()));
        row.insert("total".into(), Value::Float(99.99));

        let bindings = RowBindings::after_insert("orders", row);
        let sql = "INSERT INTO audit (id, amount) VALUES (NEW.id, NEW.total)";
        let result = bindings.substitute(sql);

        assert!(result.contains("'ord-1'"), "got: {result}");
        assert!(result.contains("99.99"), "got: {result}");
    }

    #[test]
    fn substitute_tg_op() {
        let bindings = RowBindings::after_insert("orders", HashMap::new());
        let result = bindings.substitute("VALUES (TG_OP, TG_TABLE_NAME)");
        assert!(result.contains("'INSERT'"));
        assert!(result.contains("'orders'"));
    }

    #[test]
    fn substitute_null_value() {
        let mut row = HashMap::new();
        row.insert("x".into(), Value::Null);
        let bindings = RowBindings::after_insert("c", row);
        let result = bindings.substitute("SELECT NEW.x");
        assert!(result.contains("NULL"));
    }

    #[test]
    fn value_sql_literals() {
        assert_eq!(Value::Null.to_sql_literal(), "NULL");
        assert_eq!(Value::Bool(true).to_sql_literal(), "TRUE");
        assert_eq!(Value::Integer(42).to_sql_literal(), "42");
        assert_eq!(Value::String("hello".into()).to_sql_literal(), "'hello'");
        assert_eq!(Value::String("it's".into()).to_sql_literal(), "'it''s'");
    }

    #[test]
    fn substitute_spaced_qualified_field_reference() {
        let mut row = HashMap::new();
        row.insert("id".into(), Value::String("as1".into()));
        let bindings = RowBindings::after_insert("orders", row);
        let result = bindings.substitute("VALUES (NEW . id | | '_log', NEW . id)");
        assert_eq!(result, "VALUES ('as1' | | '_log', 'as1')");
    }
}
