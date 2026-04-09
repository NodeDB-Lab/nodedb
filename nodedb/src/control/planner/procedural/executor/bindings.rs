//! Row variable bindings for trigger execution.
//!
//! Provides NEW/OLD row references and statement-level variables (TG_OP, etc.)
//! that are substituted into SQL text before planning.
//!
//! Uses `nodedb_types::Value` internally instead of `serde_json::Value` for
//! fewer heap allocations. JSON values are converted at the API boundary.

use std::collections::HashMap;

use nodedb_types::Value;

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
        Self {
            new_row: None,
            old_row: None,
            tg_op: String::new(),
            tg_table_name: String::new(),
            tg_when: String::new(),
            variables: params,
        }
    }

    /// Create a copy with an additional variable binding (for loop variables).
    pub fn with_variable(&self, name: &str, value: &str) -> Self {
        let mut copy = self.clone();
        copy.variables.insert(name.to_string(), value.to_string());
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
        let mut result = sql.to_string();

        // Replace NEW.field references.
        if let Some(ref new_row) = self.new_row {
            for (field, value) in new_row {
                let literal = value.to_sql_literal();
                result = replace_qualified_field_reference(&result, "NEW", field, &literal);
            }
        }

        // Replace OLD.field references.
        if let Some(ref old_row) = self.old_row {
            for (field, value) in old_row {
                let literal = value.to_sql_literal();
                result = replace_qualified_field_reference(&result, "OLD", field, &literal);
            }
        }

        // Replace user-defined variables (loop vars, procedure params).
        for (name, value) in &self.variables {
            result = replace_case_insensitive(&result, name, value);
        }

        // Replace statement-level variables.
        result = replace_case_insensitive(&result, "TG_OP", &format!("'{}'", self.tg_op));
        result = replace_case_insensitive(
            &result,
            "TG_TABLE_NAME",
            &format!("'{}'", self.tg_table_name),
        );
        result = replace_case_insensitive(&result, "TG_WHEN", &format!("'{}'", self.tg_when));

        result
    }
}

fn replace_qualified_field_reference(
    input: &str,
    qualifier: &str,
    field: &str,
    replacement: &str,
) -> String {
    let bytes = input.as_bytes();
    let qual_len = qualifier.len();
    let field_len = field.len();
    let mut result = String::with_capacity(input.len());
    let mut i = 0;

    while i < bytes.len() {
        if !matches_identifier_at(input, i, qualifier) {
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }

        let mut cursor = i + qual_len;
        cursor = skip_ascii_whitespace(bytes, cursor);
        if bytes.get(cursor) != Some(&b'.') {
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }

        cursor += 1;
        cursor = skip_ascii_whitespace(bytes, cursor);
        if !matches_identifier_at(input, cursor, field) {
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }

        let field_end = cursor + field_len;
        if is_identifier_char(bytes.get(field_end).copied()) {
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }

        result.push_str(replacement);
        i = field_end;
    }

    result
}

fn matches_identifier_at(input: &str, start: usize, ident: &str) -> bool {
    let bytes = input.as_bytes();
    let ident_len = ident.len();
    let Some(slice) = input.get(start..start + ident_len) else {
        return false;
    };

    if !slice.eq_ignore_ascii_case(ident) {
        return false;
    }

    if start > 0 && is_identifier_char(bytes.get(start - 1).copied()) {
        return false;
    }

    !is_identifier_char(bytes.get(start + ident_len).copied())
}

use super::sql_bytes::{is_identifier_char, skip_ascii_whitespace};

/// Case-insensitive string replacement (simple, not regex).
fn replace_case_insensitive(input: &str, pattern: &str, replacement: &str) -> String {
    if pattern.is_empty() {
        return input.to_string();
    }
    let lower_input = input.to_lowercase();
    let lower_pattern = pattern.to_lowercase();
    let mut result = String::with_capacity(input.len());
    let mut search_from = 0;

    while let Some(pos) = lower_input[search_from..].find(&lower_pattern) {
        let abs_pos = search_from + pos;
        result.push_str(&input[search_from..abs_pos]);
        result.push_str(replacement);
        search_from = abs_pos + pattern.len();
    }
    result.push_str(&input[search_from..]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

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
