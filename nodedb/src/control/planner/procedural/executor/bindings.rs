//! Row variable bindings for trigger execution.
//!
//! Provides NEW/OLD row references and statement-level variables (TG_OP, etc.)
//! that are substituted into SQL text before planning.

use std::collections::HashMap;

/// Row bindings available during trigger/procedure body execution.
#[derive(Debug, Clone)]
pub struct RowBindings {
    /// NEW row fields (INSERT/UPDATE). None for DELETE.
    new_row: Option<HashMap<String, serde_json::Value>>,
    /// OLD row fields (UPDATE/DELETE). None for INSERT.
    old_row: Option<HashMap<String, serde_json::Value>>,
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

    /// Create bindings for an AFTER INSERT trigger.
    pub fn after_insert(collection: &str, new_row: HashMap<String, serde_json::Value>) -> Self {
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
        old_row: HashMap<String, serde_json::Value>,
        new_row: HashMap<String, serde_json::Value>,
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

    /// Create bindings for an AFTER DELETE trigger.
    pub fn after_delete(collection: &str, old_row: HashMap<String, serde_json::Value>) -> Self {
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
                let pattern_upper = format!("NEW.{}", field.to_uppercase());
                let pattern_lower = format!("NEW.{field}");
                let literal = json_to_sql_literal(value);
                result = replace_case_insensitive(&result, &pattern_upper, &literal);
                result = replace_case_insensitive(&result, &pattern_lower, &literal);
            }
        }

        // Replace OLD.field references.
        if let Some(ref old_row) = self.old_row {
            for (field, value) in old_row {
                let pattern_upper = format!("OLD.{}", field.to_uppercase());
                let pattern_lower = format!("OLD.{field}");
                let literal = json_to_sql_literal(value);
                result = replace_case_insensitive(&result, &pattern_upper, &literal);
                result = replace_case_insensitive(&result, &pattern_lower, &literal);
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

/// Convert a JSON value to a SQL literal string.
fn json_to_sql_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".into(),
        serde_json::Value::Bool(b) => {
            if *b {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(arr) => {
            let elements: Vec<String> = arr.iter().map(json_to_sql_literal).collect();
            format!("ARRAY[{}]", elements.join(", "))
        }
        serde_json::Value::Object(_) => {
            // Serialize objects as JSON string literals.
            format!("'{}'", value.to_string().replace('\'', "''"))
        }
    }
}

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
        row.insert("id".into(), serde_json::json!("ord-1"));
        row.insert("total".into(), serde_json::json!(99.99));

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
        row.insert("x".into(), serde_json::Value::Null);
        let bindings = RowBindings::after_insert("c", row);
        let result = bindings.substitute("SELECT NEW.x");
        assert!(result.contains("NULL"));
    }

    #[test]
    fn json_literals() {
        assert_eq!(json_to_sql_literal(&serde_json::json!(null)), "NULL");
        assert_eq!(json_to_sql_literal(&serde_json::json!(true)), "TRUE");
        assert_eq!(json_to_sql_literal(&serde_json::json!(42)), "42");
        assert_eq!(json_to_sql_literal(&serde_json::json!("hello")), "'hello'");
        assert_eq!(json_to_sql_literal(&serde_json::json!("it's")), "'it''s'");
    }
}
