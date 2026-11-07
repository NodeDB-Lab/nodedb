// SPDX-License-Identifier: BUSL-1.1

//! NDJSON and JSON array import for `COPY FROM`.
//!
//! `plan_and_dispatch` returns the protocol-neutral
//! [`DdlError`] directly (it is the neutral collection-DML helper), so this
//! module's own file-read/parse errors are built as `DdlError` at their call
//! sites to keep one error type end to end.

use nodedb_mem::ScopedMemory;

use crate::control::server::shared::ddl::neutral::collection::dml::{
    fields_to_insert_sql, plan_and_dispatch,
};
use crate::control::server::shared::ddl::result::DdlError;

use super::entry::wrap_row_error;
use super::import_ctx::ImportCtx;
use super::row_memory::reserve_row;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// The non-empty lines of an NDJSON file with their 1-based line numbers.
fn ndjson_lines(content: &str) -> impl Iterator<Item = (usize, &str)> {
    content
        .lines()
        .enumerate()
        .map(|(idx, line)| (idx + 1, line.trim()))
        .filter(|(_, line)| !line.is_empty())
}

/// Parse every line of an NDJSON file, keeping none. Returns the row count.
fn check_ndjson(content: &str, path: &str, memory: &ScopedMemory) -> Result<usize, DdlError> {
    let mut rows = 0;
    for (line_no, line) in ndjson_lines(content) {
        let _row = reserve_row(memory, path, line_no, line, None)?;
        json_object_to_fields(line, line_no)?;
        rows += 1;
    }
    Ok(rows)
}

/// Import NDJSON `bytes`, read from `path` (one JSON object per non-empty
/// line).
///
/// A check pass parses every line before any INSERT, so a JSON parse error on
/// any line writes no row. The insert pass parses again, row by row, so the
/// load never holds more than one parsed row.
pub(super) async fn import_ndjson(
    ctx: &ImportCtx<'_>,
    collection: &str,
    path: &str,
    bytes: &[u8],
) -> Result<usize, DdlError> {
    let content = std::str::from_utf8(bytes).map_err(|e| {
        ddl_err(
            "22021",
            format!("COPY: file '{path}' is not valid UTF-8: {e}"),
        )
    })?;

    let rows = check_ndjson(content, path, ctx.memory)?;

    for (line_no, line) in ndjson_lines(content) {
        let _row = reserve_row(ctx.memory, path, line_no, line, None)?;
        let fields = json_object_to_fields(line, line_no)?;
        let sql = fields_to_insert_sql(collection, &fields);
        plan_and_dispatch(
            ctx.state,
            ctx.identity,
            ctx.tenant_id,
            ctx.database_id,
            &sql,
            ctx.txn_ctx,
            // Each row fires its collection's triggers, as an INSERT does.
            true,
        )
        .await
        .map_err(|e| wrap_row_error(e, line_no, "NDJSON"))?;
    }

    Ok(rows)
}

/// The `22P02` refusal of a file that is no JSON array.
fn not_an_array(path: &str, e: impl std::fmt::Display) -> DdlError {
    ddl_err(
        "22P02",
        format!("COPY: file '{path}' is not a valid JSON array: {e}"),
    )
}

/// The field map of array element `line_no`, from its raw text.
fn array_element_fields(
    raw: &str,
    line_no: usize,
    path: &str,
) -> Result<std::collections::HashMap<String, nodedb_types::Value>, DdlError> {
    let elem: serde_json::Value = sonic_rs::from_str(raw).map_err(|e| not_an_array(path, e))?;
    let obj = elem.as_object().ok_or_else(|| {
        ddl_err(
            "22P02",
            format!("COPY: row {line_no} in '{path}' is not a JSON object"),
        )
    })?;
    let mut fields = std::collections::HashMap::new();
    for (key, val) in obj.iter() {
        fields.insert(key.clone(), serde_json_value_to_nodedb(val));
    }
    Ok(fields)
}

/// The raw text of each top-level element of a JSON array. The text must be a
/// syntactically valid array. This borrows from the text and allocates nothing.
struct ArrayElements<'a> {
    content: &'a str,
    /// The byte after the last element read.
    pos: usize,
}

impl<'a> ArrayElements<'a> {
    /// The elements of `content`, which [`check_json_array`] accepted.
    fn new(content: &'a str) -> Self {
        let open = content.find('[').map_or(content.len(), |at| at + 1);
        Self { content, pos: open }
    }
}

impl<'a> Iterator for ArrayElements<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut in_string = false;
        let mut escaped = false;
        let tail = self.content.as_bytes().get(start..)?;
        for (offset, &byte) in tail.iter().enumerate() {
            if in_string {
                match byte {
                    _ if escaped => escaped = false,
                    b'\\' => escaped = true,
                    b'"' => in_string = false,
                    _ => {}
                }
                continue;
            }
            match byte {
                b'"' => in_string = true,
                b'{' | b'[' => depth += 1,
                b',' | b']' | b'}' if depth > 0 => {
                    if byte != b',' {
                        depth -= 1;
                    }
                }
                b',' | b']' => {
                    let end = start + offset;
                    self.pos = if byte == b',' {
                        end + 1
                    } else {
                        self.content.len()
                    };
                    let element = self.content.get(start..end)?.trim();
                    return (!element.is_empty()).then_some(element);
                }
                _ => {}
            }
        }
        None
    }
}

/// Check a JSON array file and parse each element, keeping none. Returns the
/// row count.
///
/// The whole-document pass allocates nothing for the document. It refuses a
/// syntax error anywhere and any text after the closing bracket.
fn check_json_array(content: &str, path: &str, memory: &ScopedMemory) -> Result<usize, DdlError> {
    sonic_rs::from_str::<serde::de::IgnoredAny>(content).map_err(|e| not_an_array(path, e))?;
    if !content.trim_start().starts_with('[') {
        return Err(not_an_array(path, "the document is not an array"));
    }
    let mut rows = 0;
    for (idx, raw) in ArrayElements::new(content).enumerate() {
        let line_no = idx + 1;
        let _row = reserve_row(memory, path, line_no, raw, None)?;
        array_element_fields(raw, line_no, path)?;
        rows += 1;
    }
    Ok(rows)
}

/// Import JSON array `bytes`, read from `path` (`[{...}, {...}, ...]`).
///
/// A check pass reads every element before any INSERT, so a malformed array
/// or a non-object element writes no row. The insert pass parses again,
/// element by element, so the load never holds more than one parsed element.
pub(super) async fn import_json_array(
    ctx: &ImportCtx<'_>,
    collection: &str,
    path: &str,
    bytes: &[u8],
) -> Result<usize, DdlError> {
    let content = std::str::from_utf8(bytes).map_err(|e| not_an_array(path, e))?;
    let rows = check_json_array(content, path, ctx.memory)?;

    for (idx, raw) in ArrayElements::new(content).enumerate() {
        let line_no = idx + 1;
        let _row = reserve_row(ctx.memory, path, line_no, raw, None)?;
        let fields = array_element_fields(raw, line_no, path)?;
        let sql = fields_to_insert_sql(collection, &fields);
        plan_and_dispatch(
            ctx.state,
            ctx.identity,
            ctx.tenant_id,
            ctx.database_id,
            &sql,
            ctx.txn_ctx,
            // Each row fires its collection's triggers, as an INSERT does.
            true,
        )
        .await
        .map_err(|e| wrap_row_error(e, line_no, "JSON array"))?;
    }

    Ok(rows)
}

/// Parse a JSON object string into a field map.
fn json_object_to_fields(
    json: &str,
    line_no: usize,
) -> Result<std::collections::HashMap<String, nodedb_types::Value>, DdlError> {
    let val: serde_json::Value = sonic_rs::from_str(json)
        .map_err(|e| ddl_err("22P02", format!("COPY: line {line_no}: invalid JSON: {e}")))?;

    let obj = val.as_object().ok_or_else(|| {
        ddl_err(
            "22P02",
            format!("COPY: line {line_no}: expected JSON object, got other type"),
        )
    })?;

    let mut fields = std::collections::HashMap::new();
    for (key, v) in obj.iter() {
        fields.insert(key.clone(), serde_json_value_to_nodedb(v));
    }
    Ok(fields)
}

/// Convert a `serde_json::Value` to `nodedb_types::Value`.
pub(super) fn serde_json_value_to_nodedb(val: &serde_json::Value) -> nodedb_types::Value {
    match val {
        serde_json::Value::Null => nodedb_types::Value::Null,
        serde_json::Value::Bool(b) => nodedb_types::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                nodedb_types::Value::Integer(i)
            } else if let Some(u) = n.as_u64() {
                nodedb_types::Value::Integer(u as i64)
            } else if let Some(f) = n.as_f64() {
                nodedb_types::Value::Float(f)
            } else {
                nodedb_types::Value::Null
            }
        }
        serde_json::Value::String(s) => nodedb_types::Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            nodedb_types::Value::Array(arr.iter().map(serde_json_value_to_nodedb).collect())
        }
        serde_json::Value::Object(_) => {
            // Nested object: serialize back to JSON string for storage.
            let s = sonic_rs::to_string(val).unwrap_or_else(|_| "{}".to_string());
            nodedb_types::Value::String(s)
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_mem::EngineId;

    use super::super::row_memory::test_support::memory_of;
    use super::*;

    const BUDGET: usize = 1 << 20;

    fn elements(content: &str) -> Vec<&str> {
        ArrayElements::new(content).collect()
    }

    #[test]
    fn elements_split_at_the_top_level_only() {
        assert_eq!(
            elements(r#" [ {"a": [1, 2], "b": {"c": 3}} , {"d": "x,]}"} ]  "#),
            vec![r#"{"a": [1, 2], "b": {"c": 3}}"#, r#"{"d": "x,]}"}"#]
        );
    }

    #[test]
    fn elements_honour_escaped_quotes() {
        assert_eq!(
            elements(r#"[{"a": "q\"],"}, {"b": "\\"}]"#),
            vec![r#"{"a": "q\"],"}"#, r#"{"b": "\\"}"#]
        );
    }

    #[test]
    fn an_empty_array_has_no_elements() {
        assert!(elements("[]").is_empty());
        assert!(elements("[ \n ]").is_empty());
    }

    #[test]
    fn a_json_array_counts_its_rows_and_holds_nothing_after() {
        let memory = memory_of(BUDGET);
        let content = r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#;
        assert_eq!(check_json_array(content, "p", &memory).expect("valid"), 3);
        assert_eq!(memory.governor().budget(EngineId::Query).allocated(), 0);
    }

    /// Every one of these fails the check pass, so the insert pass never runs
    /// and the load writes no row.
    #[test]
    fn a_bad_document_fails_the_check_pass() {
        let memory = memory_of(BUDGET);
        for content in [
            r#"[{"id": 1}, {"id": 2}"#,
            r#"[{"id": 1}, {not json}]"#,
            r#"[{"id": 1}] trailing"#,
            r#"[{"id": 1}] ]"#,
            r#"{"id": 1}"#,
            "7",
            "",
        ] {
            let error = check_json_array(content, "p", &memory).expect_err(content);
            assert_eq!(error.sqlstate, "22P02", "{content}");
        }
    }

    #[test]
    fn a_non_object_element_names_its_row() {
        let memory = memory_of(BUDGET);
        let error = check_json_array(r#"[{"id": 1}, 2]"#, "p", &memory).expect_err("not an object");
        assert_eq!(error.sqlstate, "22P02");
        assert!(error.message.contains("row 2"), "{}", error.message);
    }

    #[test]
    fn an_element_over_the_budget_is_refused() {
        let memory = memory_of(4096);
        let content = format!(r#"[{{"blob": "{}"}}]"#, "x".repeat(8192));
        let error = check_json_array(&content, "p", &memory).expect_err("over the budget");
        assert_eq!(error.sqlstate, "53200");
    }

    #[test]
    fn a_bad_ndjson_line_fails_the_check_pass() {
        let memory = memory_of(BUDGET);
        let error = check_ndjson("{\"id\": 1}\n{not json\n{\"id\": 3}\n", "p", &memory)
            .expect_err("line 2 is not JSON");
        assert_eq!(error.sqlstate, "22P02");
        assert!(error.message.contains("line 2"), "{}", error.message);
    }

    #[test]
    fn ndjson_skips_empty_lines_and_keeps_their_numbers() {
        let lines: Vec<_> = ndjson_lines("{}\n\n  \n{}\n").collect();
        assert_eq!(lines, vec![(1, "{}"), (4, "{}")]);
        let memory = memory_of(BUDGET);
        assert_eq!(check_ndjson("{}\n\n{}\n", "p", &memory).expect("valid"), 2);
    }

    #[test]
    fn an_ndjson_line_over_the_budget_is_refused() {
        let memory = memory_of(4096);
        let content = format!(r#"{{"blob": "{}"}}"#, "x".repeat(8192));
        let error = check_ndjson(&content, "p", &memory).expect_err("over the budget");
        assert_eq!(error.sqlstate, "53200");
    }
}
