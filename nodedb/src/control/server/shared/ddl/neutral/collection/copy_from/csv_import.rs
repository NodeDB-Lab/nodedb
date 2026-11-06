// SPDX-License-Identifier: BUSL-1.1

//! CSV import for `COPY FROM`.
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

/// CSV-specific parsing options.
#[derive(Clone, Copy, Debug)]
pub(super) struct CsvOptions {
    pub(super) delimiter: char,
    pub(super) has_header: bool,
}

/// Import CSV `bytes`, read from `path` (header row → column names; each
/// subsequent row → INSERT).
pub(super) async fn import_csv(
    ctx: &ImportCtx<'_>,
    collection: &str,
    path: &str,
    bytes: &[u8],
    opts: CsvOptions,
) -> Result<usize, DdlError> {
    let CsvOptions {
        delimiter,
        has_header,
    } = opts;
    // A check pass parses every row before any INSERT, so a malformed row
    // writes nothing. The insert pass parses again, row by row, so the load
    // never holds more than one parsed row.
    let content = std::str::from_utf8(bytes).map_err(|e| {
        ddl_err(
            "22021",
            format!("COPY: file '{path}' is not valid UTF-8: {e}"),
        )
    })?;

    // If HEADER true (default), first non-empty line is the header. The header
    // stays parsed for the whole load, so its reservation does too.
    let (headers, _header_row) = if has_header {
        match content.lines().next() {
            None => return Ok(0),
            Some(h) => {
                let reserved = reserve_row(ctx.memory, path, 1, h, Some(delimiter))?;
                (parse_csv_row(h, delimiter), Some(reserved))
            }
        }
    } else {
        (Vec::new(), None)
    };

    let rows = check_csv(content, path, opts, &headers, ctx.memory)?;

    for (ln, line) in data_lines(content, has_header) {
        let _row = reserve_row(ctx.memory, path, ln, line, Some(delimiter))?;
        let fields = csv_row_fields(line, ln, opts, &headers)?;
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
        .map_err(|e| wrap_row_error(e, ln, "CSV"))?;
    }

    Ok(rows)
}

/// The non-empty data lines with their 1-based line numbers.
fn data_lines(content: &str, has_header: bool) -> impl Iterator<Item = (usize, &str)> {
    content
        .lines()
        .enumerate()
        .skip(usize::from(has_header))
        .map(|(idx, line)| (idx + 1, line.trim_end_matches('\r')))
        .filter(|(_, line)| !line.trim().is_empty())
}

/// Parse every data line, keeping none. Returns the row count.
fn check_csv(
    content: &str,
    path: &str,
    opts: CsvOptions,
    headers: &[String],
    memory: &ScopedMemory,
) -> Result<usize, DdlError> {
    let mut rows = 0;
    for (line_no, line) in data_lines(content, opts.has_header) {
        let _row = reserve_row(memory, path, line_no, line, Some(opts.delimiter))?;
        csv_row_fields(line, line_no, opts, headers)?;
        rows += 1;
    }
    Ok(rows)
}

/// The field map of data line `line_no`. A wrong column count is `22P02`.
fn csv_row_fields(
    line: &str,
    line_no: usize,
    opts: CsvOptions,
    headers: &[String],
) -> Result<std::collections::HashMap<String, nodedb_types::Value>, DdlError> {
    let values = parse_csv_row(line, opts.delimiter);
    let mut fields: std::collections::HashMap<String, nodedb_types::Value> = if opts.has_header {
        if values.len() != headers.len() {
            return Err(ddl_err(
                "22P02",
                format!(
                    "COPY: row {line_no} has {} columns, header has {}",
                    values.len(),
                    headers.len()
                ),
            ));
        }
        headers
            .iter()
            .zip(values.iter())
            .map(|(h, v)| (h.clone(), coerce_csv_value(v)))
            .collect()
    } else {
        values
            .iter()
            .enumerate()
            .map(|(i, v)| (format!("col_{i}"), coerce_csv_value(v)))
            .collect()
    };
    // Inject a unique row number as id if the field map has no id.
    // This prevents duplicate-key errors on schemaless collections
    // where all rows will otherwise receive the same empty-string id.
    if !fields.contains_key("id") {
        fields.insert(
            "id".to_string(),
            nodedb_types::Value::Integer(line_no as i64),
        );
    }
    Ok(fields)
}

/// Parse a single CSV row respecting double-quote quoting and escaped quotes (`""`).
fn parse_csv_row(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next(); // consume second "
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == delimiter {
            fields.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    fields.push(current.trim().to_string());
    fields
}

/// Coerce a CSV string value to a `nodedb_types::Value`.
///
/// Type precedence: integer → float → bool → string. This is the natural
/// order for untyped CSV where numeric values are the most common case.
fn coerce_csv_value(s: &str) -> nodedb_types::Value {
    let trimmed = s.trim();
    if let Ok(i) = trimmed.parse::<i64>() {
        return nodedb_types::Value::Integer(i);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return nodedb_types::Value::Float(f);
    }
    match trimmed.to_lowercase().as_str() {
        "true" => return nodedb_types::Value::Bool(true),
        "false" => return nodedb_types::Value::Bool(false),
        _ => {}
    }
    nodedb_types::Value::String(s.to_string())
}

#[cfg(test)]
mod tests {
    use nodedb_mem::EngineId;

    use super::super::row_memory::test_support::memory_of;
    use super::*;

    const OPTS: CsvOptions = CsvOptions {
        delimiter: ',',
        has_header: true,
    };

    fn headers() -> Vec<String> {
        vec!["id".to_string(), "name".to_string()]
    }

    #[test]
    fn data_lines_keep_file_line_numbers_past_blanks_and_the_header() {
        let lines: Vec<_> = data_lines("id,name\r\n1,a\r\n\r\n2,b\n", true).collect();
        assert_eq!(lines, vec![(2, "1,a"), (4, "2,b")]);
        let no_header: Vec<_> = data_lines("1,a\n2,b\n", false).collect();
        assert_eq!(no_header, vec![(1, "1,a"), (2, "2,b")]);
    }

    #[test]
    fn a_clean_file_counts_its_rows_and_holds_nothing_after() {
        let memory = memory_of(1 << 20);
        let rows = check_csv("id,name\n1,a\n2,b\n", "p", OPTS, &headers(), &memory);
        assert_eq!(rows.expect("valid"), 2);
        assert_eq!(memory.governor().budget(EngineId::Query).allocated(), 0);
    }

    /// The bad row is last, so the check pass reads the good rows first and
    /// still refuses before the insert pass runs.
    #[test]
    fn a_wrong_column_count_fails_the_check_pass() {
        let memory = memory_of(1 << 20);
        let error = check_csv("id,name\n1,a\n2,b\n3\n", "p", OPTS, &headers(), &memory)
            .expect_err("row 4 has one column");
        assert_eq!(error.sqlstate, "22P02");
        assert!(error.message.contains("row 4"), "{}", error.message);
    }

    #[test]
    fn a_row_over_the_budget_is_refused() {
        let memory = memory_of(4096);
        let content = format!("id,name\n1,{}\n", "x".repeat(8192));
        let error = check_csv(&content, "p", OPTS, &headers(), &memory).expect_err("over");
        assert_eq!(error.sqlstate, "53200");
    }

    #[test]
    fn a_file_without_an_id_column_takes_its_line_number_as_id() {
        let opts = CsvOptions {
            delimiter: ';',
            has_header: false,
        };
        let fields = csv_row_fields("a;b", 9, opts, &[]).expect("valid");
        assert_eq!(fields.get("id"), Some(&nodedb_types::Value::Integer(9)));
        assert_eq!(fields.len(), 3);
    }
}
