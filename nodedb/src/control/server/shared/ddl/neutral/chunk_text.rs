// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `NDB_CHUNK_TEXT(...)` table-valued function.
//!
//! `SELECT * FROM NDB_CHUNK_TEXT(text, chunk_size, overlap, strategy)` splits a
//! text value into chunks via [`nodedb_query::chunk_text`] and returns one row
//! per chunk. The handler builds [`DdlResult`] directly and carries no pgwire
//! types.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use serde_json::{Map, Value as JsonValue};

use crate::control::server::response_shape::types::{DdlColType, ShapedRows};

use super::super::result::{DdlError, DdlResult};

/// Execute `NDB_CHUNK_TEXT(text, chunk_size, overlap, strategy)` and return rows.
///
/// Parses named (`text => '...'`) or positional arguments from the SQL string,
/// delegates to `nodedb_query::chunk_text`, and returns a shaped result set.
pub fn execute_chunk_text(sql: &str) -> Result<Vec<DdlResult>, DdlError> {
    // Extract the parenthesized args from NDB_CHUNK_TEXT(...).
    let fn_pos = find_ascii_case_insensitive(sql, "NDB_CHUNK_TEXT(")
        .ok_or_else(|| err("42601", "expected NDB_CHUNK_TEXT(...)"))?;
    let paren_start = fn_pos
        + sql[fn_pos..]
            .find('(')
            .ok_or_else(|| err("42601", "expected NDB_CHUNK_TEXT(...)"))?;
    let paren_end = sql
        .rfind(')')
        .ok_or_else(|| err("42601", "expected closing ) for NDB_CHUNK_TEXT"))?;

    let inner = &sql[paren_start + 1..paren_end];

    // Parse named or positional arguments.
    let mut text_val = String::new();
    let mut chunk_size = 0usize;
    let mut overlap = 0usize;
    let mut strategy_str = String::from("character");

    // Split on commas, but respect quoted strings.
    let args = split_args_respecting_quotes(inner);

    if args.is_empty() {
        return Err(err(
            "42601",
            "NDB_CHUNK_TEXT requires at least text and chunk_size arguments",
        ));
    }

    // Detect named vs positional by checking if first arg contains `=>`.
    let is_named = args[0].contains("=>");

    if is_named {
        for arg in &args {
            if let Some((key, val)) = arg.split_once("=>") {
                let key = key.trim().to_lowercase();
                let val = val.trim().trim_matches('\'').trim_matches('"');
                match key.as_str() {
                    "text" => text_val = val.to_string(),
                    "chunk_size" => {
                        chunk_size = val
                            .parse()
                            .map_err(|_| err("22023", &format!("invalid chunk_size: {val}")))?;
                    }
                    "overlap" => {
                        overlap = val
                            .parse()
                            .map_err(|_| err("22023", &format!("invalid overlap: {val}")))?;
                    }
                    "strategy" => strategy_str = val.to_string(),
                    other => {
                        return Err(err(
                            "42601",
                            &format!("unknown NDB_CHUNK_TEXT parameter: {other}"),
                        ));
                    }
                }
            }
        }
    } else {
        // Positional: NDB_CHUNK_TEXT('text', chunk_size, overlap, 'strategy')
        if args.len() < 2 {
            return Err(err(
                "42601",
                "NDB_CHUNK_TEXT requires at least (text, chunk_size)",
            ));
        }
        text_val = args[0]
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        chunk_size = args[1]
            .trim()
            .parse()
            .map_err(|_| err("22023", "invalid chunk_size"))?;
        if args.len() > 2 {
            overlap = args[2]
                .trim()
                .parse()
                .map_err(|_| err("22023", "invalid overlap"))?;
        }
        if args.len() > 3 {
            strategy_str = args[3]
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
        }
    }

    if text_val.is_empty() {
        return Err(err("42601", "NDB_CHUNK_TEXT: text argument is required"));
    }
    if chunk_size == 0 {
        return Err(err("42601", "NDB_CHUNK_TEXT: chunk_size must be > 0"));
    }

    let strategy = nodedb_query::ChunkStrategy::parse(&strategy_str).ok_or_else(|| {
        err(
            "22023",
            &format!(
                "unknown strategy '{strategy_str}'; supported: character, sentence, paragraph"
            ),
        )
    })?;

    let chunks = nodedb_query::chunk_text(&text_val, chunk_size, overlap, strategy)
        .map_err(|e| err("22023", &e.to_string()))?;

    let columns = vec![
        "index".to_string(),
        "start".to_string(),
        "end".to_string(),
        "text".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
    ];

    let rows: Vec<Map<String, JsonValue>> = chunks
        .iter()
        .map(|c| {
            let mut row = Map::new();
            row.insert("index".to_string(), JsonValue::String(c.index.to_string()));
            row.insert("start".to_string(), JsonValue::String(c.start.to_string()));
            row.insert("end".to_string(), JsonValue::String(c.end.to_string()));
            row.insert("text".to_string(), JsonValue::String(c.text.clone()));
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}

/// Build a [`DdlError`] from a SQLSTATE + message.
fn err(sqlstate: &str, message: &str) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.to_string(),
    }
}

/// Split a comma-separated argument string, respecting single-quoted strings.
fn split_args_respecting_quotes(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for ch in s.chars() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
                current.push(ch);
            }
            '\'' if in_quote => {
                in_quote = false;
                current.push(ch);
            }
            ',' if !in_quote => {
                args.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        args.push(current);
    }
    args
}
