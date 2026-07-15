// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW CHANGES FOR <collection>` change-stream query.
//!
//! `SHOW CHANGES FOR <collection> [SINCE <timestamp>] [LIMIT <n>]` reads the
//! change stream for a collection and returns one row per recorded change. This
//! is the *query* surface over recorded changes — distinct from the change-stream
//! *DDL* (`CREATE/ALTER/DROP/SHOW CHANGE STREAM`) served by [`super::change_stream`].
//!
//! The handler builds [`DdlResult`] directly and carries no pgwire types.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use serde_json::{Map, Value as JsonValue};

use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Execute `SHOW CHANGES FOR <collection> [SINCE <timestamp>] [LIMIT <n>]`.
pub fn show_changes(state: &SharedState, sql: &str) -> Result<Vec<DdlResult>, DdlError> {
    if let Some(coll_name) =
        crate::control::server::shared::ddl::sql_parse::extract_collection_after(sql, " FOR ")
    {
        let since_ms: u64 = if let Some(since_pos) = find_ascii_case_insensitive(sql, " SINCE ") {
            let since_str = sql[since_pos + 7..]
                .split_whitespace()
                .next()
                .unwrap_or("0");
            match crate::control::server::shared::ddl::sql_parse::parse_since_timestamp(since_str) {
                Ok(ms) => ms,
                Err(msg) => {
                    return Err(DdlError {
                        sqlstate: "22007".to_string(),
                        message: msg.to_string(),
                    });
                }
            }
        } else {
            // Default: last 24 hours of changes.
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            now_ms.saturating_sub(86_400 * 1000)
        };

        let limit = find_ascii_case_insensitive(sql, " LIMIT ")
            .and_then(|pos| sql[pos + 7..].split_whitespace().next())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1000);

        let changes = state
            .change_stream
            .query_changes(Some(&coll_name), since_ms, limit);

        let columns = vec![
            "collection".to_string(),
            "operation".to_string(),
            "document_id".to_string(),
            "timestamp_ms".to_string(),
            "lsn".to_string(),
        ];
        let column_types = vec![
            DdlColType::Text,
            DdlColType::Text,
            DdlColType::Text,
            DdlColType::Text,
            DdlColType::Text,
        ];

        let mut rows = Vec::with_capacity(changes.len());
        for change in &changes {
            let mut row = Map::new();
            row.insert(
                "collection".to_string(),
                JsonValue::String(change.collection.clone()),
            );
            row.insert(
                "operation".to_string(),
                JsonValue::String(change.operation.as_str().to_string()),
            );
            row.insert(
                "document_id".to_string(),
                JsonValue::String(change.document_id.clone()),
            );
            row.insert(
                "timestamp_ms".to_string(),
                JsonValue::String(change.timestamp_ms.to_string()),
            );
            row.insert(
                "lsn".to_string(),
                JsonValue::String(change.lsn.as_u64().to_string()),
            );
            rows.push(row);
        }

        return Ok(vec![DdlResult::Rows(ShapedRows {
            columns,
            column_types,
            rows,
            notice: None,
        })]);
    }

    Err(DdlError {
        sqlstate: "42601".to_string(),
        message: "syntax: SHOW CHANGES FOR <collection> [SINCE <timestamp>]".to_string(),
    })
}
