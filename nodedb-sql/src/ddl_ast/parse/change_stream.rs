// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER CHANGE STREAM and CREATE/DROP/SHOW CONSUMER GROUP.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::{NodedbStatement, StreamViewStmt};
use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_case_insensitive;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE CHANGE STREAM ") {
            return Ok(Some(parse_create_change_stream(upper, trimmed)));
        }
        if upper.starts_with("DROP CHANGE STREAM ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "STREAM") {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::DropChangeStream { name, if_exists },
            )));
        }
        if upper.starts_with("ALTER CHANGE STREAM ") {
            // ALTER CHANGE STREAM <name> <action>
            let name = parts.get(3).map(|s| s.to_lowercase()).unwrap_or_default();
            let action = parts.get(4).map(|s| s.to_uppercase()).unwrap_or_default();
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::AlterChangeStream { name, action },
            )));
        }
        if upper.starts_with("SHOW CHANGE STREAM") {
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::ShowChangeStreams,
            )));
        }

        // Consumer group commands live in the same parse scope.
        if upper.starts_with("CREATE CONSUMER GROUP ") {
            // CREATE CONSUMER GROUP <name> ON <stream>
            let group_name = parts.get(3).map(|s| s.to_lowercase()).unwrap_or_default();
            let stream_name = parts.get(5).map(|s| s.to_lowercase()).unwrap_or_default();
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::CreateConsumerGroup {
                    group_name,
                    stream_name,
                },
            )));
        }
        if upper.starts_with("DROP CONSUMER GROUP ") {
            // DROP CONSUMER GROUP [IF EXISTS] <name> ON <stream>
            let if_exists = upper.contains("IF EXISTS");
            let name_idx = if if_exists { 5 } else { 3 };
            let name = parts
                .get(name_idx)
                .map(|s| s.to_string())
                .unwrap_or_default();
            let stream = parts
                .iter()
                .position(|p| p.eq_ignore_ascii_case("ON"))
                .and_then(|i| parts.get(i + 1))
                .map(|s| s.to_string())
                .unwrap_or_default();
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::DropConsumerGroup {
                    name,
                    stream,
                    if_exists,
                },
            )));
        }
        if upper.starts_with("SHOW CONSUMER GROUPS") {
            let stream = parts.get(3).map(|s| s.to_string());
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::ShowConsumerGroups { stream },
            )));
        }
        Ok(None)
    })()
    .transpose()
}

/// Structural extraction for `CREATE CHANGE STREAM`.
///
/// Extracts name, collection, and the raw WITH clause inner text.
/// The handler converts the raw WITH pairs to OpFilter, StreamFormat, etc.
fn parse_create_change_stream(_upper: &str, trimmed: &str) -> NodedbStatement {
    let prefix = "CREATE CHANGE STREAM ";
    let rest = &trimmed[prefix.len()..];
    let tokens: Vec<&str> = rest.split_whitespace().collect();

    let name = tokens.first().map(|s| s.to_lowercase()).unwrap_or_default();

    // collection = token after ON
    let collection = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("ON"))
        .and_then(|i| tokens.get(i + 1))
        .map(|s| {
            if *s == "*" {
                "*".to_string()
            } else {
                s.to_lowercase()
            }
        })
        .unwrap_or_default();

    // Extract WITH clause inner text (everything inside outer parens after WITH).
    let with_clause_raw = find_ascii_case_insensitive(trimmed, "WITH")
        .and_then(|pos| {
            let after = &trimmed[pos + 4..].trim_start();
            after
                .strip_prefix('(')
                .and_then(|s| s.split_once(')'))
                .map(|(inner, _)| inner.to_string())
        })
        .unwrap_or_default();

    NodedbStatement::StreamView(StreamViewStmt::CreateChangeStream {
        name,
        collection,
        with_clause_raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> NodedbStatement {
        let upper = sql.to_uppercase();
        parse_create_change_stream(&upper, sql)
    }

    #[test]
    fn basic_stream() {
        if let NodedbStatement::StreamView(StreamViewStmt::CreateChangeStream {
            name,
            collection,
            with_clause_raw,
        }) = create("CREATE CHANGE STREAM orders_stream ON orders")
        {
            assert_eq!(name, "orders_stream");
            assert_eq!(collection, "orders");
            assert!(with_clause_raw.is_empty());
        } else {
            panic!("expected CreateChangeStream");
        }
    }

    #[test]
    fn wildcard_collection() {
        if let NodedbStatement::StreamView(StreamViewStmt::CreateChangeStream {
            collection, ..
        }) = create("CREATE CHANGE STREAM all ON *")
        {
            assert_eq!(collection, "*");
        } else {
            panic!("expected CreateChangeStream");
        }
    }

    #[test]
    fn with_clause() {
        if let NodedbStatement::StreamView(StreamViewStmt::CreateChangeStream {
            with_clause_raw,
            ..
        }) =
            create("CREATE CHANGE STREAM s ON orders WITH (FORMAT = 'msgpack', INCLUDE = 'INSERT')")
        {
            assert!(with_clause_raw.to_uppercase().contains("FORMAT"));
            assert!(with_clause_raw.to_uppercase().contains("INCLUDE"));
        } else {
            panic!("expected CreateChangeStream");
        }
    }
}
