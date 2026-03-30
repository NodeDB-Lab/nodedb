//! `CREATE CHANGE STREAM` DDL handler.
//!
//! Syntax:
//! ```sql
//! CREATE CHANGE STREAM <name> ON <collection|*>
//!   [WITH (FORMAT = 'json'|'msgpack', INCLUDE = 'INSERT,UPDATE,DELETE')]
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::cdc::stream_def::{
    ChangeStreamDef, CompactionConfig, OpFilter, RetentionConfig, StreamFormat,
};

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `CREATE CHANGE STREAM <name> ON <collection> [WITH (...)]`
pub fn create_change_stream(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create change streams")?;

    let parsed = parse_create_change_stream(sql)?;
    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    // Check for existing stream.
    if let Ok(Some(_)) = catalog.get_change_stream(tenant_id, &parsed.name) {
        return Err(sqlstate_error(
            "42710",
            &format!("change stream '{}' already exists", parsed.name),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock before UNIX epoch"))?
        .as_secs();

    let def = ChangeStreamDef {
        tenant_id,
        name: parsed.name.clone(),
        collection: parsed.collection,
        op_filter: parsed.op_filter,
        format: parsed.format,
        retention: RetentionConfig::default(),
        compaction: parsed.compaction,
        owner: identity.username.clone(),
        created_at: now,
    };

    catalog
        .put_change_stream(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    state.stream_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "CREATE CHANGE STREAM {} ON {}",
            parsed.name, parsed.collection_raw
        ),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE CHANGE STREAM"))])
}

struct ParsedCreateChangeStream {
    name: String,
    collection: String,
    collection_raw: String,
    op_filter: OpFilter,
    format: StreamFormat,
    compaction: CompactionConfig,
}

/// Extract all `KEY = VALUE` pairs from a WITH clause inner string.
/// Returns `(KEY_UPPER, value_string)` tuples. Handles quoted values.
fn extract_key_value_pairs(inner: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
    // Split on commas that are NOT inside quotes.
    let mut pairs = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in inner.chars() {
        if ch == '\'' && !in_quote {
            in_quote = true;
            current.push(ch);
        } else if ch == '\'' && in_quote {
            in_quote = false;
            current.push(ch);
        } else if ch == ',' && !in_quote {
            pairs.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        pairs.push(current);
    }

    for pair in pairs {
        let pair = pair.trim();
        if let Some((key, value)) = pair.split_once('=') {
            let key = key.trim().to_uppercase();
            let value = value
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            result.push((key, value));
        }
    }
    result
}

fn parse_create_change_stream(sql: &str) -> PgWireResult<ParsedCreateChangeStream> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    let prefix = "CREATE CHANGE STREAM ";
    if !upper.starts_with(prefix) {
        return Err(sqlstate_error("42601", "expected CREATE CHANGE STREAM"));
    }
    let rest = &trimmed[prefix.len()..];

    let tokens: Vec<&str> = rest.split_whitespace().collect();
    if tokens.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "expected CREATE CHANGE STREAM <name> ON <collection>",
        ));
    }

    let name = tokens[0].to_lowercase();

    if !tokens[1].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after stream name"));
    }

    let collection_raw = tokens[2].to_string();
    let collection = if collection_raw == "*" {
        "*".to_string()
    } else {
        collection_raw.to_lowercase()
    };

    // Parse optional WITH clause.
    let mut op_filter = OpFilter::all();
    let mut format = StreamFormat::Json;
    let mut compaction = CompactionConfig::default();

    if let Some(with_pos) = upper.find("WITH") {
        let with_section = trimmed[with_pos + 4..].trim();
        let inner = with_section
            .strip_prefix('(')
            .and_then(|s| s.split_once(')'))
            .map(|(i, _)| i)
            .unwrap_or(with_section);

        for (key, val) in extract_key_value_pairs(inner) {
            match key.as_str() {
                "FORMAT" => {
                    if let Some(f) = StreamFormat::from_str_opt(&val) {
                        format = f;
                    }
                }
                "INCLUDE" => {
                    op_filter = OpFilter {
                        insert: false,
                        update: false,
                        delete: false,
                    };
                    for op in val.split(',') {
                        match op.trim().to_uppercase().as_str() {
                            "INSERT" => op_filter.insert = true,
                            "UPDATE" => op_filter.update = true,
                            "DELETE" => op_filter.delete = true,
                            _ => {}
                        }
                    }
                }
                "COMPACTION" if val.eq_ignore_ascii_case("key") => {
                    compaction.enabled = true;
                }
                "KEY" if !val.is_empty() => {
                    compaction.key_field = val;
                    compaction.enabled = true;
                }
                _ => {}
            }
        }
    }

    Ok(ParsedCreateChangeStream {
        name,
        collection,
        collection_raw,
        op_filter,
        format,
        compaction,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let parsed =
            parse_create_change_stream("CREATE CHANGE STREAM orders_stream ON orders").unwrap();
        assert_eq!(parsed.name, "orders_stream");
        assert_eq!(parsed.collection, "orders");
        assert!(parsed.op_filter.insert);
        assert!(parsed.op_filter.update);
        assert!(parsed.op_filter.delete);
    }

    #[test]
    fn parse_wildcard() {
        let parsed = parse_create_change_stream("CREATE CHANGE STREAM all_changes ON *").unwrap();
        assert_eq!(parsed.collection, "*");
    }

    #[test]
    fn parse_with_format() {
        let parsed = parse_create_change_stream(
            "CREATE CHANGE STREAM s ON orders WITH (FORMAT = 'msgpack')",
        )
        .unwrap();
        assert_eq!(parsed.format, StreamFormat::Msgpack);
    }

    #[test]
    fn parse_with_include_filter() {
        let parsed = parse_create_change_stream(
            "CREATE CHANGE STREAM s ON orders WITH (INCLUDE = 'INSERT,DELETE')",
        )
        .unwrap();
        assert!(parsed.op_filter.insert);
        assert!(!parsed.op_filter.update);
        assert!(parsed.op_filter.delete);
    }
}
