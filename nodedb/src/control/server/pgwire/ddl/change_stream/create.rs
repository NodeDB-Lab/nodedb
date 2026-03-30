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
use crate::event::cdc::stream_def::{ChangeStreamDef, OpFilter, RetentionConfig, StreamFormat};

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
}

/// Parse `CREATE CHANGE STREAM <name> ON <collection> [WITH (...)]`.
/// Extract a quoted string value or bare word from the start of input.
fn extract_quoted_or_word(s: &str) -> String {
    if s.starts_with('\'') || s.starts_with('"') {
        let quote = s.as_bytes()[0];
        if let Some(end) = s[1..].find(|c: char| c as u8 == quote) {
            return s[1..1 + end].to_string();
        }
    }
    // Bare word: take until whitespace, comma, or closing paren.
    s.split(|c: char| c.is_whitespace() || c == ',' || c == ')')
        .next()
        .unwrap_or("")
        .to_string()
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

    if let Some(with_pos) = upper.find("WITH") {
        let with_section = trimmed[with_pos + 4..].trim();
        let inner = with_section
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or(with_section);
        let inner_upper = inner.to_uppercase();

        // Extract FORMAT value.
        if let Some(fmt_pos) = inner_upper.find("FORMAT") {
            let after = inner[fmt_pos + 6..].trim().trim_start_matches('=').trim();
            let val = extract_quoted_or_word(after);
            if let Some(f) = StreamFormat::from_str_opt(&val) {
                format = f;
            }
        }

        // Extract INCLUDE value (may contain commas inside quotes).
        if let Some(inc_pos) = inner_upper.find("INCLUDE") {
            let after = inner[inc_pos + 7..].trim().trim_start_matches('=').trim();
            let val = extract_quoted_or_word(after);
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
    }

    Ok(ParsedCreateChangeStream {
        name,
        collection,
        collection_raw,
        op_filter,
        format,
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
