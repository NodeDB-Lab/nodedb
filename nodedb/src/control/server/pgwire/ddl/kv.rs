//! KV collection DDL: parsing `CREATE COLLECTION ... WITH storage = 'kv'`.
//!
//! Validates PRIMARY KEY requirement, key type restrictions, TTL expressions,
//! and optional capacity hints. Produces `CollectionType::KeyValue(KvConfig)`.

use pgwire::error::PgWireResult;

use super::super::types::sqlstate_error;
use super::collection::parse_typed_schema;

// Re-export for use from DDL dispatch.
pub(super) use nodedb_types::kv_parsing::is_kv_storage_mode;

/// Parse a KV collection DDL statement into a `CollectionType::KeyValue`.
///
/// Validates:
/// - Schema has exactly one PRIMARY KEY column
/// - PRIMARY KEY column type is a valid hash key (TEXT, UUID, INT, BIGINT, BYTES, TIMESTAMP)
/// - Optional TTL expression is well-formed
/// - Optional capacity hint is a valid positive integer
pub(super) fn parse_kv_collection(
    sql: &str,
    upper: &str,
) -> PgWireResult<nodedb_types::CollectionType> {
    let schema = parse_typed_schema(sql).map_err(|e| sqlstate_error("42601", &e))?;

    // Validate: exactly one PRIMARY KEY column required for KV collections.
    let pk_columns: Vec<_> = schema.columns.iter().filter(|c| c.primary_key).collect();
    if pk_columns.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "KV collections require a PRIMARY KEY column (the hash key)",
        ));
    }
    if pk_columns.len() > 1 {
        return Err(sqlstate_error(
            "42601",
            "KV collections support exactly one PRIMARY KEY column",
        ));
    }

    // Validate: PRIMARY KEY column type must be hashable.
    let pk = pk_columns[0];
    if !nodedb_types::is_valid_kv_key_type(&pk.column_type) {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "KV PRIMARY KEY type '{}' is not supported; \
                 use TEXT, UUID, INT, BIGINT, BYTES, or TIMESTAMP",
                pk.column_type
            ),
        ));
    }

    let ttl = parse_kv_ttl(sql, upper, &schema)?;
    let capacity_hint = parse_kv_capacity(upper);

    let config = nodedb_types::KvConfig {
        schema,
        ttl,
        capacity_hint,
        inline_threshold: nodedb_types::KV_DEFAULT_INLINE_THRESHOLD,
    };

    Ok(nodedb_types::CollectionType::KeyValue(config))
}

// ---------------------------------------------------------------------------
// TTL parsing
// ---------------------------------------------------------------------------

/// Parse the TTL expression from a KV DDL WITH clause.
///
/// Supports:
/// - `ttl = INTERVAL '15 minutes'` → `FixedDuration`
/// - `ttl = INTERVAL '1h'` → `FixedDuration` (short form)
/// - `ttl = last_active + INTERVAL '1 hour'` → `FieldBased`
fn parse_kv_ttl(
    sql: &str,
    upper: &str,
    schema: &nodedb_types::StrictSchema,
) -> PgWireResult<Option<nodedb_types::KvTtlPolicy>> {
    let ttl_pos = match nodedb_types::kv_parsing::find_with_option(upper, "TTL") {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let after_ttl = &sql[ttl_pos..];
    let after_eq = after_ttl
        .find('=')
        .map(|p| &after_ttl[p + 1..])
        .unwrap_or(after_ttl)
        .trim();

    let expr_end = nodedb_types::kv_parsing::find_with_option_end(after_eq);
    let expr = after_eq[..expr_end].trim();

    if expr.is_empty() {
        return Err(sqlstate_error("42601", "TTL expression is empty"));
    }

    // Field-based: <field_name> + INTERVAL '...'
    if let Some(plus_pos) = expr.find('+') {
        let field_name = expr[..plus_pos].trim().to_lowercase();
        let interval_part = expr[plus_pos + 1..].trim();

        if !schema.columns.iter().any(|c| c.name == field_name) {
            return Err(sqlstate_error(
                "42601",
                &format!("TTL field '{field_name}' not found in schema"),
            ));
        }

        let offset_ms = nodedb_types::kv_parsing::parse_interval_to_ms(interval_part)
            .map_err(|e| sqlstate_error("42601", &e.to_string()))?;

        return Ok(Some(nodedb_types::KvTtlPolicy::FieldBased {
            field: field_name,
            offset_ms,
        }));
    }

    // Fixed duration: INTERVAL '...'
    if expr.to_uppercase().contains("INTERVAL") {
        let duration_ms = nodedb_types::kv_parsing::parse_interval_to_ms(expr)
            .map_err(|e| sqlstate_error("42601", &e.to_string()))?;
        return Ok(Some(nodedb_types::KvTtlPolicy::FixedDuration {
            duration_ms,
        }));
    }

    Err(sqlstate_error(
        "42601",
        &format!(
            "invalid TTL expression: '{expr}'; \
             expected INTERVAL '...' or <field> + INTERVAL '...'"
        ),
    ))
}

// ---------------------------------------------------------------------------
// WITH clause helpers
// ---------------------------------------------------------------------------

/// Parse optional `capacity = N` from WITH clause.
fn parse_kv_capacity(upper: &str) -> u32 {
    if let Some(pos) = nodedb_types::kv_parsing::find_with_option(upper, "CAPACITY") {
        let after = &upper[pos + 8..]; // "CAPACITY".len()
        let after_eq = after
            .find('=')
            .map(|p| &after[p + 1..])
            .unwrap_or(after)
            .trim();
        let end = after_eq
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after_eq.len());
        after_eq[..end].trim().parse().unwrap_or(0)
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::kv_parsing::{
        find_with_option_end, is_kv_storage_mode, parse_interval_to_ms,
    };

    #[test]
    fn detect_kv_storage_mode() {
        assert!(is_kv_storage_mode("WITH STORAGE = 'KV'"));
        assert!(is_kv_storage_mode("WITH STORAGE KV"));
        assert!(is_kv_storage_mode("WITH STORAGE='KV'"));
        assert!(is_kv_storage_mode(
            "WITH STORAGE = 'KV', TTL = INTERVAL '1H'"
        ));
        assert!(!is_kv_storage_mode("WITH STORAGE = 'STRICT'"));
        assert!(!is_kv_storage_mode("WITH STORAGE = 'COLUMNAR'"));
        assert!(!is_kv_storage_mode("CREATE COLLECTION KV_STUFF"));
    }

    #[test]
    fn interval_parsing_short_form() {
        assert_eq!(parse_interval_to_ms("INTERVAL '15m'").unwrap(), 900_000);
        assert_eq!(parse_interval_to_ms("INTERVAL '1h'").unwrap(), 3_600_000);
        assert_eq!(parse_interval_to_ms("INTERVAL '30s'").unwrap(), 30_000);
        assert_eq!(parse_interval_to_ms("INTERVAL '2d'").unwrap(), 172_800_000);
        assert_eq!(parse_interval_to_ms("'500ms'").unwrap(), 500);
    }

    #[test]
    fn interval_parsing_long_form() {
        assert_eq!(
            parse_interval_to_ms("INTERVAL '15 minutes'").unwrap(),
            900_000
        );
        assert_eq!(
            parse_interval_to_ms("INTERVAL '1 hour'").unwrap(),
            3_600_000
        );
        assert_eq!(
            parse_interval_to_ms("INTERVAL '30 seconds'").unwrap(),
            30_000
        );
        assert_eq!(
            parse_interval_to_ms("INTERVAL '2 days'").unwrap(),
            172_800_000
        );
    }

    #[test]
    fn interval_parsing_bare_number() {
        assert_eq!(parse_interval_to_ms("5000").unwrap(), 5000);
    }

    #[test]
    fn interval_parsing_errors() {
        assert!(parse_interval_to_ms("INTERVAL ''").is_err());
        assert!(parse_interval_to_ms("INTERVAL 'abc'").is_err());
        assert!(parse_interval_to_ms("INTERVAL '15 foobar'").is_err());
    }

    #[test]
    fn with_option_end_respects_quotes() {
        assert_eq!(find_with_option_end("'hello, world', next"), 14);
        assert_eq!(find_with_option_end("simple, next"), 6);
        assert_eq!(find_with_option_end("no_comma"), 8);
    }
}
