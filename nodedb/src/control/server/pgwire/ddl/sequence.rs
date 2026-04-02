//! DDL handlers for CREATE/DROP/ALTER/SHOW SEQUENCE.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::catalog::sequence_types::StoredSequence;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::text_field;

/// Handle `CREATE SEQUENCE name [options...]`.
pub fn create_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let mut def = parse_create_sequence(sql, tenant_id, &identity.username)?;

    def.created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    def.validate().map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P17".to_owned(),
            e,
        )))
    })?;

    // Check if already exists.
    if state.sequence_registry.exists(tenant_id, &def.name) {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P07".to_owned(),
            format!("sequence \"{}\" already exists", def.name),
        ))));
    }

    // Persist to catalog.
    if let Some(catalog) = state.credentials.catalog() {
        catalog.put_sequence(&def).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("failed to persist sequence: {e}"),
            )))
        })?;
    }

    // Register in memory.
    state.sequence_registry.create(def).map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            e.to_string(),
        )))
    })?;

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("CREATE SEQUENCE"))])
}

/// Handle `DROP SEQUENCE name`.
pub fn drop_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    // DROP SEQUENCE [IF EXISTS] name
    let (name, if_exists) = parse_drop_target(parts, 2);

    if !state.sequence_registry.exists(tenant_id, &name) {
        if if_exists {
            return Ok(vec![Response::Execution(Tag::new("DROP SEQUENCE"))]);
        }
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P01".to_owned(),
            format!("sequence \"{name}\" does not exist"),
        ))));
    }

    // Remove from catalog.
    if let Some(catalog) = state.credentials.catalog() {
        let _ = catalog.delete_sequence(tenant_id, &name);
    }

    // Remove from memory.
    let _ = state.sequence_registry.remove(tenant_id, &name);

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("DROP SEQUENCE"))])
}

/// Handle `ALTER SEQUENCE name ...`.
pub fn alter_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    let upper = sql.to_uppercase();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // ALTER SEQUENCE name RESTART [WITH value]
    let name = parts.get(2).unwrap_or(&"").to_lowercase();

    if !state.sequence_registry.exists(tenant_id, &name) {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P01".to_owned(),
            format!("sequence \"{name}\" does not exist"),
        ))));
    }

    if upper.contains("RESTART") {
        let restart_value = if upper.contains(" WITH ") {
            // ALTER SEQUENCE name RESTART WITH value
            let with_idx = parts
                .iter()
                .position(|p| p.eq_ignore_ascii_case("WITH"))
                .unwrap_or(parts.len());
            parts
                .get(with_idx + 1)
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(1)
        } else {
            // ALTER SEQUENCE name RESTART — restart at start_value
            state
                .sequence_registry
                .get_def(tenant_id, &name)
                .map(|d| d.start_value)
                .unwrap_or(1)
        };

        state
            .sequence_registry
            .restart(tenant_id, &name, restart_value)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22023".to_owned(),
                    e.to_string(),
                )))
            })?;

        // Persist updated state.
        if let Some(catalog) = state.credentials.catalog() {
            state.sequence_registry.persist_all(catalog);
        }

        return Ok(vec![Response::Execution(Tag::new("ALTER SEQUENCE"))]);
    }

    if upper.contains("FORMAT") {
        // ALTER SEQUENCE name FORMAT 'template'
        let format_idx = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("FORMAT"))
            .unwrap_or(parts.len());
        if let Some(raw) = parts.get(format_idx + 1) {
            let raw = raw.trim_matches('\'').trim_matches('"');
            let tokens =
                crate::control::sequence::format::parse_format_template(raw).map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42601".to_owned(),
                        format!("invalid FORMAT: {e}"),
                    )))
                })?;
            // Update the stored definition with new format.
            if let Some(mut def) = state.sequence_registry.get_def(tenant_id, &name) {
                def.format_template = Some(tokens);
                // Re-persist to catalog.
                if let Some(catalog) = state.credentials.catalog() {
                    let _ = catalog.put_sequence(&def);
                }
                // Re-create in registry with updated def.
                let _ = state.sequence_registry.remove(tenant_id, &name);
                let _ = state.sequence_registry.create(def);
            }
            return Ok(vec![Response::Execution(Tag::new("ALTER SEQUENCE"))]);
        }
    }

    Err(PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42601".to_owned(),
        "ALTER SEQUENCE supports: RESTART [WITH value], FORMAT 'template'".to_owned(),
    ))))
}

/// Handle `SHOW SEQUENCES`.
pub fn show_sequences(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let sequences = state.sequence_registry.list(tenant_id);

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("current_value"),
        text_field("called"),
    ]);

    let mut rows = Vec::with_capacity(sequences.len());
    for (name, current_value, is_called) in &sequences {
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder.encode_field(name)?;
        encoder.encode_field(&current_value.to_string())?;
        encoder.encode_field(&is_called.to_string())?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(rows),
    ))])
}

/// Handle `DESCRIBE SEQUENCE name`.
pub fn describe_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let name = name.to_lowercase();

    let def = state
        .sequence_registry
        .get_def(tenant_id, &name)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("sequence \"{name}\" does not exist"),
            )))
        })?;

    let schema = Arc::new(vec![text_field("property"), text_field("value")]);

    let format_str = def
        .format_template
        .as_ref()
        .map(|_| "(defined)")
        .unwrap_or("(none)");

    let reset_str = match def.reset_scope {
        crate::control::sequence::ResetScope::Never => "NEVER",
        crate::control::sequence::ResetScope::Yearly => "YEARLY",
        crate::control::sequence::ResetScope::Monthly => "MONTHLY",
        crate::control::sequence::ResetScope::Quarterly => "QUARTERLY",
        crate::control::sequence::ResetScope::Daily => "DAILY",
    };

    let props = [
        ("name", def.name.as_str()),
        ("start_value", &def.start_value.to_string()),
        ("increment", &def.increment.to_string()),
        ("min_value", &def.min_value.to_string()),
        ("max_value", &def.max_value.to_string()),
        ("cycle", &def.cycle.to_string()),
        ("cache_size", &def.cache_size.to_string()),
        ("format", format_str),
        ("reset_scope", reset_str),
        ("gap_free", &def.gap_free.to_string()),
    ];

    let mut rows = Vec::with_capacity(props.len());
    for (k, v) in &props {
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder.encode_field(&k.to_string())?;
        encoder.encode_field(&v.to_string())?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(rows),
    ))])
}

// ── Parsing helpers ────────────────────────────────────────────────

/// Parse `CREATE SEQUENCE name [START n] [INCREMENT n] [MINVALUE n] [MAXVALUE n]
///   [CYCLE | NO CYCLE] [CACHE n]`.
fn parse_create_sequence(sql: &str, tenant_id: u32, owner: &str) -> PgWireResult<StoredSequence> {
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // CREATE SEQUENCE name ...
    let name = parts
        .get(2)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "CREATE SEQUENCE requires a name".to_owned(),
            )))
        })?
        .to_lowercase();

    let upper: Vec<String> = parts.iter().map(|p| p.to_uppercase()).collect();

    let mut def = StoredSequence::new(tenant_id, name, owner.to_string());

    // Parse options by scanning for keywords.
    let mut i = 3; // skip "CREATE SEQUENCE name"
    while i < parts.len() {
        match upper[i].as_str() {
            "START" => {
                // START [WITH] value
                i += 1;
                if i < parts.len() && upper[i] == "WITH" {
                    i += 1;
                }
                if i < parts.len() {
                    def.start_value = parse_i64(parts[i], "START")?;
                }
            }
            "INCREMENT" => {
                // INCREMENT [BY] value
                i += 1;
                if i < parts.len() && upper[i] == "BY" {
                    i += 1;
                }
                if i < parts.len() {
                    def.increment = parse_i64(parts[i], "INCREMENT")?;
                }
            }
            "MINVALUE" => {
                i += 1;
                if i < parts.len() {
                    def.min_value = parse_i64(parts[i], "MINVALUE")?;
                }
            }
            "MAXVALUE" => {
                i += 1;
                if i < parts.len() {
                    def.max_value = parse_i64(parts[i], "MAXVALUE")?;
                }
            }
            "CYCLE" => {
                def.cycle = true;
            }
            "NO" => {
                i += 1;
                if i < parts.len() && upper[i] == "CYCLE" {
                    def.cycle = false;
                }
            }
            "CACHE" => {
                i += 1;
                if i < parts.len() {
                    def.cache_size = parse_i64(parts[i], "CACHE")?;
                }
            }
            "FORMAT" => {
                // FORMAT 'template-string'
                i += 1;
                if i < parts.len() {
                    let raw = parts[i].trim_matches('\'').trim_matches('"');
                    let tokens = crate::control::sequence::format::parse_format_template(raw)
                        .map_err(|e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "42601".to_owned(),
                                format!("invalid FORMAT: {e}"),
                            )))
                        })?;
                    def.format_template = Some(tokens);
                }
            }
            "RESET" => {
                // RESET MONTHLY | RESET YEARLY | ...
                i += 1;
                if i < parts.len() {
                    def.reset_scope = crate::control::sequence::format::ResetScope::parse(parts[i])
                        .map_err(|e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "42601".to_owned(),
                                e.to_string(),
                            )))
                        })?;
                }
            }
            "GAP_FREE" => {
                def.gap_free = true;
            }
            "SCOPE" => {
                // SCOPE TENANT — informational, affects {TENANT} token resolution.
                i += 1; // skip the "TENANT" token
            }
            _ => {
                // Ignore unknown tokens (e.g., "IF NOT EXISTS" handled elsewhere).
            }
        }
        i += 1;
    }

    // Apply defaults for descending sequences.
    if def.increment < 0 && def.min_value == 1 && def.max_value == i64::MAX {
        def.max_value = -1;
        def.min_value = i64::MIN;
        if def.start_value == 1 {
            def.start_value = -1;
        }
    }

    Ok(def)
}

fn parse_i64(s: &str, ctx: &str) -> PgWireResult<i64> {
    s.parse::<i64>().map_err(|_| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22023".to_owned(),
            format!("invalid value for {ctx}: '{s}'"),
        )))
    })
}

/// Parse DROP target: extract name and IF EXISTS flag.
fn parse_drop_target(parts: &[&str], skip: usize) -> (String, bool) {
    let rest = &parts[skip..];
    if rest.len() >= 3
        && rest[0].eq_ignore_ascii_case("IF")
        && rest[1].eq_ignore_ascii_case("EXISTS")
    {
        (rest[2].to_lowercase(), true)
    } else if let Some(name) = rest.first() {
        (name.to_lowercase(), false)
    } else {
        (String::new(), false)
    }
}
