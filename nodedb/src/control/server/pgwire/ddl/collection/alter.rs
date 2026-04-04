//! ALTER TABLE and ALTER COLLECTION enforcement DDL.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;
use super::helpers::parse_origin_column_def;

/// ALTER TABLE <name> ADD [COLUMN] <name> <type> [NOT NULL] [DEFAULT ...]
pub fn alter_table_add_column(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let table_name = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "ALTER TABLE requires a table name"))?
        .to_lowercase();
    let tenant_id = identity.tenant_id;

    // Find column def after ADD [COLUMN].
    let upper = sql.to_uppercase();
    let add_pos = upper
        .find("ADD COLUMN ")
        .map(|p| p + 11)
        .or_else(|| upper.find("ADD ").map(|p| p + 4))
        .ok_or_else(|| sqlstate_error("42601", "expected ADD [COLUMN]"))?;

    let col_def_str = sql[add_pos..].trim();
    let column = parse_origin_column_def(col_def_str).map_err(|e| sqlstate_error("42601", &e))?;
    let column_name = column.name.clone(); // Save before potential move.

    // Validate: new column must be nullable or have a default.
    if !column.nullable && column.default.is_none() {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "ALTER ADD COLUMN '{}': non-nullable column must have a DEFAULT",
                column.name
            ),
        ));
    }

    // Verify collection exists.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &table_name) {
            Ok(Some(coll)) if coll.is_active => {
                // Update the stored schema if it's a strict collection.
                if coll.collection_type.is_strict()
                    && let Some(config_json) = &coll.timeseries_config
                    && let Ok(mut schema) =
                        sonic_rs::from_str::<nodedb_types::columnar::StrictSchema>(config_json)
                {
                    if schema.columns.iter().any(|c| c.name == column.name) {
                        return Err(sqlstate_error(
                            "42P07",
                            &format!("column '{}' already exists", column.name),
                        ));
                    }
                    schema.columns.push(column);
                    schema.version = schema.version.saturating_add(1);

                    let mut updated = coll;
                    updated.timeseries_config = sonic_rs::to_string(&schema).ok();
                    catalog
                        .put_collection(&updated)
                        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                }
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{table_name}' does not exist"),
                ));
            }
        }
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER TABLE '{table_name}' ADD COLUMN '{column_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER TABLE"))])
}

/// Handle ALTER COLLECTION enforcement commands: SET RETENTION, SET/RELEASE LEGAL_HOLD,
/// SET APPEND_ONLY.
pub fn alter_collection_enforcement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    kind: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let upper = sql.to_uppercase();

    let name = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' not found")))?;

    match kind {
        "retention" => {
            // ALTER COLLECTION x SET RETENTION = '7 years'
            let value = extract_set_value(&upper, "RETENTION")
                .ok_or_else(|| sqlstate_error("42601", "SET RETENTION requires = 'duration'"))?;

            // Validate the retention period parses correctly.
            crate::data::executor::enforcement::retention::parse_retention_period(&value)
                .map_err(|e| sqlstate_error("22023", &e))?;

            coll.retention_period = Some(value);
        }
        "legal_hold" => {
            if upper.contains("LEGAL_HOLD = TRUE") || upper.contains("LEGAL_HOLD=TRUE") {
                // ALTER COLLECTION x SET LEGAL_HOLD = TRUE TAG 'case-001'
                let tag = extract_tag_value(&upper).ok_or_else(|| {
                    sqlstate_error("42601", "SET LEGAL_HOLD = TRUE requires TAG 'name'")
                })?;

                // Check for duplicate tag.
                if coll.legal_holds.iter().any(|h| h.tag == tag) {
                    return Err(sqlstate_error(
                        "23505",
                        &format!("legal hold tag '{tag}' already exists on {name}"),
                    ));
                }

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                coll.legal_holds
                    .push(crate::control::security::catalog::LegalHold {
                        tag,
                        created_at: now,
                        created_by: identity.username.clone(),
                    });
            } else if upper.contains("LEGAL_HOLD = FALSE") || upper.contains("LEGAL_HOLD=FALSE") {
                // ALTER COLLECTION x SET LEGAL_HOLD = FALSE TAG 'case-001'
                let tag = extract_tag_value(&upper).ok_or_else(|| {
                    sqlstate_error("42601", "SET LEGAL_HOLD = FALSE requires TAG 'name'")
                })?;

                let before = coll.legal_holds.len();
                coll.legal_holds.retain(|h| h.tag != tag);
                if coll.legal_holds.len() == before {
                    return Err(sqlstate_error(
                        "42704",
                        &format!("legal hold tag '{tag}' not found on {name}"),
                    ));
                }
            } else {
                return Err(sqlstate_error(
                    "42601",
                    "ALTER COLLECTION SET LEGAL_HOLD requires = TRUE TAG 'name' or = FALSE TAG 'name'",
                ));
            }
        }
        "append_only" => {
            // ALTER COLLECTION x SET APPEND_ONLY = TRUE (one-way flag).
            if coll.append_only {
                return Err(sqlstate_error(
                    "42710",
                    &format!("collection '{name}' is already append-only"),
                ));
            }
            coll.append_only = true;
        }
        "last_value_cache" => {
            // ALTER COLLECTION x SET LAST_VALUE_CACHE = TRUE | FALSE
            if !coll.collection_type.is_timeseries() {
                return Err(sqlstate_error(
                    "42809",
                    &format!("'{name}' is not a timeseries collection"),
                ));
            }
            let val = extract_set_value(&upper, "LAST_VALUE_CACHE").ok_or_else(|| {
                sqlstate_error("42601", "SET LAST_VALUE_CACHE requires = TRUE or = FALSE")
            })?;
            coll.lvc_enabled = val.eq_ignore_ascii_case("TRUE");
        }
        _ => {
            return Err(sqlstate_error(
                "42601",
                &format!("unknown ALTER COLLECTION enforcement kind: '{kind}'"),
            ));
        }
    }

    catalog
        .put_collection(&coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}

/// Extract value from `SET KEY = 'value'` pattern.
fn extract_set_value(upper: &str, key: &str) -> Option<String> {
    let pattern = format!("{key} =");
    let pos = upper
        .find(&pattern)
        .or_else(|| upper.find(&format!("{key}=")))?;
    let after = upper[pos..].split('=').nth(1)?.trim();
    let value = after.trim_start_matches('\'').trim_start_matches('"');
    let end = value
        .find('\'')
        .or_else(|| value.find('"'))
        .unwrap_or(value.len());
    Some(value[..end].to_string())
}

/// Extract TAG value from `TAG 'name'` pattern.
fn extract_tag_value(upper: &str) -> Option<String> {
    let pos = upper.find("TAG ")?;
    let after = upper[pos + 4..].trim();
    let value = after.trim_start_matches('\'').trim_start_matches('"');
    let end = value
        .find('\'')
        .or_else(|| value.find('"'))
        .or_else(|| value.find(' '))
        .unwrap_or(value.len());
    if end == 0 {
        return None;
    }
    Some(value[..end].to_string())
}

/// Handle `ALTER COLLECTION accounts ADD COLUMN balance DECIMAL DEFAULT 0
///     AS MATERIALIZED_SUM SOURCE journal_entries ON journal_entries.account_id = accounts.id
///     VALUE journal_entries.signed_amount`.
pub fn add_materialized_sum(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let upper = sql.to_uppercase();

    // Target collection name.
    let target_coll = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    // Target column name: token after ADD COLUMN (or just ADD).
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))
        .or_else(|| parts.iter().position(|p| p.eq_ignore_ascii_case("ADD")))
        .ok_or_else(|| sqlstate_error("42601", "missing ADD COLUMN"))?;
    let target_column = parts
        .get(col_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "missing column name"))?
        .to_lowercase();

    // SOURCE <collection>
    let source_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("SOURCE"))
        .ok_or_else(|| sqlstate_error("42601", "MATERIALIZED_SUM requires SOURCE <collection>"))?;
    let source_coll = parts
        .get(source_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "missing collection after SOURCE"))?
        .to_lowercase();

    // ON <source>.<col> = <target>.<id> — extract the join column from source side.
    let on_idx = upper
        .find(" ON ")
        .ok_or_else(|| sqlstate_error("42601", "MATERIALIZED_SUM requires ON join_condition"))?;
    let after_on = &sql[on_idx + 4..];
    let join_column = parse_join_column(after_on, &source_coll)?;

    // VALUE <expr> — extract the value expression.
    let value_idx = upper
        .find(" VALUE ")
        .ok_or_else(|| sqlstate_error("42601", "MATERIALIZED_SUM requires VALUE expression"))?;
    let value_expr_str = sql[value_idx + 7..].trim();
    let value_expr = parse_value_expression(value_expr_str, &source_coll)?;

    let def = crate::control::security::catalog::types::MaterializedSumDef {
        target_collection: target_coll.clone(),
        target_column: target_column.clone(),
        source_collection: source_coll,
        join_column,
        value_expr,
    };

    // Store the definition on the TARGET collection.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, &target_coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{target_coll}' not found")))?;

    // Check for duplicate column binding.
    if coll
        .materialized_sums
        .iter()
        .any(|m| m.target_column == target_column)
    {
        return Err(sqlstate_error(
            "42710",
            &format!("materialized sum already defined for column '{target_column}'"),
        ));
    }

    coll.materialized_sums.push(def);
    catalog
        .put_collection(&coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.schema_version.bump();

    state.audit_record(
        AuditEvent::ConfigChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ADD MATERIALIZED_SUM {target_column} on {target_coll}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}

/// Parse join column from `source.col = target.id` — returns `col` (the source side).
fn parse_join_column(join_clause: &str, source_coll: &str) -> PgWireResult<String> {
    let eq_parts: Vec<&str> = join_clause.splitn(2, '=').collect();
    if eq_parts.len() != 2 {
        return Err(sqlstate_error("42601", "ON clause requires '=' join"));
    }

    // Find the side that references the source collection.
    let left = eq_parts[0].trim().to_lowercase();
    let right = eq_parts[1].trim().to_lowercase();

    let prefix = format!("{}.", source_coll);
    let col = if left.starts_with(&prefix) {
        left.strip_prefix(&prefix).unwrap_or(&left).to_string()
    } else if right.starts_with(&prefix) {
        right.strip_prefix(&prefix).unwrap_or(&right).to_string()
    } else {
        // No table prefix — assume left is source column.
        left.split('.').next_back().unwrap_or(&left).to_string()
    };

    // Clean up — remove anything after the column name (e.g. trailing keywords).
    let col = col.split_whitespace().next().unwrap_or(&col).to_string();

    Ok(col)
}

/// Parse value expression — simple column reference or qualified `source.column`.
fn parse_value_expression(
    expr_str: &str,
    source_coll: &str,
) -> PgWireResult<crate::bridge::expr_eval::SqlExpr> {
    use crate::bridge::expr_eval::SqlExpr;

    let trimmed = expr_str.trim().trim_end_matches(';');
    let lower = trimmed.to_lowercase();

    // Strip source collection prefix if present: `journal_entries.signed_amount` → `signed_amount`.
    let prefix = format!("{}.", source_coll);
    let col_name = if lower.starts_with(&prefix) {
        lower.strip_prefix(&prefix).unwrap_or(&lower).to_string()
    } else {
        // Could be a bare column name or a CASE expression.
        lower.to_string()
    };

    // For simple column references, return Column(name).
    // For complex expressions (CASE WHEN ...), parse recursively.
    if col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        Ok(SqlExpr::Column(col_name))
    } else {
        // Complex expression — for now, treat the whole thing as a column reference
        // to the first word (simple heuristic). Full CASE parsing would require
        // the DDL constraint parser infrastructure.
        let first_word = col_name
            .split_whitespace()
            .next()
            .unwrap_or(&col_name)
            .to_string();
        Err(sqlstate_error(
            "0A000",
            &format!(
                "complex VALUE expressions not yet supported; use a pre-computed column. Got: '{first_word}...'"
            ),
        ))
    }
}
