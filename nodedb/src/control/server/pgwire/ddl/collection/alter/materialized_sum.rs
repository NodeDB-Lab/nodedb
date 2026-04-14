//! `ALTER COLLECTION accounts ADD COLUMN balance DECIMAL DEFAULT 0 AS MATERIALIZED_SUM ...`
//! — ADD COLUMN variant that binds a computed balance to another collection's
//! per-row contribution. Atomically maintained on INSERT into the source side.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;

pub fn add_materialized_sum(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let upper = sql.to_uppercase();

    let target_coll = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))
        .or_else(|| parts.iter().position(|p| p.eq_ignore_ascii_case("ADD")))
        .ok_or_else(|| sqlstate_error("42601", "missing ADD COLUMN"))?;
    let target_column = parts
        .get(col_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "missing column name"))?
        .to_lowercase();

    let source_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("SOURCE"))
        .ok_or_else(|| sqlstate_error("42601", "MATERIALIZED_SUM requires SOURCE <collection>"))?;
    let source_coll = parts
        .get(source_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "missing collection after SOURCE"))?
        .to_lowercase();

    let on_idx = upper
        .find(" ON ")
        .ok_or_else(|| sqlstate_error("42601", "MATERIALIZED_SUM requires ON join_condition"))?;
    let after_on = &sql[on_idx + 4..];
    let join_column = parse_join_column(after_on, &source_coll)?;

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

    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, &target_coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{target_coll}' not found")))?;

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
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0 {
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

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

    let left = eq_parts[0].trim().to_lowercase();
    let right = eq_parts[1].trim().to_lowercase();

    let prefix = format!("{}.", source_coll);
    let col = if left.starts_with(&prefix) {
        left.strip_prefix(&prefix).unwrap_or(&left).to_string()
    } else if right.starts_with(&prefix) {
        right.strip_prefix(&prefix).unwrap_or(&right).to_string()
    } else {
        left.split('.').next_back().unwrap_or(&left).to_string()
    };

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

    let prefix = format!("{}.", source_coll);
    let col_name = if lower.starts_with(&prefix) {
        lower.strip_prefix(&prefix).unwrap_or(&lower).to_string()
    } else {
        lower.to_string()
    };

    if col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        Ok(SqlExpr::Column(col_name))
    } else {
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
