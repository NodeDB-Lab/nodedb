// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL handlers for adding and dropping constraints.
//!
//! The token parsing, catalog get/put, duplicate pre-checks, and
//! `schema_version.bump` run here. The result is the protocol-neutral
//! [`DdlResult`] / [`DdlError`].

use nodedb_types::DatabaseId;

use crate::control::security::catalog::types::StateTransitionDef;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::support::{err, persist_constraints};

/// Handle `ALTER COLLECTION x ADD CONSTRAINT name ON COLUMN col TRANSITIONS (...)`.
pub async fn add_state_constraint(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    let coll_name = parse_ident_token(
        parts
            .get(2)
            .ok_or_else(|| err("42601", "missing collection name"))?,
    )?;

    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &coll_name,
        "change constraints",
    )?;

    let constraint_name = parse_ident_token(
        parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("CONSTRAINT"))
            .and_then(|i| parts.get(i + 1))
            .ok_or_else(|| err("42601", "missing constraint name after CONSTRAINT"))?,
    )?;

    let column_name = parse_ident_token(
        parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("COLUMN"))
            .and_then(|i| parts.get(i + 1))
            .ok_or_else(|| err("42601", "missing column name after ON COLUMN"))?,
    )?;

    let transitions = super::parse::parse_transitions(sql)?;

    let def = StateTransitionDef {
        name: constraint_name.clone(),
        column: column_name,
        transitions,
    };

    let catalog = state.credentials.catalog();

    let mut coll = catalog
        .get_collection(database_id, tenant_id, &coll_name)
        .map_err(|e| DdlError::from_error(&e))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if coll
        .state_constraints
        .iter()
        .any(|c| c.name == constraint_name)
    {
        return Err(err(
            "42710",
            &format!("constraint '{constraint_name}' already exists"),
        ));
    }

    coll.state_constraints.push(def);
    persist_constraints(state, &coll).await?;

    Ok(vec![DdlResult::Status {
        command: "ALTER COLLECTION".to_string(),
        rows_affected: None,
    }])
}

/// Handle `ALTER COLLECTION x ADD TRANSITION CHECK name (predicate)`.
pub async fn add_transition_check(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    let coll_name = parse_ident_token(
        parts
            .get(2)
            .ok_or_else(|| err("42601", "missing collection name"))?,
    )?;

    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &coll_name,
        "change constraints",
    )?;

    let check_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("CHECK"))
        .ok_or_else(|| err("42601", "missing CHECK keyword"))?;
    let check_name = parse_ident_token(
        parts
            .get(check_idx + 1)
            .ok_or_else(|| err("42601", "missing name after TRANSITION CHECK"))?
            .trim_matches('('),
    )?;

    let predicate_expr = super::parse::extract_parenthesized_predicate(sql)?;
    let parsed = super::parse::parse_transition_predicate(&predicate_expr)?;

    let def = crate::control::security::catalog::types::TransitionCheckDef {
        name: check_name.clone(),
        predicate: parsed,
    };

    let catalog = state.credentials.catalog();

    let mut coll = catalog
        .get_collection(database_id, tenant_id, &coll_name)
        .map_err(|e| DdlError::from_error(&e))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if coll.transition_checks.iter().any(|c| c.name == check_name) {
        return Err(err(
            "42710",
            &format!("transition check '{check_name}' already exists"),
        ));
    }

    coll.transition_checks.push(def);
    persist_constraints(state, &coll).await?;

    Ok(vec![DdlResult::Status {
        command: "ALTER COLLECTION".to_string(),
        rows_affected: None,
    }])
}

/// Handle `ALTER COLLECTION x ADD CONSTRAINT name CHECK (expr)`.
pub async fn add_check_constraint(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    let coll_name = parse_ident_token(
        parts
            .get(2)
            .ok_or_else(|| err("42601", "missing collection name"))?,
    )?;

    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &coll_name,
        "change constraints",
    )?;

    let constraint_name = parse_ident_token(
        parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("CONSTRAINT"))
            .and_then(|i| parts.get(i + 1))
            .ok_or_else(|| err("42601", "missing constraint name after CONSTRAINT"))?,
    )?;

    let check_sql = super::parse::extract_check_body(sql, "CHECK")?;

    let has_subquery = {
        let upper = check_sql.to_uppercase();
        upper.contains("SELECT ") || upper.contains("SELECT\n") || upper.contains("SELECT\t")
    };

    if has_subquery {
        super::validate::validate_subquery_pattern(&check_sql)?;
    }

    if !has_subquery {
        let bare = super::validate::strip_new_prefix_for_validation(&check_sql);
        if let Err(e) = nodedb_query::expr_parse::parse_generated_expr(&bare) {
            return Err(err(
                "42601",
                &format!("CHECK expression failed to parse: {e}"),
            ));
        }
    }

    let def = crate::control::security::catalog::types::CheckConstraintDef {
        name: constraint_name.clone(),
        check_sql,
        has_subquery,
    };

    let catalog = state.credentials.catalog();

    let mut coll = catalog
        .get_collection(database_id, tenant_id, &coll_name)
        .map_err(|e| DdlError::from_error(&e))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if coll
        .state_constraints
        .iter()
        .any(|c| c.name == constraint_name)
        || coll
            .transition_checks
            .iter()
            .any(|c| c.name == constraint_name)
        || coll
            .check_constraints
            .iter()
            .any(|c| c.name == constraint_name)
    {
        return Err(err(
            "42710",
            &format!("constraint '{constraint_name}' already exists on {coll_name}"),
        ));
    }

    coll.check_constraints.push(def);
    persist_constraints(state, &coll).await?;

    Ok(vec![DdlResult::Status {
        command: "ALTER COLLECTION".to_string(),
        rows_affected: None,
    }])
}

/// Handle `DROP CONSTRAINT name ON collection`.
pub async fn drop_constraint(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    let constraint_name = parse_ident_token(
        parts
            .get(2)
            .ok_or_else(|| err("42601", "missing constraint name"))?,
    )?;

    let coll_name = parse_ident_token(
        parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("ON"))
            .and_then(|i| parts.get(i + 1))
            .ok_or_else(|| err("42601", "DROP CONSTRAINT requires ON <collection>"))?,
    )?;

    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &coll_name,
        "change constraints",
    )?;

    let catalog = state.credentials.catalog();

    let mut coll = catalog
        .get_collection(database_id, tenant_id, &coll_name)
        .map_err(|e| DdlError::from_error(&e))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    let before_state = coll.state_constraints.len();
    let before_trans_check = coll.transition_checks.len();
    let before_check = coll.check_constraints.len();

    coll.state_constraints.retain(|c| c.name != constraint_name);
    coll.transition_checks.retain(|c| c.name != constraint_name);
    coll.check_constraints.retain(|c| c.name != constraint_name);

    if coll.state_constraints.len() == before_state
        && coll.transition_checks.len() == before_trans_check
        && coll.check_constraints.len() == before_check
    {
        return Err(err(
            "42704",
            &format!("constraint '{constraint_name}' not found on {coll_name}"),
        ));
    }

    persist_constraints(state, &coll).await?;

    Ok(vec![DdlResult::Status {
        command: "DROP CONSTRAINT".to_string(),
        rows_affected: None,
    }])
}
