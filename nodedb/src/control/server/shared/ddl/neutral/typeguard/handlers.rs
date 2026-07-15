// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL handlers for type guard field constraints on
//! schemaless collections.
//!
//! Ported from the pgwire `ddl::typeguard::handlers`. All catalog logic
//! (get/put collection, schemaless check, duplicate pre-checks, `type_guards`
//! mutation, `schema_version.bump()`) is preserved verbatim; only the result
//! construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Syntax:
//! ```sql
//! CREATE TYPEGUARD ON users (
//!     email STRING REQUIRED,
//!     age   INT CHECK (age > 0),
//!     bio   STRING|NULL
//! );
//!
//! CREATE OR REPLACE TYPEGUARD ON users ( ... );
//!
//! ALTER TYPEGUARD ON users ADD score FLOAT REQUIRED CHECK (score >= 0.0);
//! ALTER TYPEGUARD ON users DROP email;
//!
//! DROP TYPEGUARD ON users;
//! DROP TYPEGUARD IF EXISTS ON users;
//!
//! SHOW TYPEGUARD ON users;
//! SHOW TYPEGUARDS;
//! ```

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use nodedb_types::DatabaseId;

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::parse::{
    err, extract_collection_name, extract_outer_parens, parse_field_list, parse_single_field,
};

/// Build a single-tag status result.
fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

// ── CREATE TYPEGUARD ──────────────────────────────────────────────────────────

/// Handle `CREATE [OR REPLACE] TYPEGUARD ON <collection> ( field TYPE [REQUIRED] [CHECK (expr)], ... )`.
pub fn create_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let upper = sql.to_uppercase();
    let or_replace = upper.contains("OR REPLACE");

    let coll_name = extract_collection_name(sql)?;
    let field_list = extract_outer_parens(sql)?;
    let guards = parse_field_list(&field_list)?;

    if guards.is_empty() {
        return Err(err(
            "42601",
            "TYPEGUARD requires at least one field definition",
        ));
    }

    let catalog = state.credentials.catalog();

    let tenant_id = identity.tenant_id.as_u64();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if !coll.collection_type.is_schemaless() {
        return Err(err(
            "0A000",
            &format!(
                "TYPEGUARD is only supported on schemaless collections; '{coll_name}' is not schemaless"
            ),
        ));
    }

    if !or_replace && !coll.type_guards.is_empty() {
        return Err(err(
            "42710",
            &format!(
                "type guards already exist on '{coll_name}'; use CREATE OR REPLACE TYPEGUARD to overwrite"
            ),
        ));
    }

    coll.type_guards = guards;
    catalog
        .put_collection(DatabaseId::DEFAULT, &coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(status("CREATE TYPEGUARD"))
}

// ── ALTER TYPEGUARD ───────────────────────────────────────────────────────────

/// Handle `ALTER TYPEGUARD ON <collection> ADD field TYPE [REQUIRED] [CHECK (expr)]`.
pub fn alter_typeguard_add(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let coll_name = extract_collection_name(sql)?;

    let add_pos = find_ascii_case_insensitive(sql, " ADD ")
        .ok_or_else(|| err("42601", "ALTER TYPEGUARD requires ADD <field> <type>"))?;
    let after_add = sql[add_pos + 5..].trim();

    let guard = parse_single_field(after_add)?;

    let catalog = state.credentials.catalog();

    let tenant_id = identity.tenant_id.as_u64();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if !coll.collection_type.is_schemaless() {
        return Err(err(
            "0A000",
            &format!(
                "TYPEGUARD is only supported on schemaless collections; '{coll_name}' is not schemaless"
            ),
        ));
    }

    if coll.type_guards.iter().any(|g| g.field == guard.field) {
        return Err(err(
            "42710",
            &format!(
                "type guard for field '{}' already exists on '{coll_name}'",
                guard.field
            ),
        ));
    }

    coll.type_guards.push(guard);
    catalog
        .put_collection(DatabaseId::DEFAULT, &coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(status("ALTER TYPEGUARD"))
}

/// Handle `ALTER TYPEGUARD ON <collection> DROP field`.
pub fn alter_typeguard_drop(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let coll_name = extract_collection_name(sql)?;

    let drop_pos = find_ascii_case_insensitive(sql, " DROP ")
        .ok_or_else(|| err("42601", "ALTER TYPEGUARD requires DROP <field>"))?;
    let field_name = sql[drop_pos + 6..].trim().to_lowercase();

    if field_name.is_empty() {
        return Err(err("42601", "ALTER TYPEGUARD DROP requires a field name"));
    }

    let catalog = state.credentials.catalog();

    let tenant_id = identity.tenant_id.as_u64();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    let before_len = coll.type_guards.len();
    coll.type_guards.retain(|g| g.field != field_name);

    if coll.type_guards.len() == before_len {
        return Err(err(
            "42704",
            &format!("type guard for field '{field_name}' not found on '{coll_name}'"),
        ));
    }

    catalog
        .put_collection(DatabaseId::DEFAULT, &coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(status("ALTER TYPEGUARD"))
}

/// Dispatch `ALTER TYPEGUARD ON <collection> ADD|DROP ...`.
pub fn alter_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let upper = sql.to_uppercase();
    if upper.contains(" ADD ") {
        alter_typeguard_add(state, identity, sql)
    } else if upper.contains(" DROP ") {
        alter_typeguard_drop(state, identity, sql)
    } else {
        Err(err(
            "42601",
            "ALTER TYPEGUARD requires ADD <field> <type> or DROP <field>",
        ))
    }
}

// ── DROP TYPEGUARD ────────────────────────────────────────────────────────────

/// Handle `DROP TYPEGUARD [IF EXISTS] ON <collection>`.
pub fn drop_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let upper = sql.to_uppercase();
    let if_exists = upper.contains("IF EXISTS");

    let coll_name = extract_collection_name(sql)?;

    let catalog = state.credentials.catalog();

    let tenant_id = identity.tenant_id.as_u64();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if coll.type_guards.is_empty() {
        if if_exists {
            return Ok(status("DROP TYPEGUARD"));
        }
        return Err(err(
            "42704",
            &format!("no type guards defined on '{coll_name}'"),
        ));
    }

    coll.type_guards.clear();
    catalog
        .put_collection(DatabaseId::DEFAULT, &coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(status("DROP TYPEGUARD"))
}

// ── SHOW TYPEGUARD / SHOW TYPEGUARDS ──────────────────────────────────────────

/// Handle `SHOW TYPEGUARD ON <collection>`.
pub fn show_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let coll_name = extract_collection_name(sql)?;

    let catalog = state.credentials.catalog();

    let tenant_id = identity.tenant_id.as_u64();
    let coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    let columns = vec![
        "field".to_string(),
        "type".to_string(),
        "required".to_string(),
        "check".to_string(),
    ];

    let mut rows = Vec::with_capacity(coll.type_guards.len());
    for guard in &coll.type_guards {
        let check_str = guard.check_expr.clone().unwrap_or_default();
        let mut row = Map::new();
        row.insert("field".to_string(), JsonValue::String(guard.field.clone()));
        row.insert(
            "type".to_string(),
            JsonValue::String(guard.type_expr.clone()),
        );
        row.insert(
            "required".to_string(),
            JsonValue::String(guard.required.to_string()),
        );
        row.insert("check".to_string(), JsonValue::String(check_str));
        rows.push(row);
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}

/// Handle `SHOW TYPEGUARDS` — list all collections with active type guards.
pub fn show_typeguards(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let catalog = state.credentials.catalog();

    let tenant_id = identity.tenant_id.as_u64();
    let collections = catalog
        .load_collections_for_tenant(DatabaseId::DEFAULT, tenant_id)
        .map_err(|e| err("XX000", &e.to_string()))?;

    let columns = vec!["collection".to_string(), "fields".to_string()];

    let mut rows = Vec::new();
    for coll in collections {
        if coll.type_guards.is_empty() {
            continue;
        }
        let field_names: Vec<&str> = coll.type_guards.iter().map(|g| g.field.as_str()).collect();
        let fields_str = field_names.join(", ");
        let mut row = Map::new();
        row.insert(
            "collection".to_string(),
            JsonValue::String(coll.name.clone()),
        );
        row.insert("fields".to_string(), JsonValue::String(fields_str));
        rows.push(row);
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}
