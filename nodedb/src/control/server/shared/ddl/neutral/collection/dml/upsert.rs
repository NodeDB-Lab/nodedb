// SPDX-License-Identifier: BUSL-1.1

//! UPSERT INTO dispatch for schemaless and KV collections.
//!
//! The result type is [`DdlError`] / [`DdlResult`].

use std::collections::HashMap;

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;

use super::parse::ParsedInsert;
use super::parse::{
    authorize_write_target, fields_to_upsert_sqls, parse_write_statement, plan_and_dispatch,
};
use crate::control::trigger::statement_txn::{fires_joined_body, in_block, with_statement_txn};
use crate::control::trigger::{DmlEvent, TriggerScope};

/// UPSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...), ...
///
/// Same parsing as INSERT but dispatches the `Upsert` plan variant:
/// if a document with the given ID exists, its fields are merged. Every row
/// rides one rebuilt statement, so the rows commit together.
pub async fn upsert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    let parsed = match parse_write_statement(state, identity, database_id, sql, "UPSERT INTO ")? {
        Ok(p) => p,
        Err(e) => return Some(Err(e)),
    };

    if let Err(error) = authorize_write_target(state, identity, database_id, &parsed.coll_name) {
        return Some(Err(error));
    }

    // A write that fires a BEFORE, INSTEAD OF or SYNC AFTER body runs in its
    // statement's transaction together with the bodies. An UPSERT fires the
    // INSERT family before its probe and either family after it.
    let scope = TriggerScope {
        database_id,
        tenant_id: identity.tenant_id,
    };
    let implicit = !in_block(txn_ctx)
        && [DmlEvent::Insert, DmlEvent::Update]
            .into_iter()
            .any(|event| fires_joined_body(state, scope, &parsed.coll_name, event));
    Some(
        with_statement_txn(
            state,
            identity,
            txn_ctx,
            implicit,
            async |ctx: &DmlTxnCtx<'_>| {
                upsert_parsed(state, identity, database_id, &parsed, ctx).await
            },
        )
        .await,
    )
}

/// Upsert every parsed row as one statement on the statement's transaction
/// `txn_ctx`.
///
/// The write takes the shared transaction route with its triggers: the route
/// reads the row the upsert finds, fires the INSERT or the UPDATE family's
/// INSTEAD OF and BEFORE bodies to match, checks the NEW row the BEFORE
/// bodies left against the CHECK constraints, and fires the matching SYNC
/// AFTER bodies once the write staged.
async fn upsert_parsed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parsed: &ParsedInsert,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let scope = TriggerScope {
        database_id,
        tenant_id,
    };
    let fires_row_body = [DmlEvent::Insert, DmlEvent::Update]
        .into_iter()
        .any(|event| fires_joined_body(state, scope, &parsed.coll_name, event));
    let rows: Vec<HashMap<String, nodedb_types::Value>> =
        parsed.rows.iter().map(|row| row.fields.clone()).collect();

    // Enforce CHECK constraints on every row. The route checks a write that
    // fires a row body, after its BEFORE bodies. Typeguards are enforced on
    // the Data Plane, on the body every write form stores.
    let catalog = state.credentials.catalog();
    if let Ok(Some(coll_def)) =
        catalog.get_collection(database_id, tenant_id.as_u64(), &parsed.coll_name)
    {
        // General CHECK constraints (Control Plane enforcement, can have subqueries).
        if !fires_row_body && !coll_def.check_constraints.is_empty() {
            for fields in &rows {
                crate::control::server::shared::check_constraint::enforce_check_constraints(
                    state,
                    identity,
                    database_id,
                    &coll_def.check_constraints,
                    fields,
                )
                .await?;
            }
        }

        // Validate enum-typed columns against the custom type registry.
        for fields in &rows {
            for (field_name, type_name) in &coll_def.fields {
                let Some(nodedb_types::Value::String(label)) = fields.get(field_name.as_str())
                else {
                    continue;
                };
                if let Err(msg) = state.custom_type_registry.validate_enum_label(
                    database_id.as_u64(),
                    tenant_id.as_u64(),
                    type_name,
                    label,
                ) {
                    return Err(ddl_err("22P02", msg));
                }
            }
        }
    }

    // Build SQL and route through nodedb-sql → EngineRules → sql_plan_convert.
    //
    // The statement is REBUILT from `fields`, so the author's `RETURNING` list
    // has to be re-attached here. Otherwise the planner never sees it and the
    // clause is silently dropped, with the caller's own submitted values
    // echoed back in its place.
    let statements = fields_to_upsert_sqls(&parsed.coll_name, &rows);
    if statements.len() > 1 && parsed.returning_clause.is_some() {
        return Err(ddl_err(
            "0A000",
            "UPSERT with RETURNING needs every object literal to name the same keys",
        ));
    }
    let mut returned_rows = Vec::new();
    for mut upsert_sql in statements {
        if let Some(ref columns) = parsed.returning_clause {
            upsert_sql.push_str(" RETURNING ");
            upsert_sql.push_str(columns);
        }
        returned_rows.extend(
            plan_and_dispatch(
                state,
                identity,
                tenant_id,
                database_id,
                &upsert_sql,
                txn_ctx,
                // The route fires the write's row bodies for the family the
                // row it finds makes the upsert.
                true,
            )
            .await?,
        );
    }

    if !returned_rows.is_empty() {
        return Ok(returned_rows);
    }

    // An upsert without RETURNING writes every row it names: carry that
    // count rather than a bare tag.
    Ok(vec![DdlResult::Status {
        command: "UPSERT".to_string(),
        rows_affected: Some(rows.len() as u64),
    }])
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}
