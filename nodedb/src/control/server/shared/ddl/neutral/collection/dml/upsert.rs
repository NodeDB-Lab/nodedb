// SPDX-License-Identifier: BUSL-1.1

//! UPSERT INTO dispatch for schemaless and KV collections.
//!
//! Relocated verbatim from the pgwire `ddl::collection::upsert` handler (now
//! deleted) except for the result type, which is [`DdlError`] / [`DdlResult`]
//! instead of pgwire `Response` / `PgWireResult`.

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;

use super::parse::{
    authorize_write_target, fields_to_upsert_sql, parse_write_statement, plan_and_dispatch,
};
use super::triggers::{
    fire_before_triggers, fire_instead_triggers, fire_sync_after_triggers,
    fire_sync_after_update_triggers,
};

/// UPSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
///
/// Same parsing as INSERT but dispatches the `Upsert` plan variant:
/// if a document with the given ID exists, its fields are merged.
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

    let tenant_id = identity.tenant_id;

    // Fire INSTEAD OF INSERT triggers (upsert treated as INSERT for triggers).
    if let Some(result) = fire_instead_triggers(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
        "UPSERT",
    )
    .await
    {
        return Some(result);
    }

    // Fire BEFORE INSERT triggers — may mutate NEW fields.
    let mut fields = match fire_before_triggers(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
    )
    .await
    {
        Ok(f) => f,
        Err(e) => return Some(e),
    };

    // Enforce type guards and CHECK constraints (after BEFORE trigger).
    let catalog = state.credentials.catalog();
    if let Ok(Some(coll_def)) =
        catalog.get_collection(database_id, tenant_id.as_u64(), &parsed.coll_name)
    {
        // Inject DEFAULT/VALUE + validate type guards (combined).
        if !coll_def.type_guards.is_empty()
            && let Err(violation) =
                crate::data::executor::enforcement::typeguard::inject_and_validate(
                    &parsed.coll_name,
                    &coll_def.type_guards,
                    &mut fields,
                )
        {
            let (_severity, code, message) = error_code_to_sqlstate(&violation);
            return Some(Err(DdlError {
                sqlstate: code.to_owned(),
                message,
            }));
        }

        // General CHECK constraints (Control Plane enforcement, may have subqueries).
        if !coll_def.check_constraints.is_empty()
            && let Err(e) =
                crate::control::server::shared::check_constraint::enforce_check_constraints(
                    state,
                    tenant_id,
                    &coll_def.check_constraints,
                    &fields,
                )
                .await
        {
            return Some(Err(e));
        }
    }

    // Validate enum-typed columns against the custom type registry.
    let catalog = state.credentials.catalog();
    if let Ok(Some(coll_def)) =
        catalog.get_collection(database_id, tenant_id.as_u64(), &parsed.coll_name)
    {
        for (field_name, type_name) in &coll_def.fields {
            if let Some(value) = fields.get(field_name.as_str()) {
                let label = match value {
                    nodedb_types::Value::String(s) => s.as_str(),
                    _ => continue,
                };
                if let Err(msg) = state.custom_type_registry.validate_enum_label(
                    tenant_id.as_u64(),
                    type_name,
                    label,
                ) {
                    return Some(Err(ddl_err("22P02", msg)));
                }
            }
        }
    }

    // Probe for an existing row BEFORE dispatch so the correct AFTER
    // trigger class fires: UPSERT onto an existing primary key is an
    // UPDATE from every downstream consumer's perspective (AFTER UPDATE
    // triggers, CDC, materialized views). Probing ahead of dispatch is
    // safe because the document primary key acts as the upsert key and
    // the probe + dispatch + AFTER-fire all run serially on this
    // connection.
    let pk_for_probe = fields
        .get("id")
        .or_else(|| fields.get("document_id"))
        .or_else(|| fields.get("key"))
        .map(|v| match v {
            nodedb_types::Value::String(s) => s.clone(),
            nodedb_types::Value::Integer(i) => i.to_string(),
            other => format!("{other:?}"),
        });
    let old_fields = if let Some(ref pk) = pk_for_probe {
        let row = crate::control::trigger::dml_hook::fetch_old_row(
            state,
            database_id,
            tenant_id,
            &parsed.coll_name,
            pk,
        )
        .await;
        if row.is_empty() { None } else { Some(row) }
    } else {
        None
    };

    // Build SQL and route through nodedb-sql → EngineRules → sql_plan_convert.
    let upsert_sql = fields_to_upsert_sql(&parsed.coll_name, &fields);
    if let Err(e) = plan_and_dispatch(
        state,
        identity,
        tenant_id,
        database_id,
        &upsert_sql,
        txn_ctx,
    )
    .await
    {
        return Some(Err(e));
    }

    // Fire the AFTER trigger family that matches the actual mutation:
    // AFTER UPDATE when a prior row existed, AFTER INSERT otherwise.
    // Firing AFTER INSERT unconditionally would silently skip AFTER
    // UPDATE subscribers on overwrites — the exact bug this routing
    // fixes.
    if let Some(ref old) = old_fields {
        if let Some(err) = fire_sync_after_update_triggers(
            state,
            identity,
            tenant_id,
            &parsed.coll_name,
            old,
            &fields,
        )
        .await
        {
            return Some(err);
        }
    } else if let Some(err) =
        fire_sync_after_triggers(state, identity, tenant_id, &parsed.coll_name, &fields).await
    {
        return Some(err);
    }

    Some(Ok(vec![DdlResult::Status {
        command: "UPSERT".to_string(),
        rows_affected: None,
    }]))
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}
