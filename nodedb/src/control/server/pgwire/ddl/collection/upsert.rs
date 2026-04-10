//! UPSERT INTO dispatch for schemaless and KV collections.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::insert_parse::{
    fire_before_triggers, fire_instead_triggers, fire_sync_after_triggers, parse_write_statement,
};

/// UPSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
///
/// Same parsing as INSERT but dispatches the `Upsert` plan variant:
/// if a document with the given ID exists, its fields are merged.
pub async fn upsert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let parsed = match parse_write_statement(state, identity, sql, "UPSERT INTO ")? {
        Ok(p) => p,
        Err(e) => return Some(Err(e)),
    };

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
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll_def)) = catalog.get_collection(tenant_id.as_u32(), &parsed.coll_name)
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
            use crate::control::server::pgwire::types::error_code_to_sqlstate;
            let (severity, code, message) = error_code_to_sqlstate(&violation);
            return Some(Err(pgwire::error::PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(severity.to_owned(), code.to_owned(), message),
            ))));
        }

        // General CHECK constraints (Control Plane enforcement, may have subqueries).
        if !coll_def.check_constraints.is_empty()
            && let Err(e) = super::check_constraint::enforce_check_constraints(
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

    // Build SQL and route through nodedb-sql → EngineRules → sql_plan_convert.
    let upsert_sql = super::insert_parse::fields_to_upsert_sql(&parsed.coll_name, &fields);
    if let Err(e) =
        super::insert_parse::plan_and_dispatch(state, identity, tenant_id, &upsert_sql).await
    {
        return Some(Err(e));
    }

    // Fire SYNC AFTER INSERT triggers.
    if let Some(err) =
        fire_sync_after_triggers(state, identity, tenant_id, &parsed.coll_name, &fields).await
    {
        return Some(err);
    }

    Some(Ok(vec![Response::Execution(Tag::new("UPSERT"))]))
}
