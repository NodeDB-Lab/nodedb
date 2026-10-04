// SPDX-License-Identifier: BUSL-1.1

//! INSERT INTO dispatch for schemaless, KV, and columnar collections.
//!
//! The result type is [`DdlError`] / [`DdlResult`].

use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::{DmlTxnCtx, PendingFieldInference};
use crate::control::state::SharedState;

use super::indexed_vector_fields::indexed_vector_fields;
use super::parse::ParsedInsert;
use super::parse::{
    authorize_write_target, dispatch_plan, extract_vector_fields, fields_to_insert_sql,
    parse_write_statement, plan_and_dispatch,
};
use super::triggers::{fire_before_triggers, fire_instead_triggers, fire_sync_after_triggers};
use crate::control::trigger::statement_txn::{fires_joined_body, in_block, with_statement_txn};
use crate::control::trigger::{DmlEvent, SyncFire, TriggerScope};

/// INSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
pub async fn insert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    let parsed = match parse_write_statement(state, identity, database_id, sql, "INSERT INTO ")? {
        Ok(p) => p,
        Err(e) => return Some(Err(e)),
    };

    if let Err(error) = authorize_write_target(state, identity, database_id, &parsed.coll_name) {
        return Some(Err(error));
    }

    // A write that fires a BEFORE, INSTEAD OF or SYNC AFTER body runs in its
    // statement's transaction together with the bodies.
    let implicit = !in_block(txn_ctx)
        && fires_joined_body(
            state,
            TriggerScope {
                database_id,
                tenant_id: identity.tenant_id,
            },
            &parsed.coll_name,
            DmlEvent::Insert,
        );
    Some(
        with_statement_txn(
            state,
            identity,
            txn_ctx,
            implicit,
            async |ctx: &DmlTxnCtx<'_>| {
                insert_parsed(state, identity, database_id, &parsed, ctx).await
            },
        )
        .await,
    )
}

/// Insert one parsed document on the statement's transaction `txn_ctx`.
async fn insert_parsed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parsed: &ParsedInsert,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    let fire = SyncFire {
        state,
        identity,
        scope: TriggerScope {
            database_id,
            tenant_id,
        },
        cascade_depth: 0,
        txn: txn_ctx,
    };

    // Fire INSTEAD OF INSERT triggers — if handled, skip normal dispatch.
    if let Some(result) =
        fire_instead_triggers(fire, &parsed.coll_name, &parsed.fields, "INSERT").await
    {
        return result;
    }

    // Fire BEFORE INSERT triggers — can reject via RAISE EXCEPTION, can mutate NEW fields.
    let fields = match fire_before_triggers(fire, &parsed.coll_name, &parsed.fields).await {
        Ok(f) => f,
        Err(e) => return e,
    };

    // Auto-generate sequence values for fields with sequence_name where the
    // INSERT didn't provide an explicit value.
    let mut fields = fields;
    let catalog = state.credentials.catalog();
    if let Ok(Some(coll_def)) =
        catalog.get_collection(database_id, tenant_id.as_u64(), &parsed.coll_name)
    {
        for field_def in &coll_def.field_defs {
            if let Some(ref seq_name) = field_def.sequence_name
                && !fields.contains_key(&field_def.name)
            {
                match state.sequence_registry.nextval_formatted(
                    database_id.as_u64(),
                    tenant_id.as_u64(),
                    seq_name,
                    "",
                    &std::collections::HashMap::new(),
                ) {
                    Ok(val) => {
                        let typed_val = match val {
                            crate::control::sequence::registry::SequenceValue::Int(i) => {
                                nodedb_types::Value::Integer(i)
                            }
                            crate::control::sequence::registry::SequenceValue::Formatted(s) => {
                                nodedb_types::Value::String(s)
                            }
                        };
                        fields.insert(field_def.name.clone(), typed_val);
                    }
                    Err(e) => {
                        return Err(DdlError::from_error(
                            &crate::control::sequence::error_map::sequence_error_to_error(
                                seq_name, e,
                            ),
                        ));
                    }
                }
            }
        }
    }

    // Enforce type guards and CHECK constraints (after BEFORE trigger + sequence injection).
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
            return Err(DdlError::new(code, message));
        }

        // General CHECK constraints (Control Plane enforcement, can have subqueries).
        if !coll_def.check_constraints.is_empty()
            && let Err(e) =
                crate::control::server::shared::check_constraint::enforce_check_constraints(
                    state,
                    identity,
                    database_id,
                    &coll_def.check_constraints,
                    &fields,
                )
                .await
        {
            return Err(e);
        }
    }

    // Validate enum-typed columns against the custom type registry.
    // Collections with user-defined enum types store them physically as TEXT;
    // label validation must happen here in the Control Plane since the Data
    // Plane sees only TEXT.
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

    // Build SQL from fields and route through nodedb-sql → sql_plan_convert.
    // This ensures all engine-type routing goes through the shared EngineRules.
    // The statement is REBUILT from `fields`, so the author's `RETURNING` list
    // has to be re-attached here or the planner will never see it and the
    // clause will be silently dropped.
    let mut insert_sql = fields_to_insert_sql(&parsed.coll_name, &fields);
    if let Some(ref columns) = parsed.returning_clause {
        insert_sql.push_str(" RETURNING ");
        insert_sql.push_str(columns);
    }
    let returned_rows = match plan_and_dispatch(
        state,
        identity,
        tenant_id,
        database_id,
        &insert_sql,
        txn_ctx,
        // The statement fired its INSTEAD OF, BEFORE and SYNC AFTER bodies
        // around this write itself.
        false,
    )
    .await
    {
        Ok(rows) => rows,
        Err(e) => return Err(e),
    };

    // Track field names in catalog for schemaless collections. Learned fields
    // are part of the replicated descriptor, so they go out through the
    // metadata path with a stamped version; a bare `put_collection` will leave
    // the local record byte-different at the same version and wedge the applier.
    if parsed
        .collection_type
        .as_ref()
        .is_none_or(|ct| ct.is_schemaless())
    {
        // Inside a transaction the merge is deferred to COMMIT. Bumping the
        // descriptor now will move the version out from under this
        // transaction's own buffered writes and drain against the lease this
        // very session is holding for them, which can never clear.
        let pending = PendingFieldInference {
            database_id,
            tenant_id: tenant_id.as_u64(),
            collection: parsed.coll_name.clone(),
            fields: inferred_field_types(&fields),
        };
        if let Some(pending) = txn_ctx
            .sessions
            .defer_field_inference(txn_ctx.session_id, pending)
            && let Err(e) = crate::control::catalog_entry::merge_collection_fields_replicated(
                state,
                pending.database_id,
                pending.tenant_id,
                &pending.collection,
                None,
                &pending.fields,
            )
            .await
        {
            return Err(DdlError::from_error_in_context(
                "record inferred schema fields",
                &e,
            ));
        }
    }

    // Fire SYNC AFTER INSERT triggers.
    if let Some(err) = fire_sync_after_triggers(fire, &parsed.coll_name, &fields).await {
        return err;
    }

    // Dispatch VectorInsert for the numeric-array fields no vector index
    // covers. The document write above already indexed the covered ones, and
    // a second insert will append a second HNSW node for the same row.
    let indexed = match indexed_vector_fields(
        state,
        database_id,
        tenant_id.as_u64(),
        &parsed.coll_name,
        parsed.collection_type.as_ref(),
    ) {
        Ok(indexed) => indexed,
        Err(e) => return Err(e),
    };
    let vec_vshard =
        nodedb_types::CollectionKey::from_bare(database_id, &parsed.coll_name).vshard();
    for (field_name, vector) in extract_vector_fields(&fields) {
        if indexed.contains(&field_name) {
            continue;
        }
        let dim = vector.len();

        {
            let catalog = state.credentials.catalog();
            let col = if field_name.is_empty() {
                "embedding"
            } else {
                field_name.as_str()
            };
            if let Ok(Some(entry)) = catalog.get_vector_model(
                database_id.as_u64(),
                tenant_id.as_u64(),
                &parsed.coll_name,
                col,
            ) && entry.metadata.strict_dimensions
                && entry.metadata.dimensions != dim
            {
                return Err(ddl_err(
                    "23514",
                    format!(
                        "strict_dimensions: vector has {} dimensions, model '{}' requires {}",
                        dim, entry.metadata.model, entry.metadata.dimensions
                    ),
                ));
            }
        }
        let surrogate = match crate::control::server::surrogate_exchange::assign_surrogate_routed(
            state,
            nodedb_types::CollectionKey::from_bare(database_id, &parsed.coll_name),
            tenant_id,
            parsed.doc_id.as_bytes(),
            crate::types::TraceId::ZERO,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                return Err(DdlError::from_error_in_context("surrogate assign", &e));
            }
        };
        let vec_plan = crate::bridge::envelope::PhysicalPlan::Vector(VectorOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(database_id, &parsed.coll_name),
            vector,
            dim,
            field_name: field_name.clone(),
            surrogate,
            pk_bytes: Some(parsed.doc_id.as_bytes().to_vec()),
            provenance: None,
        });

        // The vector write joins the statement's transaction with the
        // document write, so both commit or neither does.
        if let Some(err) =
            dispatch_plan(state, identity, database_id, vec_vshard, vec_plan, txn_ctx).await
        {
            return err;
        }
    }

    if !returned_rows.is_empty() {
        return Ok(returned_rows);
    }

    // A single-document `{ ... }` insert without RETURNING always applies
    // exactly one row — the Postgres `INSERT <oid> <rows>` tag needs a real
    // count, not a bare `INSERT` (which real `psql` cannot parse).
    Ok(vec![DdlResult::Status {
        command: "INSERT".to_string(),
        rows_affected: Some(1),
    }])
}

/// The `(field, sql_type)` pairs a schemaless write contributes to the
/// collection's inferred projection. `id` is the document key, never a
/// projected column. An integer infers `BIGINT`: the value is an `i64`, and
/// `INT` declares the 32-bit width that range-checks every later write.
fn inferred_field_types(
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Vec<(String, String)> {
    fields
        .iter()
        .filter(|(name, _)| name.as_str() != "id")
        .map(|(name, value)| {
            let sql_type = match value {
                nodedb_types::Value::Float(_) => "FLOAT",
                nodedb_types::Value::Integer(_) => "BIGINT",
                nodedb_types::Value::Bool(_) => "BOOL",
                _ => "TEXT",
            };
            (name.clone(), sql_type.to_string())
        })
        .collect()
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

#[cfg(test)]
mod tests {
    use super::super::parse::extract_vector_fields;

    #[test]
    fn extract_vector_fields_keeps_named_numeric_arrays() {
        let fields = std::collections::HashMap::from([
            (
                "embedding".to_string(),
                nodedb_types::Value::Array(vec![
                    nodedb_types::Value::Float(1.0),
                    nodedb_types::Value::Integer(2),
                    nodedb_types::Value::Float(3.5),
                ]),
            ),
            (
                "tags".to_string(),
                nodedb_types::Value::Array(vec![nodedb_types::Value::String("rust".into())]),
            ),
        ]);

        let vectors = extract_vector_fields(&fields);

        assert_eq!(
            vectors,
            vec![("embedding".to_string(), vec![1.0, 2.0, 3.5])]
        );
    }
}
