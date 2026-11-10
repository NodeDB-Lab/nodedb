// SPDX-License-Identifier: BUSL-1.1

//! INSERT INTO dispatch for schemaless, KV, and columnar collections.
//!
//! The result type is [`DdlError`] / [`DdlResult`].

use std::collections::HashMap;

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::session::{DmlTxnCtx, PendingFieldInference};
use crate::control::state::SharedState;

use super::indexed_vector_fields::indexed_vector_fields;
use super::insert_row::prepare_insert_row;
use super::insert_vectors::{RowVectorCtx, insert_row_vectors};
use super::parse::ParsedInsert;
use super::parse::{
    authorize_write_target, parse_write_statement, plan_and_dispatch, rows_to_insert_sql,
};
use super::triggers::{fire_before_triggers, fire_instead_triggers, fire_sync_after_triggers};
use crate::control::trigger::statement_txn::{fires_joined_body, in_block, with_statement_txn};
use crate::control::trigger::{DmlEvent, SyncFire, TriggerScope};

/// INSERT INTO <collection> { ... } or [{ ... }, { ... }]
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

/// One row the statement writes, after its pre-write work.
struct PreparedRow<'a> {
    doc_id: &'a str,
    fields: HashMap<String, nodedb_types::Value>,
}

/// Insert every parsed row on the statement's transaction `txn_ctx`.
///
/// Each row runs its INSTEAD OF and BEFORE bodies, sequence defaults, CHECK
/// constraints and enum labels in row order. The rows a body did not replace
/// then go out as one multi-row INSERT, so they commit together. Each written
/// row then fires its SYNC AFTER bodies and writes its uncovered vectors.
async fn insert_parsed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parsed: &ParsedInsert,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let coll_name = parsed.coll_name.as_str();
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

    // An INSTEAD OF body stands in for its row, which counts as processed.
    let mut replaced = 0u64;
    let mut prepared: Vec<PreparedRow<'_>> = Vec::with_capacity(parsed.rows.len());
    for row in &parsed.rows {
        if fire_instead_triggers(fire, coll_name, &row.fields).await? {
            replaced += 1;
            continue;
        }
        let fields = fire_before_triggers(fire, coll_name, &row.fields).await?;
        let fields = prepare_insert_row(state, identity, database_id, coll_name, fields).await?;
        prepared.push(PreparedRow {
            doc_id: &row.doc_id,
            fields,
        });
    }
    if prepared.is_empty() {
        return Ok(vec![insert_status(replaced)]);
    }

    // Build SQL from the rows and route through nodedb-sql → sql_plan_convert,
    // so every engine-type routing decision stays in the shared EngineRules.
    // The statement is REBUILT from the rows, so the author's `RETURNING`
    // list has to be re-attached here or the planner never sees it.
    let rows: Vec<HashMap<String, nodedb_types::Value>> =
        prepared.iter().map(|row| row.fields.clone()).collect();
    let mut insert_sql = rows_to_insert_sql(coll_name, &rows);
    if let Some(ref columns) = parsed.returning_clause {
        insert_sql.push_str(" RETURNING ");
        insert_sql.push_str(columns);
    }
    let returned_rows = plan_and_dispatch(
        state,
        identity,
        tenant_id,
        database_id,
        &insert_sql,
        txn_ctx,
        // The statement fires its INSTEAD OF, BEFORE and SYNC AFTER bodies
        // around this write itself.
        false,
    )
    .await?;

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
            fields: inferred_field_types(&rows),
        };
        if let Some(pending) = txn_ctx
            .sessions
            .defer_field_inference(txn_ctx.session_id, pending)
        {
            crate::control::catalog_entry::merge_collection_fields_replicated(
                state,
                pending.database_id,
                pending.tenant_id,
                &pending.collection,
                None,
                &pending.fields,
            )
            .await
            .map_err(|e| DdlError::from_error_in_context("record inferred schema fields", &e))?;
        }
    }

    let indexed = indexed_vector_fields(
        state,
        database_id,
        tenant_id.as_u64(),
        coll_name,
        parsed.collection_type.as_ref(),
    )?;
    let vectors = RowVectorCtx {
        state,
        identity,
        database_id,
        coll_name,
        indexed: &indexed,
        txn_ctx,
    };
    for row in &prepared {
        fire_sync_after_triggers(fire, coll_name, &row.fields).await?;
        insert_row_vectors(&vectors, row.doc_id, &row.fields).await?;
    }

    if !returned_rows.is_empty() {
        return Ok(returned_rows);
    }
    // A plain INSERT writes every row it names or fails the statement. The
    // Postgres `INSERT <oid> <rows>` tag needs a real count, not a bare
    // `INSERT` (which real `psql` cannot parse).
    Ok(vec![insert_status(replaced + prepared.len() as u64)])
}

fn insert_status(rows_affected: u64) -> DdlResult {
    DdlResult::Status {
        command: "INSERT".to_string(),
        rows_affected: Some(rows_affected),
    }
}

/// The `(field, sql_type)` pairs the written rows contribute to the
/// collection's inferred projection, one pair per field, from the first row
/// that names it. `id` is the document key, never a projected column. An
/// integer infers `BIGINT`: the value is an `i64`, and `INT` declares the
/// 32-bit width that range-checks every later write. A fractional literal is
/// an exact decimal and infers a plain `DECIMAL`, which keeps every digit.
fn inferred_field_types(rows: &[HashMap<String, nodedb_types::Value>]) -> Vec<(String, String)> {
    let mut seen = std::collections::HashSet::new();
    let mut inferred = Vec::new();
    for fields in rows {
        for (name, value) in fields {
            if name.as_str() == "id" || !seen.insert(name.clone()) {
                continue;
            }
            let sql_type = match value {
                nodedb_types::Value::Float(_) => "FLOAT",
                nodedb_types::Value::Decimal(_) => "DECIMAL",
                nodedb_types::Value::Integer(_) => "BIGINT",
                nodedb_types::Value::Bool(_) => "BOOL",
                _ => "TEXT",
            };
            inferred.push((name.clone(), sql_type.to_string()));
        }
    }
    inferred
}

#[cfg(test)]
mod tests {
    use super::super::parse::extract_vector_fields;
    use super::inferred_field_types;

    #[test]
    fn extract_vector_fields_keeps_named_numeric_arrays() {
        let fields = std::collections::HashMap::from([
            (
                "embedding".to_string(),
                nodedb_types::Value::Array(vec![
                    nodedb_types::Value::Float(1.0),
                    nodedb_types::Value::Integer(2),
                    nodedb_types::Value::Decimal("3.5".parse().expect("decimal")),
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

    #[test]
    fn inferred_fields_take_each_name_once_across_rows() {
        let first = std::collections::HashMap::from([
            ("id".to_string(), nodedb_types::Value::String("a".into())),
            ("n".to_string(), nodedb_types::Value::Integer(1)),
        ]);
        let second = std::collections::HashMap::from([
            ("n".to_string(), nodedb_types::Value::Integer(2)),
            ("tag".to_string(), nodedb_types::Value::String("x".into())),
        ]);
        let mut inferred = inferred_field_types(&[first, second]);
        inferred.sort();
        assert_eq!(
            inferred,
            vec![
                ("n".to_string(), "BIGINT".to_string()),
                ("tag".to_string(), "TEXT".to_string()),
            ]
        );
    }
}
