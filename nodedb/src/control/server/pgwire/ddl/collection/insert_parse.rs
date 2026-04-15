//! Shared parsing and helpers for INSERT/UPSERT dispatch.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::sql_parse::{parse_sql_value, split_values};
use crate::control::server::pgwire::types::sqlstate_error;

/// Parsed INSERT/UPSERT statement fields.
pub(super) struct ParsedInsert {
    pub coll_name: String,
    pub doc_id: String,
    pub fields: std::collections::HashMap<String, nodedb_types::Value>,
    pub has_returning: bool,
    /// Collection type looked up from the catalog. Drives the write plan.
    pub collection_type: Option<nodedb_types::CollectionType>,
}

pub(super) fn extract_vector_fields(
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Vec<(String, Vec<f32>)> {
    fields
        .iter()
        .filter_map(|(field_name, value)| match value {
            nodedb_types::Value::Array(items) => {
                let vector: Vec<f32> = items
                    .iter()
                    .map(|item| match item {
                        nodedb_types::Value::Float(v) => Some(*v as f32),
                        nodedb_types::Value::Integer(v) => Some(*v as f32),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some((field_name.clone(), vector))
            }
            _ => None,
        })
        .collect()
}

/// Parse an INSERT/UPSERT SQL statement into structured fields.
///
/// `keyword` is the SQL prefix to match (e.g., "INSERT INTO " or "UPSERT INTO ").
/// Returns `None` if the collection has a typed schema (let the SQL path handle it).
pub(super) fn parse_write_statement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    keyword: &str,
) -> Option<PgWireResult<ParsedInsert>> {
    let upper = sql.to_uppercase();
    let kw_pos = upper.find(keyword)?;
    let after_into = sql[kw_pos + keyword.len()..].trim_start();
    let coll_name_str = after_into.split_whitespace().next()?;
    let coll_name = coll_name_str.to_lowercase();

    // Check if collection is schemaless. Let the SQL path handle typed INSERT
    // with VALUES syntax, but always handle here for pre-write concerns:
    // - UPSERT (triggers + nodedb-sql handles the routing)
    // - { } object literal syntax (triggers + nodedb-sql handles the routing)
    let tenant_id = identity.tenant_id;
    let is_upsert = keyword.starts_with("UPSERT");
    let after_coll_trimmed = after_into[coll_name_str.len()..].trim_start();
    let is_object_literal =
        after_coll_trimmed.starts_with('{') || after_coll_trimmed.starts_with('[');
    let mut coll_type: Option<nodedb_types::CollectionType> = None;
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), &coll_name)
    {
        // Skip non-schemaless collections for standard VALUES INSERT (let SQL path handle).
        // But always handle here for: UPSERT, { } object literal (any collection type).
        if !is_upsert && !is_object_literal && !coll.collection_type.is_schemaless() {
            return None;
        }
        coll_type = Some(coll.collection_type.clone());
    }

    // Determine which form this statement uses: { } object literal or (cols) VALUES (vals).
    // If { }, rewrite to VALUES SQL via nodedb-sql's preprocess, then parse that.
    let after_coll_name = after_into[coll_name_str.len()..].trim_start();

    if after_coll_name.starts_with('{') || after_coll_name.starts_with('[') {
        if let Some(preprocessed) = nodedb_sql::parser::preprocess::preprocess(sql) {
            let rewritten = preprocessed.sql;
            let rewritten_upper = rewritten.to_uppercase();
            // The preprocessed SQL is always INSERT INTO regardless of original keyword.
            return parse_values_form(
                &rewritten,
                &rewritten_upper,
                "INSERT INTO ",
                &coll_name,
                coll_type,
            );
        }
        return Some(Err(sqlstate_error(
            "42601",
            "failed to parse object literal in INSERT/UPSERT statement",
        )));
    }

    parse_values_form(sql, &upper, keyword, &coll_name, coll_type)
}

/// Parse the `(cols) VALUES (vals)` form.
fn parse_values_form(
    sql: &str,
    upper: &str,
    keyword: &str,
    coll_name: &str,
    coll_type: Option<nodedb_types::CollectionType>,
) -> Option<PgWireResult<ParsedInsert>> {
    let first_open = match sql.find('(') {
        Some(p) => p,
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                &format!("missing column list in {}", keyword.trim()),
            )));
        }
    };
    let values_kw = match upper.find("VALUES") {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing VALUES clause"))),
    };
    let first_close = match sql[first_open..values_kw].rfind(')') {
        Some(p) => first_open + p,
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                "missing closing ) for column list",
            )));
        }
    };
    let cols_str = &sql[first_open + 1..first_close];
    let columns: Vec<&str> = cols_str.split(',').map(|c| c.trim()).collect();

    let after_values = sql[values_kw + 6..].trim_start();
    let vals_open = match after_values.find('(') {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing VALUES (...)"))),
    };
    let vals_close = match after_values.rfind(')') {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing closing ) for VALUES"))),
    };
    let vals_str = &after_values[vals_open + 1..vals_close];
    let values: Vec<&str> = split_values(vals_str);

    if columns.len() != values.len() {
        return Some(Err(sqlstate_error(
            "42601",
            &format!(
                "column count ({}) doesn't match value count ({})",
                columns.len(),
                values.len()
            ),
        )));
    }

    let mut doc_id = String::new();
    let mut fields = std::collections::HashMap::new();

    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if col.eq_ignore_ascii_case("id")
            || col.eq_ignore_ascii_case("document_id")
            || col.eq_ignore_ascii_case("key")
        {
            doc_id = val.trim_matches('\'').to_string();
        }
        fields.insert(col.to_string(), parse_sql_value(val));
    }

    if doc_id.is_empty() {
        doc_id = nodedb_types::id_gen::uuid_v7();
    }

    let has_returning = upper.contains("RETURNING");

    Some(Ok(ParsedInsert {
        coll_name: coll_name.to_string(),
        doc_id,
        fields,
        has_returning,
        collection_type: coll_type,
    }))
}

/// Format a RETURNING response from parsed fields.
pub(super) fn returning_response(
    doc_id: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> PgWireResult<Vec<Response>> {
    use futures::stream;
    use pgwire::api::results::{DataRowEncoder, QueryResponse};

    let mut result_doc = fields.clone();
    result_doc.insert(
        "id".to_string(),
        nodedb_types::Value::String(doc_id.to_string()),
    );
    let json_str =
        sonic_rs::to_string(&nodedb_types::Value::Object(result_doc)).unwrap_or_default();
    let schema = std::sync::Arc::new(vec![crate::control::server::pgwire::types::text_field(
        "result",
    )]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&json_str);
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// Dispatch a plan to WAL + Data Plane, returning an error response on failure.
pub(super) async fn dispatch_plan(
    state: &SharedState,
    tenant_id: nodedb_types::TenantId,
    vshard_id: crate::types::VShardId,
    plan: crate::bridge::envelope::PhysicalPlan,
) -> Option<PgWireResult<Vec<Response>>> {
    if let Err(e) = crate::control::server::dispatch_utils::wal_append_if_write(
        &state.wal, tenant_id, vshard_id, &plan,
    ) {
        return Some(Err(sqlstate_error("XX000", &e.to_string())));
    }
    if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard_id, plan, 0,
    )
    .await
    {
        return Some(Err(sqlstate_error("XX000", &e.to_string())));
    }
    None
}

/// Build a SQL INSERT statement from field map.
///
/// Produces `INSERT INTO coll (col1, col2) VALUES ('val1', 'val2')`.
pub(super) fn fields_to_insert_sql(
    collection: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> String {
    let mut cols = Vec::with_capacity(fields.len());
    let mut vals = Vec::with_capacity(fields.len());

    let mut entries: Vec<_> = fields.iter().collect();
    entries.sort_by_key(|(k, _)| k.as_str());

    for (key, value) in entries {
        cols.push(key.as_str());
        vals.push(value_to_sql_literal(value));
    }

    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        collection,
        cols.join(", "),
        vals.join(", ")
    )
}

/// Build a SQL UPSERT statement from field map.
pub(super) fn fields_to_upsert_sql(
    collection: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> String {
    let mut cols = Vec::with_capacity(fields.len());
    let mut vals = Vec::with_capacity(fields.len());

    let mut entries: Vec<_> = fields.iter().collect();
    entries.sort_by_key(|(k, _)| k.as_str());

    for (key, value) in entries {
        cols.push(key.as_str());
        vals.push(value_to_sql_literal(value));
    }

    format!(
        "UPSERT INTO {} ({}) VALUES ({})",
        collection,
        cols.join(", "),
        vals.join(", ")
    )
}

/// Delegate to the shared implementation in nodedb-sql.
fn value_to_sql_literal(value: &nodedb_types::Value) -> String {
    nodedb_sql::parser::preprocess::value_to_sql_literal(value)
}

/// Plan SQL through nodedb-sql and dispatch the resulting physical plans.
///
/// This is the shared path: SQL → nodedb-sql → EngineRules → SqlPlan → PhysicalPlan → dispatch.
/// Returns `Ok(())` on success, or a pgwire error on failure.
pub(super) async fn plan_and_dispatch(
    state: &SharedState,
    _identity: &crate::control::security::identity::AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    sql: &str,
) -> PgWireResult<()> {
    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
    let tasks = query_ctx
        .plan_sql(sql, tenant_id)
        .await
        .map_err(|e| sqlstate_error_raw("XX000", &e.to_string()))?;
    for task in tasks {
        crate::control::server::dispatch_utils::wal_append_if_write(
            &state.wal,
            tenant_id,
            task.vshard_id,
            &task.plan,
        )
        .map_err(|e| sqlstate_error_raw("XX000", &e.to_string()))?;
        crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            tenant_id,
            task.vshard_id,
            task.plan,
            0,
        )
        .await
        .map_err(|e| sqlstate_error_raw("XX000", &e.to_string()))?;
    }
    Ok(())
}

/// Create a raw PgWireError (not wrapped in Option/Result).
fn sqlstate_error_raw(code: &str, msg: &str) -> pgwire::error::PgWireError {
    pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        msg.to_owned(),
    )))
}

/// Fire SYNC AFTER INSERT triggers, returning an error response on failure.
pub(super) async fn fire_sync_after_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Option<PgWireResult<Vec<Response>>> {
    use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
    if let Err(e) = crate::control::trigger::fire::fire_after_insert(
        state,
        identity,
        tenant_id,
        coll_name,
        fields,
        0,
        Some(TriggerExecutionMode::Sync),
    )
    .await
    {
        return Some(Err(sqlstate_error("XX000", &format!("trigger error: {e}"))));
    }
    None
}

/// Fire INSTEAD OF INSERT triggers, returning the result.
pub(super) async fn fire_instead_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
    tag: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    match crate::control::trigger::fire_instead::fire_instead_of_insert(
        state, identity, tenant_id, coll_name, fields, 0,
    )
    .await
    {
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::Handled) => {
            Some(Ok(vec![Response::Execution(Tag::new(tag))]))
        }
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::NoTrigger) => None,
        Err(e) => Some(Err(sqlstate_error("XX000", &format!("trigger error: {e}")))),
    }
}

/// Fire BEFORE INSERT triggers, returning mutated fields or an error.
pub(super) async fn fire_before_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Result<std::collections::HashMap<String, nodedb_types::Value>, PgWireResult<Vec<Response>>> {
    match crate::control::trigger::fire_before::fire_before_insert(
        state, identity, tenant_id, coll_name, fields, 0,
    )
    .await
    {
        Ok(f) => Ok(f),
        Err(e) => Err(Err(sqlstate_error(
            "XX000",
            &format!("BEFORE trigger error: {e}"),
        ))),
    }
}
