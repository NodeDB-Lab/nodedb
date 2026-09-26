// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `REFRESH MATERIALIZED VIEW` — re-materialize the view target
//! by executing the view's stored `SELECT` and writing each computed row to the
//! target collection.
//!
//! The refresh runs entirely in the Control Plane:
//!
//!   1. Plan the stored `SELECT` through `nodedb-sql`.
//!   2. Dispatch each produced `PhysicalTask` to the Data Plane and collect rows.
//!   3. Clear the target (`TRUNCATE <view>`).
//!   4. Write each collected row back with `INSERT INTO <view> (cols)
//!      VALUES (...)` through the same SQL pipeline.
//!
//! Decoupling the scan from the insert is what makes projection, `WHERE`,
//! `GROUP BY`/aggregates, and JOIN work uniformly — the Data Plane never needs a
//! specialised refresh opcode; every engine feature reachable by a normal
//! `SELECT` is reachable by refresh.
//!
//! Ported from the pgwire `ddl::materialized_view::refresh` handler. The plan /
//! Data-Plane dispatch path (`plan_sql`, `wal_append_if_write`,
//! `dispatch_to_data_plane`), the scan-row normalisation, and the INSERT
//! synthesis are preserved verbatim; only the result construction changed from
//! pgwire `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] /
//! [`DdlError`].

use nodedb_types::DatabaseId;
use nodedb_types::error::sqlstate;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::decode_payload_to_json;
use crate::types::TraceId;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

pub async fn refresh_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let name = parse_refresh_target(sql)?;
    let tenant_id = identity.tenant_id;

    let view = {
        let catalog = state.credentials.catalog();
        match catalog.get_materialized_view(database_id.as_u64(), tenant_id.as_u64(), &name) {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Err(err(
                    sqlstate::UNDEFINED_TABLE,
                    format!("materialized view '{name}' does not exist"),
                ));
            }
            Err(e) => {
                let (_, code, message) = error_to_sqlstate(&e);
                return Err(err(code, message));
            }
        }
    };

    // 1) Run the stored SELECT and collect every row.
    let rows = execute_select(state, identity, database_id, &view.query_sql).await?;

    // 2) Clear the target so rows no longer selected by the SELECT
    //    (narrowed WHERE, dropped JOIN match, deleted source row,
    //    regrouped aggregate) disappear from the view.
    //
    //    `TRUNCATE`, not `DELETE FROM`: the target's post-image is decided at
    //    plan time (empty), so the write carries no predicate whose matching
    //    set has to be resolved against stored state first.
    dispatch_sql(
        state,
        identity,
        database_id,
        &format!("TRUNCATE {}", ::nodedb_types::quote_ident(&view.name)),
    )
    .await?;

    // 3) Re-insert every row produced by the SELECT.
    //
    //    Rows produced by aggregate/join paths do not carry a document
    //    `id`; the schemaless target collection needs a unique primary
    //    key per row, otherwise successive rows overwrite each other
    //    under the same default id. Synthesize a deterministic id from
    //    the row index when the SELECT output has no `id` column.
    for (idx, row) in rows.iter().enumerate() {
        let mut row = row.clone();
        if !row.contains_key("id") {
            row.insert(
                "id".to_string(),
                serde_json::Value::String(format!("mv_{idx}")),
            );
        }
        let insert_sql = build_insert_sql(&view.name, &row)?;
        dispatch_sql(state, identity, database_id, &insert_sql).await?;
    }

    tracing::info!(
        view = view.name,
        rows = rows.len(),
        "materialized view refreshed"
    );

    Ok(vec![DdlResult::Status {
        command: "REFRESH MATERIALIZED VIEW".to_string(),
        rows_affected: None,
    }])
}

/// Plan and execute a `SELECT` via the standard SQL pipeline, collect
/// the result rows as `serde_json::Map` objects. Response payloads may
/// come back as wrapped scan rows (`{id, data: {...}}`) or as flat
/// aggregate/join rows — both are normalised to the logical row map.
async fn execute_select(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, DdlError> {
    let (tasks, _output_schema, _lease_scope) =
        crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql(
            state,
            identity,
            sql,
            database_id,
        )
        .await
        .map_err(|error| {
            DdlError::new(error.sqlstate, format!("plan '{sql}': {}", error.message))
        })?;

    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    for task in tasks {
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
        let response =
            crate::control::server::shared::clone_write::intercept_authorize_and_dispatch(
                crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                    state,
                    task,
                    identity,
                    tenant_id: identity.tenant_id,
                    permissions: &state.permissions,
                    roles: &state.roles,
                    emitter: &emitter,
                },
                TraceId::ZERO,
            )
            .await
            .map_err(|e| {
                let (_, code, message) = error_to_sqlstate(&e);
                err(code, format!("dispatch: {message}"))
            })?;
        require_ok_response(&response)?;

        let payload = response.payload.as_ref();
        if payload.is_empty() {
            continue;
        }
        let json = decode_payload_to_json(payload);
        if json.is_empty() {
            continue;
        }
        let parsed: serde_json::Value = sonic_rs::from_str(&json).map_err(|e| {
            err(
                sqlstate::INTERNAL_ERROR,
                format!("decode scan payload: {e}"),
            )
        })?;

        collect_rows(parsed, &mut rows);
    }
    Ok(rows)
}

/// Normalise a decoded response into row maps. Handles arrays of rows,
/// scan wrappers, and single-object responses uniformly.
fn collect_rows(
    value: serde_json::Value,
    out: &mut Vec<serde_json::Map<String, serde_json::Value>>,
) {
    match value {
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_rows(v, out);
            }
        }
        serde_json::Value::Object(mut obj) => {
            // Scan-path rows are wrapped `{id, data: {...}}`; we want the
            // `data` payload as the logical row. If `data` is itself an
            // object, unwrap; otherwise keep the outer map (aggregates,
            // joins emit flat row objects directly).
            if obj.len() == 2
                && obj.contains_key("id")
                && matches!(obj.get("data"), Some(serde_json::Value::Object(_)))
            {
                if let Some(serde_json::Value::Object(inner)) = obj.remove("data") {
                    out.push(inner);
                }
            } else {
                out.push(obj);
            }
        }
        _ => {}
    }
}

/// Build `INSERT INTO <target> (col1, col2, ...) VALUES (lit1, lit2, ...)`
/// from a row map. Preserves insertion order of JSON map keys.
fn build_insert_sql(
    target: &str,
    row: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, DdlError> {
    if row.is_empty() {
        return Err(err(
            sqlstate::INTERNAL_ERROR,
            "materialized view SELECT produced an empty row (no columns)".to_string(),
        ));
    }
    let cols = row
        .keys()
        .map(String::as_str)
        .map(::nodedb_types::quote_ident)
        .collect::<Vec<_>>()
        .join(", ");
    let vals = row
        .values()
        .map(json_value_to_sql_literal)
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!(
        "INSERT INTO {} ({cols}) VALUES ({vals})",
        ::nodedb_types::quote_ident(target)
    ))
}

fn json_value_to_sql_literal(v: &serde_json::Value) -> Result<String, DdlError> {
    Ok(match v {
        serde_json::Value::Null => "NULL".into(),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => ::nodedb_types::quote_literal(s),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let s = sonic_rs::to_string(v).map_err(|e| {
                err(
                    sqlstate::INTERNAL_ERROR,
                    format!("encode nested value: {e}"),
                )
            })?;
            ::nodedb_types::quote_literal(&s)
        }
    })
}

async fn dispatch_sql(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<(), DdlError> {
    let (tasks, _output_schema, _lease_scope) =
        crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql(
            state,
            identity,
            sql,
            database_id,
        )
        .await
        .map_err(|error| {
            DdlError::new(error.sqlstate, format!("plan '{sql}': {}", error.message))
        })?;
    for task in tasks {
        // Mandatory chokepoint every write dispatch site runs (same gate as
        // pgwire/native/HTTP) — not a refresh-specific feature.
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
        let checked = match crate::control::server::shared::clone_write::intercept_and_authorize(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state,
                task,
                identity,
                tenant_id: identity.tenant_id,
                permissions: &state.permissions,
                roles: &state.roles,
                emitter: &emitter,
            },
        )
        .await
        .map_err(|e| {
            let (_, code, message) = error_to_sqlstate(&e);
            err(code, format!("clone-check: {message}"))
        })? {
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(resp) => {
                require_ok_response(&resp)?;
                continue;
            }
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(checked) => {
                checked
            }
        };
        // The record's outcome-floor window opens before the append and
        // closes from the task's outcome inside the funnel.
        let owner = crate::control::server::dispatch_utils::RecordOwner {
            tenant_id: identity.tenant_id,
            database_id: checked.database_id(),
            vshard_id: checked.vshard_id(),
        };
        let minted =
            crate::control::server::dispatch_utils::MintedRecords::open(&state.outcome_floor);
        if let Err(e) = minted.append_plan(
            &state.wal,
            owner,
            checked.plan(),
            // The refresh is dispatched as a client write.
            crate::event::EventSource::User,
        ) {
            // Any record appended before the error never reaches a core.
            minted
                .cancel(&state.wal, owner, 0)
                .await
                .map_err(|c| err(sqlstate::IO_ERROR, format!("cancel refresh record: {c}")))?;
            return Err(err(sqlstate::IO_ERROR, format!("wal append: {e}")));
        }
        let response =
            crate::control::server::dispatch_utils::dispatch_authorized_minted_to_data_plane(
                state,
                checked,
                TraceId::ZERO,
                minted,
            )
            .await
            .map_err(|e| err(sqlstate::CONNECTION_FAILURE, format!("dispatch: {e}")))?;
        require_ok_response(&response)?;
    }
    Ok(())
}

fn require_ok_response(response: &crate::bridge::envelope::Response) -> Result<(), DdlError> {
    if response.status == crate::bridge::envelope::Status::Ok {
        return Ok(());
    }

    match response.error_code.as_deref() {
        Some(code) => {
            let (_, sqlstate_code, message) = error_code_to_sqlstate(code);
            Err(err(
                sqlstate_code,
                format!("data-plane refresh task failed: {message}"),
            ))
        }
        None => {
            let detail = String::from_utf8_lossy(response.payload.as_ref()).into_owned();
            Err(err(
                sqlstate::INTERNAL_ERROR,
                format!("data-plane refresh task failed: {detail}"),
            ))
        }
    }
}

fn parse_refresh_target(sql: &str) -> Result<String, DdlError> {
    const PREFIX: &str = "REFRESH MATERIALIZED VIEW";
    let trimmed = sql.trim();
    let prefix = trimmed
        .get(..PREFIX.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(PREFIX))
        .ok_or_else(|| {
            err(
                sqlstate::SYNTAX_ERROR,
                "syntax: REFRESH MATERIALIZED VIEW <name>".to_string(),
            )
        })?;
    if prefix.len() == trimmed.len()
        || !trimmed[PREFIX.len()..]
            .chars()
            .next()
            .is_some_and(char::is_whitespace)
    {
        return Err(err(
            sqlstate::SYNTAX_ERROR,
            "syntax: REFRESH MATERIALIZED VIEW <name>".to_string(),
        ));
    }

    let (name, rest) = parse_identifier_token(trimmed[PREFIX.len()..].trim_start())?;
    let trailing = rest.trim();
    if !trailing.is_empty() && trailing != ";" {
        return Err(err(
            sqlstate::SYNTAX_ERROR,
            "unexpected trailing tokens after materialized view name".to_string(),
        ));
    }
    Ok(name)
}

fn parse_identifier_token(input: &str) -> Result<(String, &str), DdlError> {
    if input.is_empty() {
        return Err(err(
            sqlstate::SYNTAX_ERROR,
            "missing materialized view name".to_string(),
        ));
    }
    if let Some(mut rest) = input.strip_prefix('"') {
        let mut value = String::new();
        loop {
            let Some(ch) = rest.chars().next() else {
                return Err(err(
                    sqlstate::SYNTAX_ERROR,
                    "unterminated quoted identifier".to_string(),
                ));
            };
            rest = &rest[ch.len_utf8()..];
            if ch == '"' {
                if let Some(next) = rest.strip_prefix('"') {
                    value.push('"');
                    rest = next;
                    continue;
                }
                if value.chars().any(char::is_control) {
                    return Err(err(
                        sqlstate::SYNTAX_ERROR,
                        "identifier contains a control character".to_string(),
                    ));
                }
                return Ok((value, rest));
            }
            value.push(ch);
        }
    }

    let end = input
        .char_indices()
        .find_map(|(index, ch)| (!is_bare_identifier_char(ch)).then_some(index))
        .unwrap_or(input.len());
    let name = &input[..end];
    if name.is_empty()
        || !name
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_alphabetic())
    {
        return Err(err(
            sqlstate::SYNTAX_ERROR,
            "invalid materialized view name".to_string(),
        ));
    }
    Ok((name.to_lowercase(), &input[end..]))
}

fn is_bare_identifier_char(ch: char) -> bool {
    ch == '_' || ch == '$' || ch.is_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_insert_quotes_target_columns_and_literals() {
        let mut row = serde_json::Map::new();
        row.insert(
            "column\"; DELETE FROM audit_log; --".to_string(),
            serde_json::Value::String("value'; DELETE FROM audit_log; --".to_string()),
        );
        row.insert(
            "nested".to_string(),
            serde_json::json!({"name": "O'Reilly"}),
        );

        let sql = build_insert_sql("view\"; DROP TABLE audit_log; --", &row)
            .expect("generated INSERT is valid");

        assert_eq!(
            sql,
            "INSERT INTO \"view\"\"; DROP TABLE audit_log; --\" (\"column\"\"; DELETE FROM audit_log; --\", \"nested\") VALUES ('value''; DELETE FROM audit_log; --', '{\"name\":\"O''Reilly\"}')"
        );
    }

    #[test]
    fn generated_literals_preserve_non_string_sql_types() {
        assert_eq!(
            json_value_to_sql_literal(&serde_json::Value::Null).expect("null literal"),
            "NULL"
        );
        assert_eq!(
            json_value_to_sql_literal(&serde_json::json!(true)).expect("boolean literal"),
            "TRUE"
        );
        assert_eq!(
            json_value_to_sql_literal(&serde_json::json!(42)).expect("number literal"),
            "42"
        );
    }

    #[test]
    fn refresh_target_parses_quoted_and_bare_identifiers() {
        assert_eq!(
            parse_refresh_target("REFRESH MATERIALIZED VIEW \"Sales View\"").expect("quoted"),
            "Sales View"
        );
        assert_eq!(
            parse_refresh_target(" refresh materialized view \"a\"\"b\"; ").expect("escaped"),
            "a\"b"
        );
        assert_eq!(
            parse_refresh_target("REFRESH MATERIALIZED VIEW MiXeD_Name").expect("bare"),
            "mixed_name"
        );
    }

    #[test]
    fn refresh_target_rejects_malformed_or_trailing_input() {
        for sql in [
            "REFRESH MATERIALIZED VIEW",
            "REFRESH MATERIALIZED VIEW \"unterminated",
            "REFRESH MATERIALIZED VIEW view extra",
            "REFRESH MATERIALIZED VIEW view;;",
        ] {
            assert!(parse_refresh_target(sql).is_err(), "{sql}");
        }
    }
}
