// SPDX-License-Identifier: BUSL-1.1

//! Subquery CHECK constraint evaluation: plans and dispatches a `SELECT`
//! query built from the constraint expression.

use std::collections::HashMap;

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::control::security::catalog::types::CheckConstraintDef;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::enforce::ddl_err;
use super::simple::substitute_new_refs;

/// Evaluate a CHECK constraint with subqueries via SQL planning and dispatch.
pub(super) async fn enforce_subquery_check(
    state: &SharedState,
    tenant_id: nodedb_types::TenantId,
    constraint: &CheckConstraintDef,
    fields: &HashMap<String, nodedb_types::Value>,
) -> Result<(), DdlError> {
    let substituted = substitute_new_refs(&constraint.check_sql, fields);

    // Restructure the subquery CHECK into an executable SQL query.
    // Pattern: `val IN (SELECT col FROM tbl ...)` → `SELECT COUNT(*) AS cnt FROM tbl WHERE col = val ...`
    // General fallback: wrap in subselect.
    let restructured = restructure_subquery_check(&substituted);

    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);

    let (tasks, _output_schema) = match query_ctx
        .plan_sql(
            &restructured.sql,
            tenant_id,
            crate::types::DatabaseId::DEFAULT,
        )
        .await
    {
        Ok(t) => t,
        Err(e) => {
            return Err(ddl_err(
                "23514",
                &format!(
                    "CHECK constraint '{}' failed to evaluate: {}",
                    constraint.name, e
                ),
            ));
        }
    };

    let mut passed = false;
    for task in tasks {
        let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            tenant_id,
            task.database_id,
            task.vshard_id,
            task.plan,
            TraceId::ZERO,
        )
        .await;

        match resp {
            Ok(response) => {
                let json = crate::data::executor::response_codec::decode_payload_to_json(
                    &response.payload,
                );
                if !json.is_empty() && check_count_is_positive(&json) {
                    passed = true;
                }
            }
            Err(e) => {
                return Err(ddl_err(
                    "23514",
                    &format!(
                        "CHECK constraint '{}' failed to evaluate: {}",
                        constraint.name, e
                    ),
                ));
            }
        }
    }

    // For NOT IN: negate — count > 0 means constraint violated.
    // For IN: count > 0 means constraint passed.
    let constraint_ok = if restructured.negate { !passed } else { passed };

    if !constraint_ok {
        return Err(ddl_err(
            "23514",
            &format!(
                "CHECK constraint '{}' violated: {}",
                constraint.name, constraint.check_sql
            ),
        ));
    }

    Ok(())
}

/// Check if a COUNT(*) JSON response indicates a positive count.
///
/// Response format is typically `{"cnt":N}` or `[{"cnt":N}]`.
fn check_count_is_positive(json: &str) -> bool {
    // Parse as JSON to reliably check the count value.
    if let Ok(v) = sonic_rs::from_str::<serde_json::Value>(json) {
        // Check for {"cnt": N} or [{"cnt": N}]
        let obj = if let Some(arr) = v.as_array() {
            arr.first().and_then(|r| r.as_object())
        } else {
            v.as_object()
        };
        if let Some(obj) = obj {
            for (_, val) in obj {
                if let Some(n) = val.as_i64() {
                    return n > 0;
                }
                if let Some(n) = val.as_f64() {
                    return n > 0.0;
                }
            }
        }
    }
    // Empty array or unparseable — constraint failed (no matching rows).
    false
}

/// Result of restructuring a subquery CHECK expression.
struct RestructuredCheck {
    /// The SQL query to execute.
    sql: String,
    /// If true, a positive COUNT means the constraint is VIOLATED (NOT IN case).
    negate: bool,
}

/// Restructure a subquery CHECK expression into an executable SQL query.
///
/// Handles:
/// - `'val' IN (SELECT col FROM tbl ...)` → COUNT > 0 means pass
/// - `'val' NOT IN (SELECT col FROM tbl ...)` → COUNT = 0 means pass
fn restructure_subquery_check(expr: &str) -> RestructuredCheck {
    // Detect NOT IN vs IN.
    let (in_pos, negate) = if let Some(pos) = find_ascii_case_insensitive(expr, " NOT IN (SELECT ")
    {
        (pos, true)
    } else if let Some(pos) = find_ascii_case_insensitive(expr, " NOT IN(SELECT ") {
        (pos, true)
    } else if let Some(pos) = find_ascii_case_insensitive(expr, " IN (SELECT ") {
        (pos, false)
    } else if let Some(pos) = find_ascii_case_insensitive(expr, " IN(SELECT ") {
        (pos, false)
    } else {
        // Should not reach here — validated at DDL time.
        return RestructuredCheck {
            sql: format!("SELECT ({expr}) AS _check"),
            negate: false,
        };
    };

    let value_part = expr[..in_pos].trim();
    let keyword_len = if negate { " NOT IN (" } else { " IN (" };
    let select_part = &expr[in_pos + keyword_len.len()..];
    let inner = select_part.trim().trim_end_matches(')').trim();

    if let Some(from_pos) = find_ascii_case_insensitive(inner, " FROM ") {
        let col = inner["SELECT ".len()..from_pos].trim();
        let after_from = &inner[from_pos + 6..];
        let (table, existing_where) =
            if let Some(w) = find_ascii_case_insensitive(after_from, " WHERE ") {
                (&after_from[..w], Some(&after_from[w + 7..]))
            } else {
                (after_from.trim(), None)
            };

        let sql = if let Some(where_clause) = existing_where {
            format!(
                "SELECT COUNT(*) AS cnt FROM {} WHERE {} = {} AND {}",
                table.trim(),
                col,
                value_part,
                where_clause
            )
        } else {
            format!(
                "SELECT COUNT(*) AS cnt FROM {} WHERE {} = {}",
                table.trim(),
                col,
                value_part
            )
        };

        return RestructuredCheck { sql, negate };
    }

    RestructuredCheck {
        sql: format!("SELECT ({expr}) AS _check"),
        negate: false,
    }
}
