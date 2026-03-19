//! Query endpoint — execute SQL/operations via HTTP POST.
//!
//! POST /query { "sql": "SHOW USERS" }
//! Authorization: Bearer ndb_...

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use super::super::auth::{ApiError, AppState, resolve_identity};

/// POST /query — execute a SQL/DDL statement.
///
/// Request body: `{ "sql": "..." }`
/// Response: `{ "status": "ok", "rows": [...] }` or `{ "error": "..." }`
pub async fn query(
    headers: HeaderMap,
    State(state): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;

    let sql = body["sql"]
        .as_str()
        .ok_or_else(|| ApiError::BadRequest("missing 'sql' field".into()))?;

    // Try DDL commands first (same as pgwire handler).
    if let Some(result) =
        crate::control::server::pgwire::ddl::dispatch(&state.shared, &identity, sql.trim())
    {
        return match result {
            Ok(responses) => {
                // Convert pgwire Response to JSON.
                let json_rows = responses_to_json(responses);
                Ok(axum::Json(serde_json::json!({
                    "status": "ok",
                    "rows": json_rows,
                })))
            }
            Err(e) => Err(ApiError::BadRequest(e.to_string())),
        };
    }

    // Not a DDL — return error for now (DataFusion SQL dispatch via HTTP is future work).
    Err(ApiError::BadRequest(format!(
        "only DDL commands supported via HTTP API currently. Use pgwire (port 5432) for SQL queries. Got: {sql}"
    )))
}

/// Convert pgwire Response vec to JSON rows.
fn responses_to_json(responses: Vec<pgwire::api::results::Response>) -> Vec<serde_json::Value> {
    use pgwire::api::results::Response;

    let mut rows = Vec::new();
    for resp in responses {
        match resp {
            Response::Execution(tag) => {
                rows.push(serde_json::json!({
                    "type": "execution",
                    "tag": format!("{:?}", tag),
                }));
            }
            Response::Query(_) => {
                // QueryResponse contains a stream — we can't easily drain it here
                // without async. Return a placeholder.
                rows.push(serde_json::json!({
                    "type": "query",
                    "note": "query results available via pgwire protocol",
                }));
            }
            Response::EmptyQuery => {
                rows.push(serde_json::json!({ "type": "empty" }));
            }
            _ => {}
        }
    }
    rows
}
