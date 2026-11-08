// SPDX-License-Identifier: BUSL-1.1

//! The NDJSON answer to a lone `GRAPH` or `MATCH` statement.

use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::DatabaseId;

use super::super::super::auth::{ApiError, AppState};
use super::super::graph_statement::try_graph_rows;
use super::ndjson_response::{ndjson_response, rows_to_ndjson};

/// The response for `sql` when it is a `GRAPH` or `MATCH` statement, else `None`.
pub(super) async fn graph_response(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
    rate_limit_headers: &HeaderMap,
) -> Option<Response> {
    let answer = try_graph_rows(&state.shared, identity, sql, database_id).await?;
    Some(match answer {
        Ok(rows) => ndjson_response(rows_to_ndjson(rows), rate_limit_headers.clone()),
        Err(error) => ApiError::from(error).into_response(),
    })
}
