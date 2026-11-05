// SPDX-License-Identifier: BUSL-1.1

//! Counts every failed HTTP request once against its database.
//!
//! A request counts when the response status is 4xx or 5xx and admission
//! noted a database for it. Requests refused before admission have no
//! database and do not count. Failures that surface after the status line
//! (a streamed body) are counted by the stream writer.

use axum::extract::State;
use axum::middleware::Next;
use axum::response::Response;

use crate::control::server::session_auth::admitted_database;

use super::auth::AppState;

/// Run the request and count its failure, if any.
pub(super) async fn count_failed_requests(
    State(app_state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let (response, database_id) = admitted_database::scope(next.run(req)).await;
    let status = response.status();
    if let Some(database_id) = database_id
        && (status.is_client_error() || status.is_server_error())
    {
        app_state.shared.database_metrics.record_error(database_id);
    }
    response
}
