// SPDX-License-Identifier: BUSL-1.1

//! Gives each plain HTTP request its own request stream.
//!
//! HTTP carries no connection the server orders requests by. Each request
//! takes one id from the shared allocator and runs inside its stream scope,
//! so the requests it sends keep their order and order against nothing else.
//! A body streamed after the handler returns carries the stream into each
//! poll through `connection_stream::carry_current`.

use axum::extract::State;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::bridge::envelope::RequestStream;
use crate::control::server::shared::session::connection_stream;

use super::auth::{ApiError, AppState};

/// Run the request on a stream of its own.
pub(super) async fn scope_request_stream(
    State(app_state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    match app_state.shared.connection_ids.allocate() {
        Ok(id) => {
            let stream = RequestStream::client(app_state.shared.node_id, id);
            connection_stream::scoped(stream, next.run(req)).await
        }
        Err(error) => ApiError::Internal(error.to_string()).into_response(),
    }
}
