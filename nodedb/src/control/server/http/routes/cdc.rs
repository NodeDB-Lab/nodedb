// SPDX-License-Identifier: BUSL-1.1

//! Change Data Capture SSE and polling endpoints.

use std::convert::Infallible;
use std::str::FromStr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use serde::Deserialize;

use super::super::admission::{admit, admit_without_rate_limit};
use super::super::auth::{ApiError, AppState, ResolvedIdentity};
use super::super::peer::PeerAddr;
use super::query::{DatabaseQueryParam, resolve_database_id};
use crate::control::change_stream::{ChangeCursor, ReplayError, ReplayStart, SequencedChangeEvent};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::Permission;
use crate::control::server::shared::authorization::{authorize_collection, authorize_database};

#[derive(Deserialize, Default)]
pub struct SseParams {
    pub since_ms: Option<u64>,
    pub tenant_id: Option<u64>,
    pub database: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct PollParams {
    pub since_ms: Option<u64>,
    pub cursor: Option<String>,
    /// Retained as a string solely so it produces the explicit migration error.
    pub since_lsn: Option<String>,
    pub limit: Option<usize>,
    pub tenant_id: Option<u64>,
    pub database: Option<String>,
}

pub async fn sse_stream(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    Path(collection): Path<String>,
    Query(params): Query<SseParams>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    reject_tenant_override(params.tenant_id)?;
    let collection = collection.to_lowercase();
    let tenant_id = identity.tenant_id();
    let database_id = resolve_database_id(
        &headers,
        &DatabaseQueryParam {
            database: params.database.clone(),
        },
        &state,
    )?;
    // Blacklist + account status, no rate limit: an SSE stream is admitted
    // once at open and then served for as long as it stays connected, so it
    // is not the per-request traffic the rate limiter's cost table models.
    // Runs before the collection authorization and before any subscription or
    // snapshot is taken.
    admit_without_rate_limit(&state, &identity.0, database_id, peer.as_str())?;
    authorize(&identity, &state, database_id, &collection)?;
    let cursor = parse_last_event_id(&headers)?;
    if cursor.is_some() && params.since_ms.is_some() {
        return Err(ApiError::BadRequest(
            "since_ms is allowed only for an initial CDC stream".into(),
        ));
    }
    let start = cursor
        .map(ReplayStart::Cursor)
        .unwrap_or(ReplayStart::Timestamp(params.since_ms.unwrap_or(0)));
    let shared = Arc::clone(&state.shared);
    let mut subscription = shared.change_stream.subscribe_in_database(
        Some(collection.clone()),
        Some(tenant_id),
        database_id,
    );
    let snapshot = shared
        .change_stream
        .query_changes_in_database(tenant_id, database_id, Some(&collection), start, 10_000)
        .map_err(reset_error)?;
    let snapshot_cursor = snapshot.snapshot_cursor;
    let stream = async_stream::stream! {
        for event in snapshot.events { yield Ok(format_sse_event(&event)); }
        loop {
            match subscription.recv_sequenced().await {
                Ok(event) => {
                    if !event.cursor().same_epoch(snapshot_cursor) {
                        yield Ok(Event::default().event("reset_required").data("change stream epoch changed; reconnect with a fresh snapshot"));
                        break;
                    }
                    if !event.cursor().is_after_in_same_epoch(snapshot_cursor) { continue; }
                    yield Ok(format_sse_event(&event));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    yield Ok(Event::default().event("reset_required").data("change stream lagged; reconnect with a fresh snapshot"));
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

pub async fn poll_changes(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    Path(collection): Path<String>,
    Query(params): Query<PollParams>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    reject_tenant_override(params.tenant_id)?;
    if params.since_lsn.is_some() {
        return Err(ApiError::BadRequest(
            "since_lsn is no longer supported; use the opaque cursor parameter".into(),
        ));
    }
    let collection = collection.to_lowercase();
    let tenant_id = identity.tenant_id();
    let database_id = resolve_database_id(
        &headers,
        &DatabaseQueryParam {
            database: params.database.clone(),
        },
        &state,
    )?;
    // Full gate: a poll is a discrete per-request read of the caller's change
    // data, exactly the shape the rate limiter's cost table models.
    let rate_limit_headers = admit(&state, &identity.0, database_id, peer.as_str(), "cdc_poll")?;
    authorize(&identity, &state, database_id, &collection)?;
    let cursor = params.cursor.as_deref().map(parse_cursor).transpose()?;
    if cursor.is_some() && params.since_ms.is_some() {
        return Err(ApiError::BadRequest(
            "since_ms is allowed only for an initial CDC poll".into(),
        ));
    }
    let start = cursor
        .map(ReplayStart::Cursor)
        .unwrap_or(ReplayStart::Timestamp(params.since_ms.unwrap_or(0)));
    let limit = params.limit.unwrap_or(100).clamp(1, 10_000);
    let mut snapshot = state
        .shared
        .change_stream
        .query_changes_in_database(tenant_id, database_id, Some(&collection), start, limit + 1)
        .map_err(reset_error)?;
    let has_more = snapshot.events.len() > limit;
    if has_more {
        snapshot.events.truncate(limit);
    }
    let changes: Vec<_> = snapshot.events.iter().map(change_json).collect();
    let next_cursor = snapshot
        .events
        .last()
        .map(|event| serde_json::json!({"cursor": event.cursor().to_string()}));
    Ok((
        rate_limit_headers,
        Json(serde_json::json!({ "changes": changes, "next_cursor": next_cursor, "has_more": has_more, "count": snapshot.events.len() })),
    )
        .into_response())
}

fn reject_tenant_override(tenant_id: Option<u64>) -> Result<(), ApiError> {
    if tenant_id.is_some() {
        Err(ApiError::Forbidden("tenant_id must not be supplied as a query parameter; tenant is determined from the bearer token".into()))
    } else {
        Ok(())
    }
}

fn authorize(
    identity: &ResolvedIdentity,
    state: &AppState,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
) -> Result<(), ApiError> {
    let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
    authorize_database(&identity.0, database_id, &emitter)
        .map_err(crate::Error::from)
        .map_err(ApiError::from)?;
    authorize_collection(
        &identity.0,
        database_id,
        collection,
        Permission::Read,
        &state.shared.permissions,
        &state.shared.roles,
        &emitter,
    )
    .map_err(crate::Error::from)
    .map_err(ApiError::from)
}

fn parse_cursor(token: &str) -> Result<ChangeCursor, ApiError> {
    ChangeCursor::from_str(token)
        .map_err(|_| ApiError::BadRequest("cursor must be a valid opaque change cursor".into()))
}

fn parse_last_event_id(headers: &HeaderMap) -> Result<Option<ChangeCursor>, ApiError> {
    let Some(value) = headers.get("last-event-id") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::BadRequest("Last-Event-ID must be an opaque change cursor".into()))?
        .trim();
    if value.is_empty() {
        Ok(None)
    } else {
        parse_cursor(value).map(Some)
    }
}

fn reset_error(_: ReplayError) -> ApiError {
    ApiError::HttpStatus(
        410,
        "reset_required: cursor is expired, from a different stream epoch, or ahead of the stream"
            .into(),
    )
}

fn change_json(event: &SequencedChangeEvent) -> serde_json::Value {
    serde_json::json!({ "operation": event.operation.as_str(), "document_id": event.document_id.as_str(), "timestamp_ms": event.timestamp_ms, "lsn": event.lsn.as_u64(), "collection": event.collection, "cursor": event.cursor().to_string() })
}

fn format_sse_event(event: &SequencedChangeEvent) -> Event {
    Event::default()
        .id(event.cursor().to_string())
        .event(event.operation.as_str().to_lowercase())
        .data(change_json(event).to_string())
}
