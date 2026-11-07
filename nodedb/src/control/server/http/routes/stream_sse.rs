// SPDX-License-Identifier: BUSL-1.1

//! SSE streaming endpoint for change stream consumption.
//!
//! `GET /v1/streams/{stream}/events?group={group}&partition=3`
//!
//! Pushes events as Server-Sent Events in real-time. On each poll cycle,
//! reads new events from the buffer since the consumer group's committed
//! offset. The consumer must COMMIT OFFSET via SQL to advance the cursor.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use serde::Deserialize;
use sonic_rs;

use super::super::admission::admit_without_rate_limit;
use super::super::auth::{ApiError, AppState, ResolvedIdentity};
use super::super::peer::PeerAddr;
use super::query::{DatabaseQueryParam, resolve_database_id};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::Permission;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::security::sessions::{SessionBindRequest, bind_session, stream_until_ended};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::event::cdc::CdcSubscriberScope;
use crate::event::cdc::consume::{ConsumeError, ConsumeParams, consume_stream};

/// Query parameters.
#[derive(Deserialize, Default)]
pub struct SseParams {
    /// Consumer group name (required).
    pub group: Option<String>,
    /// Optional: stream from a specific partition only.
    pub partition: Option<u32>,
    /// Detected and rejected — callers must not supply `tenant_id` as a
    /// query parameter. Tenant is always sourced from the bearer token.
    pub tenant_id: Option<u64>,
    /// Optional database selector. The header takes precedence when present.
    pub database: Option<String>,
}

/// Drop guard that deregisters a consumer from partition assignment
/// on ALL exit paths (normal close, error, panic, task cancellation).
struct ConsumerGuard {
    shared: Arc<SharedState>,
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    stream_name: String,
    group: String,
    consumer_id: String,
}

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        self.shared.consumer_assignments.leave(
            self.database_id,
            self.tenant_id,
            &self.stream_name,
            &self.group,
            &self.consumer_id,
        );
    }
}

/// `GET /v1/streams/{stream}/events`
pub async fn stream_events(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    Path(stream_name): Path<String>,
    Query(params): Query<SseParams>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // Reject any attempt to override the caller's tenant via query string.
    if params.tenant_id.is_some() {
        return Err(ApiError::Forbidden(
            "tenant_id must not be supplied as a query parameter; \
             tenant is determined from the bearer token"
                .into(),
        ));
    }

    let group = params.group.unwrap_or_default().to_lowercase();
    let tenant_id = identity.tenant_id().as_u64();
    let database_id = resolve_database_id(
        &headers,
        &DatabaseQueryParam {
            database: params.database.clone(),
        },
        &state,
    )?;
    let stream_name = stream_name.to_lowercase();
    let partition = params.partition;

    // Blacklist + account status, no rate limit: an SSE stream is admitted
    // once at open and then served for as long as it stays connected, so it
    // is not the per-request traffic the rate limiter's cost table models. It
    // runs before authorization and before any consumer-group assignment is
    // claimed.
    admit_without_rate_limit(&state, &identity.0, database_id, peer.as_str())?;

    // Authorize the source collection before constructing the SSE body. This
    // ensures a denied request cannot claim a consumer-group assignment while
    // waiting for the stream to be polled. Topics are authorized against
    // their logical topic resources instead of a source collection.
    if let Some(topic_name) = stream_name.strip_prefix("topic:") {
        let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
        authorize_collection(
            &identity.0,
            database_id,
            &format!("topic:{topic_name}"),
            Permission::Read,
            &state.shared.permissions,
            &state.shared.roles,
            &emitter,
        )
        .map_err(crate::Error::from)
        .map_err(ApiError::from)?;
    } else if let Some(stream_def) =
        state
            .shared
            .stream_registry
            .get(database_id, tenant_id, &stream_name)
    {
        let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
        authorize_collection(
            &identity.0,
            database_id,
            &stream_def.collection,
            Permission::Read,
            &state.shared.permissions,
            &state.shared.roles,
            &emitter,
        )
        .map_err(crate::Error::from)
        .map_err(ApiError::from)?;
    }

    // Events carry the written row, so the subscriber's column redaction rules
    // apply to them exactly as they do to a SELECT of the source collection.
    // The roles belong to the request, not to any one batch, so they are
    // resolved once here and moved into the long-lived SSE body.
    let subscriber_tenant = identity.tenant_id();
    let subscriber_roles =
        RequestAuthScope::for_database(&identity.0, state.shared.auth_stores(), database_id)?
            .auth()
            .roles
            .clone();

    let binding = bind_session(
        &state.shared,
        SessionBindRequest::new(&identity.0, "sse", peer.as_str()).in_database(database_id),
    )
    .map_err(ApiError::from)?;

    let stream = async_stream::stream! {
        let mut subscriber = CdcSubscriberScope::new(subscriber_tenant, subscriber_roles);

        if group.is_empty() {
            yield Ok(Event::default()
                .event("error")
                .data("missing 'group' query parameter"));
            return;
        }

        // Generate a unique consumer ID using process ID + atomic counter.
        let consumer_id = unique_consumer_id();

        // Register consumer and create a Drop guard for guaranteed cleanup.
        state.shared.consumer_assignments.join(
            database_id,
            tenant_id,
            &stream_name,
            &group,
            &consumer_id,
        );
        let _guard = ConsumerGuard {
            shared: Arc::clone(&state.shared),
            database_id,
            tenant_id,
            stream_name: stream_name.clone(),
            group: group.clone(),
            consumer_id,
        };

        loop {
            let consume_params = ConsumeParams {
                database_id,
                tenant_id,
                stream_name: &stream_name,
                group_name: &group,
                partition,
                limit: 100,
            };

            let mut result = match consume_stream(&state.shared, &consume_params).await {
                Ok(r) => r,
                Err(ConsumeError::RemotePartition { leader_node, .. }) => {
                    match crate::event::cdc::consume::consume_remote(
                        &state.shared,
                        &consume_params,
                        leader_node,
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(e @ ConsumeError::OffsetOutOfRange { .. }) => {
                            yield Ok(Event::default()
                                .event("reset_required")
                                .data(e.to_string()));
                            return;
                        }
                        Err(e) => {
                            yield Ok(Event::default()
                                .event("error")
                                .data(e.to_string()));
                            return;
                        }
                    }
                }
                Err(e @ ConsumeError::OffsetOutOfRange { .. }) => {
                    yield Ok(Event::default()
                        .event("reset_required")
                        .data(e.to_string()));
                    return;
                }
                Err(ConsumeError::BufferEmpty(_)) => {
                    // No events yet — wait and retry.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => {
                    yield Ok(Event::default()
                        .event("error")
                        .data(e.to_string()));
                    return; // _guard dropped here → leave() called.
                }
            };
            subscriber.retain_deliverable(&state.shared.redaction, &mut result.events);
            if !result.events.is_empty() {
                for event in &result.events {
                    let json = sonic_rs::to_string(event).unwrap_or_default();
                    yield Ok(Event::default()
                        .event("change")
                        .id(event.offset_token())
                        .data(json));
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // _guard dropped here on any exit → leave() called.
    };

    Ok(Sse::new(stream_until_ended(stream, binding)).keep_alive(KeepAlive::default()))
}

/// Generate a unique consumer ID using process ID + monotonic counter.
fn unique_consumer_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("sse-{}-{seq}", std::process::id())
}
