// SPDX-License-Identifier: BUSL-1.1

//! Single WebSocket message processing for the JSON-RPC protocol.

use std::str::FromStr;
use std::sync::Arc;

use crate::control::change_stream::{ChangeCursor, CursorStep, LiveSubscriptionSet, ReplayStart};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId};

use super::execute_sql::execute_sql;
use super::format::{
    error_response, extract_collection_from_sql, format_resume_notification,
    format_sequenced_live_notification, ws_error_from_gateway,
};

pub fn extract_session_id(req: &serde_json::Value) -> Option<String> {
    req.get("params")
        .and_then(|params| params.get("session_id"))
        .and_then(|id| id.as_str())
        .filter(|id| !id.is_empty())
        .map(String::from)
}

pub(super) struct MessageContext<'a> {
    pub(super) shared: Arc<SharedState>,
    pub(super) query_ctx: &'a crate::control::planner::context::QueryContext,
    pub(super) identity: &'a AuthenticatedIdentity,
    pub(super) database_id: DatabaseId,
    pub(super) trace_id: TraceId,
    pub(super) live_tx: &'a tokio::sync::mpsc::Sender<String>,
    /// Real client socket address (`ip:port`), as extracted from the axum
    /// `ConnectInfo<SocketAddr>` at upgrade time. Threaded through to
    /// `execute_sql`'s admission gate so IP/CIDR blacklist checks apply to
    /// this transport the same as pgwire/HTTP/native.
    pub(super) peer_addr: &'a str,
}

struct ResumeContext<'a> {
    shared: Arc<SharedState>,
    identity: &'a AuthenticatedIdentity,
    database_id: DatabaseId,
    live_tx: &'a tokio::sync::mpsc::Sender<String>,
}

/// Returns whether the connection has completed its one permitted resume auth.
pub(super) async fn process_message(
    context: MessageContext<'_>,
    text: &str,
    live_set: &mut LiveSubscriptionSet,
    resume_set: &mut LiveSubscriptionSet,
    resume_authenticated: bool,
) -> (String, bool) {
    let MessageContext {
        shared,
        query_ctx,
        identity,
        database_id,
        trace_id,
        live_tx,
        peer_addr,
    } = context;
    let req: serde_json::Value = match crate::util::bounded_json::from_str(text) {
        Ok(value) => value,
        Err(error) => {
            return (
                error_response(serde_json::Value::Null, &format!("invalid JSON: {error}")),
                false,
            );
        }
    };
    let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = req
        .get("method")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    match method {
        "ping" => (
            serde_json::json!({"id": id, "result": "pong"}).to_string(),
            false,
        ),
        "auth" => {
            resume_auth(
                ResumeContext {
                    shared,
                    identity,
                    database_id,
                    live_tx,
                },
                &id,
                &req,
                resume_set,
                resume_authenticated,
            )
            .await
        }
        "query" => {
            let sql = req
                .get("params")
                .and_then(|params| params.get("sql"))
                .and_then(|sql| sql.as_str())
                .unwrap_or("");
            if sql.is_empty() {
                return (error_response(id, "missing params.sql"), false);
            }
            let response = match execute_sql(
                &shared,
                query_ctx,
                identity,
                database_id,
                sql,
                trace_id,
                peer_addr,
            )
            .await
            {
                Ok(result) => serde_json::json!({"id": id, "result": result}).to_string(),
                Err(error) => {
                    shared.database_metrics.record_error(database_id);
                    ws_error_from_gateway(&id, &error)
                }
            };
            (response, false)
        }
        "live" => {
            let sql = req
                .get("params")
                .and_then(|params| params.get("sql"))
                .and_then(|sql| sql.as_str())
                .unwrap_or("");
            let collection = extract_collection_from_sql(sql);
            if collection.is_empty() {
                return (
                    error_response(id, "missing collection in LIVE SELECT"),
                    false,
                );
            }
            let collection =
                match crate::control::server::shared::request_collection::request_collection(
                    database_id,
                    &collection,
                ) {
                    Ok(collection) => collection,
                    Err(error) => return (error_response(id, &error.to_string()), false),
                };
            let emitter = ArcAuditEmitter(Arc::clone(&shared.audit));
            if let Err(error) = authorize_collection(
                identity,
                database_id,
                &collection,
                Permission::Read,
                &shared.permissions,
                &shared.roles,
                &emitter,
            ) {
                return (
                    error_response(id, &crate::Error::from(error).to_string()),
                    false,
                );
            }
            let mut sub = shared.change_stream.subscribe_in_database(
                Some(collection.clone()),
                Some(identity.tenant_id),
                database_id,
            );
            let sub_id = sub.id;
            let live_tx = live_tx.clone();
            // The subscription receives exactly the events past its start.
            let mut cursor = sub.start_cursor().clone();
            live_set.spawn_task(async move {
                loop {
                    match sub.recv_sequenced().await {
                        Ok(event) => match cursor.accept(&event) {
                            CursorStep::Deliver => {
                                if live_tx
                                    .send(format_sequenced_live_notification(
                                        sub_id, &event, &cursor,
                                    ))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            CursorStep::Skip => {}
                            CursorStep::Reset => {
                                let _ = live_tx.send(serde_json::json!({"method":"reset_required","params":{"subscription_id":sub_id,"reason":"change feed gap"}}).to_string()).await;
                                break;
                            }
                        },
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            let _ = live_tx.send(serde_json::json!({"method":"reset_required","params":{"subscription_id":sub_id,"reason":"change stream lagged"}}).to_string()).await;
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
            (serde_json::json!({"id": id, "result": {"subscription_id": sub_id, "collection": collection, "status": "active"}}).to_string(), false)
        }
        _ => (
            error_response(id, &format!("unknown method: {method}")),
            false,
        ),
    }
}

async fn resume_auth(
    context: ResumeContext<'_>,
    id: &serde_json::Value,
    req: &serde_json::Value,
    resume_set: &mut LiveSubscriptionSet,
    already_authenticated: bool,
) -> (String, bool) {
    let ResumeContext {
        shared,
        identity,
        database_id,
        live_tx,
    } = context;
    if already_authenticated {
        return (
            error_response(
                id.clone(),
                "resume auth is permitted only once per connection",
            ),
            false,
        );
    }
    let Some(session_id) = extract_session_id(req) else {
        return (
            error_response(id.clone(), "missing params.session_id"),
            false,
        );
    };
    let params = req.get("params").unwrap_or(&serde_json::Value::Null);
    if params.get("last_lsn").is_some() {
        return (
            error_response(
                id.clone(),
                "last_lsn is no longer supported; use the opaque cursor",
            ),
            false,
        );
    }
    let cursor = match params.get("cursor") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => match value
            .as_str()
            .and_then(|value| ChangeCursor::from_str(value).ok())
        {
            Some(cursor) => Some(cursor),
            None => {
                return (
                    error_response(id.clone(), "cursor must be a valid opaque change cursor"),
                    false,
                );
            }
        },
    };
    let mut subscription =
        shared
            .change_stream
            .subscribe_in_database(None, Some(identity.tenant_id), database_id);
    let start = cursor
        .map(ReplayStart::Cursor)
        .unwrap_or(ReplayStart::Timestamp(0));
    // The ring is bounded, so the replay returns every event it holds.
    let snapshot = match shared.change_stream.query_changes_in_database(
        identity.tenant_id,
        database_id,
        None,
        start,
        usize::MAX,
    ) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            return (
                error_response(
                    id.clone(),
                    "reset_required: cursor cannot resume this stream",
                ),
                false,
            );
        }
    };
    let emitter = ArcAuditEmitter(Arc::clone(&shared.audit));
    let mut replayed = 0usize;
    for change in &snapshot.events {
        let event = &change.event;
        if authorize_collection(
            identity,
            database_id,
            &event.collection,
            Permission::Read,
            &shared.permissions,
            &shared.roles,
            &emitter,
        )
        .is_ok()
        {
            if live_tx
                .send(format_resume_notification(event, &change.cursor))
                .await
                .is_err()
            {
                break;
            }
            replayed += 1;
        }
    }
    let snapshot_cursor = snapshot.cursor.to_string();
    let mut cursor = snapshot.cursor;
    let live_tx = live_tx.clone();
    let shared = Arc::clone(&shared);
    let identity = identity.clone();
    resume_set.spawn_task(async move {
        loop {
            match subscription.recv_sequenced().await {
                Ok(event) => match cursor.accept(&event) {
                    CursorStep::Deliver => {
                        let emitter = ArcAuditEmitter(Arc::clone(&shared.audit));
                        if authorize_collection(&identity, database_id, &event.collection, Permission::Read, &shared.permissions, &shared.roles, &emitter).is_ok()
                            && live_tx.send(format_resume_notification(&event, &cursor)).await.is_err() { break; }
                    }
                    CursorStep::Skip => {}
                    CursorStep::Reset => {
                        let _ = live_tx.send(serde_json::json!({"method":"reset_required","params":{"reason":"change feed gap"}}).to_string()).await;
                        break;
                    }
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let _ = live_tx.send(serde_json::json!({"method":"reset_required","params":{"reason":"change stream lagged"}}).to_string()).await;
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    let response = serde_json::json!({"id": id, "result": {"session_id": session_id, "replayed": replayed, "snapshot_cursor": snapshot_cursor}}).to_string();
    (response, true)
}
