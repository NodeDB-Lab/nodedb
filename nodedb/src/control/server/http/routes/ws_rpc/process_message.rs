// SPDX-License-Identifier: BUSL-1.1

//! Single WebSocket message processing for the JSON-RPC protocol.

use sonic_rs;
use tracing::debug;

use crate::control::change_stream::LiveSubscriptionSet;
use crate::control::security::audit::{ArcAuditEmitter, NoopAuditEmitter};
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId};

use super::execute_sql::execute_sql;
use super::format::{
    error_response, extract_collection_from_sql, format_live_notification, ws_error_from_gateway,
};
use super::handler::save_ws_session;

/// Extract session_id from an auth request's params.
pub fn extract_session_id(req: &serde_json::Value) -> Option<String> {
    req.get("params")
        .and_then(|p| p.get("session_id"))
        .and_then(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
}

pub(super) struct MessageContext<'a> {
    pub(super) shared: &'a SharedState,
    pub(super) query_ctx: &'a crate::control::planner::context::QueryContext,
    pub(super) identity: &'a AuthenticatedIdentity,
    pub(super) database_id: DatabaseId,
    pub(super) trace_id: TraceId,
    pub(super) live_tx: &'a tokio::sync::mpsc::Sender<String>,
}

/// Process a single JSON-RPC message.
///
/// Returns `(response_json, Option<authenticated_session_id>)`.
/// The session_id is `Some` only when an "auth" request succeeds.
pub(super) async fn process_message(
    context: MessageContext<'_>,
    text: &str,
    live_set: &mut LiveSubscriptionSet,
) -> (String, Option<String>) {
    let MessageContext {
        shared,
        query_ctx,
        identity,
        database_id,
        trace_id,
        live_tx,
    } = context;
    let req: serde_json::Value = match sonic_rs::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            return (
                error_response(serde_json::Value::Null, &format!("invalid JSON: {e}")),
                None,
            );
        }
    };

    let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = req
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    match method {
        "ping" => (
            serde_json::json!({"id": id, "result": "pong"}).to_string(),
            None,
        ),

        "auth" => {
            let session_id = match extract_session_id(&req) {
                Some(sid) => sid,
                None => return (error_response(id, "missing params.session_id"), None),
            };
            let last_lsn = req
                .get("params")
                .and_then(|p| p.get("last_lsn"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            // Look up previous session's last LSN for replay.
            let replay_from_lsn = {
                let sessions = shared.ws_sessions.read().unwrap_or_else(|p| p.into_inner());
                sessions.get(&session_id).copied().unwrap_or(0)
            };

            let effective_lsn = replay_from_lsn.max(last_lsn);

            // Replay missed events from ring buffer.
            let missed = shared.change_stream.query_changes(None, 0, 10_000);
            let replay: Vec<_> = missed
                .iter()
                .filter(|event| {
                    event.lsn.as_u64() > effective_lsn
                        && event.tenant_id == identity.tenant_id
                        && authorize_collection(
                            identity,
                            database_id,
                            &event.collection,
                            Permission::Read,
                            &shared.permissions,
                            &shared.roles,
                            &NoopAuditEmitter,
                        )
                        .is_ok()
                })
                .collect();

            for event in &replay {
                let notification = format_live_notification(0, event);
                if live_tx.send(notification).await.is_err() {
                    break;
                }
            }

            // Register/update session with bounded eviction.
            save_ws_session(shared, &session_id);

            let response = serde_json::json!({
                "id": id,
                "result": {
                    "session_id": session_id,
                    "replayed": replay.len(),
                    "current_lsn": shared.change_stream.last_lsn(),
                }
            })
            .to_string();

            // Return session_id to confirm auth succeeded.
            (response, Some(session_id))
        }

        "query" => {
            let sql = req
                .get("params")
                .and_then(|p| p.get("sql"))
                .and_then(|s| s.as_str())
                .unwrap_or("");

            if sql.is_empty() {
                return (error_response(id, "missing params.sql"), None);
            }

            let response =
                match execute_sql(shared, query_ctx, identity, database_id, sql, trace_id).await {
                    Ok(result) => serde_json::json!({"id": id, "result": result}).to_string(),
                    Err(e) => ws_error_from_gateway(&id, &e),
                };
            (response, None)
        }

        "live" => {
            let sql = req
                .get("params")
                .and_then(|p| p.get("sql"))
                .and_then(|s| s.as_str())
                .unwrap_or("");

            let collection = extract_collection_from_sql(sql);

            if collection.is_empty() {
                return (
                    error_response(id, "missing collection in LIVE SELECT"),
                    None,
                );
            }

            let emitter = ArcAuditEmitter(std::sync::Arc::clone(&shared.audit));
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
                    None,
                );
            }

            let mut sub = shared
                .change_stream
                .subscribe(Some(collection.clone()), Some(identity.tenant_id));
            let sub_id = sub.id;
            let live_tx = live_tx.clone();

            // The forwarder owns the `Subscription`; aborting the task on
            // disconnect drops the Subscription, whose `Drop` decrements
            // `active_subscriptions`. No detached leak.
            live_set.spawn_task(async move {
                while let Ok(event) = sub.recv_filtered().await {
                    let notification = format_live_notification(sub_id, &event);
                    if let Err(e) = live_tx.send(notification).await {
                        debug!(sub_id, "live subscription channel closed: {e}");
                        break;
                    }
                }
            });

            let response = serde_json::json!({
                "id": id,
                "result": {
                    "subscription_id": sub_id,
                    "collection": collection,
                    "status": "active"
                }
            })
            .to_string();
            (response, None)
        }

        _ => (
            error_response(id, &format!("unknown method: {method}")),
            None,
        ),
    }
}
