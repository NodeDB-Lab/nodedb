// SPDX-License-Identifier: BUSL-1.1

//! Inbound frame routing for a sync session.

use std::sync::Arc;

use tokio_tungstenite::WebSocketStream;

use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;

use super::super::array::{build_array_inbound, dispatch_array_frame, is_array_frame};
use super::super::engine_dispatch::{EngineOutcome, dispatch_engine_frame};
use super::channels::{Flow, SessionChannels};
use crate::control::server::sync::wire::{
    DeltaPushMsg, PresenceUpdateMsg, SyncFrame, SyncMessageType,
};

type Ws = WebSocketStream<tokio::net::TcpStream>;

/// Everything the inbound router needs that is not the frame itself.
pub(super) struct InboundCtx<'a> {
    pub session: &'a mut SyncSession,
    pub channels: &'a mut SessionChannels,
    pub shared: &'a Option<Arc<SharedState>>,
    pub session_id: &'a str,
}

async fn send(ws: &mut Ws, session: &SyncSession, frame: &SyncFrame) -> bool {
    super::send::send_frame(ws, session, frame).await
}

/// Route one decoded binary frame.
pub(super) async fn handle_frame(ws: &mut Ws, ctx: InboundCtx<'_>, frame: &SyncFrame) -> Flow {
    let InboundCtx {
        session,
        channels,
        shared,
        session_id,
    } = ctx;

    // Shape frames read collection data, so they require an authenticated
    // session exactly as the engine and CRDT frames below do.
    // `handle_shape_subscribe_async` refuses without an identity too, but the
    // gate keeps unauthenticated traffic from reaching dispatch at all.
    if matches!(
        frame.msg_type,
        SyncMessageType::ResyncRequest | SyncMessageType::ShapeSubscribe
    ) {
        if !session.authenticated {
            return Flow::Continue;
        }
        let Some(shared) = shared.as_ref() else {
            return Flow::Continue;
        };
        return match frame.msg_type {
            SyncMessageType::ResyncRequest => handle_resync(ws, session, shared, frame).await,
            _ => handle_shape_subscribe(ws, session, channels, shared, session_id, frame).await,
        };
    }

    if frame.msg_type == SyncMessageType::PresenceUpdate
        && session.authenticated
        && let Some(shared) = shared.as_ref()
    {
        // A shape presence channel shows who works on a collection, so an
        // update on one needs the collection's read grant. The update goes
        // to the channel's canonical name, the one the leave after a REVOKE
        // removes.
        if let Some(mut msg) = frame.decode_body::<PresenceUpdateMsg>()
            && let Some(tenant_id) = session.tenant_id
            && let Some(channel) =
                super::presence_gate::update_channel(session, shared, &msg.channel)
        {
            msg.channel = channel;
            // Presence channels are keyed by the authenticated tenant and
            // database, so peers in one database never observe another's
            // presence under the same channel name.
            let database_id = session.database_id();
            let user_id = session.username.as_deref().unwrap_or("anonymous");
            let mut mgr = shared.presence.write().await;
            let outbound = mgr.handle_update(session_id, user_id, tenant_id, database_id, &msg);
            let senders = mgr.senders().clone();
            drop(mgr);
            outbound.send_all(&senders);
        }
        return Flow::Continue;
    }

    match dispatch_engine_frame(ws, session, frame, shared).await {
        EngineOutcome::Break => return Flow::Break,
        EngineOutcome::Handled => return Flow::Continue,
        EngineOutcome::NotEngine => {}
    }

    if is_array_frame(frame.msg_type) {
        return handle_array(ws, session, channels, shared, frame).await;
    }

    // A handshake is an authentication boundary. Every server-push
    // registration made under the previous identity (delivery scope, shapes,
    // presence, array engine) is released before it is processed, and the
    // session loop registers again under the new identity once it succeeds.
    if frame.msg_type == SyncMessageType::Handshake {
        reset_session_bindings(channels, shared, session_id).await;
    }

    handle_crdt(ws, session, shared, frame).await
}

/// Release every server-push registration of the session: delivery scope,
/// shapes, presence and the array engine. The session loop registers again
/// under the session's identity as it then stands.
pub(super) async fn reset_session_bindings(
    channels: &mut SessionChannels,
    shared: &Option<Arc<SharedState>>,
    session_id: &str,
) {
    if let Some(shared) = shared.as_ref() {
        crate::control::server::sync::listener::unregister_sync_session(shared, session_id).await;
    }
    *channels = SessionChannels::default();
}

async fn handle_resync(
    ws: &mut Ws,
    session: &mut SyncSession,
    shared: &Arc<SharedState>,
    frame: &SyncFrame,
) -> Flow {
    // A resync request costs the snapshot read a subscribe costs, so it
    // draws from the same bucket. A refused one gets a retryable reject.
    if let Err(retry_after_ms) = session.snapshot_limiter.try_acquire() {
        let shape_id = frame
            .decode_body::<nodedb_types::sync::wire::ResyncRequestMsg>()
            .map(|msg| msg.shape_id)
            .unwrap_or_default();
        return send_rate_limited(ws, session, shape_id, retry_after_ms).await;
    }
    let response = crate::control::server::sync::async_dispatch::handle_resync_request_async(
        shared, session, frame,
    )
    .await;
    match response {
        Some(r) => Flow::from_send(send(ws, session, &r).await),
        None => Flow::Continue,
    }
}

/// Answer a snapshot read the session's snapshot bucket refused.
async fn send_rate_limited(
    ws: &mut Ws,
    session: &SyncSession,
    shape_id: String,
    retry_after_ms: u64,
) -> Flow {
    let reject = nodedb_types::sync::wire::ShapeSnapshotRejectMsg {
        shape_id,
        refusal: nodedb_types::sync::wire::ShapeSnapshotRefusal::RateLimited,
        detail: format!("snapshot reads are rate limited; retry in {retry_after_ms} ms"),
    };
    match SyncFrame::try_encode(SyncMessageType::ShapeSnapshotReject, &reject) {
        Some(frame) => Flow::from_send(send(ws, session, &frame).await),
        None => Flow::Continue,
    }
}

async fn handle_shape_subscribe(
    ws: &mut Ws,
    session: &mut SyncSession,
    channels: &SessionChannels,
    shared: &Arc<SharedState>,
    session_id: &str,
    frame: &SyncFrame,
) -> Flow {
    // Decoded once, for the rate-limit refusal, the presence join and the
    // schema announce below.
    let shape_sub_msg =
        frame.decode_body::<crate::control::server::sync::wire::ShapeSubscribeMsg>();

    // Each subscribe costs a snapshot read, so a session sends them at a
    // bounded rate. A refused one gets a retryable reject.
    if let Err(retry_after_ms) = session.snapshot_limiter.try_acquire() {
        let shape_id = shape_sub_msg
            .as_ref()
            .map(|msg| msg.shape.shape_id.clone())
            .unwrap_or_default();
        return send_rate_limited(ws, session, shape_id, retry_after_ms).await;
    }

    let Some(response) =
        crate::control::server::sync::async_dispatch::handle_shape_subscribe_async(
            shared, session, frame,
        )
        .await
    else {
        return Flow::Continue;
    };

    // Only a snapshot means the subscription was authorized and registered.
    // A reject joins no presence channel and announces no schema.
    if response.msg_type != SyncMessageType::ShapeSnapshot {
        return Flow::from_send(send(ws, session, &response).await);
    }

    let database_id = session.database_id();
    let collection = shape_sub_msg
        .as_ref()
        .and_then(|msg| msg.shape.collection())
        .and_then(|name| {
            crate::control::server::shared::request_collection::request_collection(
                database_id,
                name,
            )
            .ok()
        });
    if let Some(coll) = collection {
        if channels.presence_registered
            && let Some(tenant) = session.tenant_id
        {
            shared.presence.write().await.subscribe_to_channel(
                session_id,
                tenant,
                database_id,
                &super::presence_gate::shape_channel(&coll),
            );
            session.presence_shapes.insert(coll.clone());
        }

        // Announce the collection descriptor before the shape snapshot so
        // schema strictly precedes data on the subscription path. Idempotent
        // per session; skips shape variants that carry no single collection.
        let tenant_id = session.tenant_id.map_or(0, |t| t.as_u64());
        if let Some(schema_frame) = super::super::announce::build_collection_schema_frame(
            shared,
            session,
            tenant_id,
            database_id,
            &coll,
        ) {
            if !send(ws, session, &schema_frame).await {
                return Flow::Break;
            }
            session.announced_collections.insert(coll);
        }
    }

    Flow::from_send(send(ws, session, &response).await)
}

async fn handle_array(
    ws: &mut Ws,
    session: &SyncSession,
    channels: &mut SessionChannels,
    shared: &Option<Arc<SharedState>>,
    frame: &SyncFrame,
) -> Flow {
    // Refuse before any engine is constructed or reused, so an unauthenticated
    // frame cannot touch catalog, data, or fan-out state. The rejection is
    // explicit rather than a silent drop: a client whose handshake never landed
    // otherwise waits on a reply that is never coming.
    if let Some(reject) = super::super::array::unauthenticated_array_reject(
        frame,
        session.authenticated,
        session.identity.is_some(),
    ) {
        return Flow::from_send(send(ws, session, &reject).await);
    }

    // Bind the inbound array engine to the session's authenticated tenant,
    // lazily, on first use. The gate is the tenant itself (not `authenticated`):
    // the handshake sets `tenant_id = Some(..)` in the same step it marks the
    // session authenticated, so a present tenant IS proof of authentication —
    // and there is no placeholder-tenant fallback that could misroute writes
    // under tenant 0.
    if channels.array_inbound.is_none()
        && let Some(identity) = session.identity.clone()
    {
        channels.array_inbound = build_array_inbound(shared, identity);
    }
    if let Some(inbound) = &channels.array_inbound {
        // Stamp the session's handshake-assigned identity so inbound array
        // provenance is server-authoritative.
        inbound.set_session_identity(session.producer_id, session.accepted_epoch);
        if let Some(f) = dispatch_array_frame(frame, inbound, session).await
            && !send(ws, session, &f).await
        {
            return Flow::Break;
        }
    }
    Flow::Continue
}

async fn handle_crdt(
    ws: &mut Ws,
    session: &mut SyncSession,
    shared: &Option<Arc<SharedState>>,
    frame: &SyncFrame,
) -> Flow {
    // Decode the CRDT message once for authorization and final dispatch.
    let mut delta_msg = if frame.msg_type == SyncMessageType::DeltaPush {
        frame.decode_body::<DeltaPushMsg>()
    } else {
        None
    };

    // A foreign or malformed collection name is refused before authorization.
    if let Some(delta_msg) = delta_msg.as_mut()
        && let Some(refusal) = session.refuse_delta_collection(delta_msg)
    {
        if !send(ws, session, &refusal).await {
            return Flow::Break;
        }
        return Flow::Continue;
    }

    if let Some(delta_msg) = delta_msg.as_ref() {
        let authorized = shared.as_ref().is_some_and(|shared| {
            crate::control::server::sync::async_dispatch::authorize_delta_write(
                shared,
                session.identity.as_ref(),
                &delta_msg.collection,
            )
            .is_ok()
        });
        if !authorized {
            // Never run the generic handler without authorization: it mutates
            // session accounting before its provisional ACK.
            if let Some(reject) =
                crate::control::server::sync::async_dispatch::permission_denied_delta_reject(
                    delta_msg,
                )
                && !send(ws, session, &reject).await
            {
                return Flow::Break;
            }
            return Flow::Continue;
        }
    }

    // The audit log and DLQ are deliberately NOT locked here. With `shared`
    // present, `process_frame` authorizes the delta first and only then takes
    // those two mutexes, scoped to the synchronous call. Locking them out here
    // as well deadlocks that re-acquisition — they are plain non-reentrant
    // mutexes — and every delta push hangs until the client's frame timeout.
    // The `shared`-absent path has no store to lock.
    let response = if let Some(shared) = shared.as_ref() {
        session
            .process_frame(frame, Some(&shared.rls), None, None, Some(shared))
            .await
    } else {
        session.process_frame(frame, None, None, None, None).await
    };

    let Some(response) = response else {
        return Flow::Continue;
    };

    // The purge catch-up goes ahead of the ack that carries the new
    // watermark, and ahead of every shape snapshot and push. A client that
    // dies before the ack names its old watermark again and gets the same
    // frames.
    if response.msg_type == SyncMessageType::HandshakeAck {
        for catch_up in std::mem::take(&mut session.purge_catch_up) {
            if !send(ws, session, &catch_up).await {
                return Flow::Break;
            }
        }
    }

    // An admitted delta proves the client stores the collection. A purge of
    // it reaches the session only under a read grant too: a write grant alone
    // reads nothing.
    if response.msg_type == SyncMessageType::DeltaAck
        && let Some(shared) = shared.as_ref()
        && let Some(delta_msg) = delta_msg.as_ref()
        && super::register::may_read(session, shared, &delta_msg.collection)
    {
        shared
            .crdt_sync_delivery
            .hold_collection(&session.session_id, &delta_msg.collection);
    }

    let dispatched = if response.msg_type == SyncMessageType::DeltaAck
        && let Some(shared) = shared.as_ref()
        && let Some(delta_msg) = delta_msg.as_ref()
    {
        crate::control::server::sync::async_dispatch::apply_delta_and_finalize(
            shared,
            delta_msg,
            response,
            crate::control::server::sync::async_dispatch::DeltaSessionContext {
                identity: session.identity.as_ref(),
                signing_key: session.delta_signing_key.as_ref(),
                producer_id: session.producer_id,
                epoch: session.accepted_epoch,
                peer_addr: &session.device_metadata.remote_addr,
            },
        )
        .await
    } else {
        crate::control::server::sync::async_dispatch::DeltaDispatchOutcome {
            frame: Some(response),
            trimmed_ops: 0,
        }
    };

    // What the delta carried is recorded whether or not it produced a frame: a
    // dropped frame is exactly the case where the session would otherwise close
    // with no record that anything was discarded.
    if delta_msg.is_some() {
        session.record_delta_admission(dispatched.trimmed_ops);
    }

    let Some(r) = dispatched.frame else {
        return Flow::Continue;
    };

    // Account for the terminal outcome before the frame leaves: refusals
    // decided downstream of the session's provisional ack are invisible to the
    // session otherwise, which is what let a lossy session close reporting zero
    // rejections.
    if delta_msg.is_some() {
        session.record_delta_outcome(&r);
    }
    Flow::from_send(send(ws, session, &r).await)
}
