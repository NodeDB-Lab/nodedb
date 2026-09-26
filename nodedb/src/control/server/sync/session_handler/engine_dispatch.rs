// SPDX-License-Identifier: BUSL-1.1

//! Per-engine sync-message dispatch for the session loop.
//!
//! Every engine sync message (timeseries / columnar / vector / FTS / spatial /
//! KV) follows the same shape: decode the typed body, pick the production
//! (`SharedState`) or no-op dispatcher, invoke the session handler, and forward
//! the ACK frame. [`dispatch_engine_frame`] factors that boilerplate into one
//! place so adding an engine is a single match arm.

use std::sync::Arc;

use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use super::super::session::SyncSession;
use super::super::wire::{
    ColumnarInsertMsg, FtsDeleteMsg, FtsIndexMsg, KvPushMsg, SpatialDeleteMsg, SpatialInsertMsg,
    SyncMessageType, TimeseriesPushMsg, VectorDeleteMsg, VectorInsertMsg,
};
use crate::control::state::SharedState;

/// Result of attempting to dispatch one frame as an engine sync message.
pub(super) enum EngineOutcome {
    /// Frame was an engine message and was fully handled — `continue` the loop.
    Handled,
    /// Sending the ACK failed — the session loop should `break`.
    Break,
    /// Frame was not an engine message — fall through to the generic path.
    NotEngine,
}

/// Decode one engine sync message, dispatch it, and forward the ACK.
///
/// `$make_shared` is a `|&Arc<SharedState>| -> impl EngineDispatcher` closure so
/// the production dispatcher is only constructed on the `SharedState`-present
/// path; the no-op dispatcher is used otherwise (it fails loudly rather than
/// silently dropping the write).
macro_rules! dispatch {
    ($ws:ident, $session:ident, $frame:ident, $shared:ident,
     $msg_ty:ty, $method:ident, $make_shared:expr, $noop:expr) => {{
        if let Some(msg) = $frame.decode_body::<$msg_ty>() {
            let ack = if let Some(shared) = $shared.as_ref() {
                $session.$method(&msg, &$make_shared(shared)).await
            } else {
                $session.$method(&msg, &$noop).await
            };
            if let Some(ack) = ack
                && $ws
                    .send(Message::Binary(ack.to_bytes().into()))
                    .await
                    .is_err()
            {
                return EngineOutcome::Break;
            }
        }
        return EngineOutcome::Handled;
    }};
}

pub(super) async fn dispatch_engine_frame(
    ws: &mut WebSocketStream<TcpStream>,
    session: &mut SyncSession,
    frame: &super::super::wire::SyncFrame,
    shared: &Option<Arc<SharedState>>,
) -> EngineOutcome {
    use super::super::{
        columnar_handler, fts_handler, kv_handler, spatial_handler, timeseries_handler,
        vector_handler,
    };

    let dispatcher_identity = session.identity.clone();
    let dispatcher_database = session.database_id();
    let dispatcher_peer = session.device_metadata.remote_addr.clone();

    match frame.msg_type {
        SyncMessageType::TimeseriesPush => dispatch!(
            ws,
            session,
            frame,
            shared,
            TimeseriesPushMsg,
            handle_timeseries_push,
            |s| timeseries_handler::SharedStateTimeseriesDispatcher {
                shared: s,
                identity: dispatcher_identity.as_ref(),
                database_id: dispatcher_database,
            },
            timeseries_handler::NoOpTimeseriesDispatcher
        ),
        SyncMessageType::ColumnarInsert => dispatch!(
            ws,
            session,
            frame,
            shared,
            ColumnarInsertMsg,
            handle_columnar_insert,
            |shared| {
                columnar_handler::SharedStateColumnarDispatcher::from_session(
                    shared,
                    dispatcher_identity.as_ref(),
                    dispatcher_database,
                )
            },
            columnar_handler::NoOpColumnarDispatcher
        ),
        SyncMessageType::VectorInsert => dispatch!(
            ws,
            session,
            frame,
            shared,
            VectorInsertMsg,
            handle_vector_insert,
            |s| vector_handler::SharedStateVectorDispatcher {
                shared: s,
                identity: dispatcher_identity.as_ref(),
                database_id: dispatcher_database,
            },
            vector_handler::NoOpVectorDispatcher
        ),
        SyncMessageType::VectorDelete => dispatch!(
            ws,
            session,
            frame,
            shared,
            VectorDeleteMsg,
            handle_vector_delete,
            |s| vector_handler::SharedStateVectorDispatcher {
                shared: s,
                identity: dispatcher_identity.as_ref(),
                database_id: dispatcher_database,
            },
            vector_handler::NoOpVectorDispatcher
        ),
        SyncMessageType::FtsIndex => dispatch!(
            ws,
            session,
            frame,
            shared,
            FtsIndexMsg,
            handle_fts_index,
            |s| fts_handler::SharedStateFtsDispatcher {
                shared: s,
                identity: dispatcher_identity.as_ref(),
                database_id: dispatcher_database,
            },
            fts_handler::NoOpFtsDispatcher
        ),
        SyncMessageType::FtsDelete => dispatch!(
            ws,
            session,
            frame,
            shared,
            FtsDeleteMsg,
            handle_fts_delete,
            |s| fts_handler::SharedStateFtsDispatcher {
                shared: s,
                identity: dispatcher_identity.as_ref(),
                database_id: dispatcher_database,
            },
            fts_handler::NoOpFtsDispatcher
        ),
        SyncMessageType::SpatialInsert => dispatch!(
            ws,
            session,
            frame,
            shared,
            SpatialInsertMsg,
            handle_spatial_insert,
            |s| spatial_handler::SharedStateSpatialDispatcher {
                shared: s,
                identity: dispatcher_identity.as_ref(),
                database_id: dispatcher_database,
            },
            spatial_handler::NoOpSpatialDispatcher
        ),
        SyncMessageType::SpatialDelete => dispatch!(
            ws,
            session,
            frame,
            shared,
            SpatialDeleteMsg,
            handle_spatial_delete,
            |s| spatial_handler::SharedStateSpatialDispatcher {
                shared: s,
                identity: dispatcher_identity.as_ref(),
                database_id: dispatcher_database,
            },
            spatial_handler::NoOpSpatialDispatcher
        ),
        SyncMessageType::KvPush => dispatch!(
            ws,
            session,
            frame,
            shared,
            KvPushMsg,
            handle_kv_push,
            |s| kv_handler::SharedStateKvDispatcher {
                shared: s,
                identity: dispatcher_identity.as_ref(),
                database_id: dispatcher_database,
                peer_addr: &dispatcher_peer,
            },
            kv_handler::NoOpKvDispatcher
        ),
        _ => EngineOutcome::NotEngine,
    }
}
