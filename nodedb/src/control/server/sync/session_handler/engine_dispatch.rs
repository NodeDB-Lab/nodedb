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
        if let Some(mut msg) = $frame.decode_body::<$msg_ty>() {
            $session.normalize_collection(&mut msg.collection);
            let ack = if let Some(shared) = $shared.as_ref() {
                $session.$method(&msg, &$make_shared(shared)).await
            } else {
                $session.$method(&msg, &$noop).await
            };
            if ordered_outcome_requires_close($frame.msg_type, ack.is_some()) {
                return EngineOutcome::Break;
            }
            if let Some(ack) = ack
                && $ws
                    .send(Message::Binary(ack.to_bytes().into()))
                    .await
                    .is_err()
            {
                return EngineOutcome::Break;
            }
        } else if ordered_outcome_requires_close($frame.msg_type, false) {
            return EngineOutcome::Break;
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

/// Ordered engines reconnect when the core outcome is unknown.
fn ordered_outcome_requires_close(kind: SyncMessageType, has_ack: bool) -> bool {
    !has_ack
        && matches!(
            kind,
            SyncMessageType::ColumnarInsert
                | SyncMessageType::TimeseriesPush
                | SyncMessageType::KvPush
                | SyncMessageType::VectorInsert
                | SyncMessageType::VectorDelete
                | SyncMessageType::FtsIndex
                | SyncMessageType::FtsDelete
                | SyncMessageType::SpatialInsert
                | SyncMessageType::SpatialDelete
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_engines_close_without_an_ack_and_keep_valid_outcomes() {
        for kind in [
            SyncMessageType::ColumnarInsert,
            SyncMessageType::TimeseriesPush,
            SyncMessageType::KvPush,
            SyncMessageType::VectorInsert,
            SyncMessageType::VectorDelete,
            SyncMessageType::FtsIndex,
            SyncMessageType::FtsDelete,
            SyncMessageType::SpatialInsert,
            SyncMessageType::SpatialDelete,
        ] {
            assert!(ordered_outcome_requires_close(kind, false));
            assert!(!ordered_outcome_requires_close(kind, true));
        }
        assert!(!ordered_outcome_requires_close(
            SyncMessageType::PingPong,
            false
        ));
    }

    #[tokio::test]
    async fn malformed_ordered_frames_break_the_session_dispatch_path() {
        use super::super::super::wire::SyncFrame;
        use tokio::net::TcpListener;
        use tokio_tungstenite::tungstenite::protocol::Role;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, server) = tokio::join!(TcpStream::connect(address), listener.accept());
        let _client = client.unwrap();
        let (server, _) = server.unwrap();
        let mut ws = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let mut session = SyncSession::new("fts-outcomes".to_owned());
        session.authenticated = true;
        for kind in [
            SyncMessageType::ColumnarInsert,
            SyncMessageType::TimeseriesPush,
            SyncMessageType::KvPush,
            SyncMessageType::FtsIndex,
            SyncMessageType::FtsDelete,
            SyncMessageType::SpatialInsert,
            SyncMessageType::SpatialDelete,
        ] {
            let frame = SyncFrame::try_encode(kind, &"invalid body").unwrap();
            assert!(matches!(
                dispatch_engine_frame(&mut ws, &mut session, &frame, &None).await,
                EngineOutcome::Break
            ));
        }
        let columnar = ColumnarInsertMsg {
            lite_id: "lite".to_owned(),
            collection: "metrics".to_owned(),
            rows: vec![zerompk::to_msgpack_vec(&vec![nodedb_types::Value::Integer(1)]).unwrap()],
            schema_bytes: vec![],
            batch_id: 1,
            producer_id: 1,
            epoch: 1,
            seq: 1,
        };
        let frame = SyncFrame::try_encode(SyncMessageType::ColumnarInsert, &columnar).unwrap();
        assert!(matches!(
            dispatch_engine_frame(&mut ws, &mut session, &frame, &None).await,
            EngineOutcome::Break
        ));
        let kv = KvPushMsg {
            lite_id: "lite".to_owned(),
            collection: "cfg".to_owned(),
            key: b"key".to_vec(),
            op: nodedb_types::sync::wire::KvPushOp::Delete,
            batch_id: 1,
            producer_id: 1,
            epoch: 1,
            seq: 1,
        };
        let frame = SyncFrame::try_encode(SyncMessageType::KvPush, &kv).unwrap();
        assert!(matches!(
            dispatch_engine_frame(&mut ws, &mut session, &frame, &None).await,
            EngineOutcome::Break
        ));
        let index = FtsIndexMsg {
            lite_id: "lite".to_owned(),
            collection: "articles".to_owned(),
            doc_id: "a".to_owned(),
            fields: vec![],
            batch_id: 1,
            producer_id: 1,
            epoch: 1,
            seq: 1,
        };
        let frame = SyncFrame::try_encode(SyncMessageType::FtsIndex, &index).unwrap();
        assert!(matches!(
            dispatch_engine_frame(&mut ws, &mut session, &frame, &None).await,
            EngineOutcome::Break
        ));
        let delete = FtsDeleteMsg {
            lite_id: "lite".to_owned(),
            collection: "articles".to_owned(),
            doc_id: "a".to_owned(),
            batch_id: 1,
            producer_id: 1,
            epoch: 1,
            seq: 1,
        };
        let frame = SyncFrame::try_encode(SyncMessageType::FtsDelete, &delete).unwrap();
        assert!(matches!(
            dispatch_engine_frame(&mut ws, &mut session, &frame, &None).await,
            EngineOutcome::Break
        ));
        let spatial = SpatialInsertMsg {
            lite_id: "lite".to_owned(),
            collection: "places".to_owned(),
            field: "loc".to_owned(),
            doc_id: "a".to_owned(),
            geometry_bytes: zerompk::to_msgpack_vec(&nodedb_types::geometry::SridGeometry::wgs84(
                nodedb_types::geometry::Geometry::point(1.0, 2.0),
            ))
            .unwrap(),
            batch_id: 1,
            producer_id: 1,
            epoch: 1,
            seq: 1,
        };
        let frame = SyncFrame::try_encode(SyncMessageType::SpatialInsert, &spatial).unwrap();
        assert!(matches!(
            dispatch_engine_frame(&mut ws, &mut session, &frame, &None).await,
            EngineOutcome::Break
        ));
        let spatial = SpatialDeleteMsg {
            lite_id: "lite".to_owned(),
            collection: "places".to_owned(),
            field: "loc".to_owned(),
            doc_id: "a".to_owned(),
            batch_id: 1,
            producer_id: 1,
            epoch: 1,
            seq: 1,
        };
        let frame = SyncFrame::try_encode(SyncMessageType::SpatialDelete, &spatial).unwrap();
        assert!(matches!(
            dispatch_engine_frame(&mut ws, &mut session, &frame, &None).await,
            EngineOutcome::Break
        ));
    }
}
