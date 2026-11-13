// SPDX-License-Identifier: BUSL-1.1

//! Server-push delivery: one bounded, authorized drain per loop wake.
//!
//! Every wake resolves the push gate once against the principal as it stands
//! then, and pushes at most [`MAX_PUSHES_PER_WAKE`] items from each queue.
//! The loop then returns to its wait, so a busy queue never keeps a REVOKE,
//! a role change, a session kill, the idle check or an inbound frame waiting.

use std::collections::HashMap;
use std::sync::Arc;

use tokio_tungstenite::WebSocketStream;
use tracing::{debug, warn};

use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;
use crate::event::crdt_sync::types::{DeltaAudience, OutboundDelta};

use super::channels::{Flow, SessionChannels};
use super::push_gate::{PushDecision, PushGate};
use super::send::send_bytes;

type Ws = WebSocketStream<tokio::net::TcpStream>;

/// The most items one wake pushes from each outbound queue.
pub(super) const MAX_PUSHES_PER_WAKE: usize = 256;

/// Push one bounded batch from every outbound queue.
///
/// Each push passes the gate first. A principal whose scope cannot be
/// resolved, whose address is blacklisted, or whose account is not active
/// gets nothing, and the session ends.
///
/// A session in a shape presence channel resolves the gate on every wake,
/// even with nothing queued, so it leaves the channel at the first wake
/// after its read grant is gone.
pub(super) async fn pump_deliveries(
    ws: &mut Ws,
    session: &mut SyncSession,
    channels: &mut SessionChannels,
    shared: &Option<Arc<SharedState>>,
) -> Flow {
    // Queues are registered only when `SharedState` is present and the
    // session authenticated, so without either there is nothing to push.
    let Some(state) = shared.as_ref() else {
        return Flow::Continue;
    };
    if !session.authenticated || (channels.outbound_empty() && session.presence_shapes.is_empty()) {
        return Flow::Continue;
    }
    let mut gate = match PushGate::for_session(state, session) {
        Ok(Some(gate)) => gate,
        Ok(None) => {
            warn!(
                session = %session.session_id,
                "sync: pushes suppressed; the session has no established identity"
            );
            return Flow::Break;
        }
        Err(error) => {
            warn!(
                session = %session.session_id,
                %error,
                "sync: pushes suppressed and session closed; the principal no longer passes \
                 admission"
            );
            return Flow::Break;
        }
    };

    super::presence_gate::leave_unreadable_channels(&mut gate, session, state).await;

    // A presence frame of a shape channel goes out only while the session is
    // still in the channel. One queued before the session left stays here.
    for _ in 0..MAX_PUSHES_PER_WAKE {
        let Some(frame) = channels.presence.next() else {
            break;
        };
        if !super::presence_gate::still_member(session, &frame.channel) {
            continue;
        }
        if !send_bytes(ws, session, (*frame.bytes).clone()).await {
            return Flow::Break;
        }
    }

    // An array frame carries data of one array, so it needs the read grant
    // every array read needs, as of this wake.
    for _ in 0..MAX_PUSHES_PER_WAKE {
        let Some(frame) = channels.array_delivery.next() else {
            break;
        };
        if !gate.read_granted(state, &frame.array) {
            debug!(
                session = %session.session_id,
                array = %frame.array,
                "sync: array push dropped; the principal holds no read grant on the array"
            );
            continue;
        }
        if !send_bytes(ws, session, frame.bytes).await {
            return Flow::Break;
        }
    }

    // A definition sync frame carries a function, trigger or procedure body.
    // Every principal reads those bodies, as PostgreSQL lets every role read
    // `pg_proc.prosrc`, and Lite needs them to run triggers and functions
    // offline. So a frame reaches every session of its own tenant and
    // database whose principal passes the gate above.
    for _ in 0..MAX_PUSHES_PER_WAKE {
        let Some(frame) = channels.definition_sync.next() else {
            break;
        };
        if !gate.in_scope(frame.tenant_id, frame.database_id) {
            continue;
        }
        if !send_bytes(ws, session, frame.bytes).await {
            return Flow::Break;
        }
    }

    // A control frame (`CollectionPurged`) goes out only in the principal's
    // own scope, and only while the session still holds its collection. The
    // hold was taken under a read grant. A DROP removes the grants before
    // the purge, so the grant is not checked again here.
    for _ in 0..MAX_PUSHES_PER_WAKE {
        let Some(control) = channels.crdt_control.next() else {
            break;
        };
        if !gate.in_scope(control.tenant_id, control.database_id)
            || !gate.holds(&control.collection)
        {
            continue;
        }
        if !send_bytes(ws, session, control.frame.to_bytes()).await {
            return Flow::Break;
        }
    }

    // Definition notices. The gate admits each only for a superuser or
    // tenant admin, as of this wake.
    for _ in 0..MAX_PUSHES_PER_WAKE {
        let Some((at, delta)) = channels.admin_notices.next_positioned() else {
            break;
        };
        if before_fence(&channels.admin_fences, &delta.collection, at) {
            continue;
        }
        if push_delta(ws, session, state, &mut gate, delta).await == Flow::Break {
            return Flow::Break;
        }
    }

    for _ in 0..MAX_PUSHES_PER_WAKE {
        let Some((at, delta)) = channels.crdt_delivery.next_positioned() else {
            break;
        };
        if delta.audience == DeltaAudience::Subscribers
            && before_fence(&channels.resync_fences, &delta.collection, at)
        {
            continue;
        }
        if push_delta(ws, session, state, &mut gate, delta).await == Flow::Break {
            return Flow::Break;
        }
    }

    Flow::Continue
}

/// Whether a resync already gave the client the effect of the item of
/// `collection` at queue position `at`: it was queued before the resync's
/// snapshot read began.
///
/// The cutoff is a queue position, never an LSN. An item is queued after
/// its write applied, and the snapshot read starts after the fence is read,
/// so an item before the fence is a write the read holds: a local read sees
/// every write this node applied, and a read its group's leader serves sees
/// every write committed. An item at or past the fence goes out after the
/// snapshot. Pushes of one row keep their order, so the row ends at its
/// newest write.
fn before_fence(fences: &HashMap<String, u64>, collection: &str, at: u64) -> bool {
    fences.get(collection).is_some_and(|fence| at < *fence)
}

/// Decide one delta at the gate and send what it allows.
async fn push_delta(
    ws: &mut Ws,
    session: &mut SyncSession,
    state: &SharedState,
    gate: &mut PushGate,
    mut delta: OutboundDelta,
) -> Flow {
    let op = match gate.decide(state, &mut delta) {
        PushDecision::Upsert => nodedb_types::sync::wire::RowOp::Upsert,
        PushDecision::Delete => nodedb_types::sync::wire::RowOp::Delete,
        PushDecision::Drop => return Flow::Continue,
        PushDecision::DropUnredactable => {
            warn!(
                session = %session.session_id,
                collection = %delta.collection,
                "sync: row push dropped; a row covered by a redaction policy could not \
                 be rewritten"
            );
            return Flow::Continue;
        }
    };

    // Announce the collection descriptor before its first delta so schema
    // strictly precedes data on the peer. Idempotent per session; a lookup
    // miss warns and proceeds without marking. A definition notice names a
    // pseudo-collection with no descriptor, so it announces nothing.
    if delta.audience == DeltaAudience::Subscribers
        && let Some(schema_frame) = super::super::announce::build_collection_schema_frame(
            state,
            session,
            delta.tenant_id,
            session.database_id(),
            &delta.collection,
        )
    {
        if !send_bytes(ws, session, schema_frame.to_bytes()).await {
            return Flow::Break;
        }
        session
            .announced_collections
            .insert(delta.collection.clone());
    }

    // A delete names only the document: the pre-image never leaves.
    let payload = match op {
        nodedb_types::sync::wire::RowOp::Upsert => delta.payload,
        nodedb_types::sync::wire::RowOp::Delete => Vec::new(),
    };

    // A row post-image, not a Loro delta — so this goes out as `RowPush`,
    // never `DeltaPush`. The two carry different encodings, and the peer
    // cannot tell them apart from the bytes alone.
    let push_msg = nodedb_types::sync::wire::RowPushMsg {
        collection: delta.collection,
        document_id: delta.document_id,
        payload,
        op,
        lsn: delta.lsn,
        peer_id: delta.peer_id,
        sequence: delta.sequence,
    };
    match nodedb_types::sync::wire::SyncFrame::new_msgpack(
        nodedb_types::sync::wire::SyncMessageType::RowPush,
        &push_msg,
    ) {
        Some(frame) => Flow::from_send(send_bytes(ws, session, frame.to_bytes()).await),
        None => Flow::Continue,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_queued_after_the_fence_survive_a_resync() {
        let fences = HashMap::from([("orders".to_string(), 10)]);

        assert!(before_fence(&fences, "orders", 9));
        assert!(
            !before_fence(&fences, "orders", 10),
            "queued as the read began"
        );
        assert!(!before_fence(&fences, "orders", 11));
        assert!(
            !before_fence(&fences, "users", 1),
            "another collection has no fence"
        );
    }
}
