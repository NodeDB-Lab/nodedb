// SPDX-License-Identifier: BUSL-1.1

//! Read authorization for shape presence channels.
//!
//! A `shape:{collection}` presence channel shows who works on a collection,
//! so joining it needs the read grant the shape needs. A session joins one
//! only through an authorized subscribe or a presence update under the grant,
//! and leaves it at the next wake after the grant is gone.
//!
//! A shape channel has one name per collection: `shape:` and the bare
//! collection name, as the subscribe path joins it. A presence update names
//! the collection as any request does, bare or with its own database's
//! qualifier, in any case. The update joins the canonical channel, so every
//! spelling of a collection reaches one channel, and the leave after a
//! REVOKE removes the session from it. Every other channel name is a
//! client-chosen label and passes as sent.

use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;

use super::push_gate::PushGate;
use super::register::may_read;

/// Prefix of the presence channel a shape subscription joins.
const SHAPE_CHANNEL_PREFIX: &str = "shape:";

/// The presence channel of shapes on `collection`.
pub(super) fn shape_channel(collection: &str) -> String {
    format!("{SHAPE_CHANNEL_PREFIX}{collection}")
}

/// What a presence channel name a client sent stands for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ChannelName {
    /// A client-chosen label. It passes as sent.
    Label,
    /// The shape channel of `collection`, a bare name in the session
    /// database.
    Shape { collection: String },
    /// A shape channel naming no collection of the session database.
    Invalid,
}

/// Classify `channel`, sent by a client of `database_id`.
pub(super) fn classify_channel(
    database_id: crate::types::DatabaseId,
    channel: &str,
) -> ChannelName {
    let Some(name) = channel.strip_prefix(SHAPE_CHANNEL_PREFIX) else {
        return ChannelName::Label;
    };
    match crate::control::server::shared::request_collection::request_collection(database_id, name)
    {
        Ok(collection) => ChannelName::Shape { collection },
        Err(_) => ChannelName::Invalid,
    }
}

/// The channel a presence update for `channel` goes to, or `None` when it
/// is refused.
///
/// A shape channel needs a read grant on its collection, and the update goes
/// to its canonical name. Every other channel name passes as sent.
pub(super) fn update_channel(
    session: &mut SyncSession,
    shared: &SharedState,
    channel: &str,
) -> Option<String> {
    match classify_channel(session.database_id(), channel) {
        ChannelName::Label => Some(channel.to_string()),
        ChannelName::Invalid => None,
        ChannelName::Shape { collection } => {
            if !may_read(session, shared, &collection) {
                return None;
            }
            let canonical = shape_channel(&collection);
            session.presence_shapes.insert(collection);
            Some(canonical)
        }
    }
}

/// Whether a queued presence frame of `channel` still goes to the session.
///
/// A frame of a shape channel goes out only while the session is a member:
/// a frame queued before the session left the channel stays on the server.
/// A label channel's frame always goes out.
pub(super) fn still_member(session: &SyncSession, channel: &str) -> bool {
    match channel.strip_prefix(SHAPE_CHANNEL_PREFIX) {
        Some(collection) => session.presence_shapes.contains(collection),
        None => true,
    }
}

/// Leave every shape presence channel whose collection the principal no
/// longer holds a read grant on, as of this drain's gate.
pub(super) async fn leave_unreadable_channels(
    gate: &mut PushGate,
    session: &mut SyncSession,
    shared: &SharedState,
) {
    let lost: Vec<String> = session
        .presence_shapes
        .iter()
        .filter(|collection| !gate.read_granted(shared, collection))
        .cloned()
        .collect();
    if lost.is_empty() {
        return;
    }
    let Some(tenant_id) = session.tenant_id else {
        return;
    };
    let database_id = session.database_id();
    let mut manager = shared.presence.write().await;
    let mut leaves = Vec::with_capacity(lost.len());
    for collection in &lost {
        leaves.push(manager.leave_channel(
            &session.session_id,
            tenant_id,
            database_id,
            &shape_channel(collection),
        ));
        session.presence_shapes.remove(collection);
    }
    let senders = manager.senders().clone();
    drop(manager);
    for leave in leaves {
        leave.send_all(&senders);
    }
}

#[cfg(test)]
mod tests {
    use crate::types::DatabaseId;

    use super::*;

    #[test]
    fn every_spelling_of_a_collection_names_one_shape_channel() {
        let db = DatabaseId::new(7);
        for sent in [
            "shape:orders",
            "shape:Orders",
            "shape:7/orders",
            "shape:7/ORDERS",
        ] {
            let ChannelName::Shape { collection } = classify_channel(db, sent) else {
                panic!("{sent} is a shape channel");
            };
            assert_eq!(shape_channel(&collection), "shape:orders", "{sent}");
        }
    }

    #[test]
    fn a_foreign_or_malformed_shape_channel_is_invalid() {
        let db = DatabaseId::new(7);
        assert_eq!(classify_channel(db, "shape:8/orders"), ChannelName::Invalid);
        assert_eq!(classify_channel(db, "shape:a/b"), ChannelName::Invalid);
        assert_eq!(classify_channel(db, "shape:"), ChannelName::Invalid);
    }

    #[test]
    fn a_label_channel_passes_as_sent() {
        assert_eq!(
            classify_channel(DatabaseId::DEFAULT, "doc:Orders/1"),
            ChannelName::Label
        );
    }
}
