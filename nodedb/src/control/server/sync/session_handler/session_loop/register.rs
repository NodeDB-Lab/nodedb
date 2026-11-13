// SPDX-License-Identifier: BUSL-1.1

//! Post-handshake registration of a session's server-push channels.

use std::sync::Arc;

use crate::control::security::audit::NoopAuditEmitter;
use crate::control::security::identity::Permission;
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;

use super::channels::SessionChannels;

/// Register the session's delivery channels once it has authenticated.
///
/// Every registration is gated on `session.authenticated`: an unauthenticated
/// socket must not be wired into any server-push fanout.
pub(super) async fn register_channels(
    session: &SyncSession,
    channels: &mut SessionChannels,
    shared: &Option<Arc<SharedState>>,
    session_id: &str,
) {
    if !session.authenticated {
        return;
    }
    let Some(shared) = shared.as_ref() else {
        return;
    };

    // Every fanout below is registered under the session's authenticated
    // database, so a server push is only ever routed to sessions holding that
    // database — the session ID alone does not scope delivery.
    let database_id = session.database_id();
    let tenant_id = session.tenant_id.map_or(0, |t| t.as_u64());

    if !channels.crdt_registered {
        let peer_id = session.device_metadata.peer_id;
        let config = crate::event::crdt_sync::types::DeliveryConfig::default();
        // The registration starts empty and so receives nothing. Shapes enter
        // it as their subscriptions are authorized.
        let (drx, crx) = shared.crdt_sync_delivery.register(
            session_id.to_string(),
            peer_id,
            tenant_id,
            database_id,
            &config,
        );
        hold_handshake_collections(session, shared, session_id);
        channels.crdt_delivery.attach(drx);
        channels.crdt_control.attach(crx);
        channels.crdt_registered = true;
    }

    // Definition notices reach only a superuser or tenant admin, so only
    // such a session registers their queue. A principal promoted later
    // registers at its next wake. One demoted later keeps the queue, and the
    // push-time check drops what arrives on it.
    if channels.crdt_registered
        && !channels.admin_notices_registered
        && is_tenant_admin(session)
        && let Some(rx) = shared.crdt_sync_delivery.register_admin_notices(session_id)
    {
        channels.admin_notices.attach(rx);
        channels.admin_notices_registered = true;
    }

    if !channels.array_delivery_registered {
        channels
            .array_delivery
            .attach(shared.array_delivery.register(session_id.to_string()));
        channels.array_delivery_registered = true;
    }

    if !channels.definition_sync_registered {
        channels
            .definition_sync
            .attach(shared.definition_sync_fanout.register(
                session_id.to_string(),
                tenant_id,
                database_id,
            ));
        channels.definition_sync_registered = true;
    }

    if !channels.presence_registered {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        shared.presence.write().await.register_session(
            session_id.to_string(),
            crate::control::server::sync::presence::SessionSender::new(tx),
        );
        channels.presence.attach(rx);
        channels.presence_registered = true;
    }
}

/// Whether the session's principal is a superuser or tenant admin, as of its
/// last refresh.
pub(super) fn is_tenant_admin(session: &SyncSession) -> bool {
    session.identity.as_ref().is_some_and(|identity| {
        identity.is_superuser
            || identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    })
}

/// Whether the session's principal holds a read grant on `collection` in
/// the session database. No audit row: a refusal here sends nothing.
pub(super) fn may_read(session: &SyncSession, shared: &SharedState, collection: &str) -> bool {
    let Some(identity) = session.identity.as_ref() else {
        return false;
    };
    authorize_collection(
        identity,
        session.database_id(),
        collection,
        Permission::Read,
        &shared.permissions,
        &shared.roles,
        &NoopAuditEmitter,
    )
    .is_ok()
}

/// Record the collections the client's handshake vector clock names, so a
/// purge of one reaches it.
///
/// The vector clock is client-supplied, so a collection is held only under a
/// read grant on it. A name that is not valid in the session database, or
/// that the principal holds no read grant on, is not held.
fn hold_handshake_collections(session: &SyncSession, shared: &SharedState, session_id: &str) {
    let database_id = session.database_id();
    for name in session.client_clock.keys() {
        let Ok(collection) = crate::control::server::shared::request_collection::request_collection(
            database_id,
            name,
        ) else {
            continue;
        };
        if may_read(session, shared, &collection) {
            shared
                .crdt_sync_delivery
                .hold_collection(session_id, &collection);
        }
    }
}
