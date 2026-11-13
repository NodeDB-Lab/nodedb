// SPDX-License-Identifier: BUSL-1.1

//! Session notices that are not row deltas: purge control frames and the
//! definition-notice queue of admin sessions.

use tokio::sync::mpsc;
use tracing::{info, warn};

use super::delivery::CrdtSyncDelivery;
use super::types::{ControlFrame, OutboundDelta};
use crate::types::DatabaseId;

/// The most definition notices one admin session queues. Notices follow
/// DDL, so a session that falls this far behind gets the full current set
/// of each family it missed a notice of.
pub(super) const ADMIN_NOTICE_QUEUE: usize = 256;

impl CrdtSyncDelivery {
    /// Register the session's definition-notice queue. The caller has
    /// checked that the principal is a superuser or tenant admin. `None`
    /// when the session has no delivery registration.
    ///
    /// The registration lasts until the session registers or unregisters
    /// again. A later loss of the privilege leaves it in place, and the
    /// session's push-time check drops the notices.
    pub fn register_admin_notices(
        &self,
        session_id: &str,
    ) -> Option<mpsc::Receiver<OutboundDelta>> {
        let mut sessions = self.sessions_write();
        let handle = sessions.get_mut(session_id)?;
        let (tx, rx) = mpsc::channel(ADMIN_NOTICE_QUEUE);
        handle.admin_sender = Some(tx);
        Some(rx)
    }

    /// Take the definition families whose notices the session's full admin
    /// queue dropped. The caller sends each family's full current set.
    pub fn take_admin_resync(&self, session_id: &str) -> Vec<String> {
        let sessions = self.sessions_read();
        let Some(handle) = sessions.get(session_id) else {
            return Vec::new();
        };
        let mut marks = handle
            .admin_resync
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        marks.drain().collect()
    }

    /// Mark a definition family for the session's admin resync again.
    pub fn mark_admin_resync(&self, session_id: &str, family: &str) {
        let sessions = self.sessions_read();
        if let Some(handle) = sessions.get(session_id) {
            super::delivery::mark(&handle.admin_resync, family);
        }
    }

    /// Broadcast a `CollectionPurged` control frame to every session of
    /// `(tenant_id, database_id)` whose client stores `collection`: by a shape
    /// or as a held collection. Runs once the purge's reclaim ran on this
    /// node, so every online Lite client that holds the collection learns
    /// about the hard-delete within one network round-trip and can drop local
    /// state. `purge_index` is the purge-log position of the purge. A client
    /// offline now, or whose full queue drops the frame, gets it from the
    /// purge log at its next handshake. The frame carries its scope and
    /// collection, and the session's push-time check admits it only while
    /// its principal stands and the session still holds the collection.
    pub fn broadcast_collection_purged(
        &self,
        tenant_id: u64,
        database_id: DatabaseId,
        collection: &str,
        purge_index: u64,
        incarnation: nodedb_types::Hlc,
    ) {
        let msg = nodedb_types::sync::wire::CollectionPurgedMsg {
            tenant_id,
            database_id,
            name: collection.to_string(),
            purge_index,
            incarnation,
        };
        let Some(frame) = nodedb_types::sync::wire::SyncFrame::new_msgpack(
            nodedb_types::sync::wire::SyncMessageType::CollectionPurged,
            &msg,
        ) else {
            warn!(
                tenant_id,
                database_id = database_id.as_u64(),
                collection,
                purge_index,
                "failed to encode CollectionPurgedMsg — skipping broadcast"
            );
            return;
        };
        let control = ControlFrame {
            tenant_id,
            database_id,
            collection: collection.to_string(),
            frame,
        };

        let sessions = self.sessions_read();
        let mut sent = 0usize;
        let mut dropped = 0usize;
        for session in sessions.values() {
            if session.tenant_id != tenant_id
                || session.database_id != database_id
                || !session.subscriptions.read().holds(collection)
            {
                continue;
            }
            match session.control_sender.try_send(control.clone()) {
                Ok(()) => sent += 1,
                Err(_) => dropped += 1,
            }
        }
        info!(
            tenant_id,
            database_id = database_id.as_u64(),
            collection,
            purge_index,
            sent,
            dropped,
            "broadcast CollectionPurged to sync sessions"
        );
    }
}
