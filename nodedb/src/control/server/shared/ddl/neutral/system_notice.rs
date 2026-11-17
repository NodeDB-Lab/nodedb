// SPDX-License-Identifier: BUSL-1.1

//! Definition notices the DDL handlers send to sync sessions.
//!
//! A notice carries one DDL-managed definition: an alert rule, a retention
//! policy or a schedule. It reaches only a sync session of the definition's
//! own tenant and database whose principal is a superuser or tenant admin.
//! That is the privilege the DDL that creates and drops the definition
//! requires. The session's push-time check applies the rule, so a role
//! revoked after the session connected stops the notices.

use crate::control::state::SharedState;
use crate::event::crdt_sync::types::{DeltaAudience, DeltaOp, OutboundDelta};
use crate::types::DatabaseId;

/// Where one definition notice belongs.
pub(super) struct SystemNotice<'a> {
    pub state: &'a SharedState,
    pub database_id: DatabaseId,
    pub tenant_id: u64,
    /// The pseudo-collection the definition family syncs under.
    pub collection: &'a str,
    /// The definition's key within the family.
    pub document_id: String,
}

impl SystemNotice<'_> {
    /// Send the definition's current form.
    ///
    /// A definition that does not encode sends nothing: an empty upsert
    /// erases the definition on every receiving client.
    pub(super) fn upsert<T: zerompk::ToMessagePack>(self, definition: &T) {
        match zerompk::to_msgpack_vec(definition) {
            Ok(payload) => self.enqueue(DeltaOp::Upsert, payload),
            Err(error) => tracing::error!(
                collection = self.collection,
                document_id = %self.document_id,
                %error,
                "definition notice not sent: the definition does not encode"
            ),
        }
    }

    /// Send the definition's removal.
    pub(super) fn delete(self) {
        self.enqueue(DeltaOp::Delete, Vec::new());
    }

    fn enqueue(self, op: DeltaOp, payload: Vec<u8>) {
        let delta = OutboundDelta {
            database_id: self.database_id,
            collection: self.collection.to_string(),
            document_id: self.document_id,
            payload,
            prior_payload: None,
            audience: DeltaAudience::TenantAdmins,
            op,
            lsn: 0,
            tenant_id: self.tenant_id,
            peer_id: self.state.node_id,
            sequence: 0,
        };
        self.state.crdt_sync_delivery.enqueue(delta);
    }
}
