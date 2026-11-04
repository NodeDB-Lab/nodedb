// SPDX-License-Identifier: BUSL-1.1

//! The collection name a Lite peer sends, checked against its session database.

use super::session::SyncSession;
use super::wire::{DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType};
use crate::control::server::shared::request_collection::request_collection;

impl SyncSession {
    /// The reason an engine frame naming `collection` is refused, or `None`
    /// when the session may dispatch it.
    ///
    /// An unauthenticated session is refused. So is a collection name that
    /// is empty, holds a stray `/`, or carries another database's qualifier.
    pub(super) fn entry_refusal(&self, collection: &str) -> Option<String> {
        if !self.authenticated {
            return Some("unauthenticated".to_string());
        }
        request_collection(self.database_id(), collection)
            .err()
            .map(|error| error.to_string())
    }

    /// Rewrite `collection` to the bare name of the session database.
    ///
    /// A name that `entry_refusal` refuses stays as sent.
    pub(super) fn normalize_collection(&self, collection: &mut String) {
        if let Ok(bare) = request_collection(self.database_id(), collection) {
            *collection = bare;
        }
    }

    /// Rewrite the collection of an authenticated session's delta to the bare
    /// session-database name.
    ///
    /// Returns the `DeltaReject` frame when the name is refused. An
    /// unauthenticated session returns `None`: `handle_delta_push` refuses it.
    pub(super) fn refuse_delta_collection(&mut self, msg: &mut DeltaPushMsg) -> Option<SyncFrame> {
        if !self.authenticated {
            return None;
        }
        match request_collection(self.database_id(), &msg.collection) {
            Ok(bare) => {
                msg.collection = bare;
                None
            }
            Err(error) => {
                self.mutations_rejected += 1;
                let reject = DeltaRejectMsg {
                    mutation_id: msg.mutation_id,
                    reason: error.to_string(),
                    compensation: None,
                };
                SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject)
            }
        }
    }
}
