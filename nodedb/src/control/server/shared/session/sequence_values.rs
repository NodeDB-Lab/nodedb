// SPDX-License-Identifier: BUSL-1.1

//! Session access to the per-connection `currval` map.

use std::sync::Arc;

use crate::control::sequence::SessionSequenceValues;

use super::connection::SessionId;
use super::store::SessionStore;

impl SessionStore {
    /// The `currval` map of one connection. `None` when no session is
    /// registered under `id` — a caller with no session gets no `currval`.
    pub fn sequence_values(&self, id: impl Into<SessionId>) -> Option<Arc<SessionSequenceValues>> {
        self.read_session(id, |session| Arc::clone(&session.sequence_values))
    }
}
