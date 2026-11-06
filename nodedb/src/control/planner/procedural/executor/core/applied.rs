// SPDX-License-Identifier: BUSL-1.1

//! The key a body's commit records, so a body fired again applies once.
//!
//! A cross-shard receiver commits the key of the request it applies. A
//! trigger body fired from the Event Plane commits a key of its own: its
//! source event's replicated identity with the body's origin tag. Every
//! replica records the key as the commit applies. A later owner that fires
//! the same event again finds the key and does not run the body.

use crate::wal::CrossShardAppliedKey;

use super::state::StatementExecutor;

impl StatementExecutor<'_> {
    /// The key this body's commit records, with the vShard whose redo record
    /// carries it. `None` for a body that commits no key.
    pub(super) fn commit_key(&self) -> Option<(CrossShardAppliedKey, u32)> {
        if let Some(applied) = &self.applied_key {
            return Some(applied.clone());
        }
        let origin = self.cross_shard_origin.as_ref()?;
        let body = self.body.as_ref()?;
        let key = CrossShardAppliedKey {
            source_vshard: origin.source_vshard,
            source_lsn: origin.source_lsn,
            source_sequence: origin.source_sequence,
            origin: format!("{}/local", body.origin_tag(self.database_id)),
        };
        Some((key, origin.source_vshard))
    }

    /// Whether this body's commit already applied: a body fired from the
    /// Event Plane whose key an earlier firing of the same event recorded.
    pub fn already_applied(&self) -> crate::Result<bool> {
        if self.applied_key.is_some() {
            return Ok(false);
        }
        let Some((key, _)) = self.commit_key() else {
            return Ok(false);
        };
        match self.state.cross_shard.dedup.get() {
            Some(dedup) => dedup.is_applied(&key),
            None => Ok(false),
        }
    }
}
