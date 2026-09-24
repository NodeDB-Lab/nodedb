// SPDX-License-Identifier: BUSL-1.1

//! Binding dead-letter entries to the records that produced them, and
//! restoring entries read from durable storage.
//!
//! A rejected delta leaves one entry per record. The applier binds the entry
//! to the record's log position and stores it. Replaying the same record
//! binds to the same position, so the queue keeps one entry for it.

use nodedb_crdt::DeadLetter;

use super::core::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Bind the entry the latest validated apply enqueued to the record at
    /// `source_lsn`.
    ///
    /// Returns the entry to store. Returns `None` when that apply enqueued
    /// nothing, or when an entry for the record already exists.
    pub fn bind_dead_letter_source(&mut self, source_lsn: u64) -> Option<DeadLetter> {
        let id = self.last_dead_letter.take()?;
        self.validator
            .dlq_mut()
            .bind_source(id, source_lsn)
            .cloned()
    }

    /// Put back entries read from durable storage. An entry the queue cannot
    /// hold is logged and skipped: storage keeps it.
    pub fn restore_dead_letters(&mut self, entries: Vec<DeadLetter>) {
        for entry in entries {
            let source_lsn = entry.source_lsn;
            if let Err(error) = self.validator.dlq_mut().restore(entry) {
                tracing::warn!(
                    tenant = self.tenant_id.as_u64(),
                    ?source_lsn,
                    %error,
                    "crdt: a stored dead-letter entry does not fit the queue"
                );
            }
        }
    }

    /// Every pending dead-letter entry, oldest first.
    pub fn dead_letters(&self) -> impl Iterator<Item = &DeadLetter> {
        self.validator.dlq().iter()
    }
}
