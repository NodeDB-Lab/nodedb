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

    /// Put back entries read from durable storage.
    ///
    /// An entry the queue cannot hold is an error: every stored entry was in
    /// the queue when it was stored. The caller does not install an engine
    /// that is missing an entry storage holds.
    pub fn restore_dead_letters(&mut self, entries: Vec<DeadLetter>) -> crate::Result<()> {
        for entry in entries {
            let source_lsn = entry.source_lsn;
            if let Err(error) = self.validator.dlq_mut().restore(entry) {
                crate::diag::crdt_dead_letter_not_restored(
                    &error,
                    self.tenant_id.as_u64(),
                    source_lsn,
                );
                return Err(crate::Error::Crdt(error));
            }
        }
        Ok(())
    }

    /// Remove entry `id` from the queue. The caller removes an entry whose
    /// store failed, so the queue holds no entry that storage lacks.
    pub fn discard_dead_letter(&mut self, id: u64) {
        self.validator.dlq_mut().remove(id);
    }

    /// Whether the queue holds an entry produced by the record at
    /// `source_lsn`.
    pub fn dead_letter_recorded(&self, source_lsn: u64) -> bool {
        self.validator
            .dlq()
            .iter()
            .any(|entry| entry.source_lsn == Some(source_lsn))
    }

    /// Every pending dead-letter entry, oldest first.
    pub fn dead_letters(&self) -> impl Iterator<Item = &DeadLetter> {
        self.validator.dlq().iter()
    }

    /// Enqueue placeholder entries until the dead-letter queue refuses one.
    /// Returns how many it took.
    #[cfg(test)]
    pub(crate) fn fill_dead_letter_queue_for_test(&mut self) -> usize {
        let filler = nodedb_crdt::Constraint {
            name: "filler".into(),
            collection: "filler".into(),
            field: String::new(),
            kind: nodedb_crdt::ConstraintKind::Check {
                expr: String::new(),
                description: "filler".into(),
            },
        };
        let mut taken = 0;
        while self
            .validator
            .dlq_mut()
            .enqueue(nodedb_crdt::EnqueueDeadLetterArgs {
                peer_id: 0,
                user_id: 0,
                tenant_id: self.tenant_id.as_u64(),
                delta: Vec::new(),
                constraint: &filler,
                reason: String::new(),
                hint: nodedb_crdt::CompensationHint::ManualIntervention {
                    reason: String::new(),
                },
            })
            .is_ok()
        {
            taken += 1;
        }
        taken
    }
}

#[cfg(test)]
mod tests {
    use nodedb_crdt::constraint::ConstraintSet;
    use nodedb_crdt::{
        CompensationHint, Constraint, ConstraintKind, DeadLetterQueue, EnqueueDeadLetterArgs,
    };

    use super::*;
    use crate::types::TenantId;

    fn stored_entry(source_lsn: u64) -> DeadLetter {
        let mut dlq = DeadLetterQueue::new(1);
        let id = dlq
            .enqueue(EnqueueDeadLetterArgs {
                peer_id: 1,
                user_id: 0,
                tenant_id: 3,
                delta: b"delta".to_vec(),
                constraint: &Constraint {
                    name: "email_unique".into(),
                    collection: "users".into(),
                    field: "email".into(),
                    kind: ConstraintKind::Unique,
                },
                reason: "duplicate".into(),
                hint: CompensationHint::ManualIntervention {
                    reason: "duplicate".into(),
                },
            })
            .expect("enqueue");
        dlq.bind_source(id, source_lsn).cloned().expect("bound")
    }

    /// A stored entry the full queue refuses fails the restore: the engine
    /// is not installed missing an entry storage holds.
    #[test]
    fn a_stored_entry_the_full_queue_refuses_fails_the_restore() {
        let mut engine =
            TenantCrdtEngine::new(TenantId::new(3), 0, ConstraintSet::new()).expect("engine");
        engine.fill_dead_letter_queue_for_test();
        match engine.restore_dead_letters(vec![stored_entry(5)]) {
            Err(crate::Error::Crdt(nodedb_crdt::CrdtError::DlqFull { .. })) => {}
            other => panic!("expected DlqFull, got {other:?}"),
        }
        assert!(!engine.dead_letter_recorded(5));
    }

    #[test]
    fn a_restored_entry_is_recorded_and_a_discarded_one_is_not() {
        let mut engine =
            TenantCrdtEngine::new(TenantId::new(3), 0, ConstraintSet::new()).expect("engine");
        let entry = stored_entry(5);
        let id = entry.id;
        engine.restore_dead_letters(vec![entry]).expect("restore");
        assert!(engine.dead_letter_recorded(5));
        engine.discard_dead_letter(id);
        assert!(!engine.dead_letter_recorded(5));
    }
}
