// SPDX-License-Identifier: Apache-2.0

//! Dead-letter queue for rejected CRDT deltas.
//!
//! When a delta fails constraint validation at commit time, it's not silently
//! dropped — it's routed to a dead-letter queue with actionable compensation
//! hints that tell the application exactly how to recover.
//!
//! ## Developer Experience
//!
//! The DX of handling constraint rejections must be pristine. If developers
//! have to write massive amounts of complex undo/compensation logic, they
//! will abandon the database. The DLQ provides:
//!
//! 1. The original delta that was rejected.
//! 2. Which constraint was violated and why.
//! 3. A machine-readable `CompensationHint` suggesting how to fix it.

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::constraint::Constraint;
use crate::error::{CrdtError, Result};

/// Suggested action the application should take to resolve a constraint violation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CompensationHint {
    /// Retry with a different value for the conflicting field.
    /// Example: UNIQUE violation — suggest appending a suffix.
    RetryWithDifferentValue {
        field: String,
        conflicting_value: String,
        suggestion: String,
    },

    /// Delete the conflicting row first, then retry.
    /// Example: UNIQUE violation where the application wants to "upsert".
    DeleteThenRetry {
        collection: String,
        conflicting_key: String,
    },

    /// The referenced row doesn't exist — create it first.
    /// Example: FK violation — the parent row is missing.
    CreateReferencedRow {
        ref_collection: String,
        ref_key: String,
        missing_value: String,
    },

    /// The field must not be empty — provide a value.
    ProvideRequiredField { field: String },

    /// No automatic compensation available — manual intervention required.
    ManualIntervention { reason: String },
}

/// A rejected delta with metadata for debugging and recovery.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DeadLetter {
    /// Unique ID for this dead letter entry.
    pub id: u64,

    /// The peer that produced this delta.
    pub peer_id: u64,

    /// The authenticated user_id that submitted this delta (0 = unauthenticated/legacy).
    #[serde(default)]
    pub user_id: u64,

    /// The tenant this delta belongs to (0 = system).
    #[serde(default)]
    pub tenant_id: u64,

    /// The raw delta bytes that were rejected.
    pub delta: Vec<u8>,

    /// The constraint that was violated.
    pub violated_constraint: String,

    /// The collection where the violation occurred.
    pub collection: String,

    /// Human-readable explanation of why the delta was rejected.
    pub reason: String,

    /// Machine-readable hint for how to fix the violation.
    pub hint: CompensationHint,

    /// Timestamp when the rejection occurred (unix millis).
    ///
    /// Node-local and NON-DETERMINISTIC: sourced from `SystemTime::now()` at
    /// enqueue time, so two replicas that reject the same delta will record
    /// different values. Safe today because the DLQ is a per-replica in-memory
    /// structure that is never replicated or compared across nodes. If the DLQ
    /// is ever replicated, snapshotted into shared state, or diffed between
    /// replicas, this field MUST first be switched to a deterministic stamp
    /// (HLC or the applying log index) — comparing the current value across
    /// replicas will spuriously diverge.
    pub rejected_at: u64,

    /// Number of times this delta has been retried.
    pub retry_count: u32,

    /// The log position of the record whose apply rejected this delta, once
    /// the applier binds it. One record yields at most one entry, so a
    /// re-applied record never adds a second.
    #[serde(default)]
    pub source_lsn: Option<u64>,
}

/// Parameters for [`DeadLetterQueue::enqueue`].
pub struct EnqueueDeadLetterArgs<'a> {
    pub peer_id: u64,
    pub user_id: u64,
    pub tenant_id: u64,
    pub delta: Vec<u8>,
    pub constraint: &'a Constraint,
    pub reason: String,
    pub hint: CompensationHint,
}

/// Bounded dead-letter queue.
///
/// Stores rejected deltas for later inspection and retry. The queue is bounded
/// to prevent unbounded memory growth from a flood of invalid deltas.
pub struct DeadLetterQueue {
    entries: VecDeque<DeadLetter>,
    capacity: usize,
    next_id: u64,
}

impl DeadLetterQueue {
    /// Create a new DLQ with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity.min(1024)),
            capacity,
            next_id: 1,
        }
    }

    /// Enqueue a rejected delta with full auth context.
    pub fn enqueue(&mut self, args: EnqueueDeadLetterArgs<'_>) -> Result<u64> {
        let EnqueueDeadLetterArgs {
            peer_id,
            user_id,
            tenant_id,
            delta,
            constraint,
            reason,
            hint,
        } = args;

        if self.entries.len() >= self.capacity {
            return Err(CrdtError::DlqFull {
                capacity: self.capacity,
                pending: self.entries.len(),
            });
        }

        let id = self.next_id;
        self.next_id += 1;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.entries.push_back(DeadLetter {
            id,
            peer_id,
            user_id,
            tenant_id,
            delta,
            violated_constraint: constraint.name.clone(),
            collection: constraint.collection.clone(),
            reason,
            hint,
            rejected_at: now,
            retry_count: 0,
            source_lsn: None,
        });

        Ok(id)
    }

    /// Bind entry `id` to the record at `source_lsn` that produced it.
    ///
    /// Returns the bound entry. Returns `None` when another entry already
    /// names that record: entry `id` is a repeat of it and is removed. Also
    /// `None` when no entry `id` exists.
    pub fn bind_source(&mut self, id: u64, source_lsn: u64) -> Option<&DeadLetter> {
        let repeat = self
            .entries
            .iter()
            .any(|dl| dl.id != id && dl.source_lsn == Some(source_lsn));
        if repeat {
            self.remove(id);
            return None;
        }
        let entry = self.entries.iter_mut().find(|dl| dl.id == id)?;
        entry.source_lsn = Some(source_lsn);
        Some(&*entry)
    }

    /// Put back an entry read from durable storage.
    ///
    /// An entry whose source record an existing entry already names is
    /// skipped. Fresh ids stay above every restored id.
    pub fn restore(&mut self, entry: DeadLetter) -> Result<()> {
        let known = entry.source_lsn.is_some()
            && self
                .entries
                .iter()
                .any(|dl| dl.source_lsn == entry.source_lsn);
        if known {
            return Ok(());
        }
        if self.entries.len() >= self.capacity {
            return Err(CrdtError::DlqFull {
                capacity: self.capacity,
                pending: self.entries.len(),
            });
        }
        self.next_id = self.next_id.max(entry.id.saturating_add(1));
        self.entries.push_back(entry);
        Ok(())
    }

    /// Every pending entry, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = &DeadLetter> {
        self.entries.iter()
    }

    /// Peek at the oldest dead letter without removing it.
    pub fn peek(&self) -> Option<&DeadLetter> {
        self.entries.front()
    }

    /// Dequeue the oldest dead letter for retry or inspection.
    pub fn dequeue(&mut self) -> Option<DeadLetter> {
        self.entries.pop_front()
    }

    /// Get a dead letter by ID.
    pub fn get(&self, id: u64) -> Option<&DeadLetter> {
        self.entries.iter().find(|dl| dl.id == id)
    }

    /// Remove a dead letter by ID (e.g., after successful manual resolution).
    pub fn remove(&mut self, id: u64) -> Option<DeadLetter> {
        if let Some(pos) = self.entries.iter().position(|dl| dl.id == id) {
            self.entries.remove(pos)
        } else {
            None
        }
    }

    /// Drop every entry for `(tenant_id, collection)`. Returns the
    /// number of entries removed. Called during collection hard-delete
    /// so stale rejected deltas don't resurface if a collection of the
    /// same name is recreated inside the retention window's shadow.
    pub fn purge_collection(&mut self, tenant_id: u64, collection: &str) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|e| !(e.tenant_id == tenant_id && e.collection == collection));
        before - self.entries.len()
    }

    /// Drain all entries for a specific peer.
    pub fn drain_peer(&mut self, peer_id: u64) -> Vec<DeadLetter> {
        let mut drained = Vec::new();
        self.entries.retain(|dl| {
            if dl.peer_id == peer_id {
                drained.push(dl.clone());
                false
            } else {
                true
            }
        });
        drained
    }

    /// Number of pending dead letters.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Capacity of the queue.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constraint::{Constraint, ConstraintKind};

    fn test_constraint() -> Constraint {
        Constraint {
            name: "users_email_unique".into(),
            collection: "users".into(),
            field: "email".into(),
            kind: ConstraintKind::Unique,
        }
    }

    #[test]
    fn enqueue_and_dequeue() {
        let mut dlq = DeadLetterQueue::new(10);
        let c = test_constraint();

        let id = dlq
            .enqueue(EnqueueDeadLetterArgs {
                peer_id: 42,
                user_id: 0,
                tenant_id: 0,
                delta: b"delta-bytes".to_vec(),
                constraint: &c,
                reason: "email already exists".into(),
                hint: CompensationHint::RetryWithDifferentValue {
                    field: "email".into(),
                    conflicting_value: "alice@example.com".into(),
                    suggestion: "alice+1@example.com".into(),
                },
            })
            .unwrap();

        assert_eq!(dlq.len(), 1);
        assert_eq!(id, 1);

        let dl = dlq.dequeue().unwrap();
        assert_eq!(dl.peer_id, 42);
        assert_eq!(dl.violated_constraint, "users_email_unique");
        assert!(dlq.is_empty());
    }

    #[test]
    fn capacity_enforced() {
        let mut dlq = DeadLetterQueue::new(2);
        let c = test_constraint();
        let hint = CompensationHint::ManualIntervention {
            reason: "test".into(),
        };

        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 1,
            user_id: 0,
            tenant_id: 0,
            delta: vec![],
            constraint: &c,
            reason: "r1".into(),
            hint: hint.clone(),
        })
        .unwrap();
        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 2,
            user_id: 0,
            tenant_id: 0,
            delta: vec![],
            constraint: &c,
            reason: "r2".into(),
            hint: hint.clone(),
        })
        .unwrap();

        let err = dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 3,
            user_id: 0,
            tenant_id: 0,
            delta: vec![],
            constraint: &c,
            reason: "r3".into(),
            hint,
        });
        assert!(matches!(err, Err(CrdtError::DlqFull { .. })));
    }

    #[test]
    fn purge_collection_drops_only_matching_entries() {
        let mut dlq = DeadLetterQueue::new(10);
        let users_c = Constraint {
            name: "users_email_unique".into(),
            collection: "users".into(),
            field: "email".into(),
            kind: ConstraintKind::Unique,
        };
        let orders_c = Constraint {
            name: "orders_sku_unique".into(),
            collection: "orders".into(),
            field: "sku".into(),
            kind: ConstraintKind::Unique,
        };
        let hint = CompensationHint::ManualIntervention { reason: "t".into() };

        // Two entries for tenant 1 / users, one for tenant 1 / orders,
        // one for tenant 2 / users — only the first two should be
        // dropped by `purge_collection(1, "users")`.
        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 10,
            user_id: 0,
            tenant_id: 1,
            delta: vec![],
            constraint: &users_c,
            reason: "a".into(),
            hint: hint.clone(),
        })
        .unwrap();
        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 11,
            user_id: 0,
            tenant_id: 1,
            delta: vec![],
            constraint: &users_c,
            reason: "b".into(),
            hint: hint.clone(),
        })
        .unwrap();
        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 12,
            user_id: 0,
            tenant_id: 1,
            delta: vec![],
            constraint: &orders_c,
            reason: "c".into(),
            hint: hint.clone(),
        })
        .unwrap();
        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 13,
            user_id: 0,
            tenant_id: 2,
            delta: vec![],
            constraint: &users_c,
            reason: "d".into(),
            hint: hint.clone(),
        })
        .unwrap();

        let removed = dlq.purge_collection(1, "users");
        assert_eq!(removed, 2);
        assert_eq!(dlq.len(), 2);

        // Idempotent — repeated call is a no-op.
        assert_eq!(dlq.purge_collection(1, "users"), 0);

        // Remaining entries are the orders row (t1) and users row (t2).
        let remaining: Vec<_> = (0..dlq.len()).map(|_| dlq.dequeue().unwrap()).collect();
        assert!(
            remaining
                .iter()
                .any(|d| d.collection == "orders" && d.tenant_id == 1)
        );
        assert!(
            remaining
                .iter()
                .any(|d| d.collection == "users" && d.tenant_id == 2)
        );
    }

    #[test]
    fn drain_by_peer() {
        let mut dlq = DeadLetterQueue::new(10);
        let c = test_constraint();
        let hint = CompensationHint::ManualIntervention {
            reason: "test".into(),
        };

        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 1,
            user_id: 0,
            tenant_id: 0,
            delta: vec![],
            constraint: &c,
            reason: "a".into(),
            hint: hint.clone(),
        })
        .unwrap();
        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 2,
            user_id: 0,
            tenant_id: 0,
            delta: vec![],
            constraint: &c,
            reason: "b".into(),
            hint: hint.clone(),
        })
        .unwrap();
        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id: 1,
            user_id: 0,
            tenant_id: 0,
            delta: vec![],
            constraint: &c,
            reason: "c".into(),
            hint,
        })
        .unwrap();

        let peer1 = dlq.drain_peer(1);
        assert_eq!(peer1.len(), 2);
        assert_eq!(dlq.len(), 1);
    }

    #[test]
    fn remove_by_id() {
        let mut dlq = DeadLetterQueue::new(10);
        let c = test_constraint();
        let hint = CompensationHint::ManualIntervention {
            reason: "test".into(),
        };

        let id1 = dlq
            .enqueue(EnqueueDeadLetterArgs {
                peer_id: 1,
                user_id: 0,
                tenant_id: 0,
                delta: vec![],
                constraint: &c,
                reason: "a".into(),
                hint: hint.clone(),
            })
            .unwrap();
        let _id2 = dlq
            .enqueue(EnqueueDeadLetterArgs {
                peer_id: 1,
                user_id: 0,
                tenant_id: 0,
                delta: vec![],
                constraint: &c,
                reason: "b".into(),
                hint,
            })
            .unwrap();

        let removed = dlq.remove(id1).unwrap();
        assert_eq!(removed.reason, "a");
        assert_eq!(dlq.len(), 1);
    }

    fn enqueue_one(dlq: &mut DeadLetterQueue, peer_id: u64) -> u64 {
        dlq.enqueue(EnqueueDeadLetterArgs {
            peer_id,
            user_id: 0,
            tenant_id: 0,
            delta: b"delta".to_vec(),
            constraint: &test_constraint(),
            reason: "duplicate".into(),
            hint: CompensationHint::ManualIntervention {
                reason: "duplicate".into(),
            },
        })
        .expect("enqueue")
    }

    #[test]
    fn a_record_bound_twice_keeps_one_entry() {
        let mut dlq = DeadLetterQueue::new(10);
        let first = enqueue_one(&mut dlq, 1);
        assert!(dlq.bind_source(first, 7).is_some());
        let repeat = enqueue_one(&mut dlq, 1);
        assert!(dlq.bind_source(repeat, 7).is_none());
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq.peek().map(|dl| dl.source_lsn), Some(Some(7)));
    }

    #[test]
    fn a_restored_entry_is_kept_once_and_fresh_ids_stay_above_it() {
        let mut source = DeadLetterQueue::new(10);
        let id = enqueue_one(&mut source, 1);
        let stored = source.bind_source(id, 9).cloned().expect("bound");

        let mut restored = DeadLetterQueue::new(10);
        restored.restore(stored.clone()).expect("restore");
        restored.restore(stored.clone()).expect("restore again");
        assert_eq!(restored.len(), 1);
        assert_eq!(restored.peek(), Some(&stored));
        let fresh = enqueue_one(&mut restored, 2);
        assert!(fresh > stored.id);
    }

    #[test]
    fn a_dead_letter_round_trips_through_messagepack() {
        let mut dlq = DeadLetterQueue::new(10);
        let id = enqueue_one(&mut dlq, 3);
        let entry = dlq.bind_source(id, 11).cloned().expect("bound");
        let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
        let decoded: DeadLetter = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, entry);
    }
}
