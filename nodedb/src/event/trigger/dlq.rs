// SPDX-License-Identifier: BUSL-1.1

//! Dead-Letter Queue for failed async trigger events.
//!
//! When an async trigger's DML fails after max retries, the failed event
//! is enqueued here with full context for debugging and manual replay.
//! Separate from the sync DLQ (which handles CRDT constraint violations).
//!
//! Bounded per tenant: oldest entries evicted when capacity is reached.
//! Persisted to redb for durability across restarts.
//!
//! # Requeue delivery guarantee
//!
//! REQUEUE TRIGGER DLQ delivers at least once.
//! - The entry stays unresolved until its consumer holds the action in the
//!   retry queue, so a crash before that point leaves it to requeue again.
//! - A claimed entry is not evicted and cannot be claimed twice. A refused
//!   submit gives the claim back, and the entry is listed again.
//! - A crash after the retry queue takes the action and before the entry
//!   resolves leaves both. A later requeue of that entry runs the action again,
//!   under the same action key.
//! - A claim is in-memory only, so a restart makes every unresolved entry
//!   claimable again.
//!
//! Eviction remains the one loss: at capacity an unclaimed entry can leave
//! before an operator requeues it, and the eviction logs it.

use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::TableDefinition;
use tracing::{debug, warn};

use crate::event::action::FailedAction;
use crate::event::redb_store::RedbStore;

/// redb table: monotonic entry_id → MessagePack-serialized `TriggerDlqEntry`.
const TRIGGER_DLQ: TableDefinition<u64, &[u8]> = TableDefinition::new("trigger_dlq");

/// Maximum DLQ entries per node (bounded to prevent unbounded growth).
const DEFAULT_MAX_ENTRIES: usize = 100_000;

/// An action that exhausted its retries, kept for an operator to inspect and
/// put back.
///
/// The action carries its own tenant, collection, row, and source position, so
/// none of that is copied alongside it — a second copy would be one more thing
/// that can disagree with what a requeue actually re-runs.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct TriggerDlqEntry {
    /// Unique entry ID (monotonic within this node).
    pub entry_id: u64,
    /// The action, as it will be re-run if requeued.
    pub action: FailedAction,
    /// Timestamp (Unix epoch millis) when the entry was created.
    pub created_at: u64,
    /// Whether this entry has been resolved, by an operator or by a requeue.
    pub resolved: bool,
}

impl TriggerDlqEntry {
    /// Tenant that owns the action.
    pub fn tenant_id(&self) -> u64 {
        self.action.context.tenant_id
    }

    /// Collection whose write produced the action.
    pub fn collection(&self) -> &str {
        &self.action.context.collection
    }

    /// The trigger or event definition the action belongs to.
    pub fn owner(&self) -> &str {
        self.action.owner()
    }

    /// Error text from the attempt that exhausted the action's retries.
    pub fn error(&self) -> &str {
        &self.action.last_error
    }

    /// Attempts made before the action was dead-lettered.
    pub fn retry_count(&self) -> u32 {
        self.action.attempts
    }
}

/// Why a requeue did not claim an entry for another attempt.
#[derive(Debug, thiserror::Error)]
pub enum RequeueTakeError {
    #[error("no dead-letter entry {entry_id}")]
    NotFound { entry_id: u64 },

    #[error("dead-letter entry {entry_id} is already resolved")]
    AlreadyResolved { entry_id: u64 },

    /// A requeue of this entry waits for its consumer to take the action.
    #[error("dead-letter entry {entry_id} is already requeued")]
    AlreadyClaimed { entry_id: u64 },
}

/// Trigger dead-letter queue.
///
/// Every mutation writes redb first and changes the in-memory index only
/// after the write commits, so the index never holds a change redb refused.
pub struct TriggerDlq {
    store: RedbStore<TriggerDlqEntry>,
    /// In-memory index for fast listing. Mirrors redb.
    entries: VecDeque<TriggerDlqEntry>,
    next_entry_id: u64,
    max_entries: usize,
    /// Entries a requeue holds until the retry queue owns their action. At
    /// most one per inbox slot, so eviction skipping them stays bounded.
    claimed: HashSet<u64>,
    /// Claimed entries whose consumer holds the action and whose resolved
    /// mark redb has not taken yet.
    unsettled: Vec<u64>,
}

impl crate::storage::RedbBacked for TriggerDlq {
    fn redb_database(&self) -> &redb::Database {
        self.store.redb_database()
    }
}

impl TriggerDlq {
    /// Open or create the trigger DLQ at `{data_dir}/event_plane/trigger_dlq.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let store = RedbStore::open(data_dir, "trigger_dlq.redb", TRIGGER_DLQ, "trigger DLQ")?;
        let loaded = store.load()?;
        debug!(
            entries = loaded.records.len(),
            next_id = loaded.next_key,
            "trigger DLQ loaded"
        );
        Ok(Self {
            store,
            entries: loaded.records,
            next_entry_id: loaded.next_key,
            max_entries: DEFAULT_MAX_ENTRIES,
            claimed: HashSet::new(),
            unsettled: Vec::new(),
        })
    }

    /// Record an action that has exhausted its retries.
    ///
    /// At capacity the oldest unclaimed entries are removed in the same redb
    /// transaction that writes the new one. On error nothing changes.
    pub fn enqueue(&mut self, action: FailedAction) -> crate::Result<u64> {
        let entry_id = self.next_entry_id;
        let entry = TriggerDlqEntry {
            entry_id,
            action,
            created_at: now_ms(),
            resolved: false,
        };

        let evict_count = (self.entries.len() + 1).saturating_sub(self.max_entries);
        let evicted: Vec<u64> = self
            .entries
            .iter()
            .filter(|e| !self.claimed.contains(&e.entry_id))
            .take(evict_count)
            .map(|e| e.entry_id)
            .collect();
        self.store.put_evicting(entry_id, &entry, &evicted)?;

        self.next_entry_id += 1;
        self.entries.retain(|old| {
            let evict = evicted.contains(&old.entry_id);
            if evict {
                warn!(
                    entry_id = old.entry_id,
                    owner = %old.owner(),
                    "trigger DLQ evicted oldest entry (at capacity)"
                );
            }
            !evict
        });
        debug!(
            entry_id,
            owner = %entry.owner(),
            "deferred action sent to DLQ"
        );
        self.entries.push_back(entry);
        Ok(entry_id)
    }

    /// List the DLQ entries an operator can still act on: unresolved, with no
    /// requeue in flight.
    pub fn list_unresolved(&self) -> Vec<&TriggerDlqEntry> {
        self.entries
            .iter()
            .filter(|e| !e.resolved && !self.claimed.contains(&e.entry_id))
            .collect()
    }

    /// Whether a requeue of `entry_id` is in flight.
    pub fn is_claimed(&self, entry_id: u64) -> bool {
        self.claimed.contains(&entry_id)
    }

    /// Mark an entry as resolved. Returns `Ok(false)` when no entry has
    /// `entry_id`. On error the entry stays unresolved.
    pub fn resolve(&mut self, entry_id: u64) -> crate::Result<bool> {
        let Some(entry) = self.entries.iter_mut().find(|e| e.entry_id == entry_id) else {
            return Ok(false);
        };
        mark_resolved(&self.store, entry)?;
        self.claimed.remove(&entry_id);
        Ok(true)
    }

    /// Claim an unresolved entry and return a copy of its action, to run it
    /// again.
    ///
    /// The entry stays in redb, unresolved, and eviction skips it, until
    /// [`Self::settle_claims`] resolves it or [`Self::release_claim`] gives it
    /// back. So no step between this call and the retry queue can lose the
    /// action, a crash included.
    pub fn claim_for_requeue(&mut self, entry_id: u64) -> Result<FailedAction, RequeueTakeError> {
        let entry = self
            .entries
            .iter()
            .find(|e| e.entry_id == entry_id)
            .ok_or(RequeueTakeError::NotFound { entry_id })?;
        if entry.resolved {
            return Err(RequeueTakeError::AlreadyResolved { entry_id });
        }
        if self.claimed.contains(&entry_id) {
            return Err(RequeueTakeError::AlreadyClaimed { entry_id });
        }
        let action = entry.action.clone();
        self.claimed.insert(entry_id);
        Ok(action)
    }

    /// Give a claim back, because nothing took its action. The entry is listed
    /// and claimable again.
    pub fn release_claim(&mut self, entry_id: u64) {
        self.claimed.remove(&entry_id);
    }

    /// Resolve claimed entries whose action the retry queue durably holds,
    /// along with any earlier one redb refused.
    ///
    /// On error the refused entries stay claimed and unresolved, and the next
    /// call tries them again. The first error is returned.
    pub fn settle_claims(&mut self, entry_ids: &[u64]) -> crate::Result<()> {
        self.unsettled.extend_from_slice(entry_ids);
        let mut first_error = None;
        for entry_id in std::mem::take(&mut self.unsettled) {
            match self.resolve(entry_id) {
                // An entry that is gone has nothing left to resolve.
                Ok(_) => {
                    self.claimed.remove(&entry_id);
                }
                Err(e) => {
                    self.unsettled.push(entry_id);
                    first_error.get_or_insert(e);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Every entry, newest last, for operator introspection.
    pub fn list(&self) -> impl Iterator<Item = &TriggerDlqEntry> {
        self.entries.iter()
    }

    /// Total entries (resolved + unresolved).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Persist `entry` as resolved, then set the flag in memory.
fn mark_resolved(
    store: &RedbStore<TriggerDlqEntry>,
    entry: &mut TriggerDlqEntry,
) -> crate::Result<()> {
    let mut updated = entry.clone();
    updated.resolved = true;
    store.put(updated.entry_id, &updated)?;
    entry.resolved = true;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::action::{ActionContext, ActionId, ActionKey, ActionPayload};

    /// One dead-lettered ROW trigger.
    fn failed(collection: &str, row_id: &str, trigger: &str, lsn: u64) -> FailedAction {
        FailedAction {
            key: ActionKey {
                source_lsn: lsn,
                source_sequence: 1,
                source_vshard: 0,
                action: ActionId::TriggerRow {
                    trigger_name: trigger.to_owned(),
                },
            },
            payload: ActionPayload::TriggerRow {
                operation: "INSERT".into(),
                new_fields: None,
                old_fields: None,
            },
            context: ActionContext {
                database_id: nodedb_types::DatabaseId::DEFAULT,
                tenant_id: 1,
                collection: collection.to_owned(),
                row_id: row_id.to_owned(),
                cascade_depth: 0,
            },
            attempts: 5,
            last_error: "timeout".into(),
        }
    }

    #[test]
    fn dlq_enqueue_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();

        let id = dlq
            .enqueue(failed("orders", "order-1", "audit_trigger", 100))
            .unwrap();

        assert_eq!(dlq.len(), 1);
        let unresolved = dlq.list_unresolved();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].entry_id, id);
        assert_eq!(unresolved[0].owner(), "audit_trigger");
        assert_eq!(unresolved[0].collection(), "orders");
        assert_eq!(unresolved[0].retry_count(), 5);
    }

    /// A claim given back leaves the entry claimable, and a claim never
    /// reaches redb: a crash before the retry queue holds the action leaves
    /// the entry to requeue again.
    #[test]
    fn a_claim_given_back_or_lost_to_a_restart_leaves_the_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        let id = dlq
            .enqueue(failed("orders", "order-1", "audit", 100))
            .unwrap();

        dlq.claim_for_requeue(id).unwrap();
        assert!(dlq.list_unresolved().is_empty());
        dlq.release_claim(id);
        assert_eq!(dlq.list_unresolved().len(), 1);
        dlq.claim_for_requeue(id).unwrap();
        drop(dlq);

        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        assert_eq!(dlq.list_unresolved().len(), 1);
        dlq.claim_for_requeue(id).unwrap();
    }

    /// Eviction cannot take a claimed entry, so a refused submit always finds
    /// its entry. It takes the oldest unclaimed entry instead.
    #[test]
    fn eviction_skips_a_claimed_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        dlq.max_entries = 2;
        let claimed = dlq.enqueue(failed("c", "1", "t", 1)).unwrap();
        let oldest_unclaimed = dlq.enqueue(failed("c", "2", "t", 2)).unwrap();
        dlq.claim_for_requeue(claimed).unwrap();

        let newest = dlq.enqueue(failed("c", "3", "t", 3)).unwrap();

        let held: Vec<u64> = dlq.list().map(|e| e.entry_id).collect();
        assert_eq!(held, vec![claimed, newest]);
        assert!(!held.contains(&oldest_unclaimed));
        dlq.release_claim(claimed);
        assert_eq!(dlq.list_unresolved().len(), 2);
        drop(dlq);
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        let held: Vec<u64> = dlq.list().map(|e| e.entry_id).collect();
        assert_eq!(held, vec![claimed, newest], "the same set in redb");
    }

    /// Only settling resolves the entry, and the resolve survives a restart.
    #[test]
    fn a_settled_claim_resolves_the_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        let id = dlq.enqueue(failed("c", "1", "t", 1)).unwrap();
        dlq.claim_for_requeue(id).unwrap();

        dlq.settle_claims(&[id]).unwrap();

        assert!(!dlq.is_claimed(id));
        assert!(matches!(
            dlq.claim_for_requeue(id).unwrap_err(),
            RequeueTakeError::AlreadyResolved { .. }
        ));
        drop(dlq);
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        assert!(dlq.list_unresolved().is_empty());
    }

    #[test]
    fn dlq_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        let id = dlq
            .enqueue(failed("orders", "order-1", "audit", 100))
            .unwrap();

        assert!(dlq.resolve(id).unwrap());
        assert!(dlq.list_unresolved().is_empty());
        assert!(!dlq.resolve(999).unwrap());
    }

    #[test]
    fn a_resolve_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = TriggerDlq::open(dir.path()).unwrap();
            let id = dlq
                .enqueue(failed("orders", "order-1", "audit", 100))
                .unwrap();
            assert!(dlq.resolve(id).unwrap());
        }
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        assert!(dlq.list_unresolved().is_empty());
    }

    #[test]
    fn eviction_is_persisted_with_the_new_entry() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = TriggerDlq::open(dir.path()).unwrap();
            dlq.max_entries = 2;
            for i in 0u64..4 {
                dlq.enqueue(failed("c", &format!("r-{i}"), "t", i)).unwrap();
            }
        }
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        let rows: Vec<&str> = dlq
            .list()
            .map(|e| e.action.context.row_id.as_str())
            .collect();
        assert_eq!(rows, vec!["r-2", "r-3"]);
        assert_eq!(dlq.next_entry_id, 5);
    }

    #[test]
    fn dlq_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = TriggerDlq::open(dir.path()).unwrap();
            dlq.enqueue(failed("orders", "o-1", "t1", 100)).unwrap();
        }
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq.list_unresolved()[0].owner(), "t1");
    }

    #[test]
    fn dlq_evicts_oldest_at_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        dlq.max_entries = 3;

        for i in 0u64..5 {
            dlq.enqueue(failed("c", &format!("r-{i}"), "t", i)).unwrap();
        }
        assert_eq!(dlq.len(), 3);
        assert_eq!(dlq.entries.front().unwrap().action.context.row_id, "r-2");
    }

    #[test]
    fn an_entry_can_be_claimed_for_another_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        let id = dlq
            .enqueue(failed("orders", "order-1", "audit", 100))
            .unwrap();

        let action = dlq.claim_for_requeue(id).expect("claim");
        assert_eq!(action.owner(), "audit");
        assert!(
            dlq.list_unresolved().is_empty(),
            "a claimed entry is not offered for a second requeue"
        );
    }

    #[test]
    fn an_entry_cannot_be_claimed_twice() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        let id = dlq
            .enqueue(failed("orders", "order-1", "audit", 100))
            .unwrap();
        dlq.claim_for_requeue(id).expect("first claim");
        assert!(matches!(
            dlq.claim_for_requeue(id).unwrap_err(),
            RequeueTakeError::AlreadyClaimed { entry_id } if entry_id == id
        ));
    }

    #[test]
    fn an_unknown_entry_reports_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        assert!(matches!(
            dlq.claim_for_requeue(99).unwrap_err(),
            RequeueTakeError::NotFound { entry_id: 99 }
        ));
    }

    #[test]
    fn a_settled_claim_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = TriggerDlq::open(dir.path()).unwrap();
            let id = dlq
                .enqueue(failed("orders", "order-1", "audit", 100))
                .unwrap();
            dlq.claim_for_requeue(id).expect("claim");
            dlq.settle_claims(&[id]).expect("settle");
        }
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        assert!(
            dlq.list_unresolved().is_empty(),
            "a restart must not offer already-requeued work again"
        );
        assert_eq!(dlq.len(), 1, "the record itself is kept for history");
    }
}
