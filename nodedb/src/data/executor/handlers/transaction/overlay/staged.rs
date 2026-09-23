// SPDX-License-Identifier: BUSL-1.1

//! Per-transaction staging overlay data types.
//!
//! Holds the not-yet-durable writes an in-flight transaction has executed at
//! statement time (`MetaOp::StageWrite`), so an in-transaction point write
//! returns its real command tag and raises constraint violations immediately,
//! while COMMIT's `TransactionBatch` replay remains the sole durable apply.
//!
//! Keying rationale: the real storage key for a document is the SURROGATE
//! (`u32`) — `apply_point_put` keys `sparse.versioned_put_in_txn` by
//! surrogate. `doc_id_to_surrogate` resolves a row's client identity
//! ([`RowIdentity`]) to its staged surrogate, so a not-yet-persisted insert
//! is found by the identity a point read carries. A KV row's identity is
//! its raw key, hex encoded, taken verbatim (`stage_kv::kv_row_identity`).

use std::collections::HashMap;

use nodedb_types::RowIdentity;

use super::lease::LeaseStamp;
use super::staged_sidecar::{BitemporalStamp, StagedTtl};
use crate::types::{DatabaseId, TenantId};

/// Per-core upper bound on the total staged-body bytes a single transaction's
/// overlay may hold. Staging a point write that would push the overlay past
/// this budget is rejected with `program_limit_exceeded` (SQLSTATE 54000)
/// rather than growing a core's resident memory without bound.
pub const MAX_TXN_OVERLAY_BYTES: usize = 256 * 1024 * 1024;

/// A single staged mutation for one surrogate row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Staged {
    /// A staged insert/update: the new encoded row body.
    Put(Vec<u8>),
    /// A staged delete.
    Tombstone,
}

/// Staged mutations for a single collection within one transaction.
#[derive(Debug, Default)]
pub struct CollectionOverlay {
    /// Staged mutation per surrogate — the authoritative storage key.
    pub(super) by_surrogate: HashMap<u32, Staged>,
    /// Resolves a row's client identity to its staged surrogate, for inserts
    /// that have not yet been made durable (and therefore have no other way
    /// to be looked up by identity).
    pub(super) doc_id_to_surrogate: HashMap<RowIdentity, u32>,
    /// Staged KV TTL delta per surrogate — sibling to `by_surrogate`, never
    /// consulted by non-KV engines. See [`StagedTtl`].
    pub(super) ttl_by_surrogate: HashMap<u32, StagedTtl>,
    /// Bitemporal stamp per surrogate — sibling to `by_surrogate`, written at
    /// COMMIT resolve time for `bitemporal=true` document `Put`s and read back
    /// by the commit-time base install so redo and install share one stamp.
    /// See [`BitemporalStamp`]. Never consulted by non-bitemporal collections.
    pub(super) bitemporal_by_surrogate: HashMap<u32, BitemporalStamp>,
    /// Primary key (MessagePack) of the base row a staged columnar write
    /// displaced, per surrogate. Recorded when a statement first stages a
    /// base row, read by COMMIT resolve so the redo names the row it removes.
    /// Never consulted by non-columnar collections.
    pub(super) base_pk_by_surrogate: HashMap<u32, Vec<u8>>,
    /// The instant a staged timeseries ingest read as its default row
    /// timestamp, keyed by the batch's first surrogate. COMMIT resolve stamps
    /// the batch's untimed rows with it. Never consulted by other engines.
    pub(super) ingest_now_by_surrogate: HashMap<u32, i64>,
}

impl CollectionOverlay {
    /// Whether this collection carries no staged state in any sidecar.
    fn is_empty(&self) -> bool {
        self.by_surrogate.is_empty()
            && self.doc_id_to_surrogate.is_empty()
            && self.ttl_by_surrogate.is_empty()
            && self.bitemporal_by_surrogate.is_empty()
            && self.base_pk_by_surrogate.is_empty()
            && self.ingest_now_by_surrogate.is_empty()
    }
}

/// One overlay slot's state captured immediately before a staged value/TTL
/// mutation overwrote it. The undo journal of these entries is what makes
/// `ROLLBACK TO SAVEPOINT` correct: last-writer-wins overwrite in
/// `by_surrogate` / `ttl_by_surrogate` keeps only the newest value, so
/// dropping post-savepoint entries would lose an earlier same-slot write.
/// Restoring the recorded prior slot rewinds without that loss.
#[derive(Debug, Clone)]
struct OverlayUndo {
    coll_key: (DatabaseId, TenantId, String),
    surrogate: u32,
    doc_id: RowIdentity,
    /// Prior `by_surrogate` entry, or `None` if the slot was absent.
    prev_value: Option<Staged>,
    /// Prior `ttl_by_surrogate` entry, or `None` if absent.
    prev_ttl: Option<StagedTtl>,
    /// Prior `doc_id_to_surrogate` binding, or `None` if unbound.
    prev_doc_binding: Option<u32>,
}

/// One undo-journal entry: a slot mutation or a truncate marker.
#[derive(Debug, Clone)]
enum JournalEntry {
    /// A slot's prior state, captured before a staged value/TTL mutation.
    Slot(OverlayUndo),
    /// A truncate marker set on `coll_key`. `prev` is the journal position
    /// of the marker it replaced, `None` when the collection was not
    /// truncated before.
    Truncated {
        coll_key: (DatabaseId, TenantId, String),
        prev: Option<usize>,
    },
}

/// Per-transaction staging overlay: holds not-yet-durable writes for every
/// collection touched by the transaction, keyed by
/// `(DatabaseId, TenantId, collection)`.
#[derive(Debug, Default)]
pub struct TxnOverlay {
    pub(super) collections: HashMap<(DatabaseId, TenantId, String), CollectionOverlay>,
    /// Collections this transaction truncated, keyed to the journal position
    /// of the marker. A truncated collection hides every base row that has
    /// no newer overlay entry.
    truncated: HashMap<(DatabaseId, TenantId, String), usize>,
    /// Append-only undo journal recording each slot's prior state before a
    /// staged value/TTL mutation, and each truncate marker. `journal_len`
    /// reads its length (the savepoint marker); `rollback_to` replays it in
    /// reverse down to a marker. Always appended to by the value/TTL mutators
    /// and `mark_truncated` so nothing escapes it; dropped with the overlay
    /// when the transaction resolves.
    journal: Vec<JournalEntry>,
    /// Advanced by every staged write AND every in-transaction
    /// read-your-own-write, so a live transaction's stamp always tracks the
    /// clock. See [`LeaseStamp`].
    lease: LeaseStamp,
}

impl TxnOverlay {
    /// Create an empty overlay.
    pub fn new() -> Self {
        Self::default()
    }

    /// Refresh the overlay's lease stamp to `ord` (a monotonic ordinal-clock
    /// value). Called by the write choke point on staging and by every
    /// read-your-own-write path so an active transaction never ages out.
    pub fn touch(&self, ord: i64) {
        self.lease.touch(ord);
    }

    /// The overlay's last lease stamp (0 for a freshly-created overlay that has
    /// not yet been touched). Read by the lease reaper.
    pub fn last_touch(&self) -> i64 {
        self.lease.last_touch()
    }

    /// Record the current slot state for `(coll_key, surrogate, doc_id)` onto
    /// the undo journal before a staged mutation overwrites it.
    ///
    /// This is the single chokepoint every value/TTL mutator calls, so no
    /// mutation of `by_surrogate` / `ttl_by_surrogate` / `doc_id_to_surrogate`
    /// escapes the journal — the guarantee `ROLLBACK TO SAVEPOINT` relies on.
    pub(super) fn record_undo(
        &mut self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
        doc_id: &RowIdentity,
    ) {
        let (prev_value, prev_ttl, prev_doc_binding) = match self.collections.get(coll_key) {
            Some(overlay) => (
                overlay.by_surrogate.get(&surrogate).cloned(),
                overlay.ttl_by_surrogate.get(&surrogate).copied(),
                overlay.doc_id_to_surrogate.get(doc_id).copied(),
            ),
            None => (None, None, None),
        };
        self.journal.push(JournalEntry::Slot(OverlayUndo {
            coll_key: coll_key.clone(),
            surrogate,
            doc_id: doc_id.clone(),
            prev_value,
            prev_ttl,
            prev_doc_binding,
        }));
    }

    /// Stage a TRUNCATE of `coll_key`. Every row already staged in the
    /// collection is tombstoned through the normal undo path, so a savepoint
    /// rollback restores it; the marker then hides every base row that has
    /// no newer overlay entry.
    pub fn mark_truncated(&mut self, coll_key: (DatabaseId, TenantId, String)) {
        let staged_puts: Vec<(RowIdentity, u32)> = self
            .collections
            .get(&coll_key)
            .map(|overlay| {
                overlay
                    .doc_id_to_surrogate
                    .iter()
                    .filter(|(_, surrogate)| {
                        matches!(overlay.by_surrogate.get(surrogate), Some(Staged::Put(_)))
                    })
                    .map(|(doc_id, surrogate)| (doc_id.clone(), *surrogate))
                    .collect()
            })
            .unwrap_or_default();
        for (doc_id, surrogate) in staged_puts {
            self.insert_tombstone(coll_key.clone(), surrogate, &doc_id);
        }
        let prev = self.truncated.get(&coll_key).copied();
        let position = self.journal.len();
        self.journal.push(JournalEntry::Truncated {
            coll_key: coll_key.clone(),
            prev,
        });
        self.truncated.insert(coll_key, position);
    }

    /// Whether `coll_key` is truncated in this transaction.
    pub fn is_truncated(&self, coll_key: &(DatabaseId, TenantId, String)) -> bool {
        self.truncated.contains_key(coll_key)
    }

    /// Whether a base row of `coll_key` with no staged mutation is visible to
    /// this transaction: hidden while the collection is truncated.
    pub fn base_visible(&self, coll_key: &(DatabaseId, TenantId, String)) -> bool {
        !self.is_truncated(coll_key)
    }

    /// Stage a put (insert/update) for `surrogate` in the given collection.
    pub fn insert_put(
        &mut self,
        coll_key: (DatabaseId, TenantId, String),
        surrogate: u32,
        doc_id: &RowIdentity,
        body: Vec<u8>,
    ) {
        self.record_undo(&coll_key, surrogate, doc_id);
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.by_surrogate.insert(surrogate, Staged::Put(body));
        overlay
            .doc_id_to_surrogate
            .insert(doc_id.clone(), surrogate);
    }

    /// Stage a tombstone (delete) for `surrogate` in the given collection.
    pub fn insert_tombstone(
        &mut self,
        coll_key: (DatabaseId, TenantId, String),
        surrogate: u32,
        doc_id: &RowIdentity,
    ) {
        self.record_undo(&coll_key, surrogate, doc_id);
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.by_surrogate.insert(surrogate, Staged::Tombstone);
        overlay
            .doc_id_to_surrogate
            .insert(doc_id.clone(), surrogate);
    }

    /// Look up the staged mutation for `surrogate` in the given collection.
    pub fn get(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
    ) -> Option<&Staged> {
        self.collections
            .get(coll_key)
            .and_then(|overlay| overlay.by_surrogate.get(&surrogate))
    }

    /// Look up the staged mutation for `doc_id` in the given collection,
    /// resolving through `doc_id_to_surrogate` first.
    pub fn get_by_doc_id(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        doc_id: &RowIdentity,
    ) -> Option<&Staged> {
        let overlay = self.collections.get(coll_key)?;
        let surrogate = overlay.doc_id_to_surrogate.get(doc_id)?;
        overlay.by_surrogate.get(surrogate)
    }

    /// Resolve the surrogate a staged `doc_id` is bound to, without
    /// consulting the staged mutation itself. Used by callers that need the
    /// row's identity (e.g. to write a tombstone) rather than its body.
    pub fn surrogate_for_doc_id(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        doc_id: &RowIdentity,
    ) -> Option<u32> {
        self.collections
            .get(coll_key)?
            .doc_id_to_surrogate
            .get(doc_id)
            .copied()
    }

    /// Current length of the overlay undo journal — the savepoint marker a
    /// later `rollback_to` rewinds toward. Returned to the Control Plane by
    /// `MetaOp::MarkSavepoint`.
    pub fn journal_len(&self) -> usize {
        self.journal.len()
    }

    /// Revert every staged value/TTL mutation and truncate marker recorded
    /// after `marker`, restoring each slot to its pre-mutation state (or
    /// removing it when the prior slot was absent), then truncate the journal
    /// to `marker`.
    ///
    /// Entries are replayed strictly in reverse so repeated writes to one slot
    /// unwind to the exact value present at the marked point. A `marker` at or
    /// beyond the current length is a no-op.
    pub fn rollback_to(&mut self, marker: usize) {
        while self.journal.len() > marker {
            let Some(entry) = self.journal.pop() else {
                break;
            };
            let undo = match entry {
                JournalEntry::Slot(undo) => undo,
                JournalEntry::Truncated { coll_key, prev } => {
                    match prev {
                        Some(position) => self.truncated.insert(coll_key, position),
                        None => self.truncated.remove(&coll_key),
                    };
                    continue;
                }
            };
            let Some(overlay) = self.collections.get_mut(&undo.coll_key) else {
                continue;
            };
            match undo.prev_value {
                Some(staged) => {
                    overlay.by_surrogate.insert(undo.surrogate, staged);
                }
                None => {
                    overlay.by_surrogate.remove(&undo.surrogate);
                }
            }
            match undo.prev_ttl {
                Some(ttl) => {
                    overlay.ttl_by_surrogate.insert(undo.surrogate, ttl);
                }
                None => {
                    overlay.ttl_by_surrogate.remove(&undo.surrogate);
                }
            }
            match undo.prev_doc_binding {
                Some(surrogate) => {
                    overlay
                        .doc_id_to_surrogate
                        .insert(undo.doc_id.clone(), surrogate);
                }
                None => {
                    overlay.doc_id_to_surrogate.remove(&undo.doc_id);
                }
            }
        }
        self.collections.retain(|_, overlay| !overlay.is_empty());
    }

    /// Iterate all staged `(surrogate, Staged)` pairs for a collection.
    /// Yields nothing if the collection has no overlay entries.
    pub fn iter_for_collection<'a>(
        &'a self,
        coll_key: &(DatabaseId, TenantId, String),
    ) -> impl Iterator<Item = (u32, &'a Staged)> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(|overlay| overlay.by_surrogate.iter().map(|(k, v)| (*k, v)))
    }

    /// Iterate all staged `(identity, Staged)` pairs for a collection.
    ///
    /// Unlike [`iter_for_collection`](Self::iter_for_collection) (keyed by
    /// surrogate, the Document scan's row identity), this is keyed by the
    /// row's client identity -- the identity a KV scan merge needs, since a
    /// KV row's scan identity is its raw key bytes, not a surrogate.
    pub fn iter_doc_entries_for_collection<'a>(
        &'a self,
        coll_key: &(DatabaseId, TenantId, String),
    ) -> impl Iterator<Item = (&'a RowIdentity, &'a Staged)> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(|overlay| {
                overlay
                    .doc_id_to_surrogate
                    .iter()
                    .filter_map(move |(doc_id, surrogate)| {
                        overlay
                            .by_surrogate
                            .get(surrogate)
                            .map(|staged| (doc_id, staged))
                    })
            })
    }

    /// True if no collection has any staged mutation or truncate marker.
    pub fn is_empty(&self) -> bool {
        self.truncated.is_empty()
            && self
                .collections
                .values()
                .all(|overlay| overlay.by_surrogate.is_empty())
    }

    /// Total number of staged mutations across all collections.
    pub fn len(&self) -> usize {
        self.collections
            .values()
            .map(|overlay| overlay.by_surrogate.len())
            .sum()
    }

    /// Sum of staged `Put` body byte lengths across all collections.
    ///
    /// Placeholder for a future memory cap — not enforced here.
    pub fn memory_size_estimate(&self) -> usize {
        self.collections
            .values()
            .flat_map(|overlay| overlay.by_surrogate.values())
            .map(|staged| match staged {
                Staged::Put(body) => body.len(),
                Staged::Tombstone => 0,
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(coll: &str) -> (DatabaseId, TenantId, String) {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    fn id(text: &str) -> RowIdentity {
        RowIdentity::from_user_key(text)
    }

    #[test]
    fn empty_overlay_has_no_entries() {
        let overlay = TxnOverlay::new();
        assert!(overlay.is_empty());
        assert_eq!(overlay.len(), 0);
        assert_eq!(overlay.memory_size_estimate(), 0);
        assert!(overlay.get(&key("users"), 1).is_none());
        assert!(overlay.get_by_doc_id(&key("users"), &id("abc")).is_none());
        assert_eq!(overlay.iter_for_collection(&key("users")).count(), 0);
    }

    #[test]
    fn insert_put_and_lookup() {
        let mut overlay = TxnOverlay::new();
        overlay.insert_put(key("users"), 7, &id("doc-1"), vec![1, 2, 3]);

        assert!(!overlay.is_empty());
        assert_eq!(overlay.len(), 1);
        assert_eq!(overlay.memory_size_estimate(), 3);
        assert_eq!(
            overlay.get(&key("users"), 7),
            Some(&Staged::Put(vec![1, 2, 3]))
        );
        assert_eq!(
            overlay.get_by_doc_id(&key("users"), &id("doc-1")),
            Some(&Staged::Put(vec![1, 2, 3]))
        );
        let collected: Vec<_> = overlay.iter_for_collection(&key("users")).collect();
        assert_eq!(collected.len(), 1);
    }

    #[test]
    fn insert_tombstone_and_lookup() {
        let mut overlay = TxnOverlay::new();
        overlay.insert_tombstone(key("users"), 9, &id("doc-2"));

        assert_eq!(overlay.get(&key("users"), 9), Some(&Staged::Tombstone));
        assert_eq!(overlay.memory_size_estimate(), 0);
    }

    #[test]
    fn bulk_staged_row_is_found_by_the_point_get_identity() {
        // A bulk path keys the row by `RowIdentity::of_stored_row`. A point
        // get carries the plan's resolved identity: the `id` column value.
        // Both must land on the same overlay slot.
        let mut body_obj = HashMap::new();
        body_obj.insert(
            "id".to_string(),
            nodedb_types::Value::String("user-7".into()),
        );
        body_obj.insert(
            "name".to_string(),
            nodedb_types::Value::String("ann".into()),
        );
        let body = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(body_obj))
            .expect("encode msgpack");
        let storage_key = nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(7));
        let bulk_identity = RowIdentity::of_stored_row(&body, None, storage_key);

        let mut overlay = TxnOverlay::new();
        overlay.insert_put(key("users"), 7, &bulk_identity, body.clone());

        let point_get_identity = RowIdentity::from_user_key("user-7");
        assert_eq!(
            overlay.get_by_doc_id(&key("users"), &point_get_identity),
            Some(&Staged::Put(body))
        );
        assert_eq!(
            overlay.surrogate_for_doc_id(&key("users"), &point_get_identity),
            Some(7)
        );
        assert!(
            overlay
                .get_by_doc_id(&key("users"), &storage_key.to_identity())
                .is_none(),
            "a row carrying an `id` column is never keyed by its surrogate"
        );
    }

    // ── KV TTL delta (`StagedTtl`) ──────────────────────────────────────
    //
    // `KvOp::Expire` / `KvOp::Persist` / `GetTtl` have no SQL or native-DSL
    // surface in this codebase today (no `KV_EXPIRE`/`KV_PERSIST`/
    // `KV_GET_TTL` function, unlike `KV_INCR`/`KV_CAS`/`KV_GETSET`), so a
    // pgwire `TestServer` end-to-end test (as used by
    // `sql_transactions_kv_overlay.rs` / `sql_transactions_kv_atomic_overlay.rs`)
    // cannot exercise them -- same gap `BatchPut` was already flagged with in
    // `sql_transactions_kv_atomic_overlay.rs`. These unit tests cover the
    // overlay data structure directly instead: staging, doc-id resolution,
    // `Persist` overriding a prior `ExpireAt`, and that a fresh `TxnOverlay`
    // (what `MetaOp::DropTxnOverlay` replaces the map entry with on commit /
    // rollback) starts with no TTL deltas.

    #[test]
    fn set_ttl_and_get_ttl_round_trip() {
        let mut overlay = TxnOverlay::new();
        overlay.set_ttl(key("cache"), 3, &id("6b6579"), StagedTtl::ExpireAt(5_000));

        assert_eq!(
            overlay.get_ttl(&key("cache"), 3),
            Some(StagedTtl::ExpireAt(5_000))
        );
        assert_eq!(
            overlay.get_ttl_by_doc_id(&key("cache"), &id("6b6579")),
            Some(StagedTtl::ExpireAt(5_000))
        );
    }

    #[test]
    fn set_ttl_persist_overrides_prior_expire() {
        let mut overlay = TxnOverlay::new();
        overlay.set_ttl(key("cache"), 3, &id("6b6579"), StagedTtl::ExpireAt(5_000));
        overlay.set_ttl(key("cache"), 3, &id("6b6579"), StagedTtl::Persist);

        assert_eq!(overlay.get_ttl(&key("cache"), 3), Some(StagedTtl::Persist));
    }

    #[test]
    fn get_ttl_none_when_nothing_staged() {
        let overlay = TxnOverlay::new();
        assert_eq!(overlay.get_ttl(&key("cache"), 3), None);
        assert_eq!(
            overlay.get_ttl_by_doc_id(&key("cache"), &id("6b6579")),
            None
        );
    }

    #[test]
    fn set_ttl_binds_doc_id_without_a_staged_value() {
        // `Expire` on a key whose value was never staged in this transaction
        // (only a base row exists) must still resolve by doc_id -- `set_ttl`
        // binds `doc_id_to_surrogate` itself, independent of `insert_put`.
        let mut overlay = TxnOverlay::new();
        overlay.set_ttl(key("cache"), 42, &id("6b6579"), StagedTtl::ExpireAt(9_999));

        assert!(
            overlay
                .get_by_doc_id(&key("cache"), &id("6b6579"))
                .is_none()
        );
        assert_eq!(
            overlay.get_ttl_by_doc_id(&key("cache"), &id("6b6579")),
            Some(StagedTtl::ExpireAt(9_999))
        );
    }

    #[test]
    fn ttl_delta_is_per_collection() {
        let mut overlay = TxnOverlay::new();
        overlay.set_ttl(key("a"), 1, &id("6b"), StagedTtl::ExpireAt(1_000));
        assert_eq!(overlay.get_ttl(&key("b"), 1), None);
    }

    #[test]
    fn rollback_prunes_post_marker_collection_without_touching_prior_collection() {
        let mut overlay = TxnOverlay::new();
        let retained = key("retained");
        let post_marker = key("post_marker");
        overlay.insert_put(retained.clone(), 7, &id("stable"), vec![1, 2, 3]);
        let marker = overlay.journal_len();

        overlay.insert_put(post_marker.clone(), 9, &id("temporary"), vec![4, 5]);
        overlay.rollback_to(marker);

        assert!(
            !overlay.collections.contains_key(&post_marker),
            "a collection created entirely after the savepoint must be removed"
        );
        assert_eq!(overlay.collections.len(), 1);
        assert_eq!(
            overlay.get(&retained, 7),
            Some(&Staged::Put(vec![1, 2, 3])),
            "the pre-savepoint body must remain byte-exact"
        );
        assert_eq!(
            overlay.get_by_doc_id(&retained, &id("stable")),
            Some(&Staged::Put(vec![1, 2, 3]))
        );
        assert_eq!(overlay.journal_len(), marker);
    }

    #[test]
    fn mark_truncated_tombstones_staged_rows_and_hides_base() {
        let mut overlay = TxnOverlay::new();
        let users = key("users");
        overlay.insert_put(users.clone(), 7, &id("doc-7"), vec![1]);
        assert!(overlay.base_visible(&users));

        overlay.mark_truncated(users.clone());

        assert!(overlay.is_truncated(&users));
        assert!(!overlay.base_visible(&users));
        assert_eq!(overlay.get(&users, 7), Some(&Staged::Tombstone));
        assert_eq!(
            overlay.get_by_doc_id(&users, &id("doc-7")),
            Some(&Staged::Tombstone)
        );
        assert!(!overlay.is_truncated(&key("other")));
        assert!(!overlay.is_empty());
    }

    #[test]
    fn rollback_to_before_marker_untruncates() {
        let mut overlay = TxnOverlay::new();
        let users = key("users");
        overlay.insert_put(users.clone(), 7, &id("doc-7"), vec![1]);
        let marker = overlay.journal_len();

        overlay.mark_truncated(users.clone());
        overlay.insert_put(users.clone(), 9, &id("doc-9"), vec![2]);
        overlay.rollback_to(marker);

        assert!(!overlay.is_truncated(&users));
        assert_eq!(overlay.get(&users, 7), Some(&Staged::Put(vec![1])));
        assert_eq!(overlay.get(&users, 9), None);
        assert_eq!(overlay.journal_len(), marker);

        overlay.mark_truncated(users.clone());
        overlay.rollback_to(0);
        assert!(!overlay.is_truncated(&users));
        assert!(overlay.is_empty());
    }

    #[test]
    fn insert_put_after_truncate_is_visible() {
        let mut overlay = TxnOverlay::new();
        let users = key("users");
        overlay.mark_truncated(users.clone());
        overlay.insert_put(users.clone(), 11, &id("doc-11"), vec![3]);

        assert!(overlay.is_truncated(&users));
        assert_eq!(overlay.get(&users, 11), Some(&Staged::Put(vec![3])));
        assert_eq!(
            overlay.get_by_doc_id(&users, &id("doc-11")),
            Some(&Staged::Put(vec![3]))
        );

        // A second truncate tombstones the newer put and keeps the marker.
        let marker = overlay.journal_len();
        overlay.mark_truncated(users.clone());
        assert_eq!(overlay.get(&users, 11), Some(&Staged::Tombstone));
        overlay.rollback_to(marker);
        assert!(overlay.is_truncated(&users));
        assert_eq!(overlay.get(&users, 11), Some(&Staged::Put(vec![3])));
    }

    #[test]
    fn fresh_overlay_has_no_ttl_deltas() {
        // What `MetaOp::DropTxnOverlay` effectively produces (the map entry
        // for the transaction is removed, so any later staging starts from a
        // fresh `TxnOverlay::default()`).
        let overlay = TxnOverlay::new();
        assert_eq!(overlay.get_ttl(&key("cache"), 3), None);
    }
}
