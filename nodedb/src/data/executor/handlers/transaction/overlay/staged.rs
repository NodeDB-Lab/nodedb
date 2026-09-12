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

use std::cell::Cell;
use std::collections::HashMap;

use nodedb_types::RowIdentity;

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

/// A staged TTL delta for one KV row, kept OUTSIDE `Staged` because TTL is
/// KV-specific (only KV entries carry `expire_at_ms`,
/// `engine/kv/entry.rs::KvEntry.expire_at_ms`) while `Staged` is shared by
/// every engine's read-merge. Only KV reads ever consult this map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedTtl {
    /// `EXPIRE` staged: the row expires at this absolute epoch-ms instant.
    ExpireAt(u64),
    /// `PERSIST` staged: any base TTL is cleared, the row never expires.
    Persist,
}

/// The bitemporal system/valid-time stamp assigned to one staged document
/// `Put` at COMMIT resolve time, kept OUTSIDE `Staged` because it is only
/// meaningful for a `bitemporal=true` document collection (like [`StagedTtl`]
/// is only meaningful for KV).
///
/// Assigning it ONCE at resolve — rather than re-deriving it at both the
/// commit-time base install and WAL replay — is what keeps a normal restart
/// from writing a SECOND version of the same row: the redo sub-record carries
/// this stamp verbatim, and the base install reads the identical stamp back
/// out of the overlay sidecar so both agree on the version key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitemporalStamp {
    /// System-time key (`_ts_system`) the version row is appended at.
    pub sys_from_ms: i64,
    /// Valid-time lower bound (`i64::MIN` = unbounded).
    pub valid_from_ms: i64,
    /// Valid-time upper bound (`i64::MAX` = unbounded).
    pub valid_until_ms: i64,
}

/// Staged mutations for a single collection within one transaction.
#[derive(Debug, Default)]
pub struct CollectionOverlay {
    /// Staged mutation per surrogate — the authoritative storage key.
    by_surrogate: HashMap<u32, Staged>,
    /// Resolves a row's client identity to its staged surrogate, for inserts
    /// that have not yet been made durable (and therefore have no other way
    /// to be looked up by identity).
    doc_id_to_surrogate: HashMap<RowIdentity, u32>,
    /// Staged KV TTL delta per surrogate — sibling to `by_surrogate`, never
    /// consulted by non-KV engines. See [`StagedTtl`].
    ttl_by_surrogate: HashMap<u32, StagedTtl>,
    /// Bitemporal stamp per surrogate — sibling to `by_surrogate`, written at
    /// COMMIT resolve time for `bitemporal=true` document `Put`s and read back
    /// by the commit-time base install so redo and install share one stamp.
    /// See [`BitemporalStamp`]. Never consulted by non-bitemporal collections.
    bitemporal_by_surrogate: HashMap<u32, BitemporalStamp>,
}

impl CollectionOverlay {
    /// Whether this collection carries no staged state in any sidecar.
    fn is_empty(&self) -> bool {
        self.by_surrogate.is_empty()
            && self.doc_id_to_surrogate.is_empty()
            && self.ttl_by_surrogate.is_empty()
            && self.bitemporal_by_surrogate.is_empty()
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

/// Per-transaction staging overlay: holds not-yet-durable writes for every
/// collection touched by the transaction, keyed by
/// `(DatabaseId, TenantId, collection)`.
#[derive(Debug, Default)]
pub struct TxnOverlay {
    collections: HashMap<(DatabaseId, TenantId, String), CollectionOverlay>,
    /// Append-only undo journal recording each slot's prior state before a
    /// staged value/TTL mutation. `journal_len` reads its length (the savepoint
    /// marker); `rollback_to` replays it in reverse down to a marker. Always
    /// appended to by the value/TTL mutators so nothing escapes it; dropped with
    /// the overlay when the transaction resolves.
    journal: Vec<OverlayUndo>,
    /// Ordinal-clock stamp of the last time this transaction touched its
    /// overlay — advanced by every staged write AND every in-transaction
    /// read-your-own-write, so a live transaction's stamp always tracks the
    /// clock. The overlay lease reaper reclaims overlays whose stamp has aged
    /// past `OVERLAY_LEASE_NS` (an abandoned txn whose teardown never ran).
    ///
    /// `Cell` (interior mutability) so read-your-own-write paths — which hold
    /// only `&self` while a scan borrows other core state — can refresh the
    /// stamp without threading `&mut self` through the entire read pipeline.
    /// Sound because a `CoreLoop` is `!Send` and single-threaded per core.
    last_touch_ord: Cell<i64>,
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
        self.last_touch_ord.set(ord);
    }

    /// The overlay's last lease stamp (0 for a freshly-created overlay that has
    /// not yet been touched). Read by the lease reaper.
    pub fn last_touch(&self) -> i64 {
        self.last_touch_ord.get()
    }

    /// Record the current slot state for `(coll_key, surrogate, doc_id)` onto
    /// the undo journal before a staged mutation overwrites it.
    ///
    /// This is the single chokepoint every value/TTL mutator calls, so no
    /// mutation of `by_surrogate` / `ttl_by_surrogate` / `doc_id_to_surrogate`
    /// escapes the journal — the guarantee `ROLLBACK TO SAVEPOINT` relies on.
    fn record_undo(
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
        self.journal.push(OverlayUndo {
            coll_key: coll_key.clone(),
            surrogate,
            doc_id: doc_id.clone(),
            prev_value,
            prev_ttl,
            prev_doc_binding,
        });
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

    /// Stage a KV TTL delta (`EXPIRE` / `PERSIST`) for `surrogate` in the
    /// given collection, binding `doc_id` to `surrogate` the same way
    /// `insert_put` / `insert_tombstone` do — a `GetTtl` (or a later
    /// `Expire`/`Persist`/`Incr` in the same transaction) resolves the same
    /// slot by the KV row's identity.
    pub fn set_ttl(
        &mut self,
        coll_key: (DatabaseId, TenantId, String),
        surrogate: u32,
        doc_id: &RowIdentity,
        ttl: StagedTtl,
    ) {
        self.record_undo(&coll_key, surrogate, doc_id);
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.ttl_by_surrogate.insert(surrogate, ttl);
        overlay
            .doc_id_to_surrogate
            .insert(doc_id.clone(), surrogate);
    }

    /// Current length of the overlay undo journal — the savepoint marker a
    /// later `rollback_to` rewinds toward. Returned to the Control Plane by
    /// `MetaOp::MarkSavepoint`.
    pub fn journal_len(&self) -> usize {
        self.journal.len()
    }

    /// Revert every staged value/TTL mutation recorded after `marker`,
    /// restoring each slot to its pre-mutation state (or removing it when the
    /// prior slot was absent), then truncate the journal to `marker`.
    ///
    /// Entries are replayed strictly in reverse so repeated writes to one slot
    /// unwind to the exact value present at the marked point. A `marker` at or
    /// beyond the current length is a no-op.
    pub fn rollback_to(&mut self, marker: usize) {
        while self.journal.len() > marker {
            let Some(undo) = self.journal.pop() else {
                break;
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

    /// Look up the staged TTL delta for `surrogate` in the given collection.
    pub fn get_ttl(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
    ) -> Option<StagedTtl> {
        self.collections
            .get(coll_key)?
            .ttl_by_surrogate
            .get(&surrogate)
            .copied()
    }

    /// Look up the staged TTL delta for `doc_id` in the given collection,
    /// resolving through `doc_id_to_surrogate` first.
    pub fn get_ttl_by_doc_id(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        doc_id: &RowIdentity,
    ) -> Option<StagedTtl> {
        let overlay = self.collections.get(coll_key)?;
        let surrogate = overlay.doc_id_to_surrogate.get(doc_id)?;
        overlay.ttl_by_surrogate.get(surrogate).copied()
    }

    /// Record the resolve-time bitemporal stamp for `surrogate` in the given
    /// collection. Assigned exactly once, at COMMIT resolve, after all
    /// savepoint activity for the transaction has completed — so no undo
    /// journalling is needed (it is never rolled back mid-statement).
    pub fn set_bitemporal(
        &mut self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
        stamp: BitemporalStamp,
    ) {
        self.collections
            .entry(coll_key.clone())
            .or_default()
            .bitemporal_by_surrogate
            .insert(surrogate, stamp);
    }

    /// Look up the resolve-time bitemporal stamp for `surrogate` in the given
    /// collection. `Some` only for a `bitemporal=true` collection's staged
    /// `Put` whose stamp was assigned at resolve.
    pub fn get_bitemporal(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
    ) -> Option<BitemporalStamp> {
        self.collections
            .get(coll_key)?
            .bitemporal_by_surrogate
            .get(&surrogate)
            .copied()
    }

    /// Iterate every `(surrogate, BitemporalStamp)` staged across all
    /// collections in this overlay. Surrogates are globally unique, so the
    /// commit-time install flattens these into one per-core scratch map.
    pub fn all_bitemporal_stamps(&self) -> impl Iterator<Item = (u32, BitemporalStamp)> + '_ {
        self.collections.values().flat_map(|overlay| {
            overlay
                .bitemporal_by_surrogate
                .iter()
                .map(|(surrogate, stamp)| (*surrogate, *stamp))
        })
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

    /// True if no collection has any staged mutation.
    pub fn is_empty(&self) -> bool {
        self.collections
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
    fn fresh_overlay_has_no_ttl_deltas() {
        // What `MetaOp::DropTxnOverlay` effectively produces (the map entry
        // for the transaction is removed, so any later staging starts from a
        // fresh `TxnOverlay::default()`).
        let overlay = TxnOverlay::new();
        assert_eq!(overlay.get_ttl(&key("cache"), 3), None);
    }
}
