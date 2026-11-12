// SPDX-License-Identifier: BUSL-1.1

//! Per-transaction staging overlay data types.
//!
//! Holds the not-yet-durable writes an in-flight transaction has executed at
//! statement time (`MetaOp::StageWrite`), so an in-transaction point write
//! returns its real command tag and raises constraint violations immediately,
//! while COMMIT's redo install remains the sole durable apply.
//!
//! Keying rationale: the real storage key for a document is the SURROGATE
//! (`u32`) — `apply_point_put` keys `sparse.versioned_put_in_txn` by
//! surrogate. `doc_id_to_surrogate` resolves a row's client identity
//! ([`RowIdentity`]) to its staged surrogate, so a not-yet-persisted insert
//! is found by the identity a point read carries. A KV row's identity is
//! its raw key, hex encoded, taken verbatim (`stage_kv::kv_row_identity`).
//!
//! Every per-row map is a [`TieredMap`]: a hot tier in RAM and sorted runs
//! in the transaction's spill directory. Past `[tuning.query]
//! max_txn_overlay_bytes` of hot entries, the overlay spills every hot tier
//! and the row-sized part of its undo journal. No overlay size is refused.

use std::borrow::Cow;
use std::collections::HashMap;

use nodedb_types::RowIdentity;
use zerompk::{Read, Write};

use super::lease::LeaseStamp;
use super::staged_journal::{JournalEntry, OverlayJournal};
use super::staged_sidecar::{PkBytes, StagedTtl};
use crate::data::executor::handlers::spill::tiered::{SpillSpace, SpillWeight, TieredMap};
use crate::engine::timeseries::columnar_memtable::ColumnarSchema;
use crate::types::{DatabaseId, TenantId};

/// An overlay collection key: `(database, tenant, collection)`.
pub(super) type CollKey = (DatabaseId, TenantId, String);

/// The MessagePack `nil` marker.
const NIL: u8 = 0xc0;

/// A single staged mutation for one surrogate row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Staged {
    /// A staged insert/update: the new encoded row body.
    Put(Vec<u8>),
    /// A staged delete.
    Tombstone,
}

/// A put spills as MessagePack `bin` of its body, a tombstone as `nil`.
impl zerompk::ToMessagePack for Staged {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            Staged::Put(body) => writer.write_binary(body),
            Staged::Tombstone => writer.write_nil(),
        }
    }
}

impl<'a> zerompk::FromMessagePack<'a> for Staged {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        if reader.peek_marker()? == NIL {
            reader.read_nil()?;
            Ok(Staged::Tombstone)
        } else {
            Ok(Staged::Put(reader.read_binary()?.into_owned()))
        }
    }
}

impl SpillWeight for Staged {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<Staged>()
            + match self {
                Staged::Put(body) => body.len(),
                Staged::Tombstone => 0,
            }
    }
}

/// Staged mutations for a single collection within one transaction.
#[derive(Debug, Default)]
pub struct CollectionOverlay {
    /// Staged mutation per surrogate — the authoritative storage key.
    pub(super) by_surrogate: TieredMap<u32, Staged>,
    /// Resolves a row's client identity to its staged surrogate, for inserts
    /// that have not yet been made durable (and therefore have no other way
    /// to be looked up by identity).
    pub(super) doc_id_to_surrogate: TieredMap<RowIdentity, u32>,
    /// Staged KV TTL delta per surrogate — sibling to `by_surrogate`, never
    /// consulted by non-KV engines. See [`StagedTtl`].
    pub(super) ttl_by_surrogate: TieredMap<u32, StagedTtl>,
    /// Primary key (MessagePack) of the base row a staged columnar write
    /// displaced, per surrogate. Recorded when a statement first stages a
    /// base row, read by COMMIT resolve so the redo names the row it removes.
    /// Never consulted by non-columnar collections.
    pub(super) base_pk_by_surrogate: TieredMap<u32, PkBytes>,
    /// The instant a staged timeseries ingest read as its default row
    /// timestamp, keyed by the batch's first surrogate. COMMIT resolve stamps
    /// the batch's untimed rows with it. One entry per staged batch, so it
    /// stays in RAM. Never consulted by other engines.
    pub(super) ingest_now_by_surrogate: HashMap<u32, i64>,
    /// The instant each staged unkeyed timeseries ingest read, in stage
    /// order. COMMIT resolve stamps the untimed rows of the Nth unkeyed
    /// ingest with the Nth instant.
    pub(super) unkeyed_ingest_now: Vec<i64>,
    /// The schema the transaction's last staged timeseries ingest into this
    /// collection previewed to. The next stage-time preview resolves against
    /// it, the chain the COMMIT resolve follows.
    pub(super) ts_preview_schema: Option<ColumnarSchema>,
}

impl CollectionOverlay {
    /// Whether this collection carries no staged state in any sidecar.
    pub(super) fn is_empty(&self) -> bool {
        self.by_surrogate.is_empty()
            && self.doc_id_to_surrogate.is_empty()
            && self.ttl_by_surrogate.is_empty()
            && self.base_pk_by_surrogate.is_empty()
            && self.ingest_now_by_surrogate.is_empty()
            && self.unkeyed_ingest_now.is_empty()
            && self.ts_preview_schema.is_none()
    }

    /// Weight of every hot tier.
    pub(super) fn hot_bytes(&self) -> usize {
        self.by_surrogate.hot_bytes()
            + self.doc_id_to_surrogate.hot_bytes()
            + self.ttl_by_surrogate.hot_bytes()
            + self.base_pk_by_surrogate.hot_bytes()
    }

    /// Write every hot tier as a run.
    pub(super) fn spill(&mut self, space: &mut SpillSpace) -> crate::Result<()> {
        self.by_surrogate.spill(space, "value")?;
        self.doc_id_to_surrogate.spill(space, "binding")?;
        self.ttl_by_surrogate.spill(space, "ttl")?;
        self.base_pk_by_surrogate.spill(space, "base-pk")
    }

    /// Drop every version written at or after journal position `bound`.
    pub(super) fn rollback(&mut self, bound: u64, space: &mut SpillSpace) -> crate::Result<()> {
        self.by_surrogate.rollback(bound, space, "value")?;
        self.doc_id_to_surrogate.rollback(bound, space, "binding")?;
        self.ttl_by_surrogate.rollback(bound, space, "ttl")?;
        self.base_pk_by_surrogate.rollback(bound, space, "base-pk")
    }
}

/// One overlay slot a staged mutation touched after a journal marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TouchedSlot {
    pub surrogate: u32,
    /// The client identity the first mutation after the marker bound.
    pub doc_id: RowIdentity,
    /// The slot's staged value at the marker. `None` means the row was
    /// unstaged, so its value at the marker is its base row.
    pub before: Option<Staged>,
    /// The slot's staged value now.
    pub after: Option<Staged>,
}

/// Per-transaction staging overlay: holds not-yet-durable writes for every
/// collection touched by the transaction, keyed by
/// `(DatabaseId, TenantId, collection)`.
#[derive(Debug)]
pub struct TxnOverlay {
    pub(super) collections: HashMap<CollKey, CollectionOverlay>,
    /// Collections this transaction truncated, keyed to the journal position
    /// of the marker. A truncated collection hides every base row that has
    /// no newer overlay entry.
    pub(super) truncated: HashMap<CollKey, usize>,
    /// The undo journal: each slot's hot state before a staged mutation, each
    /// truncate marker and each tag change. `journal_len` reads its length
    /// (the savepoint marker). `rollback_to` rewinds to a marker.
    pub(super) journal: OverlayJournal,
    /// Advanced by every staged write AND every in-transaction
    /// read-your-own-write, so a live transaction's stamp always tracks the
    /// clock. See [`LeaseStamp`].
    lease: LeaseStamp,
    /// Rows only trigger bodies staged. See [`super::row_tags`].
    pub(super) row_tags: super::row_tags::RowTags,
    /// The spill directory, the hot-tier threshold, and the lookup reader.
    pub(super) space: SpillSpace,
    /// Set when a spill or a rollback failed part way. The overlay then no
    /// longer matches the statements the transaction ran, so every later
    /// staging, read and resolve fails with this text.
    pub(super) poisoned: Option<String>,
}

#[cfg(test)]
impl Default for TxnOverlay {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnOverlay {
    /// An overlay that spills into `space`.
    pub(in crate::data::executor) fn with_spill(space: SpillSpace) -> Self {
        Self {
            collections: HashMap::new(),
            truncated: HashMap::new(),
            journal: OverlayJournal::default(),
            lease: LeaseStamp::default(),
            row_tags: super::row_tags::RowTags::default(),
            space,
            poisoned: None,
        }
    }

    /// An overlay that never spills, for tests.
    #[cfg(test)]
    pub fn new() -> Self {
        Self::with_spill(SpillSpace::never_spills())
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

    /// Fail when an earlier spill or rollback left the overlay unusable.
    pub(super) fn guard(&self) -> crate::Result<()> {
        match &self.poisoned {
            None => Ok(()),
            Some(reason) => Err(crate::Error::Internal {
                detail: format!(
                    "transaction staging overlay is unusable after a spill error: {reason}; \
                     roll the transaction back"
                ),
            }),
        }
    }

    /// Record the current slot state for `(coll_key, surrogate, doc_id)` onto
    /// the undo journal before a staged mutation overwrites it. Returns the
    /// journal position the mutation's versions carry.
    ///
    /// This is the single chokepoint every value/TTL mutator calls, so no
    /// mutation of `by_surrogate` / `ttl_by_surrogate` / `doc_id_to_surrogate`
    /// escapes the journal — the guarantee `ROLLBACK TO SAVEPOINT` relies on.
    /// The journal keeps the hot versions only: a spilled version stays in
    /// its run until a rollback drops the versions written after it.
    pub(super) fn record_undo(
        &mut self,
        coll_key: &CollKey,
        surrogate: u32,
        doc_id: &RowIdentity,
    ) -> crate::Result<u64> {
        self.guard()?;
        let reader = self.space.reader();
        let (prev_value, prev_ttl, prev_doc_binding, was_staged) =
            match self.collections.get(coll_key) {
                Some(overlay) => {
                    let prev_value = overlay
                        .by_surrogate
                        .hot(&surrogate)
                        .map(|hot| (hot.value.clone(), hot.seq));
                    let was_staged = prev_value.is_some()
                        || (overlay.by_surrogate.has_runs()
                            && overlay.by_surrogate.contains(&surrogate, reader)?);
                    (
                        prev_value,
                        overlay
                            .ttl_by_surrogate
                            .hot(&surrogate)
                            .map(|hot| (hot.value, hot.seq)),
                        overlay
                            .doc_id_to_surrogate
                            .hot(doc_id)
                            .map(|hot| (hot.value, hot.seq)),
                        was_staged,
                    )
                }
                None => (None, None, None, false),
            };
        let prev_body_tag = self.row_tags.note(coll_key, doc_id, was_staged);
        let seq = self
            .journal
            .push(JournalEntry::Slot(super::staged_journal::OverlayUndo {
                coll_key: coll_key.clone(),
                surrogate,
                doc_id: doc_id.clone(),
                prev_value,
                prev_ttl,
                prev_doc_binding,
                prev_body_tag,
            }));
        Ok(seq as u64)
    }

    /// Whether `coll_key` is truncated in this transaction.
    pub fn is_truncated(&self, coll_key: &CollKey) -> bool {
        self.truncated.contains_key(coll_key)
    }

    /// Whether this transaction staged anything for `coll_key`: a value, a
    /// tombstone, a TTL delta, or a truncate marker.
    pub fn stages_collection(&self, coll_key: &CollKey) -> bool {
        self.is_truncated(coll_key) || self.collections.contains_key(coll_key)
    }

    /// Whether a base row of `coll_key` with no staged mutation is visible to
    /// this transaction: hidden while the collection is truncated.
    pub fn base_visible(&self, coll_key: &CollKey) -> bool {
        !self.is_truncated(coll_key)
    }

    /// Stage a put (insert/update) for `surrogate` in the given collection.
    pub fn insert_put(
        &mut self,
        coll_key: CollKey,
        surrogate: u32,
        doc_id: &RowIdentity,
        body: Vec<u8>,
    ) -> crate::Result<()> {
        self.stage_value(coll_key, surrogate, doc_id, Staged::Put(body))
    }

    /// Stage a tombstone (delete) for `surrogate` in the given collection.
    pub fn insert_tombstone(
        &mut self,
        coll_key: CollKey,
        surrogate: u32,
        doc_id: &RowIdentity,
    ) -> crate::Result<()> {
        self.stage_value(coll_key, surrogate, doc_id, Staged::Tombstone)
    }

    fn stage_value(
        &mut self,
        coll_key: CollKey,
        surrogate: u32,
        doc_id: &RowIdentity,
        value: Staged,
    ) -> crate::Result<()> {
        let seq = self.record_undo(&coll_key, surrogate, doc_id)?;
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.by_surrogate.insert(surrogate, value, seq);
        overlay
            .doc_id_to_surrogate
            .insert(doc_id.clone(), surrogate, seq);
        self.spill_past_threshold()
    }

    /// Look up the staged mutation for `surrogate` in the given collection.
    pub fn get(
        &self,
        coll_key: &CollKey,
        surrogate: u32,
    ) -> crate::Result<Option<Cow<'_, Staged>>> {
        self.guard()?;
        match self.collections.get(coll_key) {
            Some(overlay) => overlay.by_surrogate.get(&surrogate, self.space.reader()),
            None => Ok(None),
        }
    }

    /// Look up the staged mutation for `doc_id` in the given collection,
    /// resolving through `doc_id_to_surrogate` first.
    pub fn get_by_doc_id(
        &self,
        coll_key: &CollKey,
        doc_id: &RowIdentity,
    ) -> crate::Result<Option<Cow<'_, Staged>>> {
        self.guard()?;
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(None);
        };
        let reader = self.space.reader();
        let Some(surrogate) = overlay.doc_id_to_surrogate.get(doc_id, reader)? else {
            return Ok(None);
        };
        overlay.by_surrogate.get(&*surrogate, reader)
    }

    /// Resolve the surrogate a staged `doc_id` is bound to, without
    /// consulting the staged mutation itself. Used by callers that need the
    /// row's identity (e.g. to write a tombstone) rather than its body.
    pub fn surrogate_for_doc_id(
        &self,
        coll_key: &CollKey,
        doc_id: &RowIdentity,
    ) -> crate::Result<Option<u32>> {
        self.guard()?;
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(None);
        };
        Ok(overlay
            .doc_id_to_surrogate
            .get(doc_id, self.space.reader())?
            .map(Cow::into_owned))
    }

    /// Current length of the overlay undo journal — the savepoint marker a
    /// later `rollback_to` rewinds toward. `MetaOp::MarkSavepoint` records it
    /// on the core.
    pub fn journal_len(&self) -> usize {
        self.journal.next_seq()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(coll: &str) -> CollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    fn id(text: &str) -> RowIdentity {
        RowIdentity::from_user_key(text)
    }

    fn put(body: &[u8]) -> Option<Staged> {
        Some(Staged::Put(body.to_vec()))
    }

    fn get(overlay: &TxnOverlay, coll: &CollKey, surrogate: u32) -> Option<Staged> {
        overlay.get(coll, surrogate).unwrap().map(Cow::into_owned)
    }

    fn get_by_id(overlay: &TxnOverlay, coll: &CollKey, doc: &str) -> Option<Staged> {
        overlay
            .get_by_doc_id(coll, &id(doc))
            .unwrap()
            .map(Cow::into_owned)
    }

    #[test]
    fn empty_overlay_has_no_entries() {
        let overlay = TxnOverlay::new();
        assert!(overlay.is_empty());
        assert_eq!(overlay.len().unwrap(), 0);
        assert_eq!(overlay.hot_bytes(), 0);
        assert!(get(&overlay, &key("users"), 1).is_none());
        assert!(get_by_id(&overlay, &key("users"), "abc").is_none());
        assert_eq!(overlay.iter_for_collection(&key("users")).count(), 0);
    }

    #[test]
    fn insert_put_and_lookup() {
        let mut overlay = TxnOverlay::new();
        overlay
            .insert_put(key("users"), 7, &id("doc-1"), vec![1, 2, 3])
            .unwrap();

        assert!(!overlay.is_empty());
        assert_eq!(overlay.len().unwrap(), 1);
        assert!(overlay.hot_bytes() > 3);
        assert_eq!(get(&overlay, &key("users"), 7), put(&[1, 2, 3]));
        assert_eq!(get_by_id(&overlay, &key("users"), "doc-1"), put(&[1, 2, 3]));
        let collected: Vec<_> = overlay
            .iter_for_collection(&key("users"))
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(collected.len(), 1);
    }

    /// A slot touched twice after the marker is reported once, with its value
    /// at the marker and its value now. Other collections and earlier
    /// mutations are left out.
    #[test]
    fn slots_touched_since_reports_each_slot_once_with_its_marker_value() {
        let mut overlay = TxnOverlay::new();
        overlay
            .insert_put(key("users"), 1, &id("a"), vec![1])
            .unwrap();
        let marker = overlay.journal_len();
        overlay
            .insert_put(key("users"), 1, &id("a"), vec![2])
            .unwrap();
        overlay
            .insert_put(key("users"), 2, &id("b"), vec![3])
            .unwrap();
        overlay.insert_tombstone(key("users"), 1, &id("a")).unwrap();
        overlay
            .insert_put(key("orders"), 1, &id("x"), vec![4])
            .unwrap();

        let slots = overlay.slots_touched_since(marker, &key("users")).unwrap();

        assert_eq!(slots.len(), 2);
        assert_eq!(slots[0].surrogate, 1);
        assert_eq!(slots[0].doc_id, id("a"));
        assert_eq!(slots[0].before, put(&[1]));
        assert_eq!(slots[0].after, Some(Staged::Tombstone));
        assert_eq!(slots[1].surrogate, 2);
        assert_eq!(slots[1].before, None);
        assert_eq!(slots[1].after, put(&[3]));
    }

    #[test]
    fn insert_tombstone_and_lookup() {
        let mut overlay = TxnOverlay::new();
        overlay
            .insert_tombstone(key("users"), 9, &id("doc-2"))
            .unwrap();

        assert_eq!(get(&overlay, &key("users"), 9), Some(Staged::Tombstone));
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
        overlay
            .insert_put(key("users"), 7, &bulk_identity, body.clone())
            .unwrap();

        assert_eq!(
            get_by_id(&overlay, &key("users"), "user-7"),
            Some(Staged::Put(body))
        );
        assert_eq!(
            overlay
                .surrogate_for_doc_id(&key("users"), &RowIdentity::from_user_key("user-7"))
                .unwrap(),
            Some(7)
        );
        assert!(
            overlay
                .get_by_doc_id(&key("users"), &storage_key.to_identity())
                .unwrap()
                .is_none(),
            "a row carrying an `id` column is never keyed by its surrogate"
        );
    }

    // ── KV TTL delta (`StagedTtl`) ──────────────────────────────────────
    //
    // `KvOp::Expire` / `KvOp::Persist` / `GetTtl` have no SQL or native-DSL
    // surface, so these unit tests cover the overlay data structure
    // directly: staging, doc-id resolution, `Persist` overriding a prior
    // `ExpireAt`, and a fresh `TxnOverlay` (what `MetaOp::DropTxnOverlay`
    // leaves for later staging) carrying no TTL delta.

    #[test]
    fn set_ttl_and_get_ttl_round_trip() {
        let mut overlay = TxnOverlay::new();
        overlay
            .set_ttl(key("cache"), 3, &id("6b6579"), StagedTtl::ExpireAt(5_000))
            .unwrap();

        assert_eq!(
            overlay.get_ttl(&key("cache"), 3).unwrap(),
            Some(StagedTtl::ExpireAt(5_000))
        );
        assert_eq!(
            overlay
                .get_ttl_by_doc_id(&key("cache"), &id("6b6579"))
                .unwrap(),
            Some(StagedTtl::ExpireAt(5_000))
        );
    }

    #[test]
    fn set_ttl_persist_overrides_prior_expire() {
        let mut overlay = TxnOverlay::new();
        overlay
            .set_ttl(key("cache"), 3, &id("6b6579"), StagedTtl::ExpireAt(5_000))
            .unwrap();
        overlay
            .set_ttl(key("cache"), 3, &id("6b6579"), StagedTtl::Persist)
            .unwrap();

        assert_eq!(
            overlay.get_ttl(&key("cache"), 3).unwrap(),
            Some(StagedTtl::Persist)
        );
    }

    #[test]
    fn get_ttl_none_when_nothing_staged() {
        let overlay = TxnOverlay::new();
        assert_eq!(overlay.get_ttl(&key("cache"), 3).unwrap(), None);
        assert_eq!(
            overlay
                .get_ttl_by_doc_id(&key("cache"), &id("6b6579"))
                .unwrap(),
            None
        );
    }

    #[test]
    fn set_ttl_binds_doc_id_without_a_staged_value() {
        // `Expire` on a key whose value was never staged in this transaction
        // (only a base row exists) must still resolve by doc_id -- `set_ttl`
        // binds `doc_id_to_surrogate` itself, independent of `insert_put`.
        let mut overlay = TxnOverlay::new();
        overlay
            .set_ttl(key("cache"), 42, &id("6b6579"), StagedTtl::ExpireAt(9_999))
            .unwrap();

        assert!(get_by_id(&overlay, &key("cache"), "6b6579").is_none());
        assert_eq!(
            overlay
                .get_ttl_by_doc_id(&key("cache"), &id("6b6579"))
                .unwrap(),
            Some(StagedTtl::ExpireAt(9_999))
        );
    }

    #[test]
    fn ttl_delta_is_per_collection() {
        let mut overlay = TxnOverlay::new();
        overlay
            .set_ttl(key("a"), 1, &id("6b"), StagedTtl::ExpireAt(1_000))
            .unwrap();
        assert_eq!(overlay.get_ttl(&key("b"), 1).unwrap(), None);
    }

    #[test]
    fn rollback_prunes_post_marker_collection_without_touching_prior_collection() {
        let mut overlay = TxnOverlay::new();
        let retained = key("retained");
        let post_marker = key("post_marker");
        overlay
            .insert_put(retained.clone(), 7, &id("stable"), vec![1, 2, 3])
            .unwrap();
        let marker = overlay.journal_len();

        overlay
            .insert_put(post_marker.clone(), 9, &id("temporary"), vec![4, 5])
            .unwrap();
        overlay.rollback_to(marker).unwrap();

        assert!(
            !overlay.collections.contains_key(&post_marker),
            "a collection created entirely after the savepoint must be removed"
        );
        assert_eq!(overlay.collections.len(), 1);
        assert_eq!(
            get(&overlay, &retained, 7),
            put(&[1, 2, 3]),
            "the pre-savepoint body must remain byte-exact"
        );
        assert_eq!(get_by_id(&overlay, &retained, "stable"), put(&[1, 2, 3]));
        assert_eq!(overlay.journal_len(), marker);
    }

    #[test]
    fn mark_truncated_tombstones_staged_rows_and_hides_base() {
        let mut overlay = TxnOverlay::new();
        let users = key("users");
        overlay
            .insert_put(users.clone(), 7, &id("doc-7"), vec![1])
            .unwrap();
        assert!(overlay.base_visible(&users));

        overlay.mark_truncated(users.clone()).unwrap();

        assert!(overlay.is_truncated(&users));
        assert!(!overlay.base_visible(&users));
        assert_eq!(get(&overlay, &users, 7), Some(Staged::Tombstone));
        assert_eq!(
            get_by_id(&overlay, &users, "doc-7"),
            Some(Staged::Tombstone)
        );
        assert!(!overlay.is_truncated(&key("other")));
        assert!(!overlay.is_empty());
    }

    #[test]
    fn rollback_to_before_marker_untruncates() {
        let mut overlay = TxnOverlay::new();
        let users = key("users");
        overlay
            .insert_put(users.clone(), 7, &id("doc-7"), vec![1])
            .unwrap();
        let marker = overlay.journal_len();

        overlay.mark_truncated(users.clone()).unwrap();
        overlay
            .insert_put(users.clone(), 9, &id("doc-9"), vec![2])
            .unwrap();
        overlay.rollback_to(marker).unwrap();

        assert!(!overlay.is_truncated(&users));
        assert_eq!(get(&overlay, &users, 7), put(&[1]));
        assert_eq!(get(&overlay, &users, 9), None);
        assert_eq!(overlay.journal_len(), marker);

        overlay.mark_truncated(users.clone()).unwrap();
        overlay.rollback_to(0).unwrap();
        assert!(!overlay.is_truncated(&users));
        assert!(overlay.is_empty());
    }

    #[test]
    fn insert_put_after_truncate_is_visible() {
        let mut overlay = TxnOverlay::new();
        let users = key("users");
        overlay.mark_truncated(users.clone()).unwrap();
        overlay
            .insert_put(users.clone(), 11, &id("doc-11"), vec![3])
            .unwrap();

        assert!(overlay.is_truncated(&users));
        assert_eq!(get(&overlay, &users, 11), put(&[3]));
        assert_eq!(get_by_id(&overlay, &users, "doc-11"), put(&[3]));

        // A second truncate tombstones the newer put and keeps the marker.
        let marker = overlay.journal_len();
        overlay.mark_truncated(users.clone()).unwrap();
        assert_eq!(get(&overlay, &users, 11), Some(Staged::Tombstone));
        overlay.rollback_to(marker).unwrap();
        assert!(overlay.is_truncated(&users));
        assert_eq!(get(&overlay, &users, 11), put(&[3]));
    }

    #[test]
    fn fresh_overlay_has_no_ttl_deltas() {
        // What `MetaOp::DropTxnOverlay` effectively produces (the map entry
        // for the transaction is removed, so any later staging starts from a
        // fresh overlay).
        let overlay = TxnOverlay::new();
        assert_eq!(overlay.get_ttl(&key("cache"), 3).unwrap(), None);
    }
}
