// SPDX-License-Identifier: BUSL-1.1

//! Per-core last-write-LSN version index.
//!
//! Records, for every committed write applied on this Data-Plane core, the WAL
//! LSN of the write against the written key (`last_write_lsn`) and against the
//! written collection (`coll_write_lsn`). The WAL LSN is allocated in the
//! Control Plane at wal-dispatch time and threaded onto the write task; the
//! apply chokepoints on this core feed it here.
//!
//! This is the shard-local write-version substrate the optimistic-concurrency
//! commit path validates a transaction's read-set against (see
//! [`CoreLoop::read_set_still_current`]). Because the index lives on the
//! `!Send` core it is a plain `HashMap`: no atomics, no locks, no cross-core
//! sharing.
//!
//! The per-key map is bounded: horizon GC (run from the periodic maintenance
//! hook) evicts entries far below the core watermark and enforces a hard
//! entry-count backstop. The per-collection map is bounded by the number of
//! live collections and is never LSN-GC'd.

use std::collections::HashMap;

use nodedb_types::calvin::{ReadKeyIdent, VersionedReadEntry};
use nodedb_types::{DatabaseId, TenantId};

use crate::types::{Lsn, VShardId};

use super::CoreLoop;

/// Row identity type, re-exported from its plane-neutral home
/// ([`crate::types::KeyRepr`]) so Data-Plane call sites can keep referring to
/// it through this module. Read keys and write keys share this one namespace.
pub use crate::types::KeyRepr;

/// Horizon retain window for per-key entries, in LSNs. Horizon GC evicts any
/// `last_write_lsn` entry whose LSN is more than this far below the core
/// watermark. Sized in the same order of magnitude as the idempotency-cache
/// cap (16,384 entries): a bounded recent-write history — enough to validate
/// in-flight transactions — not an unbounded write log.
const RETAIN_WINDOW: u64 = 16_384;

/// Hard upper bound on the `last_write_lsn` entry count. When horizon GC leaves
/// more entries than this (a burst of distinct keys all inside the retain
/// window), the lowest-LSN (oldest) entries are dropped until the map is back
/// within bound.
const MAX_KEY_ENTRIES: usize = 65_536;

/// Fully-qualified per-key version-index key. Scoped by `(database, tenant)`
/// exactly like the write path, so two tenants (or databases) never alias.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteKey {
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: Box<str>,
    pub key: KeyRepr,
}

/// Fully-qualified per-collection version-index key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CollKey {
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: Box<str>,
}

/// Per-core last-write-LSN version index.
#[derive(Default)]
pub struct WriteVersionIndex {
    /// Last committed-write LSN per written key.
    last_write_lsn: HashMap<WriteKey, Lsn>,
    /// Last committed-write LSN per written collection (the phantom-safe floor:
    /// a predicate reader validates against this when it owns no per-key entry).
    coll_write_lsn: HashMap<CollKey, Lsn>,
    /// Per-secondary-index-dimension write-VALUE versions — the finer-grained
    /// sibling of `coll_write_lsn` an index-range read validates against.
    pub(in crate::data::executor) index_values: super::index_value_versions::IndexValueVersionIndex,
}

impl WriteVersionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a committed write at `lsn`.
    ///
    /// Always advances the collection floor `coll_write_lsn[collection]` to the
    /// max of its current value and `lsn`. When `key` is `Some`, also advances
    /// the per-key version `last_write_lsn[key]` monotonically. Advancing the
    /// core watermark is the caller's responsibility (see
    /// [`CoreLoop::note_write_lsn`]).
    pub fn note_write_lsn(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        lsn: Lsn,
    ) {
        let coll_key = CollKey {
            db,
            tenant,
            collection: Box::from(collection),
        };
        let slot = self.coll_write_lsn.entry(coll_key).or_insert(Lsn::ZERO);
        if lsn > *slot {
            *slot = lsn;
        }

        if let Some(key) = key {
            let write_key = WriteKey {
                db,
                tenant,
                collection: Box::from(collection),
                key,
            };
            let slot = self.last_write_lsn.entry(write_key).or_insert(Lsn::ZERO);
            if lsn > *slot {
                *slot = lsn;
            }
        }
    }

    /// Current per-key version, if recorded.
    pub(crate) fn key_write_lsn(&self, key: &WriteKey) -> Option<Lsn> {
        self.last_write_lsn.get(key).copied()
    }

    /// Current per-collection floor version, if recorded.
    pub(crate) fn collection_write_lsn(&self, key: &CollKey) -> Option<Lsn> {
        self.coll_write_lsn.get(key).copied()
    }

    /// Whether a previously observed read is still current against this
    /// core's recorded write versions.
    ///
    /// A `Point` read is current iff no write to that exact key has been
    /// recorded since the read (`last_write_lsn <= read_lsn`); a key with no
    /// recorded write has never been written on this core since the read, so
    /// it is treated as version zero and is always current. A `Predicate`
    /// read is current iff no write to the collection has been recorded
    /// since the read, checked the same way against the collection floor.
    pub(crate) fn read_is_valid(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: &ReadKeyIdent,
        read_lsn: Lsn,
    ) -> bool {
        // Collection-floor check shared by `Predicate` and the fallback for
        // untracked secondary-index dimensions: a read is current iff no write
        // to the collection has been recorded since it. `IndexEq` / `IndexRange`
        // consult the per-value substrate first and only fall back here when the
        // `(collection, field)` dimension is untracked.
        let collection_floor_current = || {
            let coll_key = CollKey {
                db,
                tenant,
                collection: Box::from(collection),
            };
            self.collection_write_lsn(&coll_key).unwrap_or(Lsn::ZERO) <= read_lsn
        };

        match key {
            ReadKeyIdent::Point(repr) => {
                let write_key = WriteKey {
                    db,
                    tenant,
                    collection: Box::from(collection),
                    key: repr.clone(),
                };
                self.key_write_lsn(&write_key).unwrap_or(Lsn::ZERO) <= read_lsn
            }
            ReadKeyIdent::Predicate => collection_floor_current(),
            ReadKeyIdent::IndexEq { field, value } => {
                match self
                    .index_values
                    .eq_max_lsn(db, tenant, collection, field, value)
                {
                    Some(max) => max <= read_lsn,
                    None => collection_floor_current(),
                }
            }
            ReadKeyIdent::IndexRange { field, lo, hi } => {
                match self.index_values.range_max_lsn(
                    db,
                    tenant,
                    collection,
                    field,
                    lo.as_deref(),
                    hi.as_deref(),
                ) {
                    Some(max) => max <= read_lsn,
                    None => collection_floor_current(),
                }
            }
        }
    }

    /// Horizon garbage-collect the per-key map against `watermark`.
    ///
    /// Evicts every entry whose LSN falls below `watermark - RETAIN_WINDOW`,
    /// then, if more than [`MAX_KEY_ENTRIES`] remain, drops the lowest-LSN
    /// entries until back within bound. The per-collection map is bounded by
    /// the live-collection count and is intentionally left untouched.
    pub fn gc(&mut self, watermark: Lsn) {
        let floor = watermark.as_u64().saturating_sub(RETAIN_WINDOW);
        self.last_write_lsn.retain(|_, lsn| lsn.as_u64() >= floor);

        if self.last_write_lsn.len() > MAX_KEY_ENTRIES {
            let overflow = self.last_write_lsn.len() - MAX_KEY_ENTRIES;
            // Drop the `overflow` oldest (lowest-LSN) entries.
            let mut by_lsn: Vec<(Lsn, WriteKey)> = self
                .last_write_lsn
                .iter()
                .map(|(k, lsn)| (*lsn, k.clone()))
                .collect();
            // TOTAL order so tied-LSN eviction is replica-identical: a plain
            // `sort_by_key(lsn)` over a `HashMap`-collected Vec would let the
            // dropped set depend on hash-iteration layout, diverging across
            // replicas. `DatabaseId`/`TenantId` lack `Ord` (compare via
            // `as_u64()`); `KeyRepr` is `Ord`.
            by_lsn.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| a.1.db.as_u64().cmp(&b.1.db.as_u64()))
                    .then_with(|| a.1.tenant.as_u64().cmp(&b.1.tenant.as_u64()))
                    .then_with(|| a.1.collection.cmp(&b.1.collection))
                    .then_with(|| a.1.key.cmp(&b.1.key))
            });
            for (_, key) in by_lsn.into_iter().take(overflow) {
                self.last_write_lsn.remove(&key);
            }
        }

        self.index_values.gc(watermark);
    }
}

impl CoreLoop {
    /// Record a committed write into the per-core version index and advance the
    /// core watermark monotonically.
    ///
    /// Called once per written key at every Data-Plane apply chokepoint, using
    /// the WAL LSN the Control Plane allocated at wal-dispatch and threaded onto
    /// the write task. `key` is `None` for engines whose per-key identity is
    /// internal (columnar / timeseries / array / spatial / FTS) — those record
    /// only the collection floor.
    ///
    /// In the install pass of a committed-redo apply the version waits in the
    /// apply scope. The apply publishes it once the record settled, so a
    /// rolled-back install leaves no version and no watermark behind.
    pub(in crate::data::executor) fn note_write_lsn(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        lsn: Lsn,
    ) {
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.defer_write_version(db, tenant, collection, key, lsn);
            return;
        }
        self.publish_write_version(db, tenant, collection, key, lsn);
    }

    /// Record a write version and advance the core watermark monotonically.
    pub(in crate::data::executor) fn publish_write_version(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        lsn: Lsn,
    ) {
        self.write_index
            .note_write_lsn(db, tenant, collection, key, lsn);
        if lsn > self.watermark {
            self.watermark = lsn;
        }
    }

    /// Record a committed write's collection floor only (no per-key entry),
    /// if a WAL LSN was threaded onto `task`. Shared by the columnar-family
    /// write handlers (columnar / timeseries / array / spatial / FTS) whose
    /// per-key identity is internal — a predicate reader validates against the
    /// collection floor when it owns no per-key version.
    pub(in crate::data::executor) fn note_collection_write_lsn(
        &mut self,
        task: &super::super::task::ExecutionTask,
        collection: &str,
    ) {
        if let Some(lsn) = task.wal_lsn() {
            self.note_write_lsn(
                task.request.database_id,
                task.request.tenant_id,
                collection,
                None,
                lsn,
            );
        }
    }

    /// Run horizon GC on the per-core version index. Invoked from the periodic
    /// maintenance hook — no dedicated timer.
    pub(in crate::data::executor) fn gc_write_index(&mut self) {
        self.write_index.gc(self.watermark);
    }

    /// Whether this shard's slice of a transaction's LSN-versioned read-set was
    /// still current against the local write versions.
    ///
    /// Filters the read-set to the entries whose collection homes to this
    /// request's vShard — the only reads this core holds versions for — then
    /// checks each against the per-core write-version index via
    /// [`WriteVersionIndex::read_is_valid`]. Short-circuits on the first entry
    /// that is no longer current. An empty or fully-remote slice is vacuously
    /// current (`true`). The `(database, tenant)` scope mirrors the write-version
    /// recorder so a read validates against the same key space it was recorded
    /// in; homing uses the same collection-in-database function the scheduler
    /// routes plans with.
    pub(in crate::data::executor) fn read_set_still_current(
        &self,
        task: &super::super::task::ExecutionTask,
        tid: u64,
        versioned_reads: &[VersionedReadEntry],
    ) -> bool {
        let db = task.request.database_id;
        let tenant = TenantId::new(tid);
        let local_vshard = task.request.vshard_id.as_u32();
        versioned_reads
            .iter()
            .filter(|entry| {
                VShardId::from_collection_in_database(db, &entry.collection).as_u32()
                    == local_vshard
            })
            .all(|entry| {
                self.write_index.read_is_valid(
                    db,
                    tenant,
                    &entry.collection,
                    &entry.key,
                    entry.read_lsn,
                )
            })
    }

    /// Record a committed document/vector write's version, keyed by the
    /// written row's cross-engine surrogate, if a WAL LSN was threaded onto
    /// `task`. Shared by every per-surrogate write chokepoint (point put,
    /// point insert, point delete, bulk update, bulk delete).
    pub(in crate::data::executor) fn note_surrogate_write_lsn(
        &mut self,
        task: &super::super::task::ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: u32,
    ) {
        if let Some(lsn) = task.wal_lsn() {
            self.note_write_lsn(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                Some(KeyRepr::Surrogate(surrogate)),
                lsn,
            );
        }
    }

    /// Record a committed WAL-replay write's version. A no-op when
    /// `record_lsn == 0` (no durable LSN was recorded for this write); `key`
    /// is `None` for collection-only entries (e.g. truncate) and `Some` for
    /// per-key/per-surrogate entries, exactly like [`Self::note_write_lsn`].
    ///
    /// Shared by every WAL replay chokepoint (KV, document, document-vector):
    /// unlike the live write path, replay only has the raw `(database_id,
    /// tenant_id, record_lsn)` off the WAL record header, not an
    /// `ExecutionTask` — hence the separate `u64`-typed entry point rather
    /// than reusing [`Self::note_surrogate_write_lsn`] /
    /// [`Self::note_collection_write_lsn`].
    pub(in crate::data::executor) fn note_replay_write_lsn(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: Option<KeyRepr>,
        record_lsn: u64,
    ) {
        if record_lsn != 0 {
            self.note_write_lsn(
                DatabaseId::new(database_id),
                TenantId::new(tenant_id),
                collection,
                key,
                Lsn::new(record_lsn),
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::calvin::EngineTag;
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::handlers::graph::EdgePutParams;
    use crate::data::executor::handlers::kv::crud::KvWriteParams;
    use crate::data::executor::handlers::point::put::PointPutExec;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{ReadConsistency, RequestId, TraceId, VShardId};

    fn db() -> DatabaseId {
        DatabaseId::DEFAULT
    }

    fn tenant() -> TenantId {
        TenantId::new(1)
    }

    #[test]
    fn point_read_is_valid_when_key_never_written() {
        let index = WriteVersionIndex::new();
        let key = ReadKeyIdent::Point(KeyRepr::Surrogate(7));
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn point_read_is_valid_when_write_at_or_before_read_lsn() {
        let mut index = WriteVersionIndex::new();
        index.note_write_lsn(
            db(),
            tenant(),
            "orders",
            Some(KeyRepr::Surrogate(7)),
            Lsn::new(10),
        );

        let key = ReadKeyIdent::Point(KeyRepr::Surrogate(7));
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(20)));
    }

    #[test]
    fn point_read_is_invalid_when_write_after_read_lsn() {
        let mut index = WriteVersionIndex::new();
        index.note_write_lsn(
            db(),
            tenant(),
            "orders",
            Some(KeyRepr::Surrogate(7)),
            Lsn::new(20),
        );

        let key = ReadKeyIdent::Point(KeyRepr::Surrogate(7));
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn predicate_read_is_valid_when_collection_never_written() {
        let index = WriteVersionIndex::new();
        let key = ReadKeyIdent::Predicate;
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn predicate_read_is_valid_when_floor_at_or_before_read_lsn() {
        let mut index = WriteVersionIndex::new();
        index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(10));

        let key = ReadKeyIdent::Predicate;
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(20)));
    }

    #[test]
    fn predicate_read_is_invalid_when_floor_after_read_lsn() {
        let mut index = WriteVersionIndex::new();
        index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(20));

        let key = ReadKeyIdent::Predicate;
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn untracked_index_dimension_falls_back_to_collection_floor() {
        // A `(collection, field)` never recorded in the per-value substrate is
        // untracked → `eq_max_lsn`/`range_max_lsn` return `None` → the validator
        // falls back to the collection floor, producing the SAME verdict as a
        // `Predicate` read for every floor position.
        let index_eq = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        let index_range = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("18".to_string()),
            hi: None,
        };
        let predicate = ReadKeyIdent::Predicate;

        // Floor below the read LSN (current), at it (current), and above it
        // (stale) — the untracked index variants track `Predicate` in every case.
        for (floor, read_lsn) in [(5u64, 10u64), (10, 10), (20, 10)] {
            let mut index = WriteVersionIndex::new();
            index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(floor));
            let want =
                index.read_is_valid(db(), tenant(), "orders", &predicate, Lsn::new(read_lsn));
            assert_eq!(
                index.read_is_valid(db(), tenant(), "orders", &index_eq, Lsn::new(read_lsn)),
                want,
                "untracked IndexEq must match Predicate (floor {floor}, read {read_lsn})"
            );
            assert_eq!(
                index.read_is_valid(db(), tenant(), "orders", &index_range, Lsn::new(read_lsn)),
                want,
                "untracked IndexRange must match Predicate (floor {floor}, read {read_lsn})"
            );
        }
    }

    #[test]
    fn index_eq_disjoint_write_does_not_abort() {
        // Read of email = "a@b.c" at read_lsn 10; a later write to a DIFFERENT
        // value on the same dimension must NOT abort the read (the coarse
        // collection floor would have).
        let mut index = WriteVersionIndex::new();
        // A real write to a disjoint value advances BOTH the collection floor
        // (which the coarse `Predicate` path would abort on) AND records the
        // per-value entry — so this proves the per-value check reduces the abort,
        // not merely that nothing was written.
        index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(20));
        index
            .index_values
            .record(db(), tenant(), "orders", "email", "z@z.z", Lsn::new(20));
        let key = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_range_disjoint_write_does_not_abort() {
        // Range [10, 20] read at read_lsn 10; a write to out-of-range "50" must
        // not abort.
        let mut index = WriteVersionIndex::new();
        // Advance the collection floor too (the coarse path aborts on it) so this
        // proves range validation reduces the abort, not just an empty index.
        index.note_write_lsn(db(), tenant(), "orders", None, Lsn::new(20));
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "50", Lsn::new(20));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_eq_same_value_conflict_aborts() {
        // A write to the SAME read value after the read LSN must abort.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "email", "a@b.c", Lsn::new(20));
        let key = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_range_in_range_added_value_conflict_aborts() {
        // An added value INSIDE the read range after the read LSN must abort.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "15", Lsn::new(20));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_range_in_range_removed_value_conflict_aborts() {
        // A delete of an in-range value also records that value's LSN, so a
        // removal inside the read range must abort the read just like an insert.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "17", Lsn::new(20));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(!index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn index_range_phantom_insert_aborts() {
        // Phantom protection: the range is current while it holds no in-range
        // value (tracked → `Some(ZERO)`), then a NEW in-range value recorded
        // after the read LSN invalidates it — proving the range captures the
        // predicate, not just values extant at read time.
        let mut index = WriteVersionIndex::new();
        // Track the dimension with an OUT-of-range value so the read starts
        // current (tracked, no in-range write).
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "99", Lsn::new(5));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(
            index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)),
            "range with no in-range write is current"
        );

        // Phantom insert inside the range after the read LSN.
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "15", Lsn::new(20));
        assert!(
            !index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)),
            "phantom in-range insert must abort"
        );
    }

    #[test]
    fn tracked_index_eq_missing_value_is_current() {
        // A tracked dimension (some other value recorded) queried for a value
        // with no entry returns `Some(ZERO)` → current.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "email", "other@x.y", Lsn::new(20));
        let key = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    #[test]
    fn tracked_index_range_empty_range_is_current() {
        // A tracked dimension queried for a range with no in-range entry returns
        // `Some(ZERO)` → current.
        let mut index = WriteVersionIndex::new();
        index
            .index_values
            .record(db(), tenant(), "orders", "age", "99", Lsn::new(20));
        let key = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(index.read_is_valid(db(), tenant(), "orders", &key, Lsn::new(10)));
    }

    // ── CoreLoop-level fixtures shared with the tests below and with other
    // executor test modules (`make_core_with_dir` / `make_default_task` are
    // `pub` for exactly that reason). ────────────────────────────────────────

    fn make_core() -> (
        CoreLoop,
        nodedb_bridge::buffer::Producer<BridgeRequest>,
        nodedb_bridge::buffer::Consumer<BridgeResponse>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .unwrap();
        (core, req_tx, resp_rx, dir)
    }

    pub fn make_core_with_dir(
        dir: &std::path::Path,
    ) -> (
        CoreLoop,
        nodedb_bridge::buffer::Producer<BridgeRequest>,
        nodedb_bridge::buffer::Consumer<BridgeResponse>,
    ) {
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .unwrap();
        (core, req_tx, resp_rx)
    }

    fn make_request(plan: PhysicalPlan) -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Admitted,
        }
    }

    /// A minimal `ExecutionTask` (DEFAULT database/tenant, vShard 0, no WAL LSN)
    /// for handler unit tests that only read `request.database_id`. The carried
    /// plan is inert — edge/point handlers take their parameters directly.
    pub fn make_default_task() -> crate::data::executor::task::ExecutionTask {
        crate::data::executor::task::ExecutionTask::new(make_request(PhysicalPlan::Document(
            DocumentOp::PointGet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
                document_id: "y".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            },
        )))
    }

    /// A msgpack-tagged `{k: v}` document body.
    fn doc_value(k: &str, v: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(k.to_string(), nodedb_types::Value::String(v.into()));
        zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap()
    }

    /// An `ExecutionTask` carrying a known WAL LSN, tenant 1 / database DEFAULT.
    fn wal_task(lsn: u64) -> ExecutionTask {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
            document_id: "y".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        ExecutionTask::with_wal_lsn(make_request(plan), Some(Lsn::new(lsn)))
    }

    fn surrogate_key(collection: &str, surrogate: u32) -> WriteKey {
        WriteKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from(collection),
            key: KeyRepr::Surrogate(surrogate),
        }
    }

    fn coll_key(collection: &str) -> CollKey {
        CollKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from(collection),
        }
    }

    #[test]
    fn point_put_records_write_version_and_advances_watermark() {
        let (mut core, _, _, _dir) = make_core();

        let task = wal_task(10);
        let resp = core.execute_point_put(
            &task,
            PointPutExec {
                tid: 1,
                collection: "orders",
                document_id: "o1",
                surrogate: Surrogate::new(7),
                value: &doc_value("a", "1"),
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(resp.status, Status::Ok);

        assert_eq!(
            core.write_index.key_write_lsn(&surrogate_key("orders", 7)),
            Some(Lsn::new(10))
        );
        assert_eq!(
            core.write_index.collection_write_lsn(&coll_key("orders")),
            Some(Lsn::new(10))
        );
        assert_eq!(core.watermark, Lsn::new(10));

        // Second write to the same key with a larger LSN overwrites monotonically.
        let task = wal_task(20);
        core.execute_point_put(
            &task,
            PointPutExec {
                tid: 1,
                collection: "orders",
                document_id: "o1",
                surrogate: Surrogate::new(7),
                value: &doc_value("a", "2"),
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(
            core.write_index.key_write_lsn(&surrogate_key("orders", 7)),
            Some(Lsn::new(20))
        );
        assert_eq!(
            core.write_index.collection_write_lsn(&coll_key("orders")),
            Some(Lsn::new(20))
        );
        assert_eq!(core.watermark, Lsn::new(20));

        // A lower LSN never regresses an existing entry or the watermark.
        let task = wal_task(15);
        core.execute_point_put(
            &task,
            PointPutExec {
                tid: 1,
                collection: "orders",
                document_id: "o1",
                surrogate: Surrogate::new(7),
                value: &doc_value("a", "3"),
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(
            core.write_index.key_write_lsn(&surrogate_key("orders", 7)),
            Some(Lsn::new(20))
        );
        assert_eq!(core.watermark, Lsn::new(20));

        // A second collection tracks its own max independently.
        let task = wal_task(30);
        core.execute_point_put(
            &task,
            PointPutExec {
                tid: 1,
                collection: "items",
                document_id: "i1",
                surrogate: Surrogate::new(9),
                value: &doc_value("a", "4"),
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(
            core.write_index.collection_write_lsn(&coll_key("items")),
            Some(Lsn::new(30))
        );
        assert_eq!(
            core.write_index.collection_write_lsn(&coll_key("orders")),
            Some(Lsn::new(20))
        );
        assert_eq!(core.watermark, Lsn::new(30));
    }

    #[test]
    fn kv_put_records_kvkey_version() {
        let (mut core, _, _, _dir) = make_core();
        let task = wal_task(42);
        let resp = core.execute_kv_put(
            &task,
            KvWriteParams {
                did: DatabaseId::DEFAULT.as_u64(),
                tid: 1,
                collection: "kv",
                key: b"k1".as_slice(),
                value: b"v1".as_slice(),
                ttl_ms: 0,
                surrogate: Surrogate::new(3),
                returning: None,
                rls_filters: &[],
            },
        );
        assert_eq!(resp.status, Status::Ok);

        let wk = WriteKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("kv"),
            key: KeyRepr::KvKey(Box::from(b"k1".as_slice())),
        };
        assert_eq!(core.write_index.key_write_lsn(&wk), Some(Lsn::new(42)));
        assert_eq!(
            core.write_index.collection_write_lsn(&coll_key("kv")),
            Some(Lsn::new(42))
        );
        assert_eq!(core.watermark, Lsn::new(42));
    }

    #[test]
    fn edge_put_records_edge_version() {
        let (mut core, _, _, _dir) = make_core();
        let task = wal_task(50);
        let resp = core.execute_edge_put(
            &task,
            EdgePutParams {
                tid: 1,
                collection: "graph",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                properties: &[],
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
            },
        );
        assert_eq!(resp.status, Status::Ok);

        let wk = WriteKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("graph"),
            key: KeyRepr::Edge {
                src: Box::from("a"),
                label: Box::from("KNOWS"),
                dst: Box::from("b"),
            },
        };
        assert_eq!(core.write_index.key_write_lsn(&wk), Some(Lsn::new(50)));
        assert_eq!(core.watermark, Lsn::new(50));
    }

    #[test]
    fn a_committed_transaction_records_its_write_versions() {
        let (mut core, _, _, _dir) = make_core();
        let task = wal_task(60);
        let plans = vec![PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "batch"),
            document_id: "d1".into(),
            value: doc_value("a", "1"),
            surrogate: Surrogate::new(11),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        })];
        let resp = core.commit_plans_for_test(&task, 1, &plans, 60);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

        assert_eq!(
            core.write_index.key_write_lsn(&surrogate_key("batch", 11)),
            Some(Lsn::new(60))
        );
        assert_eq!(
            core.write_index.collection_write_lsn(&coll_key("batch")),
            Some(Lsn::new(60))
        );
        assert_eq!(core.watermark, Lsn::new(60));
    }

    #[test]
    fn no_wal_lsn_records_nothing() {
        let (mut core, _, _, _dir) = make_core();
        // Task without a WAL LSN — the version index is skipped, not advanced.
        let task = ExecutionTask::new(make_request(PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
            document_id: "y".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        })));
        core.execute_point_put(
            &task,
            PointPutExec {
                tid: 1,
                collection: "orders",
                document_id: "o1",
                surrogate: Surrogate::new(7),
                value: &doc_value("a", "1"),
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );
        assert_eq!(
            core.write_index.key_write_lsn(&surrogate_key("orders", 7)),
            None
        );
        assert_eq!(core.watermark, Lsn::ZERO);
    }

    #[test]
    fn horizon_gc_evicts_stale_keys_keeps_recent_and_collection() {
        let (mut core, _, _, _dir) = make_core();
        let db = DatabaseId::DEFAULT;
        let tenant = TenantId::new(1);

        // A stale per-key entry, then a recent write that drives the watermark far
        // past the retain window.
        core.note_write_lsn(db, tenant, "c", Some(KeyRepr::Surrogate(1)), Lsn::new(10));
        core.note_write_lsn(
            db,
            tenant,
            "c",
            Some(KeyRepr::Surrogate(2)),
            Lsn::new(1_000_000),
        );
        assert_eq!(core.watermark, Lsn::new(1_000_000));

        core.gc_write_index();

        // Stale key evicted; recent key retained; collection floor survives GC.
        assert_eq!(core.write_index.key_write_lsn(&surrogate_key("c", 1)), None);
        assert_eq!(
            core.write_index.key_write_lsn(&surrogate_key("c", 2)),
            Some(Lsn::new(1_000_000))
        );
        assert_eq!(
            core.write_index.collection_write_lsn(&coll_key("c")),
            Some(Lsn::new(1_000_000))
        );
    }

    // ── Read-set validation against the write-version index (integration,
    // through `CoreLoop::read_set_still_current`) ──────────────────────────

    use nodedb_types::calvin::VersionedReadEntry;

    /// An `ExecutionTask` homing to `vshard_id`, carrying no WAL LSN.
    fn task_with_vshard(vshard_id: VShardId) -> ExecutionTask {
        ExecutionTask::new(Request {
            vshard_id,
            ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
                document_id: "y".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }))
        })
    }

    /// An `ExecutionTask` carrying WAL LSN `lsn` and homing to `vshard_id`.
    fn wal_task_with_vshard(lsn: u64, vshard_id: VShardId) -> ExecutionTask {
        ExecutionTask::with_wal_lsn(
            Request {
                vshard_id,
                ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
                    document_id: "y".into(),
                    surrogate: Surrogate::ZERO,
                    pk_bytes: Vec::new(),
                    rls_filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                }))
            },
            Some(Lsn::new(lsn)),
        )
    }

    /// The vShard `collection` homes to in the default database — mirrors the
    /// homing `read_set_still_current` filters entries by.
    fn local_vshard(collection: &str) -> VShardId {
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, collection)
    }

    /// Some vShard other than `than`, for exercising the cross-shard filter.
    fn other_vshard(than: VShardId) -> VShardId {
        VShardId::new((than.as_u32() + 1) % VShardId::COUNT)
    }

    fn point_entry(collection: &str, surrogate: u32, read_lsn: u64) -> VersionedReadEntry {
        VersionedReadEntry {
            engine: EngineTag::Document,
            collection: collection.to_string(),
            key: ReadKeyIdent::Point(KeyRepr::Surrogate(surrogate)),
            read_lsn: Lsn::new(read_lsn),
        }
    }

    fn predicate_entry(collection: &str, read_lsn: u64) -> VersionedReadEntry {
        VersionedReadEntry {
            engine: EngineTag::Document,
            collection: collection.to_string(),
            key: ReadKeyIdent::Predicate,
            read_lsn: Lsn::new(read_lsn),
        }
    }

    #[test]
    fn stale_point_read_is_detected_as_not_current() {
        let (mut core, _, _, _dir) = make_core();
        core.note_write_lsn(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "orders",
            Some(KeyRepr::Surrogate(7)),
            Lsn::new(20),
        );

        let task = task_with_vshard(local_vshard("orders"));
        let reads = vec![point_entry("orders", 7, 10)];
        assert!(!core.read_set_still_current(&task, 1, &reads));
    }

    #[test]
    fn fresh_point_read_is_still_current() {
        let (mut core, _, _, _dir) = make_core();
        core.note_write_lsn(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "orders",
            Some(KeyRepr::Surrogate(7)),
            Lsn::new(20),
        );

        let task = task_with_vshard(local_vshard("orders"));
        assert!(core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 20)]));
        assert!(core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 30)]));
    }

    #[test]
    fn read_entry_homing_to_a_different_vshard_is_filtered_out() {
        let (mut core, _, _, _dir) = make_core();
        core.note_write_lsn(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "orders",
            Some(KeyRepr::Surrogate(7)),
            Lsn::new(20),
        );

        let local = local_vshard("orders");
        let remote_task = task_with_vshard(other_vshard(local));
        // Would conflict (read_lsn 10 < write_lsn 20) if this shard owned the
        // entry's collection; it homes elsewhere, so it is filtered out of this
        // shard's slice and the vacuous (empty-after-filter) result is `true`.
        let reads = vec![point_entry("orders", 7, 10)];
        assert!(core.read_set_still_current(&remote_task, 1, &reads));
    }

    #[test]
    fn stale_predicate_read_is_detected_as_not_current() {
        let (mut core, _, _, _dir) = make_core();
        core.note_write_lsn(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "orders",
            None,
            Lsn::new(20),
        );

        let task = task_with_vshard(local_vshard("orders"));
        let reads = vec![predicate_entry("orders", 10)];
        assert!(!core.read_set_still_current(&task, 1, &reads));
    }

    #[test]
    fn fresh_predicate_read_is_still_current() {
        let (mut core, _, _, _dir) = make_core();
        core.note_write_lsn(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "orders",
            None,
            Lsn::new(20),
        );

        let task = task_with_vshard(local_vshard("orders"));
        let reads = vec![predicate_entry("orders", 20)];
        assert!(core.read_set_still_current(&task, 1, &reads));
    }

    #[test]
    fn empty_read_set_is_vacuously_current() {
        let (core, _, _, _dir) = make_core();
        let task = task_with_vshard(VShardId::new(0));
        assert!(core.read_set_still_current(&task, 1, &[]));
    }

    #[test]
    fn a_read_that_predates_a_committed_write_is_no_longer_current() {
        let (mut core, _, _, _dir) = make_core();
        let vshard = local_vshard("orders");

        // A committed write to key 7 in "orders" records its version at LSN 10.
        let write_task = wal_task_with_vshard(10, vshard);
        let write_plans = vec![PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            document_id: "o7".into(),
            value: doc_value("a", "1"),
            surrogate: Surrogate::new(7),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        })];
        let write_resp = core.commit_plans_for_test(&write_task, 1, &write_plans, 10);
        assert_eq!(write_resp.status, Status::Ok, "{:?}", write_resp.error_code);

        // A read of key 7 observed at LSN 5 predates the write; one observed
        // at LSN 10 does not.
        let task = task_with_vshard(vshard);
        assert!(!core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 5)]));
        assert!(core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 10)]));
    }
}
