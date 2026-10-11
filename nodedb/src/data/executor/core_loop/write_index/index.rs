// SPDX-License-Identifier: BUSL-1.1

//! The per-core write-version index and its read validation.

use std::collections::HashMap;

use nodedb_types::calvin::ReadKeyIdent;
use nodedb_types::{DatabaseId, TenantId, WriteVersion};

use crate::types::VShardId;

use super::super::index_value_versions::{IndexDimRef, IndexValueVersionIndex, IndexVersion};
use super::keys::{CollKey, KeyRepr, WriteKey};
use super::record_stamps::RecordStamps;

/// Horizon retain window for per-key entries. Horizon GC evicts a
/// `last_write` entry more than this many log entries (or WAL LSNs, for
/// writes that apply no entry) below its vShard's latest version. Sized in
/// the same order of magnitude as the idempotency-cache cap (16,384
/// entries): a bounded recent-write history, enough to validate in-flight
/// transactions, not an unbounded write log.
pub const RETAIN_WINDOW: u64 = 16_384;

/// Hard upper bound on the `last_write` entry count. When horizon GC leaves
/// more entries than this, the lowest versions are dropped until the map is
/// back within bound.
const MAX_KEY_ENTRIES: usize = 65_536;

/// One vShard's version bounds on this core.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct VShardBounds {
    /// The highest version recorded for the vShard.
    latest: WriteVersion,
    /// The version the vShard's state holds without per-row versions: a
    /// snapshot install's cut, or the newest WAL record restart replay read.
    installed: WriteVersion,
    /// The highest per-key or per-value version horizon GC evicted.
    evicted: WriteVersion,
}

/// Per-core write-version index.
///
/// Every version is a position in one vShard's history, so every map is
/// keyed by vShard and versions of two vShards are never compared.
#[derive(Default)]
pub struct WriteVersionIndex {
    /// Last committed-write version per written key.
    last_write: HashMap<WriteKey, WriteVersion>,
    /// Last committed-write version per written collection (the phantom-safe
    /// floor: a predicate reader validates against it). Never GC'd: bounded
    /// by the live collections of the core's vShards.
    coll_write: HashMap<CollKey, WriteVersion>,
    /// Per-secondary-index-dimension write-VALUE versions, the finer-grained
    /// sibling of `coll_write` an index-range read validates against.
    pub(in crate::data::executor) index_values: IndexValueVersionIndex,
    /// Per-vShard bounds.
    vshards: HashMap<VShardId, VShardBounds>,
    /// The stamps of the WAL records the replay arms apply: every record
    /// during restart replay, the installed record during a committed redo
    /// install.
    pub(in crate::data::executor) record_stamps: RecordStamps,
}

impl WriteVersionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a committed write at `version` on `vshard`.
    ///
    /// Advances the collection's version and, when `key` is `Some`, the
    /// key's version, each to the max of its current value and `version`.
    /// Raises the vShard's latest version the same way.
    pub fn note_write(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        vshard: VShardId,
        collection: &str,
        key: Option<KeyRepr>,
        version: WriteVersion,
    ) {
        let coll_key = CollKey {
            vshard,
            db,
            tenant,
            collection: Box::from(collection),
        };
        let slot = self.coll_write.entry(coll_key).or_default();
        *slot = (*slot).max(version);

        if let Some(key) = key {
            let write_key = WriteKey {
                vshard,
                db,
                tenant,
                collection: Box::from(collection),
                key,
            };
            let slot = self.last_write.entry(write_key).or_default();
            *slot = (*slot).max(version);
        }
        self.raise_latest(vshard, version);
    }

    /// Raise `vshard`'s latest version to `version` when it is higher.
    pub(in crate::data::executor) fn raise_latest(
        &mut self,
        vshard: VShardId,
        version: WriteVersion,
    ) {
        let bounds = self.vshards.entry(vshard).or_default();
        bounds.latest = bounds.latest.max(version);
    }

    /// The vShard's state now holds every write through `version`, with no
    /// per-row versions: a snapshot install landed it. Every recorded version
    /// of the vShard is dropped, because the install replaced the rows they
    /// describe. A read older than `version` no longer validates.
    pub fn install(&mut self, vshard: VShardId, version: WriteVersion) {
        self.last_write.retain(|key, _| key.vshard != vshard);
        self.coll_write.retain(|key, _| key.vshard != vshard);
        self.index_values.drop_vshard(vshard);
        let bounds = self.vshards.entry(vshard).or_default();
        bounds.installed = bounds.installed.max(version);
        bounds.latest = bounds.latest.max(version);
    }

    /// The vShard's state holds writes through `version` that no per-row
    /// version describes: restart replay read records it did not apply. A
    /// read older than `version` no longer validates against a row with no
    /// recorded version.
    pub fn raise_installed(&mut self, vshard: VShardId, version: WriteVersion) {
        let bounds = self.vshards.entry(vshard).or_default();
        bounds.installed = bounds.installed.max(version);
        bounds.latest = bounds.latest.max(version);
    }

    /// Whether this core recorded any version of `vshard`.
    pub(in crate::data::executor) fn holds(&self, vshard: VShardId) -> bool {
        self.vshards.contains_key(&vshard)
    }

    /// The highest version recorded for `vshard`, `ZERO` when none is.
    pub(in crate::data::executor) fn latest(&self, vshard: VShardId) -> WriteVersion {
        self.vshards
            .get(&vshard)
            .map(|bounds| bounds.latest)
            .unwrap_or_default()
    }

    /// Every vShard this core recorded a version of, with its latest one.
    pub(in crate::data::executor) fn latest_by_vshard(
        &self,
    ) -> impl Iterator<Item = (VShardId, WriteVersion)> + '_ {
        self.vshards
            .iter()
            .map(|(vshard, bounds)| (*vshard, bounds.latest))
    }

    /// Recorded per-key version, if any.
    #[cfg(test)]
    pub(crate) fn key_version(&self, key: &WriteKey) -> Option<WriteVersion> {
        self.last_write.get(key).copied()
    }

    /// Recorded per-collection version, if any.
    #[cfg(test)]
    pub(crate) fn collection_version(&self, key: &CollKey) -> Option<WriteVersion> {
        self.coll_write.get(key).copied()
    }

    /// Every recorded per-key version.
    #[cfg(test)]
    pub(crate) fn recorded_keys(&self) -> impl Iterator<Item = (&WriteKey, WriteVersion)> {
        self.last_write.iter().map(|(key, version)| (key, *version))
    }

    /// Every recorded per-collection version.
    #[cfg(test)]
    pub(crate) fn recorded_collections(&self) -> impl Iterator<Item = (&CollKey, WriteVersion)> {
        self.coll_write.iter().map(|(key, version)| (key, *version))
    }

    /// The version of `collection` on `vshard` a read compares against: the
    /// recorded one, else the version the vShard's state was installed at.
    pub(in crate::data::executor) fn collection_current(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        vshard: VShardId,
        collection: &str,
    ) -> WriteVersion {
        let key = CollKey {
            vshard,
            db,
            tenant,
            collection: Box::from(collection),
        };
        self.coll_write
            .get(&key)
            .copied()
            .unwrap_or_else(|| self.bounds(vshard).installed)
    }

    fn bounds(&self, vshard: VShardId) -> VShardBounds {
        self.vshards.get(&vshard).copied().unwrap_or_default()
    }

    /// Whether a read of `collection` on `vshard`, observed at `read`, is
    /// still current.
    ///
    /// A `Point` read is current iff the key's version is at or below the
    /// read's. A `Predicate` read compares the collection's version. An
    /// index read compares the versions of the values it observed, and falls
    /// back to the collection's version for a dimension no write recorded.
    ///
    /// A key or value with no recorded version was not written since the
    /// vShard was installed, or GC evicted it. Its version is then at most
    /// the installed or evicted bound, and never above the collection's.
    pub(crate) fn read_is_valid(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        vshard: VShardId,
        collection: &str,
        key: &ReadKeyIdent,
        read: WriteVersion,
    ) -> bool {
        let collection_current = self.collection_current(db, tenant, vshard, collection);
        let bounds = self.bounds(vshard);
        let unrecorded = collection_current.min(bounds.installed.max(bounds.evicted));
        match key {
            ReadKeyIdent::Point(repr) => {
                let write_key = WriteKey {
                    vshard,
                    db,
                    tenant,
                    collection: Box::from(collection),
                    key: repr.clone(),
                };
                self.last_write
                    .get(&write_key)
                    .copied()
                    .unwrap_or(unrecorded)
                    <= read
            }
            ReadKeyIdent::Predicate => collection_current <= read,
            ReadKeyIdent::IndexEq { field, value } => {
                let dim = IndexDimRef {
                    vshard,
                    db,
                    tenant,
                    collection,
                    field,
                };
                match self.index_values.eq_version(dim, value) {
                    IndexVersion::Untracked => collection_current <= read,
                    IndexVersion::Tracked(recorded) => recorded.unwrap_or(unrecorded) <= read,
                }
            }
            ReadKeyIdent::IndexRange { field, lo, hi } => {
                let dim = IndexDimRef {
                    vshard,
                    db,
                    tenant,
                    collection,
                    field,
                };
                match self
                    .index_values
                    .range_version(dim, lo.as_deref(), hi.as_deref())
                {
                    IndexVersion::Untracked => collection_current <= read,
                    // A value GC evicted from the range is unknown, so the
                    // range takes the unrecorded bound as well.
                    IndexVersion::Tracked(recorded) => {
                        recorded.unwrap_or_default().max(unrecorded) <= read
                    }
                }
            }
        }
    }

    /// Horizon garbage-collect the per-key and per-value maps.
    ///
    /// Evicts every entry more than [`RETAIN_WINDOW`] below its vShard's
    /// latest version in the same epoch, then, if more than
    /// [`MAX_KEY_ENTRIES`] per-key entries remain, drops the lowest versions
    /// until back within bound. Each vShard's evicted bound rises to the
    /// highest version it lost. The per-collection map is left untouched.
    pub fn gc(&mut self) {
        let latest: HashMap<VShardId, WriteVersion> = self.latest_by_vshard().collect();
        let mut evicted: HashMap<VShardId, WriteVersion> = HashMap::new();
        self.last_write.retain(|key, version| {
            let keep = latest
                .get(&key.vshard)
                .and_then(|latest| latest.distance_above(*version))
                .is_none_or(|distance| distance <= RETAIN_WINDOW);
            if !keep {
                let slot = evicted.entry(key.vshard).or_default();
                *slot = (*slot).max(*version);
            }
            keep
        });

        if self.last_write.len() > MAX_KEY_ENTRIES {
            let overflow = self.last_write.len() - MAX_KEY_ENTRIES;
            let mut by_version: Vec<(WriteVersion, WriteKey)> = self
                .last_write
                .iter()
                .map(|(key, version)| (*version, key.clone()))
                .collect();
            // TOTAL order so tied-version eviction is replica-identical: a
            // plain sort by version over a `HashMap`-collected Vec would let
            // the dropped set depend on hash-iteration layout. `DatabaseId` /
            // `TenantId` lack `Ord` (compare via `as_u64()`); `KeyRepr` is
            // `Ord`.
            by_version.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| a.1.vshard.cmp(&b.1.vshard))
                    .then_with(|| a.1.db.as_u64().cmp(&b.1.db.as_u64()))
                    .then_with(|| a.1.tenant.as_u64().cmp(&b.1.tenant.as_u64()))
                    .then_with(|| a.1.collection.cmp(&b.1.collection))
                    .then_with(|| a.1.key.cmp(&b.1.key))
            });
            for (version, key) in by_version.into_iter().take(overflow) {
                let slot = evicted.entry(key.vshard).or_default();
                *slot = (*slot).max(version);
                self.last_write.remove(&key);
            }
        }

        for (vshard, version) in self.index_values.gc(&latest) {
            let slot = evicted.entry(vshard).or_default();
            *slot = (*slot).max(version);
        }
        for (vshard, version) in evicted {
            let bounds = self.vshards.entry(vshard).or_default();
            bounds.evicted = bounds.evicted.max(version);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> DatabaseId {
        DatabaseId::DEFAULT
    }

    fn tenant() -> TenantId {
        TenantId::new(1)
    }

    fn vshard() -> VShardId {
        VShardId::new(5)
    }

    fn at(index: u64) -> WriteVersion {
        WriteVersion::logged(0, index)
    }

    fn point(surrogate: u32) -> ReadKeyIdent {
        ReadKeyIdent::Point(KeyRepr::Surrogate(surrogate))
    }

    fn valid(index: &WriteVersionIndex, key: &ReadKeyIdent, read: u64) -> bool {
        index.read_is_valid(db(), tenant(), vshard(), "orders", key, at(read))
    }

    fn write(index: &mut WriteVersionIndex, key: Option<KeyRepr>, version: u64) {
        index.note_write(db(), tenant(), vshard(), "orders", key, at(version));
    }

    fn record_value(index: &mut WriteVersionIndex, field: &str, value: &str, version: u64) {
        let dim = IndexDimRef {
            vshard: vshard(),
            db: db(),
            tenant: tenant(),
            collection: "orders",
            field,
        };
        index.index_values.record(dim, value, at(version));
    }

    #[test]
    fn point_read_is_valid_when_key_never_written() {
        let index = WriteVersionIndex::new();
        assert!(valid(&index, &point(7), 10));
    }

    #[test]
    fn point_read_is_valid_when_write_at_or_before_read() {
        let mut index = WriteVersionIndex::new();
        write(&mut index, Some(KeyRepr::Surrogate(7)), 10);
        assert!(valid(&index, &point(7), 10));
        assert!(valid(&index, &point(7), 20));
    }

    #[test]
    fn point_read_is_invalid_when_write_after_read() {
        let mut index = WriteVersionIndex::new();
        write(&mut index, Some(KeyRepr::Surrogate(7)), 20);
        assert!(!valid(&index, &point(7), 10));
    }

    #[test]
    fn predicate_read_follows_the_collection_version() {
        let mut index = WriteVersionIndex::new();
        assert!(valid(&index, &ReadKeyIdent::Predicate, 10));
        write(&mut index, None, 10);
        assert!(valid(&index, &ReadKeyIdent::Predicate, 10));
        assert!(valid(&index, &ReadKeyIdent::Predicate, 20));
        write(&mut index, None, 20);
        assert!(!valid(&index, &ReadKeyIdent::Predicate, 10));
    }

    #[test]
    fn a_write_on_another_vshard_never_moves_a_read() {
        let mut index = WriteVersionIndex::new();
        index.note_write(
            db(),
            tenant(),
            VShardId::new(6),
            "orders",
            Some(KeyRepr::Surrogate(7)),
            at(900),
        );
        assert!(valid(&index, &point(7), 10));
        assert!(valid(&index, &ReadKeyIdent::Predicate, 10));
    }

    #[test]
    fn untracked_index_dimension_falls_back_to_collection_version() {
        let index_eq = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        let index_range = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("18".to_string()),
            hi: None,
        };
        for (floor, read) in [(5u64, 10u64), (10, 10), (20, 10)] {
            let mut index = WriteVersionIndex::new();
            write(&mut index, None, floor);
            let want = valid(&index, &ReadKeyIdent::Predicate, read);
            assert_eq!(valid(&index, &index_eq, read), want, "eq {floor} {read}");
            assert_eq!(
                valid(&index, &index_range, read),
                want,
                "range {floor} {read}"
            );
        }
    }

    #[test]
    fn a_disjoint_index_value_write_does_not_abort() {
        let mut index = WriteVersionIndex::new();
        write(&mut index, None, 20);
        record_value(&mut index, "email", "z@z.z", 20);
        record_value(&mut index, "age", "50", 20);
        let eq = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        let range = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(valid(&index, &eq, 10));
        assert!(valid(&index, &range, 10));
    }

    #[test]
    fn an_index_value_written_after_the_read_aborts() {
        let mut index = WriteVersionIndex::new();
        record_value(&mut index, "email", "a@b.c", 20);
        record_value(&mut index, "age", "15", 20);
        let eq = ReadKeyIdent::IndexEq {
            field: "email".to_string(),
            value: "a@b.c".to_string(),
        };
        let range = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(!valid(&index, &eq, 10));
        assert!(!valid(&index, &range, 10));
    }

    #[test]
    fn a_phantom_insert_into_a_read_range_aborts() {
        let mut index = WriteVersionIndex::new();
        record_value(&mut index, "age", "99", 5);
        let range = ReadKeyIdent::IndexRange {
            field: "age".to_string(),
            lo: Some("10".to_string()),
            hi: Some("20".to_string()),
        };
        assert!(valid(&index, &range, 10), "no in-range write");
        record_value(&mut index, "age", "15", 20);
        assert!(!valid(&index, &range, 10), "phantom in-range insert");
    }

    #[test]
    fn horizon_gc_evicts_stale_keys_and_an_evicted_key_bounds_old_reads() {
        let mut index = WriteVersionIndex::new();
        write(&mut index, Some(KeyRepr::Surrogate(1)), 10);
        write(&mut index, Some(KeyRepr::Surrogate(2)), 1_000_000);
        index.gc();

        let key = |surrogate| WriteKey {
            vshard: vshard(),
            db: db(),
            tenant: tenant(),
            collection: Box::from("orders"),
            key: KeyRepr::Surrogate(surrogate),
        };
        assert_eq!(index.key_version(&key(1)), None);
        assert_eq!(index.key_version(&key(2)), Some(at(1_000_000)));
        // The evicted key's version is unknown, so a read older than the
        // eviction bound no longer validates; a newer one does.
        assert!(!valid(&index, &point(1), 5));
        assert!(valid(&index, &point(1), 10));
    }

    #[test]
    fn an_install_drops_the_vshards_versions_and_bounds_every_read() {
        let mut index = WriteVersionIndex::new();
        write(&mut index, Some(KeyRepr::Surrogate(7)), 10);
        index.install(vshard(), at(50));
        assert_eq!(index.latest(vshard()), at(50));
        // The install replaced key 7: its version is unknown up to the cut.
        assert!(!valid(&index, &point(7), 20));
        assert!(!valid(&index, &ReadKeyIdent::Predicate, 20));
        assert!(valid(&index, &point(7), 50));
        assert!(valid(&index, &ReadKeyIdent::Predicate, 50));
    }
}
