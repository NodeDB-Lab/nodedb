// SPDX-License-Identifier: BUSL-1.1

//! Per-core, per-index write-VALUE version substrate.
//!
//! Sibling of [`super::write_index::WriteVersionIndex`]. That index records,
//! per written key/collection, the version of the last committed write; this
//! one records — per `(vshard, database, tenant, collection, field)`
//! secondary-index dimension — the last committed-write version against each
//! distinct indexed VALUE. An index-range read validates against the max
//! version over its value range instead of the coarse collection version.
//!
//! Presence of the outer dimension key is the monotonic "tracked" flag: once
//! any committed write records a value for a field, that field stays tracked
//! (horizon GC keeps the, possibly empty, inner map). An untracked field
//! answers [`IndexVersion::Untracked`] (the validator falls back to the
//! collection version). A tracked field MUST have had every committed
//! write's values recorded: completeness is the recording chokepoints'
//! contract.
//!
//! Lives on the `!Send` core: plain `HashMap`/`BTreeMap`, no atomics/locks.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use nodedb_types::{DatabaseId, TenantId, WriteVersion};

use crate::types::VShardId;

use super::CoreLoop;
use super::write_index::RETAIN_WINDOW;
use super::write_index::WriteStamp;

/// Hard upper bound on total per-value entries across every tracked dimension.
const MAX_INDEX_VALUE_ENTRIES: usize = 65_536;

/// Per-index dimension key: one secondary-index `field` of a
/// `(database, tenant, collection)` on one vShard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexKey {
    pub vshard: VShardId,
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: Box<str>,
    pub field: Box<str>,
}

/// What the substrate knows about the values a read observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexVersion {
    /// No committed write recorded a value of the dimension.
    Untracked,
    /// The dimension is tracked. The highest version of the values the read
    /// covers, `None` when none of them has a recorded version.
    Tracked(Option<WriteVersion>),
}

/// The identity of an index dimension expressed as borrowable parts, so a
/// `HashMap` keyed by owned [`IndexKey`] can be probed with borrowed `&str`s
/// without allocating an owned key per lookup.
trait IndexDim {
    fn parts(&self) -> (VShardId, DatabaseId, TenantId, &str, &str);
}

impl IndexDim for IndexKey {
    fn parts(&self) -> (VShardId, DatabaseId, TenantId, &str, &str) {
        (
            self.vshard,
            self.db,
            self.tenant,
            &self.collection,
            &self.field,
        )
    }
}

/// One index dimension, borrowed: the probe into the per-index map. Holds
/// `&str`s and allocates nothing.
#[derive(Debug, Clone, Copy)]
pub struct IndexDimRef<'a> {
    pub vshard: VShardId,
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: &'a str,
    pub field: &'a str,
}

impl IndexDim for IndexDimRef<'_> {
    fn parts(&self) -> (VShardId, DatabaseId, TenantId, &str, &str) {
        (
            self.vshard,
            self.db,
            self.tenant,
            self.collection,
            self.field,
        )
    }
}

impl Hash for dyn IndexDim + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (vshard, db, tenant, collection, field) = self.parts();
        vshard.hash(state);
        db.hash(state);
        tenant.hash(state);
        collection.hash(state);
        field.hash(state);
    }
}

impl PartialEq for dyn IndexDim + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn IndexDim + '_ {}

impl<'a> Borrow<dyn IndexDim + 'a> for IndexKey {
    fn borrow(&self) -> &(dyn IndexDim + 'a) {
        self
    }
}

impl Hash for IndexKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self as &dyn IndexDim).hash(state);
    }
}

/// Per-core, per-index write-VALUE version index.
#[derive(Default)]
pub struct IndexValueVersionIndex {
    per_index: HashMap<IndexKey, BTreeMap<Box<str>, WriteVersion>>,
}

impl IndexValueVersionIndex {
    /// Record a committed write of `value` on `dim` at `version`. Marks the
    /// dimension tracked if new; advances the value slot to
    /// `max(current, version)`.
    pub fn record(&mut self, dim: IndexDimRef<'_>, value: &str, version: WriteVersion) {
        let key = IndexKey {
            vshard: dim.vshard,
            db: dim.db,
            tenant: dim.tenant,
            collection: Box::from(dim.collection),
            field: Box::from(dim.field),
        };
        let values = self.per_index.entry(key).or_default();
        let slot = values.entry(Box::from(value)).or_default();
        *slot = (*slot).max(version);
    }

    /// The recorded version of an exact indexed value.
    pub(in crate::data::executor) fn eq_version(
        &self,
        dim: IndexDimRef<'_>,
        value: &str,
    ) -> IndexVersion {
        match self.dimension(dim) {
            None => IndexVersion::Untracked,
            Some(values) => IndexVersion::Tracked(values.get(value).copied()),
        }
    }

    /// The highest recorded version over the inclusive value range `[lo, hi]`
    /// (a `None` bound is open).
    pub(in crate::data::executor) fn range_version(
        &self,
        dim: IndexDimRef<'_>,
        lo: Option<&str>,
        hi: Option<&str>,
    ) -> IndexVersion {
        use std::ops::Bound;
        let Some(values) = self.dimension(dim) else {
            return IndexVersion::Untracked;
        };
        let lo_b = match lo {
            Some(s) => Bound::Included(s),
            None => Bound::Unbounded,
        };
        let hi_b = match hi {
            Some(s) => Bound::Included(s),
            None => Bound::Unbounded,
        };
        IndexVersion::Tracked(
            values
                .range::<str, _>((lo_b, hi_b))
                .map(|(_value, version)| *version)
                .max(),
        )
    }

    fn dimension(&self, dim: IndexDimRef<'_>) -> Option<&BTreeMap<Box<str>, WriteVersion>> {
        self.per_index.get(&dim as &dyn IndexDim)
    }

    /// Test accessor: the recorded version of a dimension's `value`, or
    /// `None` if the dimension or value is untracked.
    #[cfg(test)]
    pub(in crate::data::executor) fn value_version(
        &self,
        dim: IndexDimRef<'_>,
        value: &str,
    ) -> Option<WriteVersion> {
        self.dimension(dim)
            .and_then(|values| values.get(value).copied())
    }

    /// Forget every dimension of `vshard`: a snapshot install replaced its
    /// rows.
    pub(in crate::data::executor) fn drop_vshard(&mut self, vshard: VShardId) {
        self.per_index.retain(|key, _| key.vshard != vshard);
    }

    /// Horizon GC against each vShard's `latest` version, mirroring the
    /// per-key index. Evicts value entries more than the retain window below
    /// their vShard's latest version, keeping the (possibly empty) inner map
    /// so the dimension stays tracked. A count backstop drops the lowest
    /// versions under a TOTAL order so tied-version eviction is
    /// replica-identical. Returns the highest version evicted per vShard.
    pub fn gc(
        &mut self,
        latest: &HashMap<VShardId, WriteVersion>,
    ) -> HashMap<VShardId, WriteVersion> {
        let mut evicted: HashMap<VShardId, WriteVersion> = HashMap::new();
        let mut total = 0usize;
        for (key, values) in self.per_index.iter_mut() {
            let latest = latest.get(&key.vshard).copied();
            values.retain(|_, version| {
                let keep = latest
                    .and_then(|latest| latest.distance_above(*version))
                    .is_none_or(|distance| distance <= RETAIN_WINDOW);
                if !keep {
                    let slot = evicted.entry(key.vshard).or_default();
                    *slot = (*slot).max(*version);
                }
                keep
            });
            total += values.len();
        }
        if total > MAX_INDEX_VALUE_ENTRIES {
            let overflow = total - MAX_INDEX_VALUE_ENTRIES;
            let mut all: Vec<(WriteVersion, IndexKey, Box<str>)> = self
                .per_index
                .iter()
                .flat_map(|(k, values)| {
                    values
                        .iter()
                        .map(move |(v, version)| (*version, k.clone(), v.clone()))
                })
                .collect();
            all.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| a.1.vshard.cmp(&b.1.vshard))
                    .then_with(|| a.1.db.as_u64().cmp(&b.1.db.as_u64()))
                    .then_with(|| a.1.tenant.as_u64().cmp(&b.1.tenant.as_u64()))
                    .then_with(|| a.1.collection.cmp(&b.1.collection))
                    .then_with(|| a.1.field.cmp(&b.1.field))
                    .then_with(|| a.2.cmp(&b.2))
            });
            for (version, key, value) in all.into_iter().take(overflow) {
                let slot = evicted.entry(key.vshard).or_default();
                *slot = (*slot).max(version);
                if let Some(values) = self.per_index.get_mut(&key) {
                    values.remove(&value);
                }
            }
        }
        evicted
    }
}

impl CoreLoop {
    /// Record a committed document write's touched secondary-index values into
    /// the per-index substrate. `tuples` is the write's `(field_path, value)`
    /// pairs (added ∪ removed), already materialized by the apply path. No-op on
    /// empty `tuples`.
    pub(in crate::data::executor) fn note_index_write_values(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        tuples: &[(String, String)],
        stamp: WriteStamp,
    ) {
        if tuples.is_empty() {
            return;
        }
        let version = self.write_version_of(stamp);
        for (field, value) in tuples {
            let dim = IndexDimRef {
                vshard: stamp.vshard,
                db,
                tenant,
                collection,
                field,
            };
            self.write_index.index_values.record(dim, value, version);
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
        VShardId::new(3)
    }

    fn at(index: u64) -> WriteVersion {
        WriteVersion::logged(0, index)
    }

    fn key(field: &str) -> IndexKey {
        IndexKey {
            vshard: vshard(),
            db: db(),
            tenant: tenant(),
            collection: Box::from("orders"),
            field: Box::from(field),
        }
    }

    fn email() -> IndexDimRef<'static> {
        IndexDimRef {
            vshard: vshard(),
            db: db(),
            tenant: tenant(),
            collection: "orders",
            field: "email",
        }
    }

    fn record(index: &mut IndexValueVersionIndex, value: &str, version: WriteVersion) {
        index.record(email(), value, version);
    }

    fn latest(version: WriteVersion) -> HashMap<VShardId, WriteVersion> {
        HashMap::from([(vshard(), version)])
    }

    #[test]
    fn record_tracks_dimension_and_stores_value_version() {
        let mut index = IndexValueVersionIndex::default();
        record(&mut index, "a@b.c", at(10));
        let values = index
            .per_index
            .get(&key("email"))
            .expect("dimension tracked after record");
        assert_eq!(values.get("a@b.c"), Some(&at(10)));
    }

    #[test]
    fn record_is_monotonic_max_per_value() {
        let mut index = IndexValueVersionIndex::default();
        record(&mut index, "a@b.c", at(10));
        record(&mut index, "a@b.c", at(30));
        record(&mut index, "a@b.c", at(20));
        let values = index.per_index.get(&key("email")).expect("tracked");
        assert_eq!(values.get("a@b.c"), Some(&at(30)));
    }

    #[test]
    fn another_vshard_does_not_track_the_dimension() {
        let mut index = IndexValueVersionIndex::default();
        record(&mut index, "a@b.c", at(10));
        let other = IndexDimRef {
            vshard: VShardId::new(4),
            ..email()
        };
        assert_eq!(index.eq_version(other, "a@b.c"), IndexVersion::Untracked);
    }

    #[test]
    fn gc_evicts_old_values_keeps_dimension_tracked_and_reports_the_bound() {
        let mut index = IndexValueVersionIndex::default();
        record(&mut index, "old", at(1));
        record(&mut index, "new", at(RETAIN_WINDOW + 100));
        let evicted = index.gc(&latest(at(RETAIN_WINDOW + 100)));
        let values = index.per_index.get(&key("email")).expect("tracked");
        assert_eq!(values.get("old"), None);
        assert_eq!(values.get("new"), Some(&at(RETAIN_WINDOW + 100)));
        assert_eq!(evicted.get(&vshard()), Some(&at(1)));
    }

    #[test]
    fn gc_count_backstop_is_insert_order_independent() {
        let n = MAX_INDEX_VALUE_ENTRIES + 1;
        let version = at(1_000_000);

        let build = |ascending: bool| {
            let mut index = IndexValueVersionIndex::default();
            let mut order: Vec<usize> = (0..n).collect();
            if !ascending {
                order.reverse();
            }
            for i in order {
                record(&mut index, &format!("{i:08}"), version);
            }
            index.gc(&latest(version));
            index
        };

        let asc = build(true);
        let desc = build(false);

        assert_eq!(asc.per_index, desc.per_index);
        let dropped = format!("{:08}", 0);
        let survivor = format!("{:08}", 1);
        let values = asc.per_index.get(&key("email")).expect("tracked");
        assert_eq!(values.get(dropped.as_str()), None);
        assert_eq!(values.get(survivor.as_str()), Some(&version));
    }
}
