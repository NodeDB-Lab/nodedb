// SPDX-License-Identifier: BUSL-1.1

//! The per-core cache of the newest system-time ordinal each collection
//! slice and each vShard holds.
//!
//! A slice is the part of one collection that lives on one vShard: the whole
//! collection for every engine but graph edges, whose versions live on the
//! home vShard of each endpoint. A slice's newest ordinal is a pure function
//! of the stored versions: the highest `sys_from` (or delete ordinal) any
//! version of the slice holds, live or tombstone. A vShard's newest ordinal
//! is the highest over its slices.
//!
//! The cache holds a value only while it equals that function. A committed
//! keyed write raises its slice to the row's newest stored version. A write
//! with no per-key identity, a removal (purge, drop, truncate of a whole
//! store), a snapshot install, and core open forget values, and the next
//! read recomputes them from storage. So every replica that stored the same
//! versions answers alike, whatever it cached, and a write whose install was
//! rolled back never raised anything: its version notes publish only once
//! the install settled.

use std::collections::HashMap;

use crate::types::{DatabaseId, TenantId, VShardId};

/// One collection's part on one vShard.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::data::executor) struct SliceKey {
    pub vshard: VShardId,
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: Box<str>,
}

/// Known newest ordinals. An absent entry is unknown. `Some(None)` is a
/// slice or vShard known to hold no version.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct NewestOrdinals {
    slices: HashMap<SliceKey, Option<i64>>,
    vshards: HashMap<VShardId, Option<i64>>,
}

impl NewestOrdinals {
    /// The cached newest ordinal of `key`. `None` when unknown.
    pub(in crate::data::executor) fn slice(&self, key: &SliceKey) -> Option<Option<i64>> {
        self.slices.get(key).copied()
    }

    /// Cache the newest ordinal of `key`, read from storage.
    pub(in crate::data::executor) fn set_slice(&mut self, key: SliceKey, newest: Option<i64>) {
        self.slices.insert(key, newest);
    }

    /// The cached newest ordinal of `vshard`. `None` when unknown.
    pub(in crate::data::executor) fn vshard(&self, vshard: VShardId) -> Option<Option<i64>> {
        self.vshards.get(&vshard).copied()
    }

    /// Cache the newest ordinal of `vshard`, read from storage.
    pub(in crate::data::executor) fn set_vshard(&mut self, vshard: VShardId, newest: Option<i64>) {
        self.vshards.insert(vshard, newest);
    }

    /// A committed version at `ordinal` now belongs to `key`. Raises the
    /// slice and its vShard where their values are known. An unknown value
    /// stays unknown: the next read recomputes it with the version.
    pub(in crate::data::executor) fn raise(&mut self, key: &SliceKey, ordinal: i64) {
        if let Some(slot) = self.slices.get_mut(key) {
            *slot = (*slot).max(Some(ordinal));
        }
        if let Some(slot) = self.vshards.get_mut(&key.vshard) {
            *slot = (*slot).max(Some(ordinal));
        }
    }

    /// Forget every slice of `collection` and the vShards they lie on, and
    /// `home`, the collection's own vShard.
    pub(in crate::data::executor) fn forget_collection(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        home: VShardId,
    ) {
        let vshards = &mut self.vshards;
        vshards.remove(&home);
        self.slices.retain(|key, _| {
            let same = key.db == db && key.tenant == tenant && &*key.collection == collection;
            if same {
                vshards.remove(&key.vshard);
            }
            !same
        });
    }

    /// Forget the value of `vshard`.
    pub(in crate::data::executor) fn forget_vshard(&mut self, vshard: VShardId) {
        self.vshards.remove(&vshard);
    }

    /// Forget every value.
    pub(in crate::data::executor) fn clear(&mut self) {
        self.slices.clear();
        self.vshards.clear();
    }
}

/// The vShard `collection` of database `db` lives on. A name without its
/// database's qualifier is a bare name.
pub(in crate::data::executor) fn collection_vshard(db: DatabaseId, collection: &str) -> VShardId {
    nodedb_types::CollectionKey::from_qualified_str(db, collection)
        .unwrap_or_else(|_| nodedb_types::CollectionKey::from_bare(db, collection))
        .vshard()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(collection: &str, vshard: u32) -> SliceKey {
        SliceKey {
            vshard: VShardId::new(vshard),
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from(collection),
        }
    }

    #[test]
    fn a_raise_moves_known_values_only() {
        let mut cache = NewestOrdinals::default();
        cache.raise(&key("c", 3), 50);
        assert_eq!(cache.slice(&key("c", 3)), None, "unknown stays unknown");

        cache.set_slice(key("c", 3), None);
        cache.set_vshard(VShardId::new(3), Some(10));
        cache.raise(&key("c", 3), 50);
        assert_eq!(cache.slice(&key("c", 3)), Some(Some(50)));
        assert_eq!(cache.vshard(VShardId::new(3)), Some(Some(50)));

        cache.raise(&key("c", 3), 20);
        assert_eq!(
            cache.slice(&key("c", 3)),
            Some(Some(50)),
            "an older version never lowers the newest"
        );
    }

    #[test]
    fn forgetting_a_collection_forgets_its_slices_and_their_vshards() {
        let mut cache = NewestOrdinals::default();
        cache.set_slice(key("c", 3), Some(5));
        cache.set_slice(key("c", 4), Some(6));
        cache.set_slice(key("d", 5), Some(7));
        for v in [3, 4, 5, 9] {
            cache.set_vshard(VShardId::new(v), Some(1));
        }
        cache.forget_collection(DatabaseId::DEFAULT, TenantId::new(1), "c", VShardId::new(9));
        assert_eq!(cache.slice(&key("c", 3)), None);
        assert_eq!(cache.slice(&key("c", 4)), None);
        assert_eq!(cache.slice(&key("d", 5)), Some(Some(7)));
        assert_eq!(cache.vshard(VShardId::new(3)), None);
        assert_eq!(cache.vshard(VShardId::new(9)), None);
        assert_eq!(cache.vshard(VShardId::new(5)), Some(Some(1)));
    }

    #[test]
    fn a_qualified_name_names_its_database() {
        assert_eq!(
            collection_vshard(DatabaseId::new(7), "7/orders"),
            nodedb_types::CollectionKey::from_bare(DatabaseId::new(7), "orders").vshard()
        );
    }
}
