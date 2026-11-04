// SPDX-License-Identifier: BUSL-1.1

//! The newest system-time ordinal of a collection slice or a vShard, read
//! from every engine's storage on this core.
//!
//! Each engine answers from its stored versions only:
//! - documents: every version key of the versioned table;
//! - KV: each table's live rows and ended versions;
//! - columnar and spatial: every row stamp of the mutation engine;
//! - timeseries: the newest `_ts_system` ordinal of the memtable and of
//!   every partition, so a flush never moves the answer, and every
//!   TRUNCATE end a partition carries;
//! - arrays: every stored tile version placed on the slice's vShard;
//! - graph edges: every edge version whose source or destination homes to
//!   the slice's vShard.

use nodedb_array::types::ArrayId;

use crate::types::{DatabaseId, TenantId, VShardId};

use super::super::CoreLoop;
use super::cache::{SliceKey, collection_vshard};

/// A stored `_ts_system` ordinal, or `None` for the `0` a store with no
/// `_ts_system` value reports.
fn ts_ordinal(ordinal: i64) -> Option<i64> {
    (ordinal > 0).then_some(ordinal)
}

impl CoreLoop {
    /// The newest ordinal `key` holds in storage. `Err` when a store does
    /// not read.
    pub(in crate::data::executor) fn stored_slice_newest(
        &self,
        key: &SliceKey,
    ) -> crate::Result<Option<i64>> {
        let SliceKey {
            vshard,
            db,
            tenant,
            collection,
        } = key;
        let collection: &str = collection;
        let mut newest = self.edge_store.collection_newest_on_home(
            db.as_u64(),
            tenant.as_u64(),
            collection,
            *vshard,
        )?;
        newest =
            newest.max(self.stored_array_tiles_newest(*vshard, |id| {
                names_array(id, *db, *tenant, collection)
            })?);
        if collection_vshard(*db, collection)? != *vshard {
            return Ok(newest);
        }
        newest = newest.max(self.sparse.versioned_collection_newest(
            db.as_u64(),
            tenant.as_u64(),
            collection,
        )?);
        newest = newest.max(self.kv_engine.collection_newest_ordinal(
            db.as_u64(),
            tenant.as_u64(),
            collection,
        ));
        let owner = (*db, *tenant, collection.to_string());
        newest = newest.max(
            self.columnar_engines
                .get(&owner)
                .and_then(|engine| engine.newest_ordinal()),
        );
        newest = newest.max(self.stored_timeseries_newest(&owner));
        Ok(newest)
    }

    /// The newest ordinal any slice of `vshard` holds in storage. Reads
    /// every store of the core. `Err` when a store does not read.
    pub(in crate::data::executor) fn stored_vshard_newest(
        &self,
        vshard: VShardId,
    ) -> crate::Result<Option<i64>> {
        let on_vshard = |db: DatabaseId, collection: &str| -> crate::Result<bool> {
            Ok(collection_vshard(db, collection)? == vshard)
        };
        let mut newest: Option<i64> = None;
        for row in self.edge_store.newest_by_home()? {
            if row.vshard == vshard {
                newest = newest.max(Some(row.newest));
            }
        }
        for row in self.sparse.versioned_newest_by_collection()? {
            if on_vshard(DatabaseId::new(row.database_id), &row.collection)? {
                newest = newest.max(Some(row.newest));
            }
        }
        for row in self.kv_engine.newest_ordinal_by_collection() {
            if on_vshard(row.database_id, &row.collection)? {
                newest = newest.max(Some(row.newest));
            }
        }
        for ((db, _, collection), engine) in &self.columnar_engines {
            if on_vshard(*db, collection)? {
                newest = newest.max(engine.newest_ordinal());
            }
        }
        let ts_owners = self
            .columnar_memtables
            .keys()
            .chain(self.ts_registries.keys());
        for owner in ts_owners {
            if on_vshard(owner.0, &owner.2)? {
                newest = newest.max(self.stored_timeseries_newest(owner));
            }
        }
        newest = newest.max(self.stored_array_tiles_newest(vshard, |_| true)?);
        Ok(newest)
    }

    /// The newest ordinal a timeseries collection holds: the newest
    /// `_ts_system` ordinal across its memtable and every partition, and the
    /// ordinal of every TRUNCATE end a partition carries.
    fn stored_timeseries_newest(&self, owner: &(DatabaseId, TenantId, String)) -> Option<i64> {
        let memtable = self
            .columnar_memtables
            .get(owner)
            .map(|memtable| memtable.flush_view().max_system_ts);
        let partitions = self.ts_registries.get(owner).and_then(|registry| {
            registry
                .iter()
                .map(|(_, entry)| {
                    entry
                        .meta
                        .max_system_ts
                        .max(entry.meta.ended_at.unwrap_or(0))
                })
                .max()
        });
        ts_ordinal(memtable.max(partitions)?)
    }
}

/// Whether `id` is the array `collection` of `db` and `tenant` names, by its
/// bare or its qualified name.
fn names_array(id: &ArrayId, db: DatabaseId, tenant: TenantId, collection: &str) -> bool {
    id.tenant_id == tenant
        && id.database_id == db
        && (id.name == collection
            || nodedb_types::QualifiedCollection::new(db, &id.name).as_str() == collection)
}
