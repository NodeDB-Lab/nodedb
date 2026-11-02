// SPDX-License-Identifier: BUSL-1.1

//! Identity-resolving iteration over a [`KvEngine`]'s live collections, plus the
//! per-collection index state a checkpoint has to carry with the rows.
//!
//! The engine keys its tables by an FxHash of `(database_id, tenant_id,
//! collection)`, which is not reversible. The checkpoint writer needs the
//! human-readable identity back so it can name one file per collection, so it
//! resolves each table through the `hash_to_database` / `hash_to_tenant` /
//! `hash_to_collection` reverse maps the engine maintains alongside `tables`.

use nodedb_types::DatabaseId;

use super::super::hash_table::KvHashTable;
use super::super::index::KvIndexSet;
use super::super::sorted_index::SortedIndexSnapshot;
use super::KvEngine;

/// One live KV collection paired with the identity its table is keyed by.
pub struct KvCollectionRef<'a> {
    /// Owning database.
    pub database_id: DatabaseId,
    /// Owning tenant.
    pub tenant_id: u64,
    /// Db-qualified collection name (`"{database_id}/{name}"` outside the
    /// default database, bare name inside it) — stored verbatim as the engine
    /// received it, so it hashes back to the same table key.
    pub collection: &'a str,
    /// The hashed `(database_id, tenant_id, collection)` key this collection's
    /// state is filed under, so the caller can reach its indexes without
    /// re-deriving the database id.
    pub table_key: u64,
    /// The collection's hash table, or `None` when it holds only index
    /// registrations — `CREATE INDEX` before the first `INSERT` leaves a
    /// collection that is real, checkpointable, and has no rows yet.
    pub table: Option<&'a KvHashTable>,
}

impl KvEngine {
    /// Iterate every live collection with its `(database_id, tenant_id,
    /// collection)` identity.
    ///
    /// Driven by the identity map rather than by `tables`, because a collection
    /// can hold index registrations before it holds a single row, and those
    /// registrations must reach the checkpoint too.
    ///
    /// Skips any entry whose database or tenant mapping is missing. That is
    /// unreachable while the maps are populated together on every path that creates a
    /// collection, but an unattributable collection cannot be given a checkpoint
    /// filename, so there is nothing this could do with it but skip.
    pub fn live_collections(&self) -> impl Iterator<Item = KvCollectionRef<'_>> {
        self.hash_to_collection.iter().filter_map(|(tkey, name)| {
            Some(KvCollectionRef {
                database_id: *self.hash_to_database.get(tkey)?,
                tenant_id: *self.hash_to_tenant.get(tkey)?,
                collection: name.as_str(),
                table_key: *tkey,
                table: self.tables.get(tkey),
            })
        })
    }

    /// The secondary (single-field and composite) index registrations filed
    /// under `table_key`, or `None` when the collection has none.
    pub fn index_set(&self, table_key: u64) -> Option<&KvIndexSet> {
        self.indexes.get(&table_key)
    }

    /// Every sorted index registered on `table_key`, with its full content.
    pub fn sorted_index_snapshots(&self, table_key: u64) -> Vec<SortedIndexSnapshot<'_>> {
        self.sorted_indexes.export_for_table(table_key)
    }
}
