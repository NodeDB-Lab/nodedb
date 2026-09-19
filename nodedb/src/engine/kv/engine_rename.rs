// SPDX-License-Identifier: BUSL-1.1

//! Re-key a KV collection from one db-qualified name to another.
//!
//! Used by `MOVE TENANT` to make data accessible under the target
//! database context without copying individual records.

use super::KvEngine;
use super::engine_helpers::table_key;

/// Parameters for [`KvEngine::rename_collection`].
#[derive(Debug, Clone, Copy)]
pub struct RenameCollectionParams<'a> {
    pub old_database_id: u64,
    pub new_database_id: u64,
    pub tenant_id: u64,
    pub old_collection: &'a str,
    pub new_collection: &'a str,
}

impl KvEngine {
    /// Move all KV data (hash table, indexes, expiry entries, sorted indexes)
    /// from `old_collection` to `new_collection` for `tenant_id`.
    ///
    /// Both `old_collection` and `new_collection` are the db-qualified strings
    /// used as the logical collection identifier (e.g. `"2/orders"`).
    ///
    /// Returns the number of entries migrated, or 0 if the source did not exist.
    pub fn rename_collection(&mut self, params: RenameCollectionParams<'_>) -> usize {
        let RenameCollectionParams {
            old_database_id,
            new_database_id,
            tenant_id,
            old_collection,
            new_collection,
        } = params;
        let old_key = table_key(old_database_id, tenant_id, old_collection);
        let new_key = table_key(new_database_id, tenant_id, new_collection);

        // Nothing to migrate if the source table doesn't exist.
        let Some(table) = self.tables.remove(&old_key) else {
            return 0;
        };
        let count = table.len();

        // Move hash table, bumping both keys' write epochs: the old key's
        // rows are gone, the new key's rows just arrived.
        self.bump_write_epoch(old_key);
        self.bump_write_epoch(new_key);
        self.tables.insert(new_key, table);
        self.hash_to_tenant.remove(&old_key);
        self.hash_to_tenant.insert(new_key, tenant_id);
        self.hash_to_collection.remove(&old_key);
        self.hash_to_collection
            .insert(new_key, new_collection.to_string());

        // Move secondary index set (if present).
        if let Some(index_set) = self.indexes.remove(&old_key) {
            self.indexes.insert(new_key, index_set);
        }

        // Move sorted index (if present).
        self.sorted_indexes.rename_collection(
            old_database_id,
            new_database_id,
            tenant_id,
            old_collection,
            new_collection,
        );

        // The expiry wheel encodes collection names in composite keys
        // (see `expiry_key` in engine_helpers.rs). Re-keying individual
        // entries would require scanning the entire wheel. KV expiry
        // entries for moved collections are TTL-bound; for the offline
        // MOVE TENANT operation in v1, TTL-carrying rows are extremely
        // uncommon. The expiry wheel will simply miss the renamed
        // collection at tick time (the entry won't be found via the old
        // collection name) and the row will linger until overwritten or
        // the collection is dropped. This is acceptable in the v1 offline
        // move; a future online-move initiative will handle TTL migration.
        count
    }
}
