// SPDX-License-Identifier: BUSL-1.1

//! SortedIndexManager: manages all sorted indexes for a KV engine core.
//!
//! Each sorted index is identified by `(tenant_id, collection, index_name)`.
//! The manager handles:
//! - Registration and dropping of sorted indexes
//! - Auto-maintenance on KV PUT/DELETE (updating sort keys in the tree)
//! - Query dispatch (rank, top_k, range, count)
//! - Rebuild from existing KV data (backfill)

use std::collections::{BTreeSet, HashMap};

use super::index::SortedIndex;
use super::key::SortKeyEncoder;
use super::window::WindowConfig;

/// Definition of a sorted index (metadata).
#[derive(Debug, Clone)]
pub struct SortedIndexDef {
    /// Index name (e.g., "lb_global").
    pub name: String,
    /// Collection this index covers.
    pub collection: String,
    /// Column used as the primary key in the sorted index (e.g., "player_id").
    pub key_column: String,
    /// Sort key encoder (columns + directions).
    pub encoder: SortKeyEncoder,
    /// Time-window configuration (optional).
    pub window: WindowConfig,
}

/// Manages all sorted indexes on a single TPC core.
///
/// Key: `(tenant_hash, index_name)` where tenant_hash is the same hash
/// used by KvEngine to scope tables by tenant+collection.
#[derive(Debug)]
pub struct SortedIndexManager {
    /// All sorted indexes. Key: `"{tenant_id}:{index_name}"`.
    pub(super) indexes: HashMap<String, SortedIndex>,
    /// Reverse map: collection table key → the index keys built over it. Used
    /// to find which sorted indexes to update on PUT/DELETE.
    ///
    /// A set, not a list: an index key appearing twice would make every PUT do
    /// its work twice and would export the same index twice into a checkpoint,
    /// which restore then reinstates twice. Nothing here can detect that after
    /// the fact, so the collection cannot hold the duplicate in the first
    /// place. `BTreeSet` also fixes iteration order, which the checkpoint
    /// export walks — a `HashSet` would reorder a collection's indexes between
    /// generations for no reason.
    pub(super) collection_indexes: HashMap<u64, BTreeSet<String>>,
}

impl SortedIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            collection_indexes: HashMap::new(),
        }
    }

    /// Register a sorted index, rebuilding it from `existing_entries`. Returns
    /// the number of entries backfilled.
    ///
    /// `existing_entries` is an iterator of `(primary_key_bytes, value_bytes)`
    /// pairs from the KV hash table, used to populate the index from existing
    /// data.
    ///
    /// Registering a name that already exists REPLACES it — the previous tree
    /// and its binding are dropped first (via [`Self::drop`]) and the index is
    /// rebuilt from the rows handed in. It is not an error, deliberately:
    ///
    /// * WAL replay legitimately re-applies a `kv_register_sorted_index` record
    ///   over a registration a checkpoint already restored, and the call site
    ///   there consumes a backfill count, not a `Result` — making this an error
    ///   would either fail replay or need replay to special-case it.
    /// * Rejecting a duplicate `CREATE SORTED INDEX` is the catalog's job, and
    ///   the index registry already owns that record; a second gate here would
    ///   be a second source of truth for the same rule.
    ///
    /// Dropping first is also what makes re-registering under a DIFFERENT
    /// collection correct: without it the old collection keeps a binding to
    /// this index, and its next PUT would splice rows from the wrong collection
    /// into the rebuilt tree.
    pub fn register(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        def: SortedIndexDef,
        existing_entries: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> u32 {
        self.drop(database_id, tenant_id, &def.name);

        let idx_key = index_key(database_id, tenant_id, &def.name);
        let tbl_key =
            super::super::engine_helpers::table_key(database_id, tenant_id, &def.collection);

        let (index, backfilled) = SortedIndex::build(def, existing_entries);

        self.collection_indexes
            .entry(tbl_key)
            .or_default()
            .insert(idx_key.clone());

        self.indexes.insert(idx_key, index);
        backfilled
    }

    /// Drop every sorted index belonging to `(tenant_id, collection)`.
    /// Returns the number of indexes removed.
    pub fn purge_collection(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> usize {
        let tbl_key = super::super::engine_helpers::table_key(database_id, tenant_id, collection);
        let idx_keys = self.collection_indexes.remove(&tbl_key).unwrap_or_default();
        let mut removed = 0;
        for idx_key in &idx_keys {
            if self.indexes.remove(idx_key).is_some() {
                removed += 1;
            }
        }
        removed
    }

    /// Drop a sorted index. Returns `true` if it existed.
    pub fn drop(&mut self, database_id: u64, tenant_id: u64, index_name: &str) -> bool {
        let idx_key = index_key(database_id, tenant_id, index_name);

        let Some(idx) = self.indexes.remove(&idx_key) else {
            return false;
        };

        let tbl_key =
            super::super::engine_helpers::table_key(database_id, tenant_id, &idx.def.collection);
        if let Some(bound) = self.collection_indexes.get_mut(&tbl_key) {
            bound.remove(&idx_key);
        }

        true
    }

    /// Called on every KV PUT. Updates all sorted indexes on this collection.
    ///
    /// `field_values` is a map of field_name → field_value_bytes extracted from
    /// the MessagePack value.
    pub fn on_put(
        &mut self,
        table_key: u64,
        primary_key: &[u8],
        field_values: &[(String, Vec<u8>)],
    ) {
        let Some(idx_keys) = self.collection_indexes.get(&table_key) else {
            return;
        };

        // Clone keys to avoid borrow conflict with self.indexes.
        let idx_keys: Vec<String> = idx_keys.iter().cloned().collect();
        for idx_key in &idx_keys {
            let Some(idx) = self.indexes.get_mut(idx_key) else {
                continue;
            };

            if let Some(sort_key) = build_sort_key_from_fields(&idx.def, field_values) {
                idx.tree.insert(sort_key, primary_key.to_vec());
            }
        }
    }

    /// Called on every KV DELETE. Removes entries from all sorted indexes.
    pub fn on_delete(&mut self, table_key: u64, primary_key: &[u8]) {
        let Some(idx_keys) = self.collection_indexes.get(&table_key) else {
            return;
        };

        // Clone keys to avoid borrow conflict with self.indexes.
        let idx_keys: Vec<String> = idx_keys.iter().cloned().collect();
        for idx_key in &idx_keys {
            if let Some(idx) = self.indexes.get_mut(idx_key) {
                idx.tree.remove(primary_key);
            }
        }
    }

    /// Check if any sorted indexes exist for a table key.
    pub fn has_indexes(&self, table_key: u64) -> bool {
        self.collection_indexes
            .get(&table_key)
            .is_some_and(|v| !v.is_empty())
    }

    /// Re-key all sorted indexes from `old_collection` to `new_collection`
    /// for `tenant_id`.  Used by `MOVE TENANT` to make sorted indexes
    /// accessible under the target database context.
    pub fn rename_collection(
        &mut self,
        old_database_id: u64,
        new_database_id: u64,
        tenant_id: u64,
        old_collection: &str,
        new_collection: &str,
    ) {
        use super::super::engine_helpers::table_key;

        let old_key = table_key(old_database_id, tenant_id, old_collection);
        let new_key = table_key(new_database_id, tenant_id, new_collection);

        let Some(index_names) = self.collection_indexes.remove(&old_key) else {
            return;
        };

        // Update each index's `def.collection` to the new name so future
        // writes route correctly.
        for name in &index_names {
            if let Some(idx) = self.indexes.get_mut(name) {
                idx.def.collection = new_collection.to_string();
            }
        }

        self.collection_indexes.insert(new_key, index_names);
    }

    // ── Query methods ──────────────────────────────────────────────────

    /// Get the 1-based rank of a primary key in a sorted index.
    ///
    /// For windowed indexes, only entries within the current window are counted.
    pub fn rank(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        primary_key: &[u8],
        now_ms: u64,
    ) -> Option<u32> {
        self.get_index(database_id, tenant_id, index_name)?
            .rank(primary_key, now_ms)
    }

    /// Get the top K entries from a sorted index.
    ///
    /// Returns `(rank, primary_key)` pairs.
    pub fn top_k(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        k: u32,
        now_ms: u64,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        Some(
            self.get_index(database_id, tenant_id, index_name)?
                .top_k(k, now_ms),
        )
    }

    /// Get entries in a score range from a sorted index. See
    /// [`SortedIndex::range`] for what the bounds hold.
    ///
    /// Returns `(rank, primary_key)` pairs.
    pub fn range(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        score_min: Option<&[u8]>,
        score_max: Option<&[u8]>,
        now_ms: u64,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        Some(
            self.get_index(database_id, tenant_id, index_name)?
                .range(score_min, score_max, now_ms),
        )
    }

    /// Get the total count of entries in a sorted index.
    pub fn count(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        now_ms: u64,
    ) -> Option<u32> {
        Some(
            self.get_index(database_id, tenant_id, index_name)?
                .count(now_ms),
        )
    }

    /// Get the sort key for a primary key in a sorted index (ZSCORE equivalent).
    pub fn score(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        primary_key: &[u8],
    ) -> Option<Vec<u8>> {
        self.get_index(database_id, tenant_id, index_name)?
            .score(primary_key)
    }

    /// Get the index definition.
    pub fn get_def(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
    ) -> Option<&SortedIndexDef> {
        Some(self.get_index(database_id, tenant_id, index_name)?.def())
    }

    fn get_index(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
    ) -> Option<&SortedIndex> {
        let idx_key = index_key(database_id, tenant_id, index_name);
        self.indexes.get(&idx_key)
    }
}

impl Default for SortedIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

pub(super) fn index_key(database_id: u64, tenant_id: u64, index_name: &str) -> String {
    format!("{database_id}:{tenant_id}:{index_name}")
}

/// Build a sort key from pre-extracted field name/value pairs.
fn build_sort_key_from_fields(
    def: &SortedIndexDef,
    field_values: &[(String, Vec<u8>)],
) -> Option<Vec<u8>> {
    let mut values: Vec<Vec<u8>> = Vec::with_capacity(def.encoder.column_count());

    for col in def.encoder.columns() {
        let val_bytes = field_values
            .iter()
            .find(|(name, _)| name == &col.name)
            .map(|(_, v)| v.clone())?;

        // The field value bytes from extract_all_field_values_from_msgpack are
        // already encoded as sortable bytes (integers as big-endian u64 with
        // sign-bit flip, strings as UTF-8). Use them directly.
        values.push(val_bytes);
    }

    let refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
    Some(def.encoder.encode(&refs))
}

#[cfg(test)]
mod tests {
    use super::super::key::{SortColumn, SortDirection};
    use super::*;

    fn make_def(name: &str, collection: &str) -> SortedIndexDef {
        SortedIndexDef {
            name: name.into(),
            collection: collection.into(),
            key_column: "player_id".into(),
            encoder: SortKeyEncoder::new(vec![SortColumn {
                name: "score".into(),
                direction: SortDirection::Desc,
            }]),
            window: WindowConfig::none(),
        }
    }

    fn make_entry(player_id: &str, score: i64) -> (Vec<u8>, Vec<u8>) {
        let pk = player_id.as_bytes().to_vec();
        let value = nodedb_types::json_to_msgpack(&serde_json::json!({
            "player_id": player_id,
            "score": score,
        }))
        .unwrap();
        (pk, value)
    }

    #[test]
    fn register_and_backfill() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        let entries = vec![
            make_entry("alice", 100),
            make_entry("bob", 200),
            make_entry("charlie", 150),
        ];
        let count = mgr.register(0, 1, def, entries.into_iter());
        assert_eq!(count, 3);
        assert_eq!(mgr.count(0, 1, "lb", 0), Some(3));
    }

    #[test]
    fn rank_with_desc_score() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        let entries = vec![
            make_entry("alice", 100),
            make_entry("bob", 300),
            make_entry("charlie", 200),
        ];
        mgr.register(0, 1, def, entries.into_iter());

        // DESC: bob(300) = rank 1, charlie(200) = rank 2, alice(100) = rank 3
        assert_eq!(mgr.rank(0, 1, "lb", b"bob", 0), Some(1));
        assert_eq!(mgr.rank(0, 1, "lb", b"charlie", 0), Some(2));
        assert_eq!(mgr.rank(0, 1, "lb", b"alice", 0), Some(3));
    }

    #[test]
    fn top_k() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        let entries = vec![
            make_entry("alice", 100),
            make_entry("bob", 300),
            make_entry("charlie", 200),
        ];
        mgr.register(0, 1, def, entries.into_iter());

        let top2 = mgr.top_k(0, 1, "lb", 2, 0).unwrap();
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0], (1, b"bob".to_vec()));
        assert_eq!(top2[1], (2, b"charlie".to_vec()));
    }

    #[test]
    fn on_put_updates_index() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        mgr.register(0, 1, def, std::iter::empty());

        let tbl_key = super::super::super::engine_helpers::table_key(0, 1, "scores");

        // Simulate PUTs.
        let score_bytes = SortKeyEncoder::encode_i64(100).to_vec();
        mgr.on_put(tbl_key, b"alice", &[("score".into(), score_bytes)]);

        let score_bytes = SortKeyEncoder::encode_i64(200).to_vec();
        mgr.on_put(tbl_key, b"bob", &[("score".into(), score_bytes)]);

        assert_eq!(mgr.count(0, 1, "lb", 0), Some(2));
        // DESC: bob(200) = rank 1, alice(100) = rank 2
        assert_eq!(mgr.rank(0, 1, "lb", b"bob", 0), Some(1));
        assert_eq!(mgr.rank(0, 1, "lb", b"alice", 0), Some(2));
    }

    #[test]
    fn on_delete_removes_from_index() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        let entries = vec![make_entry("alice", 100), make_entry("bob", 200)];
        mgr.register(0, 1, def, entries.into_iter());

        let tbl_key = super::super::super::engine_helpers::table_key(0, 1, "scores");
        mgr.on_delete(tbl_key, b"bob");

        assert_eq!(mgr.count(0, 1, "lb", 0), Some(1));
        assert_eq!(mgr.rank(0, 1, "lb", b"alice", 0), Some(1));
        assert!(mgr.rank(0, 1, "lb", b"bob", 0).is_none());
    }

    #[test]
    fn drop_index() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        mgr.register(0, 1, def, std::iter::empty());

        assert!(mgr.drop(0, 1, "lb"));
        assert!(!mgr.drop(0, 1, "lb")); // Already dropped.
        assert!(mgr.count(0, 1, "lb", 0).is_none());
    }

    #[test]
    fn score_lookup() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        let entries = vec![make_entry("alice", 100)];
        mgr.register(0, 1, def, entries.into_iter());

        let sort_key = mgr.score(0, 1, "lb", b"alice");
        assert!(sort_key.is_some());
        assert!(mgr.score(0, 1, "lb", b"nonexistent").is_none());
    }

    /// Registering a name that already exists must leave the collection bound
    /// to it EXACTLY once.
    ///
    /// A binding list that could hold the same index twice makes every later
    /// PUT do its work twice and exports the index twice into a checkpoint —
    /// and nothing downstream can tell the duplicate from two real indexes.
    #[test]
    fn re_registering_leaves_one_binding_and_a_working_index() {
        let mut mgr = SortedIndexManager::new();
        let tbl_key = super::super::super::engine_helpers::table_key(0, 1, "scores");

        mgr.register(
            0,
            1,
            make_def("lb", "scores"),
            vec![make_entry("alice", 100), make_entry("bob", 200)].into_iter(),
        );
        let rebuilt = mgr.register(
            0,
            1,
            make_def("lb", "scores"),
            vec![
                make_entry("alice", 100),
                make_entry("bob", 200),
                make_entry("carol", 300),
            ]
            .into_iter(),
        );

        assert_eq!(rebuilt, 3, "re-registration rebuilds from the rows given");
        assert_eq!(
            mgr.collection_indexes
                .get(&tbl_key)
                .map(|bound| bound.len()),
            Some(1),
            "the collection must be bound to the index exactly once"
        );
        assert_eq!(
            mgr.export_for_table(tbl_key).len(),
            1,
            "a checkpoint must export the index once, not twice"
        );

        // The rebuilt index still answers, from the rows it was rebuilt with.
        assert_eq!(mgr.count(0, 1, "lb", 0), Some(3));
        assert_eq!(mgr.rank(0, 1, "lb", b"carol", 0), Some(1));
        assert_eq!(mgr.rank(0, 1, "lb", b"alice", 0), Some(3));

        // And it is still maintained on write — one insert, not two trees.
        let bytes = SortKeyEncoder::encode_i64(400).to_vec();
        mgr.on_put(tbl_key, b"dave", &[("score".into(), bytes)]);
        assert_eq!(mgr.rank(0, 1, "lb", b"dave", 0), Some(1));
        assert_eq!(mgr.count(0, 1, "lb", 0), Some(4));
    }

    /// A single `drop` must fully unregister, with no second binding hiding
    /// behind it — which is what a duplicated entry would leave.
    #[test]
    fn dropping_a_re_registered_index_leaves_nothing_behind() {
        let mut mgr = SortedIndexManager::new();
        let tbl_key = super::super::super::engine_helpers::table_key(0, 1, "scores");

        mgr.register(0, 1, make_def("lb", "scores"), std::iter::empty());
        mgr.register(0, 1, make_def("lb", "scores"), std::iter::empty());

        assert!(mgr.drop(0, 1, "lb"));
        assert!(!mgr.drop(0, 1, "lb"), "one drop must remove one index");
        assert!(
            !mgr.has_indexes(tbl_key),
            "the collection must be left with no sorted index bound to it"
        );
        assert!(mgr.export_for_table(tbl_key).is_empty());
    }

    /// Re-registering the same name over a DIFFERENT collection must move the
    /// binding, not add one. Left behind, the old collection's next PUT would
    /// splice its own rows into an index that no longer covers it.
    #[test]
    fn re_registering_onto_another_collection_moves_the_binding() {
        let mut mgr = SortedIndexManager::new();
        let old_key = super::super::super::engine_helpers::table_key(0, 1, "scores");
        let new_key = super::super::super::engine_helpers::table_key(0, 1, "ladder");

        mgr.register(
            0,
            1,
            make_def("lb", "scores"),
            vec![make_entry("alice", 100)].into_iter(),
        );
        mgr.register(
            0,
            1,
            make_def("lb", "ladder"),
            vec![make_entry("bob", 200)].into_iter(),
        );

        assert!(
            !mgr.has_indexes(old_key),
            "the old collection must no longer maintain this index"
        );
        assert!(mgr.has_indexes(new_key));
        assert_eq!(mgr.count(0, 1, "lb", 0), Some(1));

        // A write to the collection the index no longer covers must not reach it.
        let bytes = SortKeyEncoder::encode_i64(999).to_vec();
        mgr.on_put(old_key, b"ghost", &[("score".into(), bytes)]);
        assert_eq!(
            mgr.rank(0, 1, "lb", b"ghost", 0),
            None,
            "a row from the abandoned collection must not enter the index"
        );
        assert_eq!(mgr.count(0, 1, "lb", 0), Some(1));
    }
}
