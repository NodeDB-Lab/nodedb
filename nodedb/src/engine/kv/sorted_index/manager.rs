//! SortedIndexManager: manages all sorted indexes for a KV engine core.
//!
//! Each sorted index is identified by `(tenant_id, collection, index_name)`.
//! The manager handles:
//! - Registration and dropping of sorted indexes
//! - Auto-maintenance on KV PUT/DELETE (updating sort keys in the tree)
//! - Query dispatch (rank, top_k, range, count)
//! - Rebuild from existing KV data (backfill)

use std::collections::HashMap;

use super::key::SortKeyEncoder;
use super::tree::OrderStatTree;
use super::window::WindowConfig;
use super::windowed_query::{self, SortedIndexRef};

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

/// A live sorted index: definition + data.
struct SortedIndex {
    def: SortedIndexDef,
    tree: OrderStatTree,
}

/// Manages all sorted indexes on a single TPC core.
///
/// Key: `(tenant_hash, index_name)` where tenant_hash is the same hash
/// used by KvEngine to scope tables by tenant+collection.
#[derive(Debug)]
pub struct SortedIndexManager {
    /// All sorted indexes. Key: `"{tenant_id}:{index_name}"`.
    indexes: HashMap<String, SortedIndex>,
    /// Reverse map: `"{tenant_id}:{collection}"` → list of index names.
    /// Used to find which sorted indexes to update on PUT/DELETE.
    collection_indexes: HashMap<u64, Vec<String>>,
}

impl std::fmt::Debug for SortedIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortedIndex")
            .field("name", &self.def.name)
            .field("collection", &self.def.collection)
            .field("count", &self.tree.count())
            .finish()
    }
}

impl SortedIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            collection_indexes: HashMap::new(),
        }
    }

    /// Register a new sorted index. Returns the number of entries backfilled.
    ///
    /// `existing_entries` is an iterator of `(primary_key_bytes, value_bytes)` pairs
    /// from the KV hash table, used to populate the index from existing data.
    pub fn register(
        &mut self,
        tenant_id: u32,
        def: SortedIndexDef,
        existing_entries: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> u32 {
        let idx_key = index_key(tenant_id, &def.name);
        let tbl_key = super::super::engine_helpers::table_key(tenant_id, &def.collection);

        let mut tree = OrderStatTree::new();
        let mut backfilled = 0u32;

        // Backfill from existing data.
        for (pk_bytes, value_bytes) in existing_entries {
            if let Some(sort_key) = extract_sort_key_from_value(&def, &value_bytes) {
                tree.insert(sort_key, pk_bytes);
                backfilled += 1;
            }
        }

        self.collection_indexes
            .entry(tbl_key)
            .or_default()
            .push(idx_key.clone());

        self.indexes.insert(idx_key, SortedIndex { def, tree });
        backfilled
    }

    /// Drop a sorted index. Returns `true` if it existed.
    pub fn drop(&mut self, tenant_id: u32, index_name: &str) -> bool {
        let idx_key = index_key(tenant_id, index_name);

        let Some(idx) = self.indexes.remove(&idx_key) else {
            return false;
        };

        let tbl_key = super::super::engine_helpers::table_key(tenant_id, &idx.def.collection);
        if let Some(list) = self.collection_indexes.get_mut(&tbl_key) {
            list.retain(|k| k != &idx_key);
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
        let idx_keys: Vec<String> = idx_keys.to_vec();
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
        let idx_keys: Vec<String> = idx_keys.to_vec();
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

    // ── Query methods ──────────────────────────────────────────────────

    /// Get the 1-based rank of a primary key in a sorted index.
    ///
    /// For windowed indexes, only entries within the current window are counted.
    pub fn rank(
        &self,
        tenant_id: u32,
        index_name: &str,
        primary_key: &[u8],
        now_ms: u64,
    ) -> Option<u32> {
        let idx = self.get_index(tenant_id, index_name)?;

        if idx.def.window.is_unwindowed() {
            return idx.tree.rank(primary_key);
        }

        // Windowed: need to count how many entries with a lower sort key
        // are within the current window. This is the expensive path.
        let idx_ref = SortedIndexRef {
            def: &idx.def,
            tree: &idx.tree,
        };
        windowed_query::windowed_rank(&idx_ref, primary_key, now_ms)
    }

    /// Get the top K entries from a sorted index.
    ///
    /// Returns `(rank, primary_key)` pairs.
    pub fn top_k(
        &self,
        tenant_id: u32,
        index_name: &str,
        k: u32,
        now_ms: u64,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        let idx = self.get_index(tenant_id, index_name)?;

        if idx.def.window.is_unwindowed() {
            let entries = idx.tree.top_k(k);
            return Some(
                entries
                    .into_iter()
                    .enumerate()
                    .map(|(i, (_, pk))| (i as u32 + 1, pk.to_vec()))
                    .collect(),
            );
        }

        let idx_ref = SortedIndexRef {
            def: &idx.def,
            tree: &idx.tree,
        };
        Some(windowed_query::windowed_top_k(&idx_ref, k, now_ms))
    }

    /// Get entries in a score range from a sorted index.
    ///
    /// `score_min` and `score_max` are raw encoded sort key bytes.
    /// Returns `(rank, primary_key)` pairs.
    pub fn range(
        &self,
        tenant_id: u32,
        index_name: &str,
        score_min: Option<&[u8]>,
        score_max: Option<&[u8]>,
        now_ms: u64,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        let idx = self.get_index(tenant_id, index_name)?;

        let entries = idx.tree.range(score_min, score_max);

        if idx.def.window.is_unwindowed() {
            // Compute rank for each entry.
            return Some(
                entries
                    .into_iter()
                    .filter_map(|(_, pk)| {
                        let rank = idx.tree.rank(pk)?;
                        Some((rank, pk.to_vec()))
                    })
                    .collect(),
            );
        }

        let idx_ref = SortedIndexRef {
            def: &idx.def,
            tree: &idx.tree,
        };
        Some(windowed_query::windowed_range(&idx_ref, &entries, now_ms))
    }

    /// Get the total count of entries in a sorted index.
    pub fn count(&self, tenant_id: u32, index_name: &str, now_ms: u64) -> Option<u32> {
        let idx = self.get_index(tenant_id, index_name)?;

        if idx.def.window.is_unwindowed() {
            return Some(idx.tree.count());
        }

        let idx_ref = SortedIndexRef {
            def: &idx.def,
            tree: &idx.tree,
        };
        Some(windowed_query::windowed_count(&idx_ref, now_ms))
    }

    /// Get the sort key for a primary key in a sorted index (ZSCORE equivalent).
    pub fn score(&self, tenant_id: u32, index_name: &str, primary_key: &[u8]) -> Option<Vec<u8>> {
        let idx = self.get_index(tenant_id, index_name)?;
        idx.tree.get_sort_key(primary_key).map(|s| s.to_vec())
    }

    /// Get the index definition.
    pub fn get_def(&self, tenant_id: u32, index_name: &str) -> Option<&SortedIndexDef> {
        let idx = self.get_index(tenant_id, index_name)?;
        Some(&idx.def)
    }

    fn get_index(&self, tenant_id: u32, index_name: &str) -> Option<&SortedIndex> {
        let idx_key = index_key(tenant_id, index_name);
        self.indexes.get(&idx_key)
    }
}

impl Default for SortedIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

fn index_key(tenant_id: u32, index_name: &str) -> String {
    format!("{tenant_id}:{index_name}")
}

/// Extract field values from a MessagePack-encoded KV value and build a sort key.
fn extract_sort_key_from_value(def: &SortedIndexDef, value_bytes: &[u8]) -> Option<Vec<u8>> {
    let doc: serde_json::Value = rmp_serde::from_slice(value_bytes).ok()?;
    let obj = doc.as_object()?;

    let mut values: Vec<Vec<u8>> = Vec::with_capacity(def.encoder.column_count());
    for col in def.encoder.columns() {
        let field_val = obj.get(&col.name)?;
        let bytes = field_value_to_sort_bytes(field_val);
        values.push(bytes);
    }

    let refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
    Some(def.encoder.encode(&refs))
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

/// Convert a JSON field value to sortable bytes.
fn field_value_to_sort_bytes(val: &serde_json::Value) -> Vec<u8> {
    match val {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SortKeyEncoder::encode_i64(i).to_vec()
            } else if let Some(f) = n.as_f64() {
                SortKeyEncoder::encode_f64(f).to_vec()
            } else {
                Vec::new()
            }
        }
        serde_json::Value::String(s) => s.as_bytes().to_vec(),
        _ => Vec::new(),
    }
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
        let value = rmp_serde::to_vec(&serde_json::json!({
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
        let count = mgr.register(1, def, entries.into_iter());
        assert_eq!(count, 3);
        assert_eq!(mgr.count(1, "lb", 0), Some(3));
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
        mgr.register(1, def, entries.into_iter());

        // DESC: bob(300) = rank 1, charlie(200) = rank 2, alice(100) = rank 3
        assert_eq!(mgr.rank(1, "lb", b"bob", 0), Some(1));
        assert_eq!(mgr.rank(1, "lb", b"charlie", 0), Some(2));
        assert_eq!(mgr.rank(1, "lb", b"alice", 0), Some(3));
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
        mgr.register(1, def, entries.into_iter());

        let top2 = mgr.top_k(1, "lb", 2, 0).unwrap();
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0], (1, b"bob".to_vec()));
        assert_eq!(top2[1], (2, b"charlie".to_vec()));
    }

    #[test]
    fn on_put_updates_index() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        mgr.register(1, def, std::iter::empty());

        let tbl_key = super::super::super::engine_helpers::table_key(1, "scores");

        // Simulate PUTs.
        let score_bytes = SortKeyEncoder::encode_i64(100).to_vec();
        mgr.on_put(tbl_key, b"alice", &[("score".into(), score_bytes)]);

        let score_bytes = SortKeyEncoder::encode_i64(200).to_vec();
        mgr.on_put(tbl_key, b"bob", &[("score".into(), score_bytes)]);

        assert_eq!(mgr.count(1, "lb", 0), Some(2));
        // DESC: bob(200) = rank 1, alice(100) = rank 2
        assert_eq!(mgr.rank(1, "lb", b"bob", 0), Some(1));
        assert_eq!(mgr.rank(1, "lb", b"alice", 0), Some(2));
    }

    #[test]
    fn on_delete_removes_from_index() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        let entries = vec![make_entry("alice", 100), make_entry("bob", 200)];
        mgr.register(1, def, entries.into_iter());

        let tbl_key = super::super::super::engine_helpers::table_key(1, "scores");
        mgr.on_delete(tbl_key, b"bob");

        assert_eq!(mgr.count(1, "lb", 0), Some(1));
        assert_eq!(mgr.rank(1, "lb", b"alice", 0), Some(1));
        assert!(mgr.rank(1, "lb", b"bob", 0).is_none());
    }

    #[test]
    fn drop_index() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        mgr.register(1, def, std::iter::empty());

        assert!(mgr.drop(1, "lb"));
        assert!(!mgr.drop(1, "lb")); // Already dropped.
        assert!(mgr.count(1, "lb", 0).is_none());
    }

    #[test]
    fn score_lookup() {
        let mut mgr = SortedIndexManager::new();
        let def = make_def("lb", "scores");
        let entries = vec![make_entry("alice", 100)];
        mgr.register(1, def, entries.into_iter());

        let sort_key = mgr.score(1, "lb", b"alice");
        assert!(sort_key.is_some());
        assert!(mgr.score(1, "lb", b"nonexistent").is_none());
    }
}
