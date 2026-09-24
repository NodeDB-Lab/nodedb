// SPDX-License-Identifier: BUSL-1.1

//! One sorted index: its definition, its order-statistic tree, and the reads
//! it answers.
//!
//! The manager holds one of these per registered index. A transaction builds
//! a private one from the collection's base rows with its staged writes
//! folded in. Both answer through the same methods, so a read inside a
//! transaction ranks, counts and windows exactly like an autocommit read.

use super::key::SortKeyEncoder;
use super::manager::SortedIndexDef;
use super::tree::OrderStatTree;
use super::windowed_query::{self, SortedIndexRef};

/// A sorted index: definition plus tree.
pub struct SortedIndex {
    pub(super) def: SortedIndexDef,
    pub(super) tree: OrderStatTree,
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

impl SortedIndex {
    /// Build an index over `rows`, `(primary_key_bytes, value_bytes)` pairs.
    ///
    /// A row that lacks a sort column is left out. Returns the index and the
    /// number of rows it holds.
    pub fn build(
        def: SortedIndexDef,
        rows: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> (Self, u32) {
        let mut tree = OrderStatTree::new();
        let mut indexed = 0u32;
        for (pk_bytes, value_bytes) in rows {
            if let Some(sort_key) = extract_sort_key_from_value(&def, &value_bytes) {
                tree.insert(sort_key, pk_bytes);
                indexed += 1;
            }
        }
        (Self { def, tree }, indexed)
    }

    /// The definition this index was built from.
    pub fn def(&self) -> &SortedIndexDef {
        &self.def
    }

    fn index_ref(&self) -> SortedIndexRef<'_> {
        SortedIndexRef {
            def: &self.def,
            tree: &self.tree,
        }
    }

    /// The 1-based rank of `primary_key`.
    ///
    /// A windowed index counts only the entries inside the current window.
    pub fn rank(&self, primary_key: &[u8], now_ms: u64) -> Option<u32> {
        if self.def.window.is_unwindowed() {
            return self.tree.rank(primary_key);
        }
        windowed_query::windowed_rank(&self.index_ref(), primary_key, now_ms)
    }

    /// The top `k` entries as `(rank, primary_key)` pairs.
    pub fn top_k(&self, k: u32, now_ms: u64) -> Vec<(u32, Vec<u8>)> {
        if self.def.window.is_unwindowed() {
            return self
                .tree
                .top_k(k)
                .into_iter()
                .enumerate()
                .map(|(i, (_, pk))| (i as u32 + 1, pk.to_vec()))
                .collect();
        }
        windowed_query::windowed_top_k(&self.index_ref(), k, now_ms)
    }

    /// The entries in a score range as `(rank, primary_key)` pairs.
    ///
    /// `score_min` and `score_max` are the raw value bytes of the index's
    /// LEADING sort column (as [`extract_sort_key_from_value`] produces them),
    /// not encoded tree keys. The caller names a score. Only the index's own
    /// encoder knows the framing and direction that turn it into a bound the
    /// tree compares against.
    pub fn range(
        &self,
        score_min: Option<&[u8]>,
        score_max: Option<&[u8]>,
        now_ms: u64,
    ) -> Vec<(u32, Vec<u8>)> {
        let (lower, upper) = self
            .def
            .encoder
            .first_column_range_bounds(score_min, score_max);
        let entries = self.tree.range(lower.as_deref(), upper.as_deref());

        if self.def.window.is_unwindowed() {
            return entries
                .into_iter()
                .filter_map(|(_, pk)| {
                    let rank = self.tree.rank(pk)?;
                    Some((rank, pk.to_vec()))
                })
                .collect();
        }
        windowed_query::windowed_range(&self.index_ref(), &entries, now_ms)
    }

    /// The number of entries, inside the current window for a windowed index.
    pub fn count(&self, now_ms: u64) -> u32 {
        if self.def.window.is_unwindowed() {
            return self.tree.count();
        }
        windowed_query::windowed_count(&self.index_ref(), now_ms)
    }

    /// The sort key of `primary_key` (ZSCORE equivalent).
    pub fn score(&self, primary_key: &[u8]) -> Option<Vec<u8>> {
        self.tree.get_sort_key(primary_key).map(|s| s.to_vec())
    }
}

/// Extract the sort columns from a MessagePack-encoded KV value and build a
/// sort key. `None` when the value is not a map or lacks a sort column.
pub(super) fn extract_sort_key_from_value(
    def: &SortedIndexDef,
    value_bytes: &[u8],
) -> Option<Vec<u8>> {
    let doc: serde_json::Value = nodedb_types::json_from_msgpack(value_bytes).ok()?;
    let obj = doc.as_object()?;

    let mut values: Vec<Vec<u8>> = Vec::with_capacity(def.encoder.column_count());
    for col in def.encoder.columns() {
        let field_val = obj.get(&col.name)?;
        values.push(field_value_to_sort_bytes(field_val));
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
