// SPDX-License-Identifier: BUSL-1.1

//! A single-field KV secondary index: one value field → the primary keys
//! carrying that value.
//!
//! Design:
//! - In-memory BTreeMap (matches the ephemeral hash table — both rebuilt from
//!   the WAL or from a checkpoint).
//! - O(log n) insert/delete/range scan.
//! - Synchronous maintenance on every PUT/DELETE — no eventual consistency.

use std::collections::{BTreeMap, BTreeSet};

/// The shape both secondary index kinds store: index key → the primary keys
/// filed under it.
pub type KvIndexTree = BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>;

/// A single secondary index on one value field.
///
/// Maps field value bytes → set of primary keys that have that value.
/// Sorted by field value (BTreeMap) for efficient range scans.
#[derive(Debug)]
pub struct KvFieldIndex {
    /// Field name this index covers.
    field: String,
    /// Field position in the schema column list (for Binary Tuple extraction).
    field_position: usize,
    /// value_bytes → set of primary_key_bytes.
    tree: KvIndexTree,
}

impl KvFieldIndex {
    pub fn new(field: impl Into<String>, field_position: usize) -> Self {
        Self {
            field: field.into(),
            field_position,
            tree: BTreeMap::new(),
        }
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn field_position(&self) -> usize {
        self.field_position
    }

    /// The whole index, value by value.
    ///
    /// The checkpoint writer persists this verbatim rather than recomputing it
    /// from the rows: an index registered with `backfill=false` deliberately
    /// omits the rows that predate it, so its content is a function of the write
    /// history, not of the current rows. Anything that rebuilt content by
    /// re-extracting every restored row would silently turn such an index into a
    /// full one.
    pub fn entries(&self) -> &KvIndexTree {
        &self.tree
    }

    /// Insert a (value, primary_key) pair into the index.
    pub fn insert(&mut self, field_value: Vec<u8>, primary_key: Vec<u8>) {
        self.tree
            .entry(field_value)
            .or_default()
            .insert(primary_key);
    }

    /// Remove a (value, primary_key) pair from the index.
    ///
    /// Returns true if the pair was found and removed.
    pub fn remove(&mut self, field_value: &[u8], primary_key: &[u8]) -> bool {
        if let Some(keys) = self.tree.get_mut(field_value) {
            let removed = keys.remove(primary_key);
            if keys.is_empty() {
                self.tree.remove(field_value);
            }
            removed
        } else {
            false
        }
    }

    /// Whether the index contains the (value, primary_key) pair.
    pub(crate) fn contains(&self, field_value: &[u8], primary_key: &[u8]) -> bool {
        self.tree
            .get(field_value)
            .is_some_and(|keys| keys.contains(primary_key))
    }

    /// Exact-match lookup: find all primary keys where field == value.
    pub fn lookup_eq(&self, field_value: &[u8]) -> Vec<&[u8]> {
        self.tree
            .get(field_value)
            .map(|keys| keys.iter().map(|k| k.as_slice()).collect())
            .unwrap_or_default()
    }

    /// Range lookup: find all primary keys where field value is in [lower, upper).
    ///
    /// `lower` = None means unbounded start. `upper` = None means unbounded end.
    pub fn lookup_range(&self, lower: Option<&[u8]>, upper: Option<&[u8]>) -> Vec<(&[u8], &[u8])> {
        use std::ops::Bound;

        let lo = match lower {
            Some(l) => Bound::Included(l.to_vec()),
            None => Bound::Unbounded,
        };
        let hi = match upper {
            Some(u) => Bound::Excluded(u.to_vec()),
            None => Bound::Unbounded,
        };

        let mut results = Vec::new();
        for (value, keys) in self.tree.range((lo, hi)) {
            for key in keys {
                results.push((value.as_slice(), key.as_slice()));
            }
        }
        results
    }

    /// Total number of index entries (sum of all primary key sets).
    pub fn entry_count(&self) -> usize {
        self.tree.values().map(|s| s.len()).sum()
    }

    /// Number of distinct field values indexed.
    pub fn distinct_values(&self) -> usize {
        self.tree.len()
    }

    /// Clear all entries (used during DROP INDEX).
    pub fn clear(&mut self) {
        self.tree.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_index_insert_and_lookup() {
        let mut idx = KvFieldIndex::new("region", 2);
        idx.insert(b"us-east".to_vec(), b"key1".to_vec());
        idx.insert(b"us-east".to_vec(), b"key2".to_vec());
        idx.insert(b"eu-west".to_vec(), b"key3".to_vec());

        let results = idx.lookup_eq(b"us-east");
        assert_eq!(results.len(), 2);
        assert!(results.contains(&b"key1".as_slice()));
        assert!(results.contains(&b"key2".as_slice()));

        let results = idx.lookup_eq(b"eu-west");
        assert_eq!(results.len(), 1);

        let results = idx.lookup_eq(b"ap-south");
        assert!(results.is_empty());
    }

    #[test]
    fn field_index_remove() {
        let mut idx = KvFieldIndex::new("status", 1);
        idx.insert(b"active".to_vec(), b"k1".to_vec());
        idx.insert(b"active".to_vec(), b"k2".to_vec());

        assert!(idx.remove(b"active", b"k1"));
        assert_eq!(idx.lookup_eq(b"active").len(), 1);

        assert!(idx.remove(b"active", b"k2"));
        assert!(idx.lookup_eq(b"active").is_empty());
        assert_eq!(idx.distinct_values(), 0);

        // Remove nonexistent.
        assert!(!idx.remove(b"active", b"k3"));
    }

    #[test]
    fn field_index_range_lookup() {
        let mut idx = KvFieldIndex::new("score", 0);
        for i in 0u32..10 {
            idx.insert(i.to_be_bytes().to_vec(), format!("k{i}").into_bytes());
        }

        // Range [3, 7)
        let results = idx.lookup_range(Some(&3u32.to_be_bytes()), Some(&7u32.to_be_bytes()));
        assert_eq!(results.len(), 4); // 3, 4, 5, 6
    }

    /// `entries()` is what the checkpoint persists; it must expose every
    /// (value, primary key) pair the lookups can see, or a restored index would
    /// silently come back short.
    #[test]
    fn entries_expose_every_indexed_pair() {
        let mut idx = KvFieldIndex::new("region", 0);
        idx.insert(b"us".to_vec(), b"k1".to_vec());
        idx.insert(b"us".to_vec(), b"k2".to_vec());
        idx.insert(b"eu".to_vec(), b"k3".to_vec());

        let pairs: usize = idx.entries().values().map(|pks| pks.len()).sum();
        assert_eq!(pairs, idx.entry_count());
        assert_eq!(idx.entries().len(), 2, "two distinct values");
    }
}
