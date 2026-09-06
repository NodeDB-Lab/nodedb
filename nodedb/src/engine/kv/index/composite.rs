// SPDX-License-Identifier: BUSL-1.1

//! A composite (multi-field) KV secondary index.

use std::collections::BTreeMap;

use super::field::KvIndexTree;

/// A composite secondary index on multiple value fields.
///
/// Maps concatenated field values → set of primary keys. The composite key
/// is built by joining individual field value bytes with a null separator.
/// Supports prefix-based lookups (e.g., `WHERE a = X` on a `(a, b)` index).
#[derive(Debug)]
pub struct KvCompositeIndex {
    /// Field names in index order.
    fields: Vec<String>,
    /// Field positions in the schema column list.
    field_positions: Vec<usize>,
    /// composite_key_bytes → set of primary_key_bytes.
    tree: KvIndexTree,
}

impl KvCompositeIndex {
    pub fn new(fields: Vec<String>, field_positions: Vec<usize>) -> Self {
        Self {
            fields,
            field_positions,
            tree: BTreeMap::new(),
        }
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    pub fn field_positions(&self) -> &[usize] {
        &self.field_positions
    }

    /// The whole index, keyed by the already-built composite key.
    ///
    /// The checkpoint persists these keys as-is instead of the field values they
    /// were built from: `build_key` joins values with `\0`, and splitting that
    /// back apart is ambiguous for any value that itself contains a null byte.
    /// Round-tripping the built key through [`KvCompositeIndex::insert_raw`]
    /// cannot lose that way.
    pub fn entries(&self) -> &KvIndexTree {
        &self.tree
    }

    /// Build a composite key from individual field values.
    ///
    /// Values are concatenated with `\0` separator. This preserves
    /// lexicographic ordering for prefix scans on leading fields.
    ///
    /// **Limitation:** field values must not contain null bytes (`\0`), as they
    /// are used as separators. This holds for typical KV keys (strings, UUIDs,
    /// integers encoded as big-endian bytes).
    fn build_key(values: &[&[u8]]) -> Vec<u8> {
        let mut key = Vec::new();
        for (i, v) in values.iter().enumerate() {
            if i > 0 {
                key.push(0); // Null separator.
            }
            key.extend_from_slice(v);
        }
        key
    }

    /// Insert a composite entry.
    pub fn insert(&mut self, field_values: &[&[u8]], primary_key: Vec<u8>) {
        let key = Self::build_key(field_values);
        self.tree.entry(key).or_default().insert(primary_key);
    }

    /// Insert an entry under an already-built composite key.
    ///
    /// The checkpoint restore path's counterpart to [`KvCompositeIndex::entries`]
    /// — it reinstalls exactly the key that was exported, with no rebuild step
    /// that could reinterpret it.
    pub fn insert_raw(&mut self, composite_key: Vec<u8>, primary_key: Vec<u8>) {
        self.tree
            .entry(composite_key)
            .or_default()
            .insert(primary_key);
    }

    /// Remove a composite entry.
    pub fn remove(&mut self, field_values: &[&[u8]], primary_key: &[u8]) -> bool {
        let key = Self::build_key(field_values);
        if let Some(keys) = self.tree.get_mut(&key) {
            let removed = keys.remove(primary_key);
            if keys.is_empty() {
                self.tree.remove(&key);
            }
            removed
        } else {
            false
        }
    }

    /// Whether the index contains the composite (field_values, primary_key) pair.
    pub(crate) fn contains(&self, field_values: &[&[u8]], primary_key: &[u8]) -> bool {
        let key = Self::build_key(field_values);
        self.tree
            .get(&key)
            .is_some_and(|keys| keys.contains(primary_key))
    }

    /// Exact-match lookup on all fields.
    pub fn lookup_eq(&self, field_values: &[&[u8]]) -> Vec<&[u8]> {
        let key = Self::build_key(field_values);
        self.tree
            .get(&key)
            .map(|keys| keys.iter().map(|k| k.as_slice()).collect())
            .unwrap_or_default()
    }

    /// Prefix lookup: match on leading fields only.
    ///
    /// E.g., on a `(region, status)` index, `lookup_prefix(&[b"us-east"])`
    /// returns all keys where `region = "us-east"` regardless of status.
    ///
    /// Uses `starts_with()` on the B-Tree range to avoid false matches from
    /// the `0xFF` upper-bound trick, which breaks if field values contain
    /// bytes >= `0xFF`.
    pub fn lookup_prefix(&self, prefix_values: &[&[u8]]) -> Vec<&[u8]> {
        let prefix = Self::build_key(prefix_values);
        let mut results = Vec::new();
        for (composite_key, primary_keys) in self.tree.range(prefix.clone()..) {
            if !composite_key.starts_with(&prefix) {
                break;
            }
            for pk in primary_keys {
                results.push(pk.as_slice());
            }
        }
        results
    }

    /// Total number of index entries.
    pub fn entry_count(&self) -> usize {
        self.tree.values().map(|s| s.len()).sum()
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.tree.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composite_index_insert_and_exact_lookup() {
        let mut ci = KvCompositeIndex::new(vec!["region".into(), "status".into()], vec![0, 1]);
        ci.insert(&[b"us-east", b"active"], b"k1".to_vec());
        ci.insert(&[b"us-east", b"inactive"], b"k2".to_vec());
        ci.insert(&[b"eu-west", b"active"], b"k3".to_vec());

        let results = ci.lookup_eq(&[b"us-east", b"active"]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], b"k1");

        let results = ci.lookup_eq(&[b"eu-west", b"active"]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], b"k3");

        // Non-matching returns empty.
        assert!(ci.lookup_eq(&[b"ap-south", b"active"]).is_empty());
    }

    #[test]
    fn composite_index_prefix_lookup() {
        let mut ci = KvCompositeIndex::new(vec!["region".into(), "status".into()], vec![0, 1]);
        ci.insert(&[b"us-east", b"active"], b"k1".to_vec());
        ci.insert(&[b"us-east", b"inactive"], b"k2".to_vec());
        ci.insert(&[b"eu-west", b"active"], b"k3".to_vec());

        // Prefix lookup on leading field only.
        let results = ci.lookup_prefix(&[b"us-east"]);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn composite_index_remove() {
        let mut ci = KvCompositeIndex::new(vec!["a".into(), "b".into()], vec![0, 1]);
        ci.insert(&[b"x", b"y"], b"k1".to_vec());
        assert_eq!(ci.entry_count(), 1);

        assert!(ci.remove(&[b"x", b"y"], b"k1"));
        assert_eq!(ci.entry_count(), 0);
    }

    /// The checkpoint export/restore pair is `entries()` → `insert_raw()`. It
    /// must reproduce an index that answers lookups identically, including for
    /// a value containing the `\0` byte that `build_key` uses as its separator
    /// — the case that makes rebuilding from split-apart values unsound.
    #[test]
    fn raw_entries_roundtrip_reproduces_lookups() {
        let mut ci = KvCompositeIndex::new(vec!["a".into(), "b".into()], vec![0, 1]);
        ci.insert(&[b"x", b"y"], b"k1".to_vec());
        ci.insert(&[b"x\0z", b"y"], b"k2".to_vec());

        let exported: Vec<(Vec<u8>, Vec<u8>)> = ci
            .entries()
            .iter()
            .flat_map(|(key, pks)| pks.iter().map(|pk| (key.clone(), pk.clone())))
            .collect();

        let mut restored = KvCompositeIndex::new(vec!["a".into(), "b".into()], vec![0, 1]);
        for (key, pk) in exported {
            restored.insert_raw(key, pk);
        }

        assert_eq!(restored.lookup_eq(&[b"x", b"y"]), vec![b"k1".as_slice()]);
        assert_eq!(restored.lookup_eq(&[b"x\0z", b"y"]), vec![b"k2".as_slice()]);
        assert_eq!(restored.entry_count(), ci.entry_count());
    }
}
