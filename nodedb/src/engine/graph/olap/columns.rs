//! Property column store — columnar property storage for graph analytics.
//!
//! Extracts node/edge properties from the edge store into columnar arrays
//! aligned with CSR node IDs. This avoids per-node redb lookups during
//! algorithm execution.
//!
//! Supported column types:
//! - **Float64Column**: dense f64 array (numeric properties, weights).
//! - **Int64Column**: dense i64 array (integer properties, timestamps).
//! - **StringColumn**: dictionary-encoded strings (labels, categories).
//!
//! ArcadeDB has ColumnStore + DictionaryEncoding for this purpose.
//! NodeDB's implementation is CSR-aligned: `column[node_id]` gives the
//! property value for that node.

use std::collections::HashMap;

/// A collection of property columns aligned to CSR node IDs.
///
/// Each column is indexed by node ID (dense u32). Missing values are
/// represented by a sentinel (NaN for f64, 0 for i64, empty string for str).
pub struct PropertyColumns {
    /// Float64 property columns: column_name → dense f64 array.
    pub f64_columns: HashMap<String, Float64Column>,
    /// Int64 property columns: column_name → dense i64 array.
    pub i64_columns: HashMap<String, Int64Column>,
    /// String property columns: column_name → dictionary-encoded column.
    pub string_columns: HashMap<String, StringColumn>,
    /// Number of nodes (all columns have this length).
    pub node_count: usize,
}

/// Dense f64 column aligned to CSR node IDs.
pub struct Float64Column {
    /// Values indexed by node ID. NaN = missing.
    pub values: Vec<f64>,
    /// Bitmap of non-missing values (bit i = 1 if values[i] is present).
    pub present: Vec<u64>,
}

/// Dense i64 column aligned to CSR node IDs.
pub struct Int64Column {
    /// Values indexed by node ID. 0 = default/missing.
    pub values: Vec<i64>,
    /// Bitmap of non-missing values.
    pub present: Vec<u64>,
}

/// Dictionary-encoded string column.
///
/// Stores unique strings in a dictionary and references them by index.
/// Memory-efficient for columns with high cardinality but many repeats
/// (e.g., edge labels, node types, status values).
pub struct StringColumn {
    /// Dictionary: unique string values.
    pub dictionary: Vec<String>,
    /// Per-node index into the dictionary. u32::MAX = missing.
    pub indices: Vec<u32>,
    /// Reverse lookup: string → dictionary index.
    lookup: HashMap<String, u32>,
}

impl PropertyColumns {
    /// Create an empty column store for `n` nodes.
    pub fn new(node_count: usize) -> Self {
        Self {
            f64_columns: HashMap::new(),
            i64_columns: HashMap::new(),
            string_columns: HashMap::new(),
            node_count,
        }
    }

    /// Add or get a Float64 column.
    pub fn f64_column(&mut self, name: &str) -> &mut Float64Column {
        let n = self.node_count;
        self.f64_columns
            .entry(name.to_string())
            .or_insert_with(|| Float64Column::new(n))
    }

    /// Add or get an Int64 column.
    pub fn i64_column(&mut self, name: &str) -> &mut Int64Column {
        let n = self.node_count;
        self.i64_columns
            .entry(name.to_string())
            .or_insert_with(|| Int64Column::new(n))
    }

    /// Add or get a StringColumn.
    pub fn string_column(&mut self, name: &str) -> &mut StringColumn {
        let n = self.node_count;
        self.string_columns
            .entry(name.to_string())
            .or_insert_with(|| StringColumn::new(n))
    }

    /// Total number of columns.
    pub fn column_count(&self) -> usize {
        self.f64_columns.len() + self.i64_columns.len() + self.string_columns.len()
    }

    /// Estimated memory usage in bytes.
    pub fn estimated_memory_bytes(&self) -> usize {
        let f64_mem: usize = self
            .f64_columns
            .values()
            .map(|c| c.values.len() * 8 + c.present.len() * 8)
            .sum();
        let i64_mem: usize = self
            .i64_columns
            .values()
            .map(|c| c.values.len() * 8 + c.present.len() * 8)
            .sum();
        let str_mem: usize = self
            .string_columns
            .values()
            .map(|c| {
                let dict_mem: usize = c.dictionary.iter().map(|s| s.len() + 24).sum();
                let idx_mem = c.indices.len() * 4;
                dict_mem + idx_mem
            })
            .sum();
        f64_mem + i64_mem + str_mem
    }
}

impl Float64Column {
    fn new(n: usize) -> Self {
        let bitmap_words = n.div_ceil(64);
        Self {
            values: vec![f64::NAN; n],
            present: vec![0u64; bitmap_words],
        }
    }

    /// Set the value for a node.
    pub fn set(&mut self, node_id: u32, value: f64) {
        let idx = node_id as usize;
        if idx < self.values.len() {
            self.values[idx] = value;
            self.present[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    /// Get the value for a node. Returns None if missing.
    pub fn get(&self, node_id: u32) -> Option<f64> {
        let idx = node_id as usize;
        if idx < self.values.len() && (self.present[idx / 64] & (1u64 << (idx % 64))) != 0 {
            Some(self.values[idx])
        } else {
            None
        }
    }

    /// Get the value or a default.
    pub fn get_or(&self, node_id: u32, default: f64) -> f64 {
        self.get(node_id).unwrap_or(default)
    }

    /// Number of non-missing values.
    pub fn count_present(&self) -> usize {
        self.present.iter().map(|w| w.count_ones() as usize).sum()
    }
}

impl Int64Column {
    fn new(n: usize) -> Self {
        let bitmap_words = n.div_ceil(64);
        Self {
            values: vec![0i64; n],
            present: vec![0u64; bitmap_words],
        }
    }

    /// Set the value for a node.
    pub fn set(&mut self, node_id: u32, value: i64) {
        let idx = node_id as usize;
        if idx < self.values.len() {
            self.values[idx] = value;
            self.present[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    /// Get the value for a node. Returns None if missing.
    pub fn get(&self, node_id: u32) -> Option<i64> {
        let idx = node_id as usize;
        if idx < self.values.len() && (self.present[idx / 64] & (1u64 << (idx % 64))) != 0 {
            Some(self.values[idx])
        } else {
            None
        }
    }

    /// Number of non-missing values.
    pub fn count_present(&self) -> usize {
        self.present.iter().map(|w| w.count_ones() as usize).sum()
    }
}

impl StringColumn {
    fn new(n: usize) -> Self {
        Self {
            dictionary: Vec::new(),
            indices: vec![u32::MAX; n],
            lookup: HashMap::new(),
        }
    }

    /// Set the value for a node. Dictionary-encodes the string.
    pub fn set(&mut self, node_id: u32, value: &str) {
        let idx = node_id as usize;
        if idx >= self.indices.len() {
            return;
        }

        let dict_idx = if let Some(&existing) = self.lookup.get(value) {
            existing
        } else {
            let new_idx = self.dictionary.len() as u32;
            self.dictionary.push(value.to_string());
            self.lookup.insert(value.to_string(), new_idx);
            new_idx
        };

        self.indices[idx] = dict_idx;
    }

    /// Get the value for a node. Returns None if missing.
    pub fn get(&self, node_id: u32) -> Option<&str> {
        let idx = node_id as usize;
        if idx < self.indices.len() && self.indices[idx] != u32::MAX {
            Some(&self.dictionary[self.indices[idx] as usize])
        } else {
            None
        }
    }

    /// Number of unique values in the dictionary.
    pub fn dictionary_size(&self) -> usize {
        self.dictionary.len()
    }

    /// Number of non-missing values.
    pub fn count_present(&self) -> usize {
        self.indices.iter().filter(|&&i| i != u32::MAX).count()
    }
}

/// Extract edge properties from an EdgeStore into a PropertyColumns aligned
/// with node IDs.
///
/// For each edge, extracts specified property names from MessagePack-encoded
/// properties. Properties are associated with the **source** node of each edge.
///
/// This is a bulk extraction — O(E) — intended for pre-computation before
/// running algorithms, not for hot-path queries.
pub fn extract_edge_properties(
    edge_store: &crate::engine::graph::edge_store::EdgeStore,
    csr: &crate::engine::graph::csr::CsrIndex,
    property_names: &[&str],
) -> Result<PropertyColumns, crate::Error> {
    let n = csr.node_count();
    let mut columns = PropertyColumns::new(n);

    let all_edges = edge_store.scan_all_edges()?;

    for edge in &all_edges {
        if edge.properties.is_empty() {
            continue;
        }

        let Some(src_id) = csr.node_id(&edge.src_id) else {
            continue;
        };

        // Parse MessagePack properties.
        let Ok(val) = rmpv::decode::read_value(&mut &edge.properties[..]) else {
            continue;
        };

        if let rmpv::Value::Map(entries) = val {
            for (k, v) in entries {
                if let rmpv::Value::String(ref key_str) = k {
                    let key = key_str.as_str().unwrap_or("");
                    if !property_names.contains(&key) {
                        continue;
                    }

                    match v {
                        rmpv::Value::F64(f) => {
                            columns.f64_column(key).set(src_id, f);
                        }
                        rmpv::Value::F32(f) => {
                            columns.f64_column(key).set(src_id, f as f64);
                        }
                        rmpv::Value::Integer(i) => {
                            if let Some(val) = i.as_i64() {
                                columns.i64_column(key).set(src_id, val);
                            }
                        }
                        rmpv::Value::String(ref s) => {
                            if let Some(s) = s.as_str() {
                                columns.string_column(key).set(src_id, s);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float64_column_basic() {
        let mut col = Float64Column::new(10);
        col.set(3, 0.75);
        col.set(7, 2.5);

        assert_eq!(col.get(3), Some(0.75));
        assert_eq!(col.get(7), Some(2.5));
        assert_eq!(col.get(0), None);
        assert_eq!(col.get_or(0, 0.0), 0.0);
        assert_eq!(col.count_present(), 2);
    }

    #[test]
    fn int64_column_basic() {
        let mut col = Int64Column::new(5);
        col.set(0, 42);
        col.set(4, -1);

        assert_eq!(col.get(0), Some(42));
        assert_eq!(col.get(4), Some(-1));
        assert_eq!(col.get(2), None);
        assert_eq!(col.count_present(), 2);
    }

    #[test]
    fn string_column_dictionary_encoding() {
        let mut col = StringColumn::new(5);
        col.set(0, "active");
        col.set(1, "inactive");
        col.set(2, "active"); // reuses dict entry
        col.set(3, "active"); // reuses dict entry

        assert_eq!(col.get(0), Some("active"));
        assert_eq!(col.get(1), Some("inactive"));
        assert_eq!(col.get(2), Some("active"));
        assert_eq!(col.get(4), None);

        // Dictionary should have only 2 unique values.
        assert_eq!(col.dictionary_size(), 2);
        assert_eq!(col.count_present(), 4);
    }

    #[test]
    fn property_columns_multi_type() {
        let mut cols = PropertyColumns::new(10);
        cols.f64_column("weight").set(0, 0.5);
        cols.i64_column("count").set(0, 42);
        cols.string_column("status").set(0, "active");

        assert_eq!(cols.column_count(), 3);
        assert_eq!(cols.f64_columns["weight"].get(0), Some(0.5));
        assert_eq!(cols.i64_columns["count"].get(0), Some(42));
        assert_eq!(cols.string_columns["status"].get(0), Some("active"));
    }

    #[test]
    fn property_columns_memory_estimate() {
        let mut cols = PropertyColumns::new(100);
        cols.f64_column("weight");
        cols.string_column("label");

        assert!(cols.estimated_memory_bytes() > 0);
    }

    #[test]
    fn extract_edge_properties_from_store() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            crate::engine::graph::edge_store::EdgeStore::open(&dir.path().join("graph.redb"))
                .unwrap();

        // Edge with properties.
        let props = rmpv::Value::Map(vec![
            (rmpv::Value::String("weight".into()), rmpv::Value::F64(0.75)),
            (
                rmpv::Value::String("label".into()),
                rmpv::Value::String("important".into()),
            ),
        ]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();
        store.put_edge("alice", "KNOWS", "bob", &buf).unwrap();

        let csr = crate::engine::graph::csr::rebuild::rebuild_from_store(&store).unwrap();

        let columns = extract_edge_properties(&store, &csr, &["weight", "label"]).unwrap();

        let alice_id = csr.node_id("alice").unwrap();
        assert_eq!(columns.f64_columns["weight"].get(alice_id), Some(0.75));
        assert_eq!(
            columns.string_columns["label"].get(alice_id),
            Some("important")
        );
    }

    #[test]
    fn extract_empty_properties() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            crate::engine::graph::edge_store::EdgeStore::open(&dir.path().join("graph.redb"))
                .unwrap();

        store.put_edge("a", "L", "b", b"").unwrap();

        let csr = crate::engine::graph::csr::rebuild::rebuild_from_store(&store).unwrap();
        let columns = extract_edge_properties(&store, &csr, &["weight"]).unwrap();

        // No properties extracted.
        assert!(columns.f64_columns.is_empty());
    }
}
