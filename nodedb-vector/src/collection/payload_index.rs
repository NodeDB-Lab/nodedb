//! In-memory payload bitmap indexes for vector-primary collections.
//!
//! Each indexed field stores a `HashMap<PayloadKey, RoaringBitmap>` mapping
//! distinct field values to the set of HNSW node ids that carry that value.
//! Pre-filter queries intersect/union bitmaps to produce a node-id allow-set
//! that is passed directly into HNSW search, eliminating costly post-filter
//! passes for high-selectivity predicates.
//!
//! **Ordering invariant**: callers must call `insert_row` *after* the HNSW
//! node id is assigned so that the id is stable. Similarly, `delete_row`
//! should be called *before* or *after* HNSW deletion is attempted — the
//! bitmap and the HNSW graph are always consistent after a successful pair.

use std::collections::HashMap;

use roaring::RoaringBitmap;

use nodedb_types::Value;

// ── Key type ─────────────────────────────────────────────────────────────────

/// A deterministic, hashable representation of a `Value` used as a bitmap key.
///
/// `Float` stores the total-ordered bit pattern of the `f64` (NaN is
/// canonicalised to a single sentinel bit-pattern so two NaNs map to the
/// same key).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum PayloadKey {
    Null,
    Bool(bool),
    Integer(i64),
    /// Total-ordered f64 bits. NaN is canonicalised to `u64::MAX`.
    Float(u64),
    String(String),
    Bytes(Vec<u8>),
    /// DateTime stored as a string for deterministic hashing.
    DateTime(String),
    Uuid(String),
}

impl PayloadKey {
    /// Convert a `Value` to a `PayloadKey`.
    ///
    /// Returns `None` for values that cannot serve as index keys (e.g. nested
    /// objects, arrays, geometry). The caller should skip the field rather than
    /// panic.
    pub fn from_value(v: &Value) -> Option<PayloadKey> {
        match v {
            Value::Null => Some(PayloadKey::Null),
            Value::Bool(b) => Some(PayloadKey::Bool(*b)),
            Value::Integer(i) => Some(PayloadKey::Integer(*i)),
            Value::Float(f) => {
                // Canonical NaN: map every NaN to the same sentinel.
                let bits = if f.is_nan() { u64::MAX } else { f.to_bits() };
                Some(PayloadKey::Float(bits))
            }
            Value::String(s) => Some(PayloadKey::String(s.clone())),
            Value::Bytes(b) => Some(PayloadKey::Bytes(b.clone())),
            Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => Some(PayloadKey::Uuid(s.clone())),
            Value::DateTime(dt) => Some(PayloadKey::DateTime(format!("{dt:?}"))),
            Value::Decimal(d) => Some(PayloadKey::String(d.to_string())),
            // Complex values that cannot be equality-indexed.
            Value::Array(_)
            | Value::Object(_)
            | Value::Set(_)
            | Value::Geometry(_)
            | Value::Duration(_)
            | Value::Range { .. }
            | Value::Record { .. }
            | Value::NdArrayCell(_) => None,
        }
    }
}

// ── Index kind ────────────────────────────────────────────────────────────────

/// Re-export the kind enum from `nodedb-types` so the on-disk snapshot and
/// the DDL config wire format share a single definition.
pub use nodedb_types::PayloadIndexKind;

// ── Single-field index ────────────────────────────────────────────────────────

/// Per-field bitmap storage. `Equality` indexes use a hash map (O(1)
/// point lookup); `Range` indexes use a B-tree (O(log n) point + range
/// scan over keys).
#[derive(Debug, Clone)]
pub enum PayloadIndexBitmaps {
    Equality(HashMap<PayloadKey, RoaringBitmap>),
    Range(std::collections::BTreeMap<PayloadKey, RoaringBitmap>),
}

impl PayloadIndexBitmaps {
    fn for_kind(kind: PayloadIndexKind) -> Self {
        match kind {
            PayloadIndexKind::Range => Self::Range(std::collections::BTreeMap::new()),
            // Boolean is rare-cardinality; equality storage is fine.
            PayloadIndexKind::Equality | PayloadIndexKind::Boolean => {
                Self::Equality(HashMap::new())
            }
        }
    }

    fn get(&self, key: &PayloadKey) -> Option<&RoaringBitmap> {
        match self {
            Self::Equality(m) => m.get(key),
            Self::Range(m) => m.get(key),
        }
    }

    fn entry_or_default(&mut self, key: PayloadKey) -> &mut RoaringBitmap {
        match self {
            Self::Equality(m) => m.entry(key).or_default(),
            Self::Range(m) => m.entry(key).or_default(),
        }
    }

    fn get_mut(&mut self, key: &PayloadKey) -> Option<&mut RoaringBitmap> {
        match self {
            Self::Equality(m) => m.get_mut(key),
            Self::Range(m) => m.get_mut(key),
        }
    }

    fn remove(&mut self, key: &PayloadKey) -> Option<RoaringBitmap> {
        match self {
            Self::Equality(m) => m.remove(key),
            Self::Range(m) => m.remove(key),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&PayloadKey, &RoaringBitmap)> + '_> {
        match self {
            Self::Equality(m) => Box::new(m.iter()),
            Self::Range(m) => Box::new(m.iter()),
        }
    }
}

/// Bitmap index for a single payload field.
#[derive(Debug, Clone)]
pub struct PayloadIndex {
    /// The field name this index covers.
    pub field: String,
    /// Index kind.
    pub kind: PayloadIndexKind,
    /// `value → set of node ids`. Storage layout depends on `kind`.
    pub bitmaps: PayloadIndexBitmaps,
}

impl Default for PayloadIndex {
    fn default() -> Self {
        Self::new(String::new(), PayloadIndexKind::Equality)
    }
}

impl PayloadIndex {
    /// Create a new empty index for `field`.
    pub fn new(field: impl Into<String>, kind: PayloadIndexKind) -> Self {
        Self {
            field: field.into(),
            kind,
            bitmaps: PayloadIndexBitmaps::for_kind(kind),
        }
    }

    /// Record that `node_id` has `value` for this field.
    pub fn insert(&mut self, node_id: u32, value: &Value) {
        let Some(key) = PayloadKey::from_value(value) else {
            return;
        };
        self.bitmaps.entry_or_default(key).insert(node_id);
    }

    /// Remove `node_id` from the bitmap for `value`.
    pub fn delete(&mut self, node_id: u32, value: &Value) {
        let Some(key) = PayloadKey::from_value(value) else {
            return;
        };
        if let Some(bm) = self.bitmaps.get_mut(&key) {
            bm.remove(node_id);
            if bm.is_empty() {
                self.bitmaps.remove(&key);
            }
        }
    }

    /// Return the bitmap of node ids with `field = value`, or `None` if the
    /// value has never been inserted (empty set).
    pub fn equality(&self, value: &Value) -> Option<&RoaringBitmap> {
        let key = PayloadKey::from_value(value)?;
        self.bitmaps.get(&key)
    }

    /// Return the union bitmap of all entries whose key falls in
    /// `[low, high]` (with `low_inclusive` / `high_inclusive` controlling
    /// strictness). `None` bounds mean "open on that side". Returns
    /// `None` when this index is not a `Range` index — the caller falls
    /// back to a full post-filter scan.
    pub fn range(
        &self,
        low: Option<&Value>,
        low_inclusive: bool,
        high: Option<&Value>,
        high_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        let PayloadIndexBitmaps::Range(map) = &self.bitmaps else {
            return None;
        };
        use std::ops::Bound;
        let low_key = low.and_then(PayloadKey::from_value);
        let high_key = high.and_then(PayloadKey::from_value);
        let low_bound = match (&low_key, low_inclusive) {
            (Some(k), true) => Bound::Included(k.clone()),
            (Some(k), false) => Bound::Excluded(k.clone()),
            (None, _) => Bound::Unbounded,
        };
        let high_bound = match (&high_key, high_inclusive) {
            (Some(k), true) => Bound::Included(k.clone()),
            (Some(k), false) => Bound::Excluded(k.clone()),
            (None, _) => Bound::Unbounded,
        };
        let mut acc = RoaringBitmap::new();
        for (_, bm) in map.range((low_bound, high_bound)) {
            acc |= bm;
        }
        Some(acc)
    }
}

// ── Multi-field index set ─────────────────────────────────────────────────────

/// Set of per-field payload bitmap indexes for a collection.
#[derive(Debug, Default)]
pub struct PayloadIndexSet {
    indexes: HashMap<String, PayloadIndex>,
}

impl PayloadIndexSet {
    /// Register a new field index. Idempotent — safe to call multiple times for
    /// the same field (subsequent calls are no-ops).
    pub fn add_index(&mut self, field: impl Into<String>, kind: PayloadIndexKind) {
        let field = field.into();
        self.indexes
            .entry(field.clone())
            .or_insert_with(|| PayloadIndex::new(field, kind));
    }

    /// Returns `true` if any index is registered.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// Insert all indexed fields from `fields` for `node_id`.
    pub fn insert_row(&mut self, node_id: u32, fields: &HashMap<String, Value>) {
        for (field, idx) in &mut self.indexes {
            if let Some(value) = fields.get(field) {
                idx.insert(node_id, value);
            }
        }
    }

    /// Remove all indexed fields for `node_id`.
    pub fn delete_row(&mut self, node_id: u32, fields: &HashMap<String, Value>) {
        for (field, idx) in &mut self.indexes {
            if let Some(value) = fields.get(field) {
                idx.delete(node_id, value);
            }
        }
    }

    /// Evaluate a filter predicate against the bitmap indexes.
    ///
    /// Returns `Some(bitmap)` when the predicate is **fully covered** by
    /// indexed fields (every leaf field has an index). Returns `None` when any
    /// leaf references an un-indexed field — the caller must then fall back to a
    /// full post-filter scan. This avoids silently dropping un-indexed
    /// predicates.
    pub fn pre_filter(&self, predicate: &FilterPredicate) -> Option<RoaringBitmap> {
        match predicate {
            FilterPredicate::Eq { field, value } => {
                let idx = self.indexes.get(field)?;
                Some(
                    idx.equality(value)
                        .cloned()
                        .unwrap_or_else(RoaringBitmap::new),
                )
            }
            FilterPredicate::In { field, values } => {
                let idx = self.indexes.get(field)?;
                let mut acc = RoaringBitmap::new();
                for v in values {
                    if let Some(bm) = idx.equality(v) {
                        acc |= bm;
                    }
                }
                Some(acc)
            }
            FilterPredicate::Range {
                field,
                low,
                low_inclusive,
                high,
                high_inclusive,
            } => {
                let idx = self.indexes.get(field)?;
                idx.range(low.as_ref(), *low_inclusive, high.as_ref(), *high_inclusive)
            }
            FilterPredicate::And(preds) => {
                let mut result: Option<RoaringBitmap> = None;
                for pred in preds {
                    let bm = self.pre_filter(pred)?;
                    result = Some(match result {
                        None => bm,
                        Some(acc) => acc & bm,
                    });
                }
                Some(result.unwrap_or_default())
            }
            FilterPredicate::Or(preds) => {
                let mut result = RoaringBitmap::new();
                for pred in preds {
                    let bm = self.pre_filter(pred)?;
                    result |= bm;
                }
                Some(result)
            }
            FilterPredicate::Not(pred) => {
                // NOT requires knowing the full set of node ids to complement
                // against; we do not have that here, so return None (full scan).
                // The predicate is still valid — callers handle it via
                // post-filter on the HNSW results.
                let _ = pred;
                None
            }
        }
    }

    /// Return an iterator over all registered field names.
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.indexes.keys().map(String::as_str)
    }
}

// ── Filter predicate ──────────────────────────────────────────────────────────

/// A structured filter predicate that can be evaluated against the bitmap
/// indexes. Mirrors the SQL WHERE predicates the planner builds.
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    /// `field = value` equality.
    Eq { field: String, value: Value },
    /// `field IN (v1, v2, ...)` — union of per-value bitmaps.
    /// Lowered to `Or(Eq, Eq, ...)` internally.
    In { field: String, values: Vec<Value> },
    /// `field` within bounded range against a sorted (`Range`) payload
    /// index. Either bound being `None` means open on that side.
    Range {
        field: String,
        low: Option<Value>,
        low_inclusive: bool,
        high: Option<Value>,
        high_inclusive: bool,
    },
    /// Conjunction.
    And(Vec<FilterPredicate>),
    /// Disjunction.
    Or(Vec<FilterPredicate>),
    /// Negation — always falls back to full scan (no complement support).
    Not(Box<FilterPredicate>),
}

// ── Serialization helpers ─────────────────────────────────────────────────────

/// Serializable snapshot of a single `PayloadIndex` for checkpointing.
#[derive(
    serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PayloadIndexSnapshot {
    pub field: String,
    pub kind: PayloadIndexKind,
    /// `(key_bytes, bitmap_bytes)` pairs — key serialised as msgpack,
    /// bitmap serialised via its standard serialization.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Serializable snapshot of the full `PayloadIndexSet`.
#[derive(
    serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PayloadIndexSetSnapshot {
    pub indexes: Vec<PayloadIndexSnapshot>,
}

impl PayloadIndexSet {
    /// Serialize to a snapshot for checkpointing.
    pub fn to_snapshot(&self) -> PayloadIndexSetSnapshot {
        let indexes = self
            .indexes
            .values()
            .map(|idx| {
                let entries = idx
                    .bitmaps
                    .iter()
                    .filter_map(|(key, bm)| {
                        let key_bytes = zerompk::to_msgpack_vec(key).ok()?;
                        let mut bm_bytes = Vec::new();
                        bm.serialize_into(&mut bm_bytes).ok()?;
                        Some((key_bytes, bm_bytes))
                    })
                    .collect();
                PayloadIndexSnapshot {
                    field: idx.field.clone(),
                    kind: idx.kind,
                    entries,
                }
            })
            .collect();
        PayloadIndexSetSnapshot { indexes }
    }

    /// Restore from a checkpoint snapshot.
    pub fn from_snapshot(snap: PayloadIndexSetSnapshot) -> Self {
        let mut indexes = HashMap::new();
        for idx_snap in snap.indexes {
            let mut bitmaps = PayloadIndexBitmaps::for_kind(idx_snap.kind);
            for (key_bytes, bm_bytes) in idx_snap.entries {
                let Ok(key) = zerompk::from_msgpack::<PayloadKey>(&key_bytes) else {
                    continue;
                };
                let Ok(bm) = RoaringBitmap::deserialize_from(bm_bytes.as_slice()) else {
                    continue;
                };
                *bitmaps.entry_or_default(key) = bm;
            }
            let idx = PayloadIndex {
                field: idx_snap.field.clone(),
                kind: idx_snap.kind,
                bitmaps,
            };
            indexes.insert(idx_snap.field, idx);
        }
        Self { indexes }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn string_val(s: &str) -> Value {
        Value::String(s.to_string())
    }
    fn int_val(i: i64) -> Value {
        Value::Integer(i)
    }

    #[test]
    fn insert_and_equality_lookup() {
        let mut idx = PayloadIndex::new("category", PayloadIndexKind::Equality);
        idx.insert(0, &string_val("A"));
        idx.insert(1, &string_val("B"));
        idx.insert(2, &string_val("A"));

        let a_bm = idx.equality(&string_val("A")).unwrap();
        assert!(a_bm.contains(0));
        assert!(a_bm.contains(2));
        assert!(!a_bm.contains(1));

        let b_bm = idx.equality(&string_val("B")).unwrap();
        assert!(b_bm.contains(1));
    }

    #[test]
    fn delete_removes_from_bitmap() {
        let mut idx = PayloadIndex::new("category", PayloadIndexKind::Equality);
        idx.insert(0, &string_val("A"));
        idx.insert(1, &string_val("A"));
        idx.delete(0, &string_val("A"));

        let bm = idx.equality(&string_val("A")).unwrap();
        assert!(!bm.contains(0));
        assert!(bm.contains(1));
    }

    #[test]
    fn delete_empty_bitmap_removed() {
        let mut idx = PayloadIndex::new("x", PayloadIndexKind::Equality);
        idx.insert(5, &string_val("v"));
        idx.delete(5, &string_val("v"));
        // Entry for "v" should be gone.
        assert!(idx.equality(&string_val("v")).is_none());
    }

    #[test]
    fn pre_filter_equality() {
        let mut set = PayloadIndexSet::default();
        set.add_index("category", PayloadIndexKind::Equality);

        let mut fields = HashMap::new();
        for i in 0u32..10 {
            fields.clear();
            fields.insert(
                "category".to_string(),
                string_val(if i < 5 { "A" } else { "B" }),
            );
            set.insert_row(i, &fields);
        }

        let pred = FilterPredicate::Eq {
            field: "category".to_string(),
            value: string_val("A"),
        };
        let bm = set.pre_filter(&pred).unwrap();
        assert_eq!(bm.len(), 5);
        for i in 0..5u32 {
            assert!(bm.contains(i));
        }
    }

    #[test]
    fn pre_filter_and() {
        let mut set = PayloadIndexSet::default();
        set.add_index("category", PayloadIndexKind::Equality);
        set.add_index("active", PayloadIndexKind::Boolean);

        let mut fields = HashMap::new();
        // node 0: A + true
        fields.insert("category".to_string(), string_val("A"));
        fields.insert("active".to_string(), Value::Bool(true));
        set.insert_row(0, &fields);
        // node 1: A + false
        fields.insert("active".to_string(), Value::Bool(false));
        set.insert_row(1, &fields);
        // node 2: B + true
        fields.insert("category".to_string(), string_val("B"));
        fields.insert("active".to_string(), Value::Bool(true));
        set.insert_row(2, &fields);

        let pred = FilterPredicate::And(vec![
            FilterPredicate::Eq {
                field: "category".to_string(),
                value: string_val("A"),
            },
            FilterPredicate::Eq {
                field: "active".to_string(),
                value: Value::Bool(true),
            },
        ]);
        let bm = set.pre_filter(&pred).unwrap();
        assert_eq!(bm.len(), 1);
        assert!(bm.contains(0));
    }

    #[test]
    fn pre_filter_or() {
        let mut set = PayloadIndexSet::default();
        set.add_index("category", PayloadIndexKind::Equality);

        let mut fields = HashMap::new();
        for i in 0u32..9 {
            fields.clear();
            let cat = match i % 3 {
                0 => "A",
                1 => "B",
                _ => "C",
            };
            fields.insert("category".to_string(), string_val(cat));
            set.insert_row(i, &fields);
        }

        let pred = FilterPredicate::Or(vec![
            FilterPredicate::Eq {
                field: "category".to_string(),
                value: string_val("A"),
            },
            FilterPredicate::Eq {
                field: "category".to_string(),
                value: string_val("B"),
            },
        ]);
        let bm = set.pre_filter(&pred).unwrap();
        assert_eq!(bm.len(), 6);
    }

    #[test]
    fn pre_filter_missing_index_returns_none() {
        let set = PayloadIndexSet::default(); // no indexes

        let pred = FilterPredicate::Eq {
            field: "category".to_string(),
            value: string_val("A"),
        };
        assert!(set.pre_filter(&pred).is_none());
    }

    #[test]
    fn pre_filter_not_returns_none() {
        let mut set = PayloadIndexSet::default();
        set.add_index("category", PayloadIndexKind::Equality);

        let pred = FilterPredicate::Not(Box::new(FilterPredicate::Eq {
            field: "category".to_string(),
            value: string_val("A"),
        }));
        // NOT always requires full scan.
        assert!(set.pre_filter(&pred).is_none());
    }

    #[test]
    fn pre_filter_and_partial_index_returns_none() {
        // AND where one field is indexed but the other is not — must return None.
        let mut set = PayloadIndexSet::default();
        set.add_index("category", PayloadIndexKind::Equality);

        let pred = FilterPredicate::And(vec![
            FilterPredicate::Eq {
                field: "category".to_string(),
                value: string_val("A"),
            },
            FilterPredicate::Eq {
                field: "unindexed_field".to_string(),
                value: int_val(42),
            },
        ]);
        assert!(set.pre_filter(&pred).is_none());
    }

    #[test]
    fn nan_equality() {
        let mut idx = PayloadIndex::new("score", PayloadIndexKind::Equality);
        idx.insert(0, &Value::Float(f64::NAN));
        idx.insert(1, &Value::Float(f64::NAN));
        idx.insert(2, &Value::Float(1.0));

        // Both NaN-bearing nodes should be in the same bucket.
        let bm = idx.equality(&Value::Float(f64::NAN)).unwrap();
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(0));
        assert!(bm.contains(1));
    }

    #[test]
    fn null_value_indexed() {
        let mut idx = PayloadIndex::new("f", PayloadIndexKind::Equality);
        idx.insert(0, &Value::Null);
        idx.insert(1, &Value::Null);
        let bm = idx.equality(&Value::Null).unwrap();
        assert_eq!(bm.len(), 2);
    }

    #[test]
    fn snapshot_roundtrip() {
        let mut set = PayloadIndexSet::default();
        set.add_index("category", PayloadIndexKind::Equality);

        let mut fields = HashMap::new();
        for i in 0u32..6 {
            fields.clear();
            fields.insert(
                "category".to_string(),
                string_val(if i < 3 { "A" } else { "B" }),
            );
            set.insert_row(i, &fields);
        }

        let snap = set.to_snapshot();
        let snap_bytes = zerompk::to_msgpack_vec(&snap).unwrap();
        let restored_snap: PayloadIndexSetSnapshot = zerompk::from_msgpack(&snap_bytes).unwrap();
        let restored = PayloadIndexSet::from_snapshot(restored_snap);

        let pred = FilterPredicate::Eq {
            field: "category".to_string(),
            value: string_val("A"),
        };
        let bm = restored.pre_filter(&pred).unwrap();
        assert_eq!(bm.len(), 3);
    }
}
