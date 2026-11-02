// SPDX-License-Identifier: BUSL-1.1

//! `KvIndexSet`: every secondary index registered on one KV collection.
//!
//! Tracks write amplification and provides the zero-index fast path check
//! (`is_empty()`), which lets the write path skip field extraction entirely.

use super::composite::KvCompositeIndex;
use super::coverage::IndexCoverage;
use super::field::KvFieldIndex;
use super::key::NULL_KEY;

/// Manages all secondary indexes for a single KV collection.
///
/// Tracks write amplification and provides the zero-index fast path check.
#[derive(Debug)]
pub struct KvIndexSet {
    /// Single-field indexes.
    indexes: Vec<KvFieldIndex>,
    /// Composite (multi-field) indexes.
    composite_indexes: Vec<KvCompositeIndex>,
    /// Total PUT operations on this collection (denominator for write-amp ratio).
    total_puts: u64,
    /// Total index write operations (numerator for write-amp ratio).
    total_index_writes: u64,
}

/// The composite key parts of a row: each indexed field's key, NULL's key
/// for a field the row lacks. Every row is filed, so a prefix probe on the
/// leading field finds every row holding that value.
fn composite_vals<'a>(ci: &KvCompositeIndex, values: &'a [(&str, &[u8])]) -> Vec<&'a [u8]> {
    ci.fields()
        .iter()
        .map(|f| {
            values
                .iter()
                .find(|(name, _)| *name == f.as_str())
                .map_or(NULL_KEY, |(_, v)| *v)
        })
        .collect()
}

impl KvIndexSet {
    pub fn new() -> Self {
        Self {
            indexes: Vec::new(),
            composite_indexes: Vec::new(),
            total_puts: 0,
            total_index_writes: 0,
        }
    }

    /// Whether this collection has zero secondary indexes (fast path eligible).
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty() && self.composite_indexes.is_empty()
    }

    /// Number of active indexes (single + composite).
    pub fn index_count(&self) -> usize {
        self.indexes.len() + self.composite_indexes.len()
    }

    /// Every single-field index, for checkpoint export.
    pub fn field_indexes(&self) -> &[KvFieldIndex] {
        &self.indexes
    }

    /// Every composite index, for checkpoint export.
    pub fn composite_indexes(&self) -> &[KvCompositeIndex] {
        &self.composite_indexes
    }

    /// Add a new index on a field. Returns false if already indexed.
    pub fn add_index(
        &mut self,
        field: &str,
        field_position: usize,
        coverage: IndexCoverage,
    ) -> bool {
        if self.indexes.iter().any(|i| i.field() == field) {
            return false;
        }
        self.indexes
            .push(KvFieldIndex::new(field, field_position, coverage));
        true
    }

    /// Remove an index on a field. Returns the removed index, or None if not found.
    pub fn remove_index(&mut self, field: &str) -> Option<KvFieldIndex> {
        if let Some(pos) = self.indexes.iter().position(|i| i.field() == field) {
            Some(self.indexes.remove(pos))
        } else {
            None
        }
    }

    /// Get an index by field name.
    pub fn get_index(&self, field: &str) -> Option<&KvFieldIndex> {
        self.indexes.iter().find(|i| i.field() == field)
    }

    /// Mutable access to one field index, for the checkpoint restore path to
    /// refill a freshly registered index with the content it was exported with.
    pub fn get_index_mut(&mut self, field: &str) -> Option<&mut KvFieldIndex> {
        self.indexes.iter_mut().find(|i| i.field() == field)
    }

    /// Add a composite index on multiple fields. Returns false if already exists.
    pub fn add_composite_index(
        &mut self,
        fields: Vec<String>,
        field_positions: Vec<usize>,
        coverage: IndexCoverage,
    ) -> bool {
        if self
            .composite_indexes
            .iter()
            .any(|ci| ci.fields() == fields)
        {
            return false;
        }
        self.composite_indexes
            .push(KvCompositeIndex::new(fields, field_positions, coverage));
        true
    }

    /// Remove a composite index. Returns the removed index, or None.
    pub fn remove_composite_index(&mut self, fields: &[String]) -> Option<KvCompositeIndex> {
        if let Some(pos) = self
            .composite_indexes
            .iter()
            .position(|ci| ci.fields() == fields)
        {
            Some(self.composite_indexes.remove(pos))
        } else {
            None
        }
    }

    /// Get a composite index by its field list.
    pub fn get_composite_index(&self, fields: &[String]) -> Option<&KvCompositeIndex> {
        self.composite_indexes
            .iter()
            .find(|ci| ci.fields() == fields)
    }

    /// Mutable access to one composite index, the restore-path counterpart of
    /// [`KvIndexSet::get_index_mut`].
    pub fn get_composite_index_mut(&mut self, fields: &[String]) -> Option<&mut KvCompositeIndex> {
        self.composite_indexes
            .iter_mut()
            .find(|ci| ci.fields() == fields)
    }

    /// The primary keys a complete index files under `key` for `field`: the
    /// field's own index, else a composite index leading with the field.
    ///
    /// `Some` is every candidate row, so `Some(empty)` means no row matches.
    /// `None` when no complete index covers the field: a
    /// `SinceRegistration` index lacks older rows, so the caller reads the
    /// whole collection.
    pub fn complete_eq_candidates(&self, field: &str, key: &[u8]) -> Option<Vec<&[u8]>> {
        if let Some(index) = self
            .indexes
            .iter()
            .find(|i| i.field() == field && i.coverage().is_complete())
        {
            return Some(index.lookup_eq(key));
        }
        self.composite_indexes
            .iter()
            .find(|ci| {
                ci.coverage().is_complete() && ci.fields().first().is_some_and(|f| f == field)
            })
            .map(|ci| ci.lookup_prefix(&[key]))
    }

    /// Record a PUT and update all indexes with the new field values.
    ///
    /// `field_values` is an iterator of `(field_name, field_value_bytes)` extracted
    /// from the value being inserted. Only indexed fields are processed.
    ///
    /// Returns the number of index writes performed.
    pub fn on_put(
        &mut self,
        primary_key: &[u8],
        field_values: &[(&str, &[u8])],
        old_field_values: Option<&[(&str, &[u8])]>,
    ) -> usize {
        self.total_puts += 1;

        if self.is_empty() {
            return 0;
        }

        let mut writes = 0;

        // Single-field indexes. Elide a no-op in-place update: when the old
        // indexed value equals the new one *and* the pair is already present,
        // remove+insert is pure churn. A row written before a backfill=false
        // registration is absent from the index; the next PUT with identical
        // bytes must insert it.
        if let Some(old_values) = old_field_values {
            for idx in &mut self.indexes {
                let f = idx.field();
                let new_val = field_values
                    .iter()
                    .find(|(field, _)| *field == f)
                    .map(|(_, v)| *v);
                let old_val = old_values
                    .iter()
                    .find(|(field, _)| *field == f)
                    .map(|(_, v)| *v);
                if new_val == old_val {
                    if let Some(v) = new_val {
                        if idx.contains(v, primary_key) {
                            continue; // true no-op: already indexed
                        }
                        // old == new but absent (backfill=false gap): insert only —
                        // there's no stale entry to remove, so don't count/perform one.
                        idx.insert(v.to_vec(), primary_key.to_vec());
                        writes += 1;
                    }
                    continue;
                }
                if let Some(v) = old_val {
                    idx.remove(v, primary_key);
                    writes += 1;
                }
                if let Some(v) = new_val {
                    idx.insert(v.to_vec(), primary_key.to_vec());
                    writes += 1;
                }
            }
        } else {
            // Insert-only (new row): no old values to compare.
            for idx in &mut self.indexes {
                for &(field, value) in field_values {
                    if field == idx.field() {
                        idx.insert(value.to_vec(), primary_key.to_vec());
                        writes += 1;
                    }
                }
            }
        }

        // Maintain composite indexes.
        for ci in &mut self.composite_indexes {
            // Composite entry, eliding the same no-op update case.
            let new_vals = composite_vals(ci, field_values);
            match old_field_values.map(|old_values| composite_vals(ci, old_values)) {
                Some(old_vals) if old_vals == new_vals => {
                    if !ci.contains(&new_vals, primary_key) {
                        ci.insert(&new_vals, primary_key.to_vec());
                        writes += 1;
                    }
                }
                Some(old_vals) => {
                    ci.remove(&old_vals, primary_key);
                    ci.insert(&new_vals, primary_key.to_vec());
                    writes += 2;
                }
                None => {
                    ci.insert(&new_vals, primary_key.to_vec());
                    writes += 1;
                }
            }
        }

        self.total_index_writes += writes as u64;
        writes
    }

    /// Remove all index entries for a deleted primary key.
    ///
    /// `field_values` are the field values from the deleted entry.
    pub fn on_delete(&mut self, primary_key: &[u8], field_values: &[(&str, &[u8])]) {
        for idx in &mut self.indexes {
            for &(field, value) in field_values {
                if field == idx.field() {
                    idx.remove(value, primary_key);
                    self.total_index_writes += 1;
                }
            }
        }

        // Maintain composite indexes on delete.
        for ci in &mut self.composite_indexes {
            let vals = composite_vals(ci, field_values);
            ci.remove(&vals, primary_key);
            self.total_index_writes += 1;
        }
    }

    /// Remove every entry of every index and keep each registration. A
    /// TRUNCATE empties the indexes with the rows it removes.
    pub fn clear_entries(&mut self) {
        for idx in &mut self.indexes {
            idx.clear();
        }
        for ci in &mut self.composite_indexes {
            ci.clear();
        }
    }

    /// Write amplification ratio: total_index_writes / total_puts.
    ///
    /// Returns 0.0 if no PUTs have been performed.
    pub fn write_amp_ratio(&self) -> f64 {
        if self.total_puts == 0 {
            return 0.0;
        }
        self.total_index_writes as f64 / self.total_puts as f64
    }

    /// Lookup primary keys by exact field value match.
    pub fn lookup_eq(&self, field: &str, value: &[u8]) -> Vec<&[u8]> {
        self.indexes
            .iter()
            .find(|i| i.field() == field)
            .map(|i| i.lookup_eq(value))
            .unwrap_or_default()
    }

    /// Lookup primary keys by field value range.
    pub fn lookup_range(
        &self,
        field: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Vec<(&[u8], &[u8])> {
        self.indexes
            .iter()
            .find(|i| i.field() == field)
            .map(|i| i.lookup_range(lower, upper))
            .unwrap_or_default()
    }

    /// Iterator over all index field names.
    pub fn indexed_fields(&self) -> impl Iterator<Item = &str> {
        self.indexes.iter().map(|i| i.field())
    }
}

impl Default for KvIndexSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPLETE: IndexCoverage = IndexCoverage::Complete;

    /// A complete index answers a miss with no candidates; a partial one
    /// answers nothing, so the caller reads every row.
    #[test]
    fn only_a_complete_index_answers_an_equality() {
        let mut set = KvIndexSet::new();
        set.add_index("region", 0, COMPLETE);
        set.add_index("status", 1, IndexCoverage::SinceRegistration);
        set.on_put(b"k1", &[("region", b"us"), ("status", b"on")], None);

        assert_eq!(
            set.complete_eq_candidates("region", b"us"),
            Some(vec![b"k1".as_slice()])
        );
        assert_eq!(
            set.complete_eq_candidates("region", b"eu"),
            Some(Vec::new())
        );
        assert_eq!(set.complete_eq_candidates("status", b"on"), None);
        assert_eq!(set.complete_eq_candidates("name", b"x"), None);
    }

    /// A partial field index gives way to a complete composite leading with
    /// the field; a partial composite answers nothing.
    #[test]
    fn a_complete_composite_answers_for_its_leading_field() {
        let mut set = KvIndexSet::new();
        set.add_index("region", 0, IndexCoverage::SinceRegistration);
        set.add_composite_index(vec!["region".into(), "status".into()], vec![0, 1], COMPLETE);
        set.add_composite_index(
            vec!["status".into(), "region".into()],
            vec![1, 0],
            IndexCoverage::SinceRegistration,
        );
        set.on_put(b"k1", &[("region", b"us"), ("status", b"on")], None);

        assert_eq!(
            set.complete_eq_candidates("region", b"us"),
            Some(vec![b"k1".as_slice()])
        );
        assert_eq!(set.complete_eq_candidates("status", b"on"), None);
    }

    #[test]
    fn index_set_zero_index_fast_path() {
        let set = KvIndexSet::new();
        assert!(set.is_empty());
        assert_eq!(set.index_count(), 0);
    }

    #[test]
    fn index_set_add_and_remove() {
        let mut set = KvIndexSet::new();
        assert!(set.add_index("region", 2, COMPLETE));
        assert!(!set.add_index("region", 2, COMPLETE)); // Duplicate.
        assert_eq!(set.index_count(), 1);
        assert!(!set.is_empty());

        assert!(set.remove_index("region").is_some());
        assert!(set.is_empty());
        assert!(set.remove_index("region").is_none());
    }

    #[test]
    fn index_set_on_put_maintains_indexes() {
        let mut set = KvIndexSet::new();
        set.add_index("region", 2, COMPLETE);
        set.add_index("status", 3, COMPLETE);

        let field_values: Vec<(&str, &[u8])> = vec![("region", b"us-east"), ("status", b"active")];

        let writes = set.on_put(b"key1", &field_values, None);
        assert_eq!(writes, 2); // One per index.

        assert_eq!(set.lookup_eq("region", b"us-east").len(), 1);
        assert_eq!(set.lookup_eq("status", b"active").len(), 1);
    }

    #[test]
    fn identical_update_elides_index_writes() {
        // An in-place update that leaves the indexed value unchanged must not
        // rewrite the index when the pair is already present.
        let mut set = KvIndexSet::new();
        set.add_index("status", 0, COMPLETE);
        set.on_put(b"k1", &[("status", b"active")], None);
        let writes = set.on_put(
            b"k1",
            &[("status", b"active")],
            Some(&[("status", b"active")]),
        );
        assert_eq!(
            writes, 0,
            "identical update must elide index writes (got {writes})"
        );
    }

    #[test]
    fn index_set_on_put_update_replaces_old() {
        let mut set = KvIndexSet::new();
        set.add_index("status", 0, COMPLETE);

        // Insert.
        set.on_put(b"k1", &[("status", b"active")], None);
        assert_eq!(set.lookup_eq("status", b"active").len(), 1);

        // Update: old was "active", new is "inactive".
        set.on_put(
            b"k1",
            &[("status", b"inactive")],
            Some(&[("status", b"active")]),
        );
        assert!(set.lookup_eq("status", b"active").is_empty());
        assert_eq!(set.lookup_eq("status", b"inactive").len(), 1);
    }

    #[test]
    fn index_set_on_delete_cleans_up() {
        let mut set = KvIndexSet::new();
        set.add_index("region", 0, COMPLETE);

        set.on_put(b"k1", &[("region", b"us")], None);
        set.on_put(b"k2", &[("region", b"us")], None);
        assert_eq!(set.lookup_eq("region", b"us").len(), 2);

        set.on_delete(b"k1", &[("region", b"us")]);
        assert_eq!(set.lookup_eq("region", b"us").len(), 1);
    }

    #[test]
    fn write_amp_ratio() {
        let mut set = KvIndexSet::new();
        set.add_index("a", 0, COMPLETE);
        set.add_index("b", 1, COMPLETE);

        for i in 0..10 {
            let k = format!("k{i}");
            set.on_put(k.as_bytes(), &[("a", b"x"), ("b", b"y")], None);
        }
        // 10 PUTs, 2 index writes each = 20 index writes.
        assert!((set.write_amp_ratio() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn unindexed_field_ignored() {
        let mut set = KvIndexSet::new();
        set.add_index("region", 0, COMPLETE);

        // PUT with a field that isn't indexed — should be ignored.
        let writes = set.on_put(b"k1", &[("name", b"alice")], None);
        assert_eq!(writes, 0);
    }

    #[test]
    fn index_set_composite_on_put() {
        let mut set = KvIndexSet::new();
        set.add_composite_index(vec!["region".into(), "status".into()], vec![0, 1], COMPLETE);

        let writes = set.on_put(b"k1", &[("region", b"us"), ("status", b"active")], None);
        assert!(writes > 0);

        // Lookup via composite index.
        let ci = set
            .get_composite_index(&["region".into(), "status".into()])
            .expect("composite index was registered");
        let results = ci.lookup_eq(&[b"us", b"active"]);
        assert_eq!(results.len(), 1);
    }

    /// A row lacking a trailing composite field is filed with that field
    /// NULL, so a prefix probe on the leading field finds it, and its delete
    /// removes it.
    #[test]
    fn a_row_missing_a_composite_field_is_filed_as_null() {
        let mut set = KvIndexSet::new();
        set.add_composite_index(vec!["region".into(), "status".into()], vec![0, 1], COMPLETE);
        let fields = vec!["region".to_string(), "status".to_string()];

        set.on_put(b"k1", &[("region", b"us")], None);
        let ci = set.get_composite_index(&fields).expect("registered");
        assert_eq!(ci.lookup_prefix(&[b"us"]), vec![b"k1".as_slice()]);
        assert_eq!(ci.lookup_eq(&[b"us", NULL_KEY]), vec![b"k1".as_slice()]);

        set.on_delete(b"k1", &[("region", b"us")]);
        let ci = set.get_composite_index(&fields).expect("registered");
        assert!(ci.lookup_prefix(&[b"us"]).is_empty());
    }

    #[test]
    fn index_set_composite_on_delete() {
        let mut set = KvIndexSet::new();
        set.add_composite_index(vec!["a".into(), "b".into()], vec![0, 1], COMPLETE);

        set.on_put(b"k1", &[("a", b"x"), ("b", b"y")], None);
        set.on_delete(b"k1", &[("a", b"x"), ("b", b"y")]);

        let ci = set
            .get_composite_index(&["a".into(), "b".into()])
            .expect("composite index was registered");
        assert!(ci.lookup_eq(&[b"x", b"y"]).is_empty());
    }

    #[test]
    fn backfill_absent_pair_still_gets_inserted() {
        // old == new alone is not enough to elide — the pair must actually be
        // present. A row written before a backfill=false registration was never
        // indexed, so the next identical PUT has to insert it.
        let mut set = KvIndexSet::new();
        set.add_index("status", 0, COMPLETE);
        // No prior on_put — simulates backfill=false: this row exists but
        // was never run through the indexer.
        let writes = set.on_put(
            b"k1",
            &[("status", b"active")],
            Some(&[("status", b"active")]), // old == new, but never indexed
        );
        assert_eq!(
            writes, 1,
            "backfill=false: absent pair must insert, got {writes}"
        );
        assert_eq!(set.lookup_eq("status", b"active"), vec![b"k1".as_slice()]);
    }

    #[test]
    fn composite_backfill_absent_pair_still_gets_inserted() {
        let mut set = KvIndexSet::new();
        set.add_composite_index(vec!["region".into(), "status".into()], vec![0, 1], COMPLETE);
        let writes = set.on_put(
            b"k1",
            &[("region", b"us-east"), ("status", b"active")],
            Some(&[("region", b"us-east"), ("status", b"active")]),
        );
        assert_eq!(
            writes, 1,
            "composite backfill=false: absent pair must insert"
        );
        let ci = set
            .get_composite_index(&["region".into(), "status".into()])
            .expect("composite index was registered");
        assert_eq!(ci.lookup_eq(&[b"us-east", b"active"]).len(), 1);
    }

    /// The export accessors must see exactly the indexes that were registered —
    /// a checkpoint that iterated a partial view would publish rows whose index
    /// registrations are missing.
    #[test]
    fn export_accessors_see_every_registration() {
        let mut set = KvIndexSet::new();
        set.add_index("region", 2, COMPLETE);
        set.add_composite_index(vec!["a".into(), "b".into()], vec![0, 1], COMPLETE);

        assert_eq!(set.field_indexes().len(), 1);
        assert_eq!(set.field_indexes()[0].field(), "region");
        assert_eq!(set.field_indexes()[0].field_position(), 2);
        assert_eq!(set.composite_indexes().len(), 1);
        assert_eq!(set.composite_indexes()[0].field_positions(), &[0, 1]);
        assert_eq!(
            set.field_indexes().len() + set.composite_indexes().len(),
            set.index_count()
        );
    }
}
