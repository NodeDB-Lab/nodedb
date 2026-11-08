// SPDX-License-Identifier: BUSL-1.1

//! Column statistics for cost-based query optimization.
//!
//! Maintains per-collection, per-field statistics in redb metadata tables,
//! updated incrementally on writes. Used by the CBO to select join strategies,
//! estimate result cardinality, and choose scan methods.

use std::sync::Arc;

use redb::{ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

use crate::engine::durability_gate::GatedDatabase;

/// Redb table for column statistics.
/// Key: "{database_id}:{tenant}:{collection}:{field}" → Value: serialized ColumnStats.
const COLUMN_STATS: TableDefinition<&str, &[u8]> = TableDefinition::new("column_stats");

/// Pre-image of a single column's stats captured before a transactional
/// observe: the composed `COLUMN_STATS` key and the serialized `ColumnStats`
/// bytes that existed before the merge (`None` = no stats existed for that
/// key). Because `observe_document_in_txn` is READ-MODIFY-WRITE, restoring
/// these exact bytes is the only way to reverse a committed stats mutation.
pub type StatsPreImage = (String, Option<Vec<u8>>);

/// Statistics for a single column in a collection.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ColumnStats {
    /// Total number of documents observed (including those without this field).
    pub row_count: u64,
    /// Number of documents that have this field with a non-null value.
    pub non_null_count: u64,
    /// Number of null values (field absent or explicitly null).
    pub null_count: u64,
    /// Approximate number of distinct values (HyperLogLog estimate).
    pub distinct_count: u64,
    /// Minimum value observed (as JSON string for cross-type comparison).
    pub min_value: Option<String>,
    /// Maximum value observed (as JSON string for cross-type comparison).
    pub max_value: Option<String>,
    /// HyperLogLog registers for cardinality estimation.
    /// 256 registers (m=256) give ~6.5% standard error, good enough for CBO.
    pub hll_registers: Vec<u8>,
}

/// Default number of HLL registers. 256 = 2^8, giving ~6.5% standard error.
/// Sourced from `SparseTuning::hll_registers` at runtime.
pub(crate) const DEFAULT_HLL_M: usize = 256;
/// Default HLL precision bits (log2 of `DEFAULT_HLL_M`).
/// Sourced from `SparseTuning::hll_precision` at runtime.
pub(crate) const DEFAULT_HLL_P: u32 = 8;

impl ColumnStats {
    /// Create empty statistics for a new column.
    pub fn new() -> Self {
        Self {
            row_count: 0,
            non_null_count: 0,
            null_count: 0,
            distinct_count: 0,
            min_value: None,
            max_value: None,
            hll_registers: vec![0u8; DEFAULT_HLL_M],
        }
    }

    /// Update statistics with a new observed value.
    ///
    /// Call this on every write (PointPut) for each field in the document.
    pub fn observe(&mut self, value: Option<&serde_json::Value>) {
        self.row_count += 1;

        match value {
            None | Some(serde_json::Value::Null) => {
                self.null_count += 1;
            }
            Some(val) => {
                self.non_null_count += 1;

                // Update min/max.
                let val_str = match val {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                match &self.min_value {
                    None => self.min_value = Some(val_str.clone()),
                    Some(min) if val_str < *min => self.min_value = Some(val_str.clone()),
                    _ => {}
                }
                match &self.max_value {
                    None => self.max_value = Some(val_str.clone()),
                    Some(max) if val_str > *max => self.max_value = Some(val_str.clone()),
                    _ => {}
                }

                // Update HyperLogLog for cardinality estimation.
                let hash = crate::util::fnv1a_hash(val_str.as_bytes());
                let register_idx = (hash as usize) & (DEFAULT_HLL_M - 1);
                let remaining = hash >> DEFAULT_HLL_P;
                let leading_zeros = if remaining == 0 {
                    (64 - DEFAULT_HLL_P) as u8
                } else {
                    remaining.trailing_zeros() as u8 + 1
                };
                if leading_zeros > self.hll_registers[register_idx] {
                    self.hll_registers[register_idx] = leading_zeros;
                }

                // Re-estimate distinct count from HLL registers.
                self.distinct_count = self.hll_estimate();
            }
        }
    }

    /// HyperLogLog cardinality estimate.
    fn hll_estimate(&self) -> u64 {
        let m = self.hll_registers.len() as f64;
        // Alpha constant for m=256.
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw: f64 = alpha * m * m
            / self
                .hll_registers
                .iter()
                .map(|&r| 2.0_f64.powi(-(r as i32)))
                .sum::<f64>();

        if raw <= 2.5 * m {
            // Small range correction.
            let zeros = self.hll_registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                (m * (m / zeros).ln()) as u64
            } else {
                raw as u64
            }
        } else {
            raw as u64
        }
    }

    /// Selectivity estimate for equality predicate (1 / distinct_count).
    pub fn eq_selectivity(&self) -> f64 {
        if self.distinct_count == 0 {
            1.0
        } else {
            1.0 / self.distinct_count as f64
        }
    }

    /// Selectivity estimate for range predicate (heuristic: 0.33).
    pub fn range_selectivity(&self) -> f64 {
        0.33
    }
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Column statistics store backed by redb.
pub struct StatsStore {
    /// The sparse engine's gated database, shared.
    db: Arc<GatedDatabase>,
}

impl StatsStore {
    /// Open or create the stats store sharing a redb database.
    pub fn open(db: Arc<GatedDatabase>) -> crate::Result<Self> {
        // Ensure the table exists.
        let write_txn = db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("open write txn: {e}"),
        })?;
        {
            let _ = write_txn.open_table(COLUMN_STATS);
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(Self { db })
    }

    /// Load statistics for a column.
    pub fn get(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
    ) -> crate::Result<Option<ColumnStats>> {
        let key = format!("{database_id}:{tenant_id}:{collection}:{field}");
        let read_txn = self.db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("read txn: {e}"),
        })?;
        let table = read_txn
            .open_table(COLUMN_STATS)
            .map_err(|e| crate::Error::Storage {
                engine: "stats".into(),
                detail: format!("open table: {e}"),
            })?;
        match table.get(key.as_str()) {
            Ok(Some(guard)) => {
                let bytes = guard.value();
                let stats: ColumnStats =
                    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
                        engine: "stats".into(),
                        detail: format!("deserialize: {e}"),
                    })?;
                Ok(Some(stats))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(crate::Error::Storage {
                engine: "stats".into(),
                detail: format!("get: {e}"),
            }),
        }
    }

    /// Persist updated statistics for a column.
    pub fn put(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        stats: &ColumnStats,
    ) -> crate::Result<()> {
        let key = format!("{database_id}:{tenant_id}:{collection}:{field}");
        let bytes = zerompk::to_msgpack_vec(stats).map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("serialize: {e}"),
        })?;
        let write_txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("write txn: {e}"),
        })?;
        {
            let mut table =
                write_txn
                    .open_table(COLUMN_STATS)
                    .map_err(|e| crate::Error::Storage {
                        engine: "stats".into(),
                        detail: format!("open table: {e}"),
                    })?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "stats".into(),
                    detail: format!("insert: {e}"),
                })?;
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }

    /// Update statistics incrementally for a document's fields.
    ///
    /// Called on every PointPut. Loads existing stats for each field,
    /// observes the new value, and persists.
    pub fn observe_document(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        doc: &serde_json::Value,
    ) -> crate::Result<()> {
        if let Some(obj) = doc.as_object() {
            for (field, value) in obj {
                let mut stats = self
                    .get(database_id, tenant_id, collection, field)?
                    .unwrap_or_default();
                stats.observe(Some(value));
                self.put(database_id, tenant_id, collection, field, &stats)?;
            }
        }
        Ok(())
    }

    /// Update statistics within an externally-owned write transaction.
    ///
    /// Opens the COLUMN_STATS table once and reads/writes all fields in a
    /// single table open, eliminating per-field transaction overhead.
    ///
    /// Returns one [`StatsPreImage`] per touched `(collection, field)` — the
    /// exact serialized `ColumnStats` bytes that existed BEFORE the merge (or
    /// `None` when no stats existed). A transactional caller records these so a
    /// rollback can restore the pre-image, closing the read-modify-write hole
    /// (an aborted redb txn does NOT reverse a stats mutation this batch already
    /// committed).
    pub fn observe_document_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        doc: &serde_json::Value,
    ) -> crate::Result<Vec<StatsPreImage>> {
        let Some(obj) = doc.as_object() else {
            return Ok(Vec::new());
        };
        if obj.is_empty() {
            return Ok(Vec::new());
        }

        let mut pre_images: Vec<StatsPreImage> = Vec::with_capacity(obj.len());
        let mut table = txn
            .open_table(COLUMN_STATS)
            .map_err(|e| crate::Error::Storage {
                engine: "stats".into(),
                detail: format!("open table: {e}"),
            })?;

        for (field, value) in obj {
            let key = format!("{database_id}:{tenant_id}:{collection}:{field}");

            // Capture the pre-image bytes (the guard is dropped by `to_vec`,
            // releasing the immutable borrow before the `insert` below), then
            // deserialize the same bytes for the merge — one read, not two.
            let prior_bytes: Option<Vec<u8>> = table
                .get(key.as_str())
                .ok()
                .flatten()
                .map(|guard| guard.value().to_vec());

            let mut stats: ColumnStats = prior_bytes
                .as_deref()
                .and_then(|b| zerompk::from_msgpack(b).ok())
                .unwrap_or_default();

            stats.observe(Some(value));

            let bytes = zerompk::to_msgpack_vec(&stats).map_err(|e| crate::Error::Storage {
                engine: "stats".into(),
                detail: format!("serialize: {e}"),
            })?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "stats".into(),
                    detail: format!("insert: {e}"),
                })?;

            pre_images.push((key, prior_bytes));
        }

        Ok(pre_images)
    }

    /// Restore a column-stats pre-image captured by
    /// [`observe_document_in_txn`](Self::observe_document_in_txn), reversing a
    /// committed read-modify-write on rollback. Opens its own write txn.
    ///
    /// `prior = Some(bytes)` rewrites the exact `ColumnStats` that existed
    /// before the op; `prior = None` removes the key (no stats existed before,
    /// so the op created it). Reuses the same `COLUMN_STATS` table and key that
    /// `observe_document_in_txn` produced.
    pub fn restore(&self, key: &str, prior: Option<&[u8]>) -> crate::Result<()> {
        let write_txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("write txn: {e}"),
        })?;
        {
            let mut table =
                write_txn
                    .open_table(COLUMN_STATS)
                    .map_err(|e| crate::Error::Storage {
                        engine: "stats".into(),
                        detail: format!("open table: {e}"),
                    })?;
            match prior {
                Some(bytes) => {
                    table
                        .insert(key, bytes)
                        .map_err(|e| crate::Error::Storage {
                            engine: "stats".into(),
                            detail: format!("insert: {e}"),
                        })?;
                }
                None => {
                    table.remove(key).map_err(|e| crate::Error::Storage {
                        engine: "stats".into(),
                        detail: format!("remove: {e}"),
                    })?;
                }
            }
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_cardinality_estimate() {
        let mut stats = ColumnStats::new();
        for i in 0..1000 {
            stats.observe(Some(&serde_json::Value::String(format!("value_{i}"))));
        }
        // HLL with 256 registers should be within ~20% of 1000.
        assert!(
            stats.distinct_count > 700,
            "too low: {}",
            stats.distinct_count
        );
        assert!(
            stats.distinct_count < 1400,
            "too high: {}",
            stats.distinct_count
        );
    }

    #[test]
    fn min_max_tracking() {
        let mut stats = ColumnStats::new();
        for v in &["charlie", "alice", "bob"] {
            stats.observe(Some(&serde_json::Value::String(v.to_string())));
        }
        assert_eq!(stats.min_value.as_deref(), Some("alice"));
        assert_eq!(stats.max_value.as_deref(), Some("charlie"));
        assert_eq!(stats.non_null_count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn null_tracking() {
        let mut stats = ColumnStats::new();
        stats.observe(None);
        stats.observe(Some(&serde_json::Value::Null));
        stats.observe(Some(&serde_json::Value::String("val".into())));
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.non_null_count, 1);
        assert_eq!(stats.row_count, 3);
    }

    #[test]
    fn eq_selectivity() {
        let mut stats = ColumnStats::new();
        for i in 0..100 {
            stats.observe(Some(&serde_json::Value::String(format!("v{i}"))));
        }
        let sel = stats.eq_selectivity();
        assert!(sel > 0.005 && sel < 0.02, "selectivity: {sel}");
    }

    #[test]
    fn stats_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(GatedDatabase::create(dir.path().join("stats.redb")).unwrap());
        let store = StatsStore::open(db).unwrap();

        let mut stats = ColumnStats::new();
        stats.observe(Some(&serde_json::Value::String("hello".into())));
        store.put(0, 1, "users", "name", &stats).unwrap();

        let loaded = store.get(0, 1, "users", "name").unwrap().unwrap();
        assert_eq!(loaded.row_count, 1);
        assert_eq!(loaded.non_null_count, 1);
        assert_eq!(loaded.min_value, Some("hello".to_string()));
    }
}
