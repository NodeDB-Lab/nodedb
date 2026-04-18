//! Column statistics for query optimizer (ANALYZE).

use super::types::{COLUMN_STATS, SystemCatalog, catalog_err};

/// Per-column statistics collected by ANALYZE.
///
/// Stored in redb under `_system.column_stats` with key `"{tenant_id}:{collection}:{column}"`.
/// Used by DataFusion's cost-based optimizer for cardinality estimation.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredColumnStats {
    pub tenant_id: u32,
    pub collection: String,
    pub column: String,
    /// Total number of rows in the collection at ANALYZE time.
    pub row_count: u64,
    /// Number of null values.
    pub null_count: u64,
    /// Number of distinct values (estimated via HLL or exact for small sets).
    pub distinct_count: u64,
    /// Minimum value as string (for display and text-based comparison).
    pub min_value: Option<String>,
    /// Maximum value as string.
    pub max_value: Option<String>,
    /// Average value length in bytes (for variable-length types).
    pub avg_value_len: Option<u32>,
    /// Timestamp of last ANALYZE (epoch millis).
    pub analyzed_at: u64,
}

impl SystemCatalog {
    /// Store column statistics.
    pub fn put_column_stats(&self, stats: &StoredColumnStats) -> crate::Result<()> {
        let key = stats_key(stats.tenant_id, &stats.collection, &stats.column);
        let bytes =
            zerompk::to_msgpack_vec(stats).map_err(|e| catalog_err("serialize column_stats", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(COLUMN_STATS)
                .map_err(|e| catalog_err("open column_stats", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert column_stats", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Load all column statistics for a collection.
    pub fn load_column_stats(
        &self,
        tenant_id: u32,
        collection: &str,
    ) -> crate::Result<Vec<StoredColumnStats>> {
        let prefix = format!("{tenant_id}:{collection}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLUMN_STATS)
            .map_err(|e| catalog_err("open column_stats", e))?;

        let mut stats = Vec::new();
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range column_stats", e))?;
        while let Some(Ok((key, value))) = range.next() {
            if key.value().starts_with(&prefix)
                && let Ok(s) = zerompk::from_msgpack::<StoredColumnStats>(value.value())
            {
                stats.push(s);
            }
        }
        Ok(stats)
    }
}

fn stats_key(tenant_id: u32, collection: &str, column: &str) -> String {
    format!("{tenant_id}:{collection}:{column}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::types::SystemCatalog;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    #[test]
    fn put_and_load_stats() {
        let cat = make_catalog();
        let stats = StoredColumnStats {
            tenant_id: 1,
            collection: "users".into(),
            column: "email".into(),
            row_count: 10000,
            null_count: 50,
            distinct_count: 9500,
            min_value: Some("a@b.com".into()),
            max_value: Some("z@z.org".into()),
            avg_value_len: Some(20),
            analyzed_at: 1700000000000,
        };
        cat.put_column_stats(&stats).unwrap();

        let loaded = cat.load_column_stats(1, "users").unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].column, "email");
        assert_eq!(loaded[0].row_count, 10000);
        assert_eq!(loaded[0].distinct_count, 9500);
    }
}
