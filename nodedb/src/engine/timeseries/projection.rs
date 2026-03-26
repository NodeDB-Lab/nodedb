//! Projections: alternate sort orders for timeseries partitions.
//!
//! Stores data sorted by both `(timestamp, host)` (default — time-range queries)
//! and `(host, timestamp)` (tag-first — "show me all data for host X" queries).
//! The query planner picks the optimal ordering based on WHERE predicates.
//!
//! The secondary projection is stored as an additional set of column files
//! per partition, with a `.proj` suffix. Read-only — built at flush time.

use super::columnar_memtable::{ColumnData, ColumnarDrainResult, ColumnarSchema};

/// A projection definition: column order for sorting.
#[derive(Debug, Clone)]
pub struct ProjectionDef {
    /// Name of this projection (e.g., "by_host").
    pub name: String,
    /// Column indices for sort order (primary, secondary, ...).
    pub sort_columns: Vec<usize>,
    /// Whether each sort column is ascending.
    pub ascending: Vec<bool>,
}

/// Sort indices for a projection.
///
/// Returns a permutation vector: `result[i]` is the original row index
/// that should appear at position `i` in the sorted output.
pub fn compute_sort_order(
    drain: &ColumnarDrainResult,
    sort_columns: &[usize],
    ascending: &[bool],
) -> Vec<usize> {
    let row_count = drain.row_count as usize;
    let mut indices: Vec<usize> = (0..row_count).collect();

    indices.sort_by(|&a, &b| {
        for (i, &col_idx) in sort_columns.iter().enumerate() {
            let asc = ascending.get(i).copied().unwrap_or(true);
            let ord = compare_column_values(&drain.columns[col_idx], a, b);
            let ord = if asc { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    indices
}

/// Apply a permutation to reorder column data.
pub fn apply_permutation(data: &ColumnData, perm: &[usize]) -> ColumnData {
    match data {
        ColumnData::Timestamp(v) => ColumnData::Timestamp(perm.iter().map(|&i| v[i]).collect()),
        ColumnData::Float64(v) => ColumnData::Float64(perm.iter().map(|&i| v[i]).collect()),
        ColumnData::Int64(v) => ColumnData::Int64(perm.iter().map(|&i| v[i]).collect()),
        ColumnData::Symbol(v) => ColumnData::Symbol(perm.iter().map(|&i| v[i]).collect()),
    }
}

/// Compare two rows by a single column's values.
fn compare_column_values(col: &ColumnData, a: usize, b: usize) -> std::cmp::Ordering {
    match col {
        ColumnData::Timestamp(v) => v[a].cmp(&v[b]),
        ColumnData::Float64(v) => v[a].partial_cmp(&v[b]).unwrap_or(std::cmp::Ordering::Equal),
        ColumnData::Int64(v) => v[a].cmp(&v[b]),
        ColumnData::Symbol(v) => v[a].cmp(&v[b]),
    }
}

/// Check if a query should use a tag-first projection.
///
/// Returns true if the query has an equality predicate on a tag column
/// but a wide time range (suggesting tag-first ordering is better).
pub fn should_use_tag_projection(
    _schema: &ColumnarSchema,
    tag_predicates: &[(usize, u32)], // (column_idx, symbol_id) equality predicates
    time_selectivity: f64,           // fraction of time range that matches (0.0 to 1.0)
) -> bool {
    if tag_predicates.is_empty() {
        return false; // No tag filter → time-first is always better.
    }

    // Heuristic: use tag-first if time selectivity > 50% (wide time range)
    // and there's at least one tag equality predicate.
    time_selectivity > 0.5
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig,
    };

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    #[test]
    fn sort_by_tag_then_timestamp() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());

        // Insert rows: host alternates between 0 and 1, timestamps ascending.
        for i in 0..10 {
            let host = if i % 2 == 0 { "prod-1" } else { "prod-2" };
            mt.ingest_row(
                (i % 2) as u64,
                &[
                    ColumnValue::Timestamp(1000 + i as i64),
                    ColumnValue::Float64(i as f64),
                    ColumnValue::Symbol(host),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();

        // Sort by (host, timestamp) — tag-first projection.
        let perm = compute_sort_order(&drain, &[2, 0], &[true, true]);

        // Verify: first 5 rows should all be host=0 (prod-1), next 5 host=1 (prod-2).
        let reordered_hosts = apply_permutation(&drain.columns[2], &perm);
        let hosts = reordered_hosts.as_symbols();
        assert!(hosts[..5].iter().all(|&h| h == hosts[0]));
        assert!(hosts[5..].iter().all(|&h| h == hosts[5]));
        assert_ne!(hosts[0], hosts[5]);
    }

    #[test]
    fn identity_permutation_for_sorted_data() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());
        for i in 0..10 {
            mt.ingest_row(
                1,
                &[ColumnValue::Timestamp(i), ColumnValue::Float64(i as f64)],
            )
            .unwrap();
        }
        let drain = mt.drain();

        let perm = compute_sort_order(&drain, &[0], &[true]);
        assert_eq!(perm, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn tag_projection_heuristic() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };

        // Wide time range + tag filter → use tag-first.
        assert!(should_use_tag_projection(&schema, &[(1, 0)], 0.8));
        // Narrow time range → use time-first.
        assert!(!should_use_tag_projection(&schema, &[(1, 0)], 0.1));
        // No tag filter → always time-first.
        assert!(!should_use_tag_projection(&schema, &[], 0.9));
    }
}
