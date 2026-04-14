//! Columnar predicate evaluation on raw column vectors.
//!
//! Evaluates `ScanFilter` predicates directly on typed columnar data
//! (`Vec<f64>`, `Vec<i64>`, `Vec<u32>` symbol IDs) without constructing
//! JSON rows. Used by timeseries scans (memtable + sealed partitions),
//! columnar aggregation, and any path that filters columnar data.
//!
//! Returns a bitmask of passing rows. Falls back to `None` for filter
//! patterns that can't be evaluated on columnar data (OR clauses, string
//! ordering, unsupported operators).

mod dict;
mod eval;

pub(crate) use eval::{apply_mask, eval_filters_bitmask, eval_filters_dense, eval_filters_sparse};

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};
use nodedb_types::timeseries::SymbolDictionary;

/// Abstraction over columnar data sources (memtable or sealed partition).
/// Provides column lookup by name and symbol dictionary access.
pub(crate) trait ColumnarSource {
    fn resolve_column(&self, name: &str) -> Option<(usize, ColumnType, &ColumnData)>;
    fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary>;
}

/// Adapter for in-memory memtable.
impl ColumnarSource for ColumnarMemtable {
    fn resolve_column(&self, name: &str) -> Option<(usize, ColumnType, &ColumnData)> {
        let schema = self.schema();
        let pos = schema.columns.iter().position(|(n, _)| n == name)?;
        let (_, ty) = &schema.columns[pos];
        Some((pos, *ty, self.column(pos)))
    }

    fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary> {
        ColumnarMemtable::symbol_dict(self, col_idx)
    }
}

/// Adapter for sealed partition data read from disk.
pub(crate) struct PartitionColumns<'a> {
    pub schema: &'a [(String, ColumnType)],
    pub columns: &'a [Option<ColumnData>],
    pub sym_dicts: &'a std::collections::HashMap<usize, SymbolDictionary>,
}

impl ColumnarSource for PartitionColumns<'_> {
    fn resolve_column(&self, name: &str) -> Option<(usize, ColumnType, &ColumnData)> {
        let pos = self.schema.iter().position(|(n, _)| n == name)?;
        let (_, ty) = &self.schema[pos];
        let data = self.columns.get(pos)?.as_ref()?;
        Some((pos, *ty, data))
    }

    fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary> {
        self.sym_dicts.get(&col_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::ScanFilter;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
    };
    use nodedb_types::timeseries::SeriesId;

    fn make_test_mt() -> ColumnarMemtable {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![],
        };
        let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
        let hosts = ["web-1", "web-2", "db-1"];
        for i in 0..30u64 {
            let sid: SeriesId = i;
            mt.ingest_row(
                sid,
                &[
                    ColumnValue::Timestamp(i as i64 * 1000),
                    ColumnValue::Float64(i as f64 * 10.0),
                    ColumnValue::Symbol(hosts[(i % 3) as usize]),
                ],
            )
            .unwrap();
        }
        mt
    }

    #[test]
    fn dense_float_filter() {
        let mt = make_test_mt();
        let f = ScanFilter {
            field: "value".into(),
            op: "gt".into(),
            value: nodedb_types::Value::Float(200.0),
            clauses: vec![],
            expr: None,
        };
        let mask = eval_filters_dense(&mt, &[f], 30).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 9);
    }

    #[test]
    fn sparse_symbol_eq_filter() {
        let mt = make_test_mt();
        let indices: Vec<u32> = (0..30).collect();
        let f = ScanFilter {
            field: "host".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("db-1".into()),
            clauses: vec![],
            expr: None,
        };
        let mask = eval_filters_sparse(&mt, &[f], &indices).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 10);
    }

    #[test]
    fn symbol_eq_not_in_dict() {
        let mt = make_test_mt();
        let indices: Vec<u32> = (0..30).collect();
        let f = ScanFilter {
            field: "host".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("nonexistent".into()),
            clauses: vec![],
            expr: None,
        };
        let mask = eval_filters_sparse(&mt, &[f], &indices).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 0);
    }

    #[test]
    fn combined_filters() {
        let mt = make_test_mt();
        let indices: Vec<u32> = (0..30).collect();
        let filters = vec![
            ScanFilter {
                field: "value".into(),
                op: "gte".into(),
                value: nodedb_types::Value::Float(100.0),
                clauses: vec![],
                expr: None,
            },
            ScanFilter {
                field: "host".into(),
                op: "eq".into(),
                value: nodedb_types::Value::String("web-1".into()),
                clauses: vec![],
                expr: None,
            },
        ];
        let mask = eval_filters_sparse(&mt, &filters, &indices).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 6);
    }

    #[test]
    fn or_clause_returns_none() {
        let mt = make_test_mt();
        let f = ScanFilter {
            field: "value".into(),
            op: "or".into(),
            value: nodedb_types::Value::Null,
            clauses: vec![vec![ScanFilter {
                field: "value".into(),
                op: "gt".into(),
                value: nodedb_types::Value::Float(100.0),
                clauses: vec![],
                expr: None,
            }]],
            expr: None,
        };
        assert!(eval_filters_dense(&mt, &[f], 30).is_none());
    }

    #[test]
    fn dict_dense_eq_filter() {
        let dictionary: Vec<String> = vec!["web-1".into(), "web-2".into(), "db-1".into()];
        let mut reverse = std::collections::HashMap::new();
        reverse.insert("web-1".to_string(), 0u32);
        reverse.insert("web-2".to_string(), 1u32);
        reverse.insert("db-1".to_string(), 2u32);
        // 6 rows: web-1, web-2, db-1, web-1, web-2, db-1
        let ids: Vec<u32> = vec![0, 1, 2, 0, 1, 2];
        let valid: Vec<bool> = vec![true, true, true, true, true, true];

        let schema = vec![("host".into(), ColumnType::Symbol)];
        let columns = vec![Some(ColumnData::DictEncoded {
            ids: ids.clone(),
            dictionary: dictionary.clone(),
            reverse: reverse.clone(),
            valid: valid.clone(),
        })];

        let src = PartitionColumns {
            schema: &schema,
            columns: &columns,
            sym_dicts: &std::collections::HashMap::new(),
        };

        let f = ScanFilter {
            field: "host".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("web-1".into()),
            clauses: vec![],
            expr: None,
        };
        let mask = eval_filters_dense(&src, &[f], 6).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 2); // indices 0 and 3
        assert!(mask[0] && mask[3]);
        assert!(!mask[1] && !mask[2] && !mask[4] && !mask[5]);
    }

    #[test]
    fn dict_dense_eq_not_in_dict() {
        let dictionary: Vec<String> = vec!["a".into(), "b".into()];
        let mut reverse = std::collections::HashMap::new();
        reverse.insert("a".to_string(), 0u32);
        reverse.insert("b".to_string(), 1u32);
        let ids = vec![0u32, 1, 0, 1];
        let valid = vec![true; 4];

        let schema = vec![("col".into(), ColumnType::Symbol)];
        let columns = vec![Some(ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid,
        })];
        let src = PartitionColumns {
            schema: &schema,
            columns: &columns,
            sym_dicts: &std::collections::HashMap::new(),
        };

        let f = ScanFilter {
            field: "col".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("z".into()),
            clauses: vec![],
            expr: None,
        };
        let mask = eval_filters_dense(&src, &[f], 4).unwrap();
        assert!(mask.iter().all(|&b| !b));
    }

    #[test]
    fn dict_dense_contains_filter() {
        let dictionary: Vec<String> = vec!["web-1".into(), "web-2".into(), "db-1".into()];
        let mut reverse = std::collections::HashMap::new();
        reverse.insert("web-1".to_string(), 0u32);
        reverse.insert("web-2".to_string(), 1u32);
        reverse.insert("db-1".to_string(), 2u32);
        let ids = vec![0u32, 1, 2, 0, 1, 2];
        let valid = vec![true; 6];

        let schema = vec![("host".into(), ColumnType::Symbol)];
        let columns = vec![Some(ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid,
        })];
        let src = PartitionColumns {
            schema: &schema,
            columns: &columns,
            sym_dicts: &std::collections::HashMap::new(),
        };

        let f = ScanFilter {
            field: "host".into(),
            op: "contains".into(),
            value: nodedb_types::Value::String("web".into()),
            clauses: vec![],
            expr: None,
        };
        let mask = eval_filters_dense(&src, &[f], 6).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 4); // web-1 and web-2 rows
    }

    #[test]
    fn dict_bitmask_eq_filter() {
        let dictionary: Vec<String> = vec!["alpha".into(), "beta".into()];
        let mut reverse = std::collections::HashMap::new();
        reverse.insert("alpha".to_string(), 0u32);
        reverse.insert("beta".to_string(), 1u32);
        // 8 rows alternating alpha/beta
        let ids: Vec<u32> = (0..8).map(|i| i % 2).collect();
        let valid = vec![true; 8];

        let schema = vec![("tag".into(), ColumnType::Symbol)];
        let columns = vec![Some(ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid,
        })];
        let src = PartitionColumns {
            schema: &schema,
            columns: &columns,
            sym_dicts: &std::collections::HashMap::new(),
        };

        let f = ScanFilter {
            field: "tag".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("alpha".into()),
            clauses: vec![],
            expr: None,
        };
        let bm = eval_filters_bitmask(&src, &[f], 8).unwrap();
        // bits 0,2,4,6 should be set (alpha rows)
        let indices = nodedb_query::simd_filter::bitmask_to_indices(&bm);
        assert_eq!(indices, vec![0u32, 2, 4, 6]);
    }

    #[test]
    fn dict_bitmask_ne_filter() {
        let dictionary: Vec<String> = vec!["x".into(), "y".into()];
        let mut reverse = std::collections::HashMap::new();
        reverse.insert("x".to_string(), 0u32);
        reverse.insert("y".to_string(), 1u32);
        let ids: Vec<u32> = vec![0, 1, 0, 0, 1];
        let valid = vec![true; 5];

        let schema = vec![("tag".into(), ColumnType::Symbol)];
        let columns = vec![Some(ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid,
        })];
        let src = PartitionColumns {
            schema: &schema,
            columns: &columns,
            sym_dicts: &std::collections::HashMap::new(),
        };

        let f = ScanFilter {
            field: "tag".into(),
            op: "ne".into(),
            value: nodedb_types::Value::String("y".into()),
            clauses: vec![],
            expr: None,
        };
        let bm = eval_filters_bitmask(&src, &[f], 5).unwrap();
        let indices = nodedb_query::simd_filter::bitmask_to_indices(&bm);
        assert_eq!(indices, vec![0u32, 2, 3]); // rows with "x"
    }

    #[test]
    fn dict_null_rows_excluded() {
        let dictionary: Vec<String> = vec!["a".into()];
        let mut reverse = std::collections::HashMap::new();
        reverse.insert("a".to_string(), 0u32);
        let ids = vec![0u32, 0, 0, 0];
        // Row 1 and 3 are null.
        let valid = vec![true, false, true, false];

        let schema = vec![("col".into(), ColumnType::Symbol)];
        let columns = vec![Some(ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid,
        })];
        let src = PartitionColumns {
            schema: &schema,
            columns: &columns,
            sym_dicts: &std::collections::HashMap::new(),
        };

        let f = ScanFilter {
            field: "col".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("a".into()),
            clauses: vec![],
            expr: None,
        };
        let mask = eval_filters_dense(&src, &[f], 4).unwrap();
        // Only rows 0 and 2 are valid and match.
        assert_eq!(mask, vec![true, false, true, false]);
    }

    #[test]
    fn partition_columns_adapter() {
        // Simulate sealed partition data.
        let schema = vec![
            ("timestamp".into(), ColumnType::Timestamp),
            ("value".into(), ColumnType::Float64),
            ("host".into(), ColumnType::Symbol),
        ];
        let ts: Vec<i64> = (0..10).map(|i| i * 1000).collect();
        let vals: Vec<f64> = (0..10).map(|i| i as f64 * 100.0).collect();
        let sym_ids: Vec<u32> = (0..10).map(|i| (i % 2) as u32).collect(); // alternating 0, 1

        let columns: Vec<Option<ColumnData>> = vec![
            Some(ColumnData::Timestamp(ts)),
            Some(ColumnData::Float64(vals)),
            Some(ColumnData::Symbol(sym_ids)),
        ];

        let mut sym_dicts = std::collections::HashMap::new();
        let mut dict = SymbolDictionary::new();
        dict.resolve("alpha", u32::MAX); // id 0
        dict.resolve("beta", u32::MAX); // id 1
        sym_dicts.insert(2, dict);

        let src = PartitionColumns {
            schema: &schema,
            columns: &columns,
            sym_dicts: &sym_dicts,
        };

        let indices: Vec<u32> = (0..10).collect();

        // Filter: value > 500
        let f = ScanFilter {
            field: "value".into(),
            op: "gt".into(),
            value: nodedb_types::Value::Float(500.0),
            clauses: vec![],
            expr: None,
        };
        let mask = eval_filters_sparse(&src, &[f], &indices).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 4); // 600, 700, 800, 900

        // Filter: host = 'alpha'
        let f2 = ScanFilter {
            field: "host".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("alpha".into()),
            clauses: vec![],
            expr: None,
        };
        let mask2 = eval_filters_sparse(&src, &[f2], &indices).unwrap();
        let passing2: usize = mask2.iter().filter(|&&b| b).count();
        assert_eq!(passing2, 5); // indices 0, 2, 4, 6, 8
    }
}
