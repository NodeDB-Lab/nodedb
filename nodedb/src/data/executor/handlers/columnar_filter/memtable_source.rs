//! `ColumnarSource` adapter for the in-memory memtable.

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};
use nodedb_types::timeseries::SymbolDictionary;

use super::source::ColumnarSource;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    use crate::engine::timeseries::columnar_memtable::{
        ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
    };
    use nodedb_types::timeseries::SeriesId;

    use super::super::{eval_filters_dense, eval_filters_sparse};

    fn make_test_mt() -> ColumnarMemtable {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
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
                    ColumnValue::Symbol(hosts[(i % 3) as usize].to_string()),
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
            op: FilterOp::Gt,
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
            op: FilterOp::Eq,
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
            op: FilterOp::Eq,
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
                op: FilterOp::Gte,
                value: nodedb_types::Value::Float(100.0),
                clauses: vec![],
                expr: None,
            },
            ScanFilter {
                field: "host".into(),
                op: FilterOp::Eq,
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
            op: FilterOp::Or,
            value: nodedb_types::Value::Null,
            clauses: vec![vec![ScanFilter {
                field: "value".into(),
                op: FilterOp::Gt,
                value: nodedb_types::Value::Float(100.0),
                clauses: vec![],
                expr: None,
            }]],
            expr: None,
        };
        assert!(eval_filters_dense(&mt, &[f], 30).is_none());
    }
}
