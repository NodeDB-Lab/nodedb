// SPDX-License-Identifier: BUSL-1.1

//! The timeseries branch of the universal collection scan.
//!
//! A timeseries collection is read by identity, so its rows come from the
//! same source the raw scan reads: the live memtable, then every flushed
//! partition. A flushed collection with no resident memtable still answers
//! with all of its rows.

use nodedb_types::Value;
use nodedb_types::columnar::schema::TS_SYSTEM;

use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::raw_scan::TsGatherParams;
use crate::data::executor::handlers::timeseries::raw_scan::row_form::TypedTsRow;
use crate::types::{DatabaseId, TenantId};

/// The full time range: a collection scan has no time bounds.
const ALL_TIME: (i64, i64) = (i64::MIN, i64::MAX);

impl CoreLoop {
    /// Load the partition registry of `collection` when one exists on disk,
    /// so a later [`CoreLoop::scan_collection`] sees every flushed partition.
    /// The registry loads lazily; a collection with no timeseries directory
    /// is left as it is.
    pub(in crate::data::executor) fn prepare_collection_scan(
        &mut self,
        did: u64,
        tid: u64,
        collection: &str,
    ) -> crate::Result<()> {
        if collection.is_empty() {
            return Ok(());
        }
        self.ensure_ts_registry(TenantId::new(tid), DatabaseId::new(did), collection)
    }

    /// Whether `collection` is a timeseries collection on this core: it has a
    /// declared timeseries shape, a resident timeseries memtable, or a
    /// partition registry.
    pub(in crate::data::executor) fn is_timeseries_collection(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
    ) -> bool {
        let database_id = DatabaseId::new(did);
        let tenant_id = TenantId::new(tid);
        if self
            .declared_timeseries(database_id, tenant_id, collection)
            .is_some()
        {
            return true;
        }
        let key = (database_id, tenant_id, collection.to_string());
        self.columnar_memtables.contains_key(&key) || self.ts_registries.contains_key(&key)
    }

    /// Committed rows of a timeseries collection as `(id, native row)`, up
    /// to `limit`. The id is the row's `id` text column, or empty.
    ///
    /// Each row is a native tagged object (`[7, {...}]`) whose cells carry
    /// their stored types: a DECIMAL is `Value::Decimal`, a UUID
    /// `Value::Uuid`, a BOOLEAN `Value::Bool`, an instant a typed instant.
    /// Joins, GROUP BY, partition routing and aggregates read these rows.
    pub(in crate::data::executor) fn scan_timeseries_docs(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        self.scan_timeseries_docs_at(did, tid, collection, limit, None)
    }

    /// [`Self::scan_timeseries_docs`] read at a system-time cut. `None` reads
    /// the current rows. `Some(ordinal)` reads the rows whose stored
    /// `_ts_system` ordinal is at or below `ordinal`, in the partitions no
    /// TRUNCATE ended at or below it. Those are exactly the rows visible at
    /// the cut.
    pub(in crate::data::executor) fn scan_timeseries_docs_at(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        limit: usize,
        at: Option<i64>,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let cut_predicates: Vec<ScanFilter> = at.map(system_cut_predicate).into_iter().collect();
        let rows = self.gather_ts_rows::<TypedTsRow>(TsGatherParams {
            database_id: DatabaseId::new(did),
            tid: TenantId::new(tid),
            collection,
            time_range: ALL_TIME,
            filter_predicates: &cut_predicates,
            rls_predicates: &[],
            limit,
            cut: at.map_or(
                nodedb_columnar::VersionCut::Current,
                nodedb_columnar::VersionCut::At,
            ),
            task: None,
        })?;
        rows.into_iter()
            .map(|row| ts_row_to_doc(row.into_value()))
            .collect()
    }
}

/// The predicate that admits the rows a read at `ordinal` sees.
fn system_cut_predicate(ordinal: i64) -> ScanFilter {
    ScanFilter {
        field: TS_SYSTEM.into(),
        op: FilterOp::Lte,
        value: nodedb_types::Value::Integer(ordinal),
        clauses: Vec::new(),
        expr: None,
    }
}

/// One typed timeseries row as `(id, native row)`.
fn ts_row_to_doc(row: Value) -> crate::Result<(String, Vec<u8>)> {
    let id = match &row {
        Value::Object(fields) => match fields.get("id") {
            Some(Value::String(id)) => id.clone(),
            _ => String::new(),
        },
        _ => String::new(),
    };
    let mp = zerompk::to_msgpack_vec(&row).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("encode timeseries row: {e}"),
    })?;
    Ok((id, mp))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_query::msgpack_scan::{build_group_key, join_key_identity, join_match_identity};
    use nodedb_types::InstantKind;

    use super::*;
    use crate::data::executor::handlers::timeseries::raw_scan::row_form::TsRowForm;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
    };

    const DEVICE: &str = "550e8400-e29b-41d4-a716-446655440000";
    const AT_MS: i64 = 1_700_000_000_000;
    const PRICE: &str = "12345678901234567.89";

    fn object(fields: Vec<(&str, Value)>) -> Value {
        Value::Object(
            fields
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn native(row: &Value) -> Vec<u8> {
        zerompk::to_msgpack_vec(row).expect("native row")
    }

    #[test]
    fn row_id_comes_from_the_id_column() {
        let row = object(vec![
            ("id", Value::String("r1".into())),
            ("v", Value::Float(2.5)),
        ]);
        let (id, mp) = ts_row_to_doc(row).expect("row encodes");
        assert_eq!(id, "r1");
        assert!(nodedb_types::ObjectView::new(&mp).is_ok());
    }

    #[test]
    fn a_cut_admits_rows_stamped_at_or_below_it() {
        let predicate = system_cut_predicate(42);
        assert_eq!(predicate.field, TS_SYSTEM);
        assert_eq!(predicate.op, FilterOp::Lte);
        assert_eq!(predicate.value, nodedb_types::Value::Integer(42));
    }

    #[test]
    fn row_without_id_has_empty_id() {
        let row = object(vec![("v", Value::Integer(1))]);
        let (id, _) = ts_row_to_doc(row).expect("row encodes");
        assert!(id.is_empty());
    }

    /// A memtable holding one row of a TIMESTAMPTZ key, a DECIMAL, a UUID
    /// and a BOOLEAN, read back as the collection scan reads it.
    fn stored_row() -> Vec<u8> {
        let schema = ColumnarSchema {
            columns: vec![
                (
                    "ts".into(),
                    ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc)),
                ),
                ("price".into(), ColumnType::Decimal(None)),
                ("device".into(), ColumnType::Uuid),
                ("ok".into(), ColumnType::Bool),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 4],
        };
        let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
        mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(AT_MS),
                ColumnValue::decimal(PRICE.parse().expect("decimal")),
                ColumnValue::Uuid(*uuid::Uuid::parse_str(DEVICE).expect("uuid").as_bytes()),
                ColumnValue::Bool(true),
            ],
        )
        .expect("ingest");
        let schema = mt.schema().clone();
        let columns: Vec<_> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| (i, name, ty, mt.column(i)))
            .collect();
        let row = TypedTsRow::from_memtable(&mt, &columns, 0).expect("row");
        ts_row_to_doc(row.into_value()).expect("doc").1
    }

    /// The collection scan emits a native row whose DECIMAL, UUID and
    /// BOOLEAN cells keep their types. The row joins, groups and routes with
    /// a schemaless native row holding the same values. Text with the same
    /// characters, and a float near the decimal, stay apart.
    #[test]
    fn timeseries_typed_cells_match_native_schemaless_rows() {
        let ts_row = stored_row();
        let view = nodedb_types::ObjectView::new(&ts_row).expect("object");
        assert_eq!(view.format(), nodedb_types::ObjectFormat::Native);
        let price = Value::Decimal(PRICE.parse().expect("decimal"));
        assert_eq!(view.get("price").expect("price"), Some(price.clone()));
        assert_eq!(
            view.get("device").expect("device"),
            Some(Value::Uuid(DEVICE.into()))
        );
        assert_eq!(view.get("ok").expect("ok"), Some(Value::Bool(true)));

        let schemaless = native(&object(vec![
            ("ts", InstantKind::Utc.from_millis(AT_MS).expect("instant")),
            ("price", price),
            ("device", Value::Uuid(DEVICE.into())),
            ("ok", Value::Bool(true)),
        ]));
        for keys in [["price"], ["device"], ["ts"], ["ok"]] {
            let ts_key = join_match_identity(&ts_row, &keys).expect("ts key");
            assert!(ts_key.is_some(), "{keys:?}");
            assert_eq!(
                ts_key,
                join_match_identity(&schemaless, &keys).expect("native key"),
                "{keys:?}"
            );
            assert_eq!(
                nodedb_query::partition_hash(&ts_row, &keys).expect("ts route"),
                nodedb_query::partition_hash(&schemaless, &keys).expect("native route"),
                "{keys:?}"
            );
        }
        let crs = nodedb_crs::CrsContext::builtin_only();
        for column in ["price", "device"] {
            let group = [nodedb_query::expr::GroupKeySpec::column(column)];
            assert_eq!(
                build_group_key(&ts_row, &group, &crs).expect("ts group"),
                build_group_key(&schemaless, &group, &crs).expect("native group"),
                "{column}"
            );
        }

        let text = native(&object(vec![
            ("price", Value::String(PRICE.into())),
            ("device", Value::String(DEVICE.into())),
        ]));
        let float = native(&object(vec![(
            "price",
            Value::Float(PRICE.parse::<f64>().expect("float")),
        )]));
        for (other, keys) in [(&text, ["price"]), (&text, ["device"]), (&float, ["price"])] {
            assert_ne!(
                join_key_identity(&ts_row, &keys).expect("ts key"),
                join_key_identity(other, &keys).expect("other key"),
                "{keys:?}"
            );
        }
    }
}
