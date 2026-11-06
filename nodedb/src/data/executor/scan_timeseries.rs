// SPDX-License-Identifier: BUSL-1.1

//! The timeseries branch of the universal collection scan.
//!
//! A timeseries collection is read by identity, so its rows come from the
//! same source the raw scan reads: the live memtable, then every flushed
//! partition. A flushed collection with no resident memtable still answers
//! with all of its rows.

use nodedb_types::columnar::schema::TS_SYSTEM;

use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::raw_scan::TsGatherParams;
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

    /// Committed rows of a timeseries collection as `(id, msgpack map)`, up
    /// to `limit`. The id is the row's `id` text column, or empty.
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
        let rows = self.gather_ts_rows(TsGatherParams {
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
        rows.into_iter().map(ts_row_to_doc).collect()
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

/// One timeseries row as `(id, msgpack map)`.
fn ts_row_to_doc(row: rmpv::Value) -> crate::Result<(String, Vec<u8>)> {
    let id = match &row {
        rmpv::Value::Map(entries) => entries
            .iter()
            .find(|(k, _)| k.as_str() == Some("id"))
            .and_then(|(_, v)| v.as_str())
            .unwrap_or_default()
            .to_string(),
        _ => String::new(),
    };
    let mut mp = Vec::new();
    rmpv::encode::write_value(&mut mp, &row).map_err(|e| crate::Error::Codec {
        detail: format!("encode timeseries row: {e}"),
    })?;
    Ok((id, mp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_id_comes_from_the_id_column() {
        let row = rmpv::Value::Map(vec![
            (rmpv::Value::from("id"), rmpv::Value::from("r1")),
            (rmpv::Value::from("v"), rmpv::Value::from(2.5)),
        ]);
        let (id, mp) = ts_row_to_doc(row).expect("row encodes");
        assert_eq!(id, "r1");
        assert!(!mp.is_empty());
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
        let row = rmpv::Value::Map(vec![(rmpv::Value::from("v"), rmpv::Value::from(1))]);
        let (id, _) = ts_row_to_doc(row).expect("row encodes");
        assert!(id.is_empty());
    }
}
