// SPDX-License-Identifier: BUSL-1.1

//! The body a staged timeseries row stores in the transaction overlay.
//!
//! A read inside the transaction returns the staged row, so the row must
//! carry the types a committed row reads with. Each declared DECIMAL, UUID
//! and BOOLEAN cell is typed once, here, by the reader the committed ingest
//! uses ([`declared_value`]): a DECIMAL fitted to its typmod, a UUID in
//! canonical text. The body is a native object, so those types survive to
//! the overlay merge and the staged aggregate fold.

use nodedb_types::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::declared_cell::declared_value;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// The overlay body of the staged row `row` of `collection`.
    ///
    /// `Err` when a declared cell does not read as its type (SQLSTATE
    /// `22P02`, `42804` or `22003`, as a committed ingest refuses it), or
    /// when the row does not encode.
    pub(in crate::data::executor) fn staged_ts_body(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        row: Value,
    ) -> crate::Result<Vec<u8>> {
        let exact = self.declared_ts_exact_columns(database_id, tid, collection);
        let Value::Object(mut fields) = row else {
            return Err(crate::Error::BadRequest {
                detail: format!("a staged row of timeseries '{collection}' is not an object"),
            });
        };
        for (name, value) in fields.iter_mut() {
            let Some((column, ty)) = exact
                .iter()
                .find(|(column, _)| column.eq_ignore_ascii_case(name))
            else {
                continue;
            };
            if let Some(typed) = declared_value(column, *ty, value)? {
                *value = typed;
            }
        }
        zerompk::to_msgpack_vec(&Value::Object(fields)).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("staged row of timeseries '{collection}': {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp, TimeseriesSchema};
    use nodedb_types::{QualifiedCollection, Value};

    use crate::bridge::envelope::{Admission, ExemptReason, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::timeseries::{
        StampedIngest, TimeseriesApplyMode, TimeseriesIngestExec, TsResolveInput,
    };
    use crate::data::executor::handlers::transaction::overlay::TimeseriesOverlayMergeParams;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::CollectionConfig;
    use crate::engine::timeseries::resolved_ingest::{RESOLVED_INGEST_FORMAT, TsDriftPolicy};
    use crate::types::{DatabaseId, TenantId, TxnId};

    const TID: u64 = 1;
    const DEVICE: &str = "550E8400-E29B-41D4-A716-446655440000";

    fn declare(core: &mut CoreLoop, collection: &str) {
        let mut config = CollectionConfig::new(collection);
        config.timeseries = Some(Box::new(TimeseriesSchema {
            time_key: "ts".into(),
            columns: vec![
                ("ts".into(), "BIGINT TIME_KEY".into()),
                ("price".into(), "DECIMAL(10,2)".into()),
                ("device".into(), "UUID".into()),
            ],
            partition_interval: "1h".into(),
            last_value_cache: false,
        }));
        core.doc_configs.insert(
            (
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                collection.to_string(),
            ),
            config,
        );
    }

    /// The row a client INSERT sends: the decimal as text, with one digit
    /// past the declared scale, and the UUID in upper case.
    fn payload() -> Vec<u8> {
        let row: HashMap<String, Value> = [
            ("ts".to_string(), Value::Integer(1_000)),
            ("price".to_string(), Value::String("12.345".into())),
            ("device".to_string(), Value::String(DEVICE.into())),
        ]
        .into_iter()
        .collect();
        nodedb_types::value_to_msgpack(&Value::Array(vec![Value::Object(row)])).expect("payload")
    }

    fn cell<'a>(row: &'a HashMap<String, Value>, name: &str) -> &'a Value {
        row.get(name).expect("cell present")
    }

    /// A row staged inside a transaction reads back typed: the DECIMAL
    /// fitted to `DECIMAL(10,2)` and the UUID in canonical text. The same
    /// payload, resolved and installed as COMMIT installs it, reads back the
    /// identical cells.
    #[test]
    fn a_staged_row_reads_back_as_its_committed_row_reads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let collection = QualifiedCollection::new(DatabaseId::DEFAULT, "metrics");
        declare(&mut core, collection.as_str());

        // Inside a transaction.
        let txn_id = TxnId::new(11);
        let mut request = make_default_task().request;
        request.txn_id = Some(txn_id);
        let task = ExecutionTask::new(request);
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.clone(),
            payload: payload(),
            format: "msgpack".to_owned(),
            wal_lsn: None,
            surrogates: vec![nodedb_types::Surrogate::new(7)],
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let staged = core.execute_stage_write(&task, TID, &plan);
        assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);
        let coll_key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.as_str().to_string(),
        );
        let staged_rows = core
            .staged_timeseries_rows(TimeseriesOverlayMergeParams {
                txn_id,
                coll_key: &coll_key,
                time_range: (i64::MIN, i64::MAX),
                filter_predicates: &[],
                has_filters: false,
                rls_predicates: &[],
                limit: usize::MAX,
            })
            .expect("staged rows");
        assert_eq!(staged_rows.rows.len(), 1);
        let Value::Object(staged_row) = &staged_rows.rows[0] else {
            panic!("a staged row is an object: {:?}", staged_rows.rows[0]);
        };
        let price = cell(staged_row, "price");
        assert_eq!(price, &Value::Decimal("12.35".parse().expect("decimal")));
        assert!(matches!(price, Value::Decimal(d) if d.to_string() == "12.35"));
        assert_eq!(
            cell(staged_row, "device"),
            &Value::Uuid(DEVICE.to_ascii_lowercase())
        );

        // After COMMIT: the lines COMMIT resolves, installed as it installs them.
        let autocommit = make_default_task();
        let lines = core
            .stamped_ingest_lines(StampedIngest {
                database_id: DatabaseId::DEFAULT,
                tid: TenantId::new(TID),
                collection: collection.as_str(),
                payload: &payload(),
                format: "msgpack",
                now_ms: 1_000,
            })
            .expect("lines");
        let batch = core
            .resolve_ts_batch(
                &autocommit,
                TsResolveInput {
                    tid: TenantId::new(TID),
                    collection: collection.as_str(),
                    lines: &lines,
                    now_ms: 1_000,
                    drift: TsDriftPolicy::Refuse,
                    needs_images: false,
                    base: None,
                },
            )
            .expect("resolve");
        let resolved = batch.to_bytes().expect("encode batch");
        let mut install = make_default_task();
        install.request.admission = Admission::Exempt(ExemptReason::AlreadyOrdered);
        let response = core.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &install,
            tid: TenantId::new(TID),
            collection: collection.as_str(),
            payload: &resolved,
            format: RESOLVED_INGEST_FORMAT,
            wal_lsn: Some(1),
            provenance: None,
            mode: TimeseriesApplyMode::Immediate,
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: &[],
        });
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);

        let committed = core
            .scan_timeseries_docs(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                collection.as_str(),
                usize::MAX,
            )
            .expect("scan");
        assert_eq!(committed.len(), 1);
        let Value::Object(committed_row) =
            nodedb_types::decode_object(&committed[0].1).expect("committed row")
        else {
            panic!("a committed row is an object");
        };
        for name in ["price", "device"] {
            assert_eq!(cell(staged_row, name), cell(&committed_row, name), "{name}");
        }
    }

    /// A staged DECIMAL past its declared precision is refused before it is
    /// staged, with SQLSTATE `22003`, as a committed ingest refuses it.
    #[test]
    fn a_staged_decimal_past_its_precision_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let collection = QualifiedCollection::new(DatabaseId::DEFAULT, "metrics");
        declare(&mut core, collection.as_str());
        let row: HashMap<String, Value> = [
            ("ts".to_string(), Value::Integer(1_000)),
            ("price".to_string(), Value::String("123456789.5".into())),
        ]
        .into_iter()
        .collect();
        let error = core
            .staged_ts_body(
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                collection.as_str(),
                Value::Object(row),
            )
            .expect_err("past DECIMAL(10,2)");
        assert!(
            matches!(error, crate::Error::NumericValueOutOfRange { .. }),
            "{error:?}"
        );
    }
}
