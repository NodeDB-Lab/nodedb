// SPDX-License-Identifier: BUSL-1.1

//! Timeseries serializer for transaction resolve. Plan-driven: a timeseries
//! ingest is append-only, so the redo carries the ingested rows through the
//! autocommit path's `RecordType::TimeseriesBatch` encoder
//! (`control::server::wal_dispatch`) and replay appends the same samples.
//! Emission is in plan order, already deterministic. Columnar writes resolve
//! from the overlay instead (`columnar_image`).
//!
//! The rows are resolved to canonical line protocol, every row with no
//! timestamp stamped here, once, with the instant its statement read. The
//! lines then resolve to the exact rows they store (`resolve_rows`), and the
//! sub-record carries those rows in the `ts-resolved` format with the
//! statement instant. The install stores exactly these rows, so every replica
//! and every restart stores the same values, a bitemporal row's system time
//! included. When some Event Plane consumer reads the collection, each row
//! also carries its image, which the install emits and WAL catch-up rebuilds.
//!
//! A sequenced transaction's ingest arrives already in the `ts-resolved`
//! format: the submitting node resolved it before it was sequenced, since
//! every replica resolves a sequenced transaction on its own. Its sub-record
//! carries those rows as they are.

use nodedb_physical::physical_plan::TimeseriesOp;
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::{
    TimeseriesIngestRecord, encode_columnar_truncate_payload, encode_timeseries_ingest_payload,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::{StampedIngest, TsResolveInput};
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::ColumnarSchema;
use crate::engine::timeseries::resolved_ingest::{
    RESOLVED_INGEST_FORMAT, ResolvedTsBatch, TsDriftPolicy,
};
use crate::types::{TenantId, TxnId};
use crate::wal::RedoSubRecord;

/// What a transaction's timeseries serializer carries from one plan op to the
/// next.
#[derive(Default)]
pub(super) struct TimeseriesResolveState {
    /// Unkeyed ingests seen per collection, in plan order.
    pub unkeyed_seen: std::collections::HashMap<String, usize>,
    /// The schema the last ingest into each collection resolved to.
    pub resolved_schemas: std::collections::HashMap<String, ColumnarSchema>,
}

impl CoreLoop {
    /// Append the redo sub-record for a single timeseries plan op to `ops`.
    /// `Ingest` tags `"timeseries"`; the scan op emits nothing.
    pub(super) fn serialize_timeseries_op(
        &self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &TimeseriesOp,
        state: &mut TimeseriesResolveState,
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        let TimeseriesResolveState {
            unkeyed_seen,
            resolved_schemas,
        } = state;
        match op {
            // Redo carries the ingested rows, not one caller's projected
            // response shape — replay reconstructs state, nothing else.
            TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                provenance,
                ..
            } if format == RESOLVED_INGEST_FORMAT => {
                // A sequenced transaction's ingest resolved to its rows on the
                // node that submitted it, before it was sequenced. Every
                // replica logs exactly those rows, and installs them by name.
                let batch =
                    ResolvedTsBatch::from_bytes(payload)?.with_drift(TsDriftPolicy::ApplyByName);
                push_resolved_batch(
                    collection.as_str(),
                    provenance,
                    &batch,
                    resolved_schemas,
                    ops,
                )
            }
            TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                wal_lsn: _,
                surrogates,
                provenance,
                rls_write_check: _,
                returning: _,
                rls_filters: _,
            } => {
                let tenant = TenantId::new(tid);
                let coll_key = (
                    task.request.database_id,
                    tenant,
                    collection.as_str().to_string(),
                );
                // The instant the statement read. A keyed ingest recorded it
                // under its first surrogate. An unkeyed ingest recorded it in
                // stage order, so the Nth unkeyed ingest into a collection
                // reads the Nth instant.
                let overlay = self.txn_overlays.get(&txn_id);
                let staged_now = match surrogates.first() {
                    Some(first) => {
                        overlay.and_then(|overlay| overlay.ingest_now(&coll_key, first.as_u32()))
                    }
                    None => {
                        let ordinal = unkeyed_seen.entry(coll_key.2.clone()).or_insert(0);
                        let now = overlay
                            .and_then(|overlay| overlay.unkeyed_ingest_now(&coll_key, *ordinal));
                        *ordinal += 1;
                        now
                    }
                };
                let now_ms = staged_now.unwrap_or_else(|| self.ingest_stamp_ms());
                let lines = self
                    .stamped_ingest_lines(StampedIngest {
                        database_id: task.request.database_id,
                        tid: tenant,
                        collection: collection.as_str(),
                        payload,
                        format,
                        now_ms,
                    })
                    .map_err(crate::Error::DataPlane)?;
                // An earlier ingest of this transaction into the same
                // collection installs first, so this one resolves against the
                // schema that ingest resolved to.
                let batch = self.resolve_ts_batch(
                    task,
                    TsResolveInput {
                        tid: tenant,
                        collection: collection.as_str(),
                        lines: &lines,
                        now_ms,
                        drift: TsDriftPolicy::ApplyByName,
                        needs_images: false,
                        base: resolved_schemas.get(collection.as_str()),
                    },
                )?;
                push_resolved_batch(
                    collection.as_str(),
                    provenance,
                    &batch,
                    resolved_schemas,
                    ops,
                )
            }

            // Same record the autocommit path appends
            // (`RecordType::TimeseriesTruncate`), replayed via
            // `replay_timeseries_truncate`.
            TimeseriesOp::Truncate {
                collection,
                restart_identity: _,
            } => {
                let sub_payload =
                    encode_columnar_truncate_payload(collection.as_str(), None, false)?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::TimeseriesTruncate as u32,
                    payload: sub_payload,
                });
                Ok(())
            }

            // Read family: no persisted post-image. The resolve pass is read-only
            // too — the ingest it reports is proposed as its own plan.
            TimeseriesOp::Scan { .. }
            | TimeseriesOp::ResolveIngest(_)
            | TimeseriesOp::ReadAggregate { .. } => Ok(()),

            // Staging refuses a refresh cut, so none reaches a redo.
            TimeseriesOp::RefreshAggregate { .. } => Err(crate::Error::FeatureNotSupported {
                detail: "REFRESH CONTINUOUS AGGREGATE runs outside a transaction".into(),
            }),

            // Staging refuses partition maintenance, so none reaches a redo.
            TimeseriesOp::Partitions { .. } => Err(crate::Error::FeatureNotSupported {
                detail: "SHOW PARTITIONS and REWRITE PARTITIONS run outside a transaction".into(),
            }),
        }
    }
}

/// Append the `ts-resolved` sub-record of `batch` to `ops`, and record the
/// schema it resolved to, which a later ingest of the transaction into
/// `collection` resolves against.
fn push_resolved_batch(
    collection: &str,
    provenance: &Option<nodedb_types::sync::wire::SyncProvenance>,
    batch: &ResolvedTsBatch,
    resolved_schemas: &mut std::collections::HashMap<String, ColumnarSchema>,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    resolved_schemas.insert(
        collection.to_owned(),
        ColumnarSchema {
            columns: batch.columns.clone(),
            timestamp_idx: usize::try_from(batch.timestamp_idx).unwrap_or(0),
            codecs: Vec::new(),
        },
    );
    let sub_payload = encode_timeseries_ingest_payload(TimeseriesIngestRecord {
        collection,
        payload: &batch.to_bytes()?,
        provenance: provenance.as_ref(),
        format: RESOLVED_INGEST_FORMAT,
        default_timestamp_ms: batch.now_ms,
    })?;
    ops.push(RedoSubRecord {
        record_type: RecordType::TimeseriesBatch as u32,
        payload: sub_payload,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::engine::timeseries::columnar_memtable::{ColumnType, ColumnValue, TimeKind};
    use crate::engine::timeseries::resolved_ingest::ResolvedTsRow;
    use crate::types::DatabaseId;

    /// A sequenced transaction's resolved ingest is logged with the rows it
    /// carries, never resolved again, and installs by column name.
    #[test]
    fn a_resolved_ingest_serializes_the_rows_it_carries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _tx, _rx) = make_core_with_dir(dir.path());
        let batch = ResolvedTsBatch {
            measurement: "metrics".to_string(),
            columns: vec![
                (
                    "timestamp".to_string(),
                    ColumnType::Timestamp(TimeKind::Millis),
                ),
                ("value".to_string(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            drift: TsDriftPolicy::Refuse,
            now_ms: 7_000,
            resolved_bytes: 16,
            emits_events: false,
            rows: vec![ResolvedTsRow {
                line: 0,
                tags: Vec::new(),
                timestamp_ms: 5_000,
                values: vec![ColumnValue::Timestamp(5_000), ColumnValue::Float64(1.5)],
                absent: Vec::new(),
                image: Vec::new(),
            }],
            rejected: 0,
            first_rejection: None,
        };
        let op = TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: batch.to_bytes().expect("encode batch"),
            format: RESOLVED_INGEST_FORMAT.to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        };
        let mut state = TimeseriesResolveState::default();
        let mut ops = Vec::new();
        core.serialize_timeseries_op(
            &make_default_task(),
            1,
            TxnId::new(1),
            &op,
            &mut state,
            &mut ops,
        )
        .expect("serialize the resolved ingest");

        assert_eq!(ops.len(), 1);
        let record = crate::wal::decode_batch_record(&ops[0].payload).expect("decode record");
        assert_eq!(record.format.as_deref(), Some(RESOLVED_INGEST_FORMAT));
        let logged = ResolvedTsBatch::from_bytes(&record.payload).expect("decode batch");
        assert_eq!(logged, batch.with_drift(TsDriftPolicy::ApplyByName));
        assert!(state.resolved_schemas.contains_key("metrics"));
    }
}
