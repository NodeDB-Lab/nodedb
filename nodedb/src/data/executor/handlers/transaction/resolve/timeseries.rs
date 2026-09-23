// SPDX-License-Identifier: BUSL-1.1

//! Timeseries serializer for transaction resolve. Plan-driven: a timeseries
//! ingest is append-only, so the redo carries the ingested rows through the
//! autocommit path's `RecordType::TimeseriesBatch` encoder
//! (`control::server::wal_dispatch`) and replay appends the same samples.
//! Emission is in plan order, already deterministic. Columnar writes resolve
//! from the overlay instead (`columnar_image`).
//!
//! The rows are resolved to canonical line protocol, and every row with no
//! timestamp is stamped here, once, with the instant its statement read. The
//! sub-record therefore stores the same rows on every replica and on every
//! restart.

use nodedb_physical::physical_plan::TimeseriesOp;
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::{
    encode_columnar_truncate_payload, encode_timeseries_batch_payload_with_format,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::StampedIngest;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};
use crate::wal::RedoSubRecord;

/// The ingest format of a resolved timeseries sub-record: canonical line
/// protocol, one string per row.
const RESOLVED_INGEST_FORMAT: &str = "ilp-msgpack";

impl CoreLoop {
    /// Append the redo sub-record for a single timeseries plan op to `ops`.
    /// `Ingest` tags `"timeseries"`; the scan op emits nothing.
    pub(super) fn serialize_timeseries_op(
        &self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &TimeseriesOp,
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        match op {
            TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                wal_lsn: _,
                surrogates,
                provenance,
                rls_write_check: _,
                // Redo carries the ingested rows, not one caller's projected
                // response shape — replay reconstructs state, nothing else.
                returning: _,
                rls_filters: _,
            } => {
                let tenant = TenantId::new(tid);
                let coll_key = (
                    task.request.database_id,
                    tenant,
                    collection.as_str().to_string(),
                );
                // The instant the statement read. An ingest staged with no
                // surrogate recorded none, and resolve reads the clock now.
                let now_ms = surrogates
                    .first()
                    .and_then(|first| {
                        self.txn_overlays
                            .get(&txn_id)?
                            .ingest_now(&coll_key, first.as_u32())
                    })
                    .unwrap_or_else(|| self.ingest_now_ms());
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
                let resolved =
                    zerompk::to_msgpack_vec(&lines).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("resolved timeseries lines: {e}"),
                    })?;
                let sub_payload = encode_timeseries_batch_payload_with_format(
                    collection.as_str(),
                    &resolved,
                    provenance.as_ref(),
                    RESOLVED_INGEST_FORMAT,
                )?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::TimeseriesBatch as u32,
                    payload: sub_payload,
                });
                Ok(())
            }

            // Same record the autocommit path appends
            // (`RecordType::TimeseriesTruncate`), replayed via
            // `replay_timeseries_truncate`.
            TimeseriesOp::Truncate {
                collection,
                restart_identity: _,
            } => {
                let sub_payload = encode_columnar_truncate_payload(collection.as_str())?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::TimeseriesTruncate as u32,
                    payload: sub_payload,
                });
                Ok(())
            }

            // Read family: no persisted post-image. The resolve pass is read-only
            // too — the ingest it reports is proposed as its own plan.
            TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_) => Ok(()),
        }
    }
}
