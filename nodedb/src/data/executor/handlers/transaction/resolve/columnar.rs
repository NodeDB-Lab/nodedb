// SPDX-License-Identifier: BUSL-1.1

//! Columnar + timeseries serializer for transaction resolve. Plan-driven,
//! like the vector serializer: these ops ride the buffered-plan path, not a
//! per-surrogate overlay, so this reads the plan node directly and reuses the
//! autocommit path's `RecordType::TimeseriesBatch` encoders
//! (`control::server::wal_dispatch`). Columnar and timeseries share that one
//! record type, disambiguated on replay by payload shape: a map (`kind =
//! "columnar"`/`"columnar_dml"`) vs. the timeseries 5-tuple. Predicate DML
//! (`Update`/`Delete`) uses the same `ColumnarDmlWalRecord` the autocommit
//! path appends, so an in-tx UPDATE/DELETE is restart-durable identically.
//! Emission is in plan order, already deterministic.

use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp};
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::{
    encode_columnar_batch_payload, encode_columnar_dml_payload,
    encode_columnar_resolved_dml_payload, encode_columnar_truncate_payload,
    encode_timeseries_batch_payload_with_format,
};
use crate::wal::RedoSubRecord;

/// Append the redo sub-record for a single columnar plan op to `ops`.
/// `Insert` tags `"columnar"`; predicate DML tags `"columnar_dml"`; reads
/// emit nothing (see module docs).
pub(super) fn serialize_columnar_op(
    op: &ColumnarOp,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    match op {
        ColumnarOp::Insert {
            collection,
            payload,
            format: _,
            intent: _,
            on_conflict_updates: _,
            surrogates,
            schema_bytes: _,
            provenance,
            wal_lsn: _,
            rls_write_check: _,
            // The redo record carries the row image, not the response shape a
            // projection and its read gate would have produced for one caller.
            returning: _,
            rls_filters: _,
        } => {
            let sub_payload = encode_columnar_batch_payload(
                collection.as_str(),
                payload,
                provenance.as_ref(),
                surrogates,
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: sub_payload,
            });
            Ok(())
        }

        // Read families: no persisted post-image. `ResolveDml` mutates
        // nothing, so it emits no redo sub-record either.
        ColumnarOp::Scan { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::ResolveDml { .. } => Ok(()),

        // Same `ColumnarDmlWalRecord` the autocommit path appends; replay
        // re-executes the predicate through the live handler, so an in-tx
        // UPDATE/DELETE is restart-durable exactly like its autocommit twin.
        ColumnarOp::Update {
            collection,
            filters,
            updates,
            rls_write_check: _,
        } => {
            let sub_payload =
                encode_columnar_dml_payload(collection.as_str(), true, filters, updates)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: sub_payload,
            });
            Ok(())
        }
        ColumnarOp::Delete {
            collection,
            filters,
            rls_write_check: _,
        } => {
            let sub_payload =
                encode_columnar_dml_payload(collection.as_str(), false, filters, &[])?;
            ops.push(RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: sub_payload,
            });
            Ok(())
        }

        // Control Plane already resolved these rows, so the redo carries the
        // exact images, never a predicate — same as the autocommit encoder.
        ColumnarOp::ResolvedUpdate {
            collection,
            rows,
            rls_write_check: _,
        } => {
            let sub_payload =
                encode_columnar_resolved_dml_payload(collection.as_str(), true, rows, &[])?;
            ops.push(RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: sub_payload,
            });
            Ok(())
        }
        ColumnarOp::ResolvedDelete {
            collection,
            pks,
            rls_write_check: _,
        } => {
            let sub_payload =
                encode_columnar_resolved_dml_payload(collection.as_str(), false, &[], pks)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: sub_payload,
            });
            Ok(())
        }
        // Same record the autocommit path appends (`RecordType::ColumnarTruncate`),
        // replayed via `replay_columnar_truncate`. `restart_identity` is a
        // Control-Plane sequence concern and never enters the redo record.
        ColumnarOp::Truncate {
            collection,
            restart_identity: _,
        } => {
            let sub_payload = encode_columnar_truncate_payload(collection.as_str())?;
            ops.push(RedoSubRecord {
                record_type: RecordType::ColumnarTruncate as u32,
                payload: sub_payload,
            });
            Ok(())
        }
    }
}

/// Append the redo sub-record for a single timeseries plan op to `ops`.
/// `Ingest` tags `"timeseries"`; the scan op emits nothing.
pub(super) fn serialize_timeseries_op(
    op: &TimeseriesOp,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    match op {
        TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            wal_lsn: _,
            surrogates: _,
            provenance,
            rls_write_check: _,
            // Redo carries the ingested payload, not one caller's projected
            // response shape — replay reconstructs state, nothing else.
            returning: _,
            rls_filters: _,
        } => {
            let sub_payload = encode_timeseries_batch_payload_with_format(
                collection.as_str(),
                payload,
                provenance.as_ref(),
                format,
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
