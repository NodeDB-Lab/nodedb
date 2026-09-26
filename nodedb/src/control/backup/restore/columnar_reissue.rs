// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored plain-columnar rows.
//!
//! Snapshot-install lands columnar state in in-memory-only Data Plane maps
//! with no WAL record or Raft entry — lost on restart, never replicated.
//! RESTORE re-issues each collection's live rows as a durable
//! `ColumnarOp::Insert`: Raft-proposed on cluster, WAL-appended + dispatched
//! on single-node — the same branch a normal write takes.

use std::collections::HashMap;

use nodedb_columnar::{ColumnarEngineSnapshot, MutationEngine, materialize_segment_live_rows};
use nodedb_types::RlsWriteCheck;
use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp};

/// Live rows of a decoded snapshot, ready to re-issue.
///
/// `rows` and `surrogates` are index-aligned (one surrogate per row). The schema
/// bytes are carried so the `Insert` plan can convey the column layout.
pub struct DecodedColumnarRows {
    pub rows: Vec<Value>,
    pub surrogates: Vec<Surrogate>,
    pub schema_bytes: Vec<u8>,
}

/// Decode one `ColumnarEngineSnapshot` into the live (non-deleted) rows of the
/// memtable plus every flushed segment, each as a `Value::Object` keyed by
/// column name, paired with its cross-engine surrogate.
///
/// `kek` is the columnar segment encryption key (the WAL encryption key); pass
/// `None` when at-rest encryption is not configured.
///
/// Every kept row must carry a `Some` surrogate — a `None` is a real identity
/// loss and surfaces as an error rather than being silently defaulted.
pub fn decode_snapshot_live_rows(
    collection: &str,
    snap: ColumnarEngineSnapshot,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<DecodedColumnarRows> {
    let schema = snap.schema.clone();
    let schema_bytes = zerompk::to_msgpack_vec(&schema).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore reissue: encode schema for '{collection}': {e}"),
    })?;
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

    // Rebuild the engine to get the memtable (with its delete bitmap applied),
    // the flushed-segment blobs, the per-segment delete bitmaps, and the flushed
    // surrogate sidecar — all from the single lossless snapshot.
    let (engine, flushed_segments, flushed_surrogates): (MutationEngine, Vec<Vec<u8>>, _) =
        MutationEngine::from_snapshot(snap).map_err(|e| Error::Storage {
            engine: "columnar".into(),
            detail: format!("restore reissue: from_snapshot for '{collection}': {e}"),
        })?;

    let mut rows: Vec<Value> = Vec::new();
    let mut surrogates: Vec<Surrogate> = Vec::new();

    // Memtable: non-deleted rows in schema column order → keyed object.
    for (surrogate, values) in engine.scan_memtable_rows_with_surrogates() {
        let surrogate = surrogate.ok_or_else(|| Error::Storage {
            engine: "columnar".into(),
            detail: format!(
                "restore reissue: memtable row for '{collection}' has no surrogate; \
                 cannot preserve cross-engine identity"
            ),
        })?;
        rows.push(row_values_to_object(&column_names, values, collection)?);
        surrogates.push(surrogate);
    }

    // Flushed segments: blob index i corresponds to segment_id i + 1 (the
    // export/flush convention). The per-segment delete bitmap lives on the
    // rebuilt engine under that segment_id; the surrogate sidecar is parallel to
    // `flushed_segments`.
    for (idx, blob) in flushed_segments.iter().enumerate() {
        let segment_id = idx as u64 + 1;
        let empty_deletes = nodedb_columnar::DeleteBitmap::new();
        let deletes = engine.delete_bitmap(segment_id).unwrap_or(&empty_deletes);
        let seg_surrogates: &[Option<Surrogate>] = flushed_surrogates
            .get(idx)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let live = materialize_segment_live_rows(blob, kek, &schema, deletes, seg_surrogates)
            .map_err(|e| Error::Storage {
                engine: "columnar".into(),
                detail: format!(
                    "restore reissue: materialize segment {segment_id} for '{collection}': {e}"
                ),
            })?;

        for (row, surrogate) in live {
            let surrogate = surrogate.ok_or_else(|| Error::Storage {
                engine: "columnar".into(),
                detail: format!(
                    "restore reissue: flushed row in segment {segment_id} for '{collection}' \
                     has no surrogate; cannot preserve cross-engine identity"
                ),
            })?;
            rows.push(row);
            surrogates.push(surrogate);
        }
    }

    Ok(DecodedColumnarRows {
        rows,
        surrogates,
        schema_bytes,
    })
}

/// Build the durable `ColumnarOp::Insert` plan from decoded rows.
///
/// The payload is the msgpack encoding of `Value::Array(rows)` (array of
/// per-row field-keyed objects) — the exact shape the columnar insert handler
/// expects.
pub fn build_columnar_insert_plan(
    collection: &str,
    decoded: DecodedColumnarRows,
) -> crate::Result<PhysicalPlan> {
    // Encode with the native-Value msgpack writer that is symmetric to the
    // handler's `value_from_msgpack` reader. `zerompk::to_msgpack_vec` frames
    // `Value` differently and would not round-trip through that reader.
    let payload = nodedb_types::value_to_msgpack(&Value::Array(decoded.rows)).map_err(|e| {
        Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: encode rows for '{collection}': {e}"),
        }
    })?;

    Ok(PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
        payload,
        format: "msgpack".into(),
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates: decoded.surrogates,
        schema_bytes: decoded.schema_bytes,
        provenance: None,
        wal_lsn: None,
        // No predicate here: a restore re-issues rows that were already
        // admitted before the backup was taken. The identity that admitted
        // them is not available during restore.
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        // A restore re-issues stored rows; no client is waiting on a
        // projection, and there is no identity whose reads need gating.
        returning: None,
        rls_filters: Vec::new(),
    }))
}

/// Convert a row's positional `Value`s (schema column order) into a field-keyed
/// `Value::Object`. Errors if the arity does not match the schema.
fn row_values_to_object(
    column_names: &[String],
    values: Vec<Value>,
    collection: &str,
) -> crate::Result<Value> {
    if values.len() != column_names.len() {
        return Err(Error::Storage {
            engine: "columnar".into(),
            detail: format!(
                "restore reissue: row arity {} != schema column count {} for '{collection}'",
                values.len(),
                column_names.len()
            ),
        });
    }
    let mut map: HashMap<String, Value> = HashMap::with_capacity(values.len());
    for (name, value) in column_names.iter().zip(values) {
        map.insert(name.clone(), value);
    }
    Ok(Value::Object(map))
}
