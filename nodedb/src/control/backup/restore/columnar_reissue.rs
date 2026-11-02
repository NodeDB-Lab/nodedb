// SPDX-License-Identifier: BUSL-1.1

//! Decode of restored plain-columnar and spatial rows.
//!
//! RESTORE re-issues every stored version of each collection's keys as
//! Calvin transactions (`redo_reissue::columnar`). Each version carries its
//! `sys_from` and `sys_to`, so `AS OF` and pinned reads after the restore
//! answer as before the backup.

use std::collections::HashMap;

use nodedb_columnar::{ColumnarEngineSnapshot, MutationEngine};
use nodedb_types::columnar::RestoredVersion;
use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::Error;

/// Every stored version of a decoded snapshot, ready to re-issue.
///
/// `rows`, `surrogates` and `versions` are index-aligned and ascending by
/// `sys_from`. The schema bytes are carried so the `Insert` plan can convey
/// the column layout.
pub struct DecodedColumnarRows {
    pub rows: Vec<Value>,
    pub surrogates: Vec<Surrogate>,
    pub versions: Vec<RestoredVersion>,
    pub schema_bytes: Vec<u8>,
}

/// Decode one `ColumnarEngineSnapshot` into every version a read at some cut
/// can see: the open rows and the ended rows kept as history, in the
/// memtable and every flushed segment. Each row is a `Value::Object` keyed by
/// column name, paired with its cross-engine surrogate and its stamps. A
/// purged row is left out: no read sees it.
///
/// The versions are ascending by `sys_from`. Installing them in this order
/// rebuilds each key's history, its current row, and its closed-key binding.
///
/// `kek` is the columnar segment encryption key (the WAL encryption key);
/// pass `None` when at-rest encryption is not configured.
///
/// Every kept row must carry a `Some` surrogate — a `None` is a real identity
/// loss and surfaces as an error rather than being silently defaulted.
pub fn decode_snapshot_versions(
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

    // Rebuild the engine to get the memtable, the per-segment delete bitmaps
    // and stamps, the flushed-segment blobs, and the flushed surrogate
    // sidecar — all from the single lossless snapshot.
    let (engine, flushed_segments, flushed_surrogates): (MutationEngine, Vec<Vec<u8>>, _) =
        MutationEngine::from_snapshot(snap).map_err(|e| Error::Storage {
            engine: "columnar".into(),
            detail: format!("restore reissue: from_snapshot for '{collection}': {e}"),
        })?;
    let stored = engine
        .stored_versions(&flushed_segments, &flushed_surrogates, kek)
        .map_err(|e| Error::Storage {
            engine: "columnar".into(),
            detail: format!("restore reissue: read the versions of '{collection}': {e}"),
        })?;

    let mut rows: Vec<Value> = Vec::with_capacity(stored.len());
    let mut surrogates: Vec<Surrogate> = Vec::with_capacity(stored.len());
    let mut versions: Vec<RestoredVersion> = Vec::with_capacity(stored.len());
    for version in stored {
        let surrogate = version.surrogate.ok_or_else(|| Error::Storage {
            engine: "columnar".into(),
            detail: format!(
                "restore reissue: a row of '{collection}' stamped {} has no surrogate; \
                 cannot preserve cross-engine identity",
                version.sys_from
            ),
        })?;
        rows.push(row_values_to_object(
            &column_names,
            version.values,
            collection,
        )?);
        surrogates.push(surrogate);
        versions.push(RestoredVersion {
            sys_from: version.sys_from,
            sys_to: version.sys_to,
            // A restore installs every backed-up version as it stands.
            clone_copy: false,
        });
    }

    Ok(DecodedColumnarRows {
        rows,
        surrogates,
        versions,
        schema_bytes,
    })
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
