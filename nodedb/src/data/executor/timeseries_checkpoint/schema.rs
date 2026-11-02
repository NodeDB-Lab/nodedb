// SPDX-License-Identifier: BUSL-1.1

//! The durable memtable schema of a timeseries collection.
//!
//! ## Why it exists
//!
//! A committed ingest rejects a row whose value conflicts with the type of
//! its column in the schema in force at the ingest's log position. Every
//! replica applies the same log in the same order, so every replica rejects
//! the same rows, provided every replica holds the same schema at that
//! position.
//!
//! A flush drains the memtable's rows and keeps its schema, so the schema
//! outlives every flush. A restart has no memtable until this file seeds one.
//! Without it, the first record replayed after a restart founds a new
//! schema, and a column's type can differ from the one every replica that
//! did not restart still holds.
//!
//! ## Where it lives
//!
//! The collection directory carries `memtable.schema`. Every flush writes it
//! before the partition's commit point, so the schema on disk always covers
//! every record a partition names. A truncate keeps it: the schema outlives
//! the truncate as it outlives a flush. Dropping the collection removes the
//! directory.
//!
//! A snapshot carries each memtable's schema from memory. A core snapshot
//! captures it as this file. A tenant snapshot, which a Raft snapshot install
//! ships, carries the memtable, and its install writes this file. So a
//! follower that catches up by snapshot holds the leader's schema at the
//! snapshot's position, before and after a restart.

use std::path::Path;

use crate::engine::timeseries::columnar_memtable::{ColumnType, ColumnarSchema};

/// File name of the durable schema in a collection directory. It does not
/// start with `ts-`, so the partition scan and the orphan sweep pass over it.
pub(in crate::data::executor) const TS_SCHEMA_FILE: &str = "memtable.schema";

/// Leading byte of an encoded schema file.
const TS_SCHEMA_FORMAT_VERSION: u8 = 1;

/// The encoded form of a memtable schema.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
struct DurableTsSchema {
    columns: Vec<(String, ColumnType)>,
    timestamp_idx: u64,
    codecs: Vec<nodedb_codec::ColumnCodec>,
}

fn schema_error(path: &Path, action: &str, detail: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".to_string(),
        detail: format!(
            "memtable schema: failed to {action} {}: {detail}",
            path.display()
        ),
    }
}

/// The bytes of a schema file holding `schema`.
pub(in crate::data::executor) fn encode_ts_schema(
    schema: &ColumnarSchema,
) -> crate::Result<Vec<u8>> {
    let durable = DurableTsSchema {
        columns: schema.columns.clone(),
        timestamp_idx: schema.timestamp_idx as u64,
        codecs: schema.codecs.clone(),
    };
    let mut bytes = vec![TS_SCHEMA_FORMAT_VERSION];
    let body = zerompk::to_msgpack_vec(&durable)
        .map_err(|e| schema_error(Path::new(TS_SCHEMA_FILE), "encode", &e))?;
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

/// Write `schema` into the collection directory `dir` durably: data fsynced
/// before the rename, `dir` fsynced after it.
pub(in crate::data::executor) fn write_ts_schema(
    dir: &Path,
    schema: &ColumnarSchema,
) -> crate::Result<()> {
    let bytes = encode_ts_schema(schema)?;
    nodedb_wal::segment::atomic_write_fsync(dir, TS_SCHEMA_FILE, &bytes)
        .map_err(|e| schema_error(&dir.join(TS_SCHEMA_FILE), "write", &e))
}

/// Read the schema in the collection directory `dir`. A missing file names
/// no schema. A file that is present but does not decode is an error: the
/// schema decides which committed rows every replica rejects.
pub(in crate::data::executor) fn read_ts_schema(
    dir: &Path,
) -> crate::Result<Option<ColumnarSchema>> {
    let path = dir.join(TS_SCHEMA_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(schema_error(&path, "read", &e)),
    };
    let Some((&version, body)) = bytes.split_first() else {
        return Err(schema_error(&path, "decode", &"the file is empty"));
    };
    if version != TS_SCHEMA_FORMAT_VERSION {
        return Err(schema_error(
            &path,
            "decode",
            &format!("format version {version}, expected {TS_SCHEMA_FORMAT_VERSION}"),
        ));
    }
    let durable: DurableTsSchema =
        zerompk::from_msgpack(body).map_err(|e| schema_error(&path, "decode", &e))?;
    let timestamp_idx = usize::try_from(durable.timestamp_idx)
        .ok()
        .filter(|idx| {
            durable
                .columns
                .get(*idx)
                .is_some_and(|(_, column_type)| column_type.is_time())
        })
        .ok_or_else(|| {
            schema_error(
                &path,
                "validate",
                &format!(
                    "time column index {} names no time column",
                    durable.timestamp_idx
                ),
            )
        })?;
    Ok(Some(ColumnarSchema {
        columns: durable.columns,
        timestamp_idx,
        codecs: durable.codecs,
    }))
}

impl crate::data::executor::core_loop::CoreLoop {
    /// Write the schema of the live memtable of `key` into its collection
    /// directory, creating the directory. A snapshot install calls this for
    /// each memtable it restores: an empty memtable does not flush, and its
    /// schema must still survive a restart.
    pub(in crate::data::executor) fn persist_ts_schema(
        &self,
        key: &super::stamp::TsCollectionKey,
    ) -> crate::Result<()> {
        let Some(mt) = self.columnar_memtables.get(key) else {
            return Ok(());
        };
        let dir = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
            &self.data_dir,
            key.0.as_u64(),
            key.1.as_u64(),
            &key.2,
        );
        std::fs::create_dir_all(&dir).map_err(|e| schema_error(&dir, "create", &e))?;
        write_ts_schema(&dir, mt.schema())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::TimeKind;

    fn schema() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("extra".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        }
    }

    #[test]
    fn a_written_schema_reads_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_ts_schema(dir.path(), &schema()).expect("write schema");
        let read = read_ts_schema(dir.path())
            .expect("read schema")
            .expect("a schema is present");
        assert_eq!(read.columns, schema().columns);
        assert_eq!(read.timestamp_idx, 0);
        assert_eq!(read.codecs, schema().codecs);
    }

    #[test]
    fn a_missing_file_names_no_schema() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(read_ts_schema(dir.path()).expect("read").is_none());
    }

    #[test]
    fn a_corrupt_file_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join(TS_SCHEMA_FILE),
            [TS_SCHEMA_FORMAT_VERSION, 0xc1],
        )
        .expect("write corrupt file");
        assert!(read_ts_schema(dir.path()).is_err());
    }
}
