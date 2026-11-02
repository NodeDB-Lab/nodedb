// SPDX-License-Identifier: BUSL-1.1

//! Backed-up timeseries partitions: decode, and re-encode for install.
//!
//! A backup copies each partition's segment files as the source core wrote
//! them. A core writes plaintext segments unless a timeseries segment key is
//! installed on it, and then wraps every file in a `SEGT` envelope under that
//! key. The decode sniffs the partition's `schema.json`: an encrypted
//! partition reads with this server's WAL key, the key segment encryption
//! takes, and a plaintext one reads with none.
//!
//! A RESTORE never copies segment bytes across keys. An ended partition is
//! decoded to its columns and written again through the segment writer with
//! no key: plaintext in the destination's segment format. The install wraps
//! each file under the destination core's own segment key
//! (`CoreLoop::install_ts_ended_partition`), and the redo record that
//! carries the files is protected by the WAL's own encryption. The writer is
//! deterministic, so a partition re-encodes to the same files, and a second
//! RESTORE finds it installed.

use std::collections::HashMap;

use nodedb_types::timeseries::{PartitionMeta, SymbolDictionary};
use nodedb_wal::crypto::WalEncryptionKey;

use crate::Error;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnarFlushView, ColumnarSchema};
use crate::engine::timeseries::columnar_segment::encrypt::is_encrypted;
use crate::engine::timeseries::columnar_segment::{ColumnarSegmentReader, ColumnarSegmentWriter};
use crate::types::TsFlushedPartitionBlob;
use crate::wal::TimeseriesEndedPartitionRedo;

/// The files a partition directory holds that the install writes itself:
/// the meta, and the source core's replay stamp.
const INSTALL_WRITTEN: [&str; 2] = ["partition.meta", "replay.stamp"];

/// The file a partition's encryption is sniffed from.
const SCHEMA_FILE: &str = "schema.json";

/// The columns of one backed-up partition.
pub(super) struct BackedUpPartition {
    pub schema: ColumnarSchema,
    pub columns: Vec<ColumnData>,
    pub sym_dicts: HashMap<usize, SymbolDictionary>,
}

fn partition_error(collection: &str, part: &str, what: &str, e: impl std::fmt::Display) -> Error {
    Error::Storage {
        engine: "timeseries".into(),
        detail: format!("restore reissue: {what} of partition '{part}' of '{collection}': {e}"),
    }
}

/// The key `part` reads with: `key` when its files are encrypted, none when
/// they are plaintext.
///
/// `Err` when the partition holds no schema, or is encrypted and this server
/// has no key.
pub(super) fn segment_key<'k>(
    collection: &str,
    part: &TsFlushedPartitionBlob,
    key: Option<&'k WalEncryptionKey>,
) -> crate::Result<Option<&'k WalEncryptionKey>> {
    let Some((_, schema)) = part.files.iter().find(|(name, _)| name == SCHEMA_FILE) else {
        return Err(partition_error(
            collection,
            &part.dir_name,
            "the schema",
            "the backup holds no schema.json",
        ));
    };
    let encrypted = is_encrypted(schema)
        .map_err(|e| partition_error(collection, &part.dir_name, "the schema", e))?;
    match (encrypted, key) {
        (false, _) => Ok(None),
        (true, Some(key)) => Ok(Some(key)),
        (true, None) => Err(partition_error(
            collection,
            &part.dir_name,
            "the segment files",
            "they are encrypted and this server has no encryption key",
        )),
    }
}

/// Decode every column of the backed-up partition `part`.
pub(super) fn read_backed_up_partition(
    collection: &str,
    part: &TsFlushedPartitionBlob,
    key: Option<&WalEncryptionKey>,
) -> crate::Result<BackedUpPartition> {
    let kek = segment_key(collection, part, key)?;
    let tmp = tempfile::Builder::new()
        .prefix("nodedb-ts-reissue-")
        .tempdir()
        .map_err(Error::Io)?;
    let part_dir = tmp.path();
    for (filename, data) in &part.files {
        std::fs::write(part_dir.join(filename), data).map_err(Error::Io)?;
    }
    let schema = ColumnarSegmentReader::read_schema(part_dir, kek)
        .map_err(|e| partition_error(collection, &part.dir_name, "the schema", e))?;
    // A re-encoded partition carries no meta: the reader takes each codec
    // from the schema then.
    let columns = ColumnarSegmentReader::read_columns(part_dir, &schema.columns, kek)
        .map_err(|e| partition_error(collection, &part.dir_name, "the columns", e))?;
    let mut sym_dicts = HashMap::new();
    for (i, (name, ty)) in schema.columns.iter().enumerate() {
        let dict_file = format!("{name}.sym");
        if !ty.is_dictionary() || !part.files.iter().any(|(file, _)| *file == dict_file) {
            continue;
        }
        let dict = ColumnarSegmentReader::read_symbol_dict(part_dir, name, kek)
            .map_err(|e| partition_error(collection, &part.dir_name, "a symbol dictionary", e))?;
        sym_dicts.insert(i, dict);
    }
    Ok(BackedUpPartition {
        schema,
        columns,
        sym_dicts,
    })
}

/// The sub-record that installs the backed-up ended partition `part`, whose
/// meta is `meta`, into the collection stored as `stored`. The files are
/// `part` decoded with `key` and written again as plaintext through the
/// segment writer.
pub fn ended_partition_redo(
    collection: &str,
    stored: &str,
    part: &TsFlushedPartitionBlob,
    meta: PartitionMeta,
    key: Option<&WalEncryptionKey>,
) -> crate::Result<TimeseriesEndedPartitionRedo> {
    let decoded = read_backed_up_partition(collection, part, key)?;
    let out = tempfile::Builder::new()
        .prefix("nodedb-ts-reencode-")
        .tempdir()
        .map_err(Error::Io)?;
    let view = ColumnarFlushView {
        columns: &decoded.columns,
        schema: &decoded.schema,
        symbol_dicts: &decoded.sym_dicts,
        row_count: meta.row_count,
        min_ts: meta.min_ts,
        max_ts: meta.max_ts,
        max_system_ts: meta.max_system_ts,
    };
    const NAME: &str = "partition";
    let written = ColumnarSegmentWriter::new(out.path())
        .write_partition(NAME, &view, meta.interval_ms, 0, None)
        .map_err(|e| partition_error(collection, &part.dir_name, "the re-encode", e))?;
    let dir = out.path().join(NAME);
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(Error::Io)? {
        let entry = entry.map_err(Error::Io)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if INSTALL_WRITTEN.contains(&name.as_str()) {
            continue;
        }
        files.push((name, std::fs::read(entry.path()).map_err(Error::Io)?));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(TimeseriesEndedPartitionRedo {
        collection: stored.to_string(),
        meta: PartitionMeta {
            size_bytes: written.size_bytes,
            column_stats: written.column_stats,
            last_flushed_wal_lsn: 0,
            ..meta
        },
        files,
    })
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::PartitionState;

    use super::*;
    use crate::engine::timeseries::columnar_memtable::{ColumnType, TimeKind};

    /// A two-row partition written under `kek`, as a backup holds it.
    fn backed_up(kek: Option<&WalEncryptionKey>) -> (TsFlushedPartitionBlob, PartitionMeta) {
        let schema = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("v".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let columns = vec![
            ColumnData::Timestamp(vec![1_000, 2_000]),
            ColumnData::Float64(vec![1.5, 2.5]),
        ];
        let dicts = HashMap::new();
        let view = ColumnarFlushView {
            columns: &columns,
            schema: &schema,
            symbol_dicts: &dicts,
            row_count: 2,
            min_ts: 1_000,
            max_ts: 2_000,
            max_system_ts: 0,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let mut meta = ColumnarSegmentWriter::new(dir.path())
            .write_partition("ts-1", &view, 0, 0, kek)
            .expect("write");
        meta.ended_at = Some(9);
        meta.state = PartitionState::Sealed;
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir.path().join("ts-1")).expect("dir") {
            let entry = entry.expect("entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            files.push((name, std::fs::read(entry.path()).expect("read")));
        }
        let blob = TsFlushedPartitionBlob {
            dir_name: "ts-1".into(),
            meta_bytes: zerompk::to_msgpack_vec(&meta).expect("meta"),
            files,
        };
        (blob, meta)
    }

    /// An encrypted backed-up partition re-encodes to plaintext files that
    /// read with no key, the same files every time, with no meta or stamp.
    #[test]
    fn an_encrypted_partition_re_encodes_as_plaintext() {
        let kek = WalEncryptionKey::from_bytes(&[0x42u8; 32]).expect("key");
        let (blob, meta) = backed_up(Some(&kek));
        let first = ended_partition_redo("m", "m", &blob, meta.clone(), Some(&kek)).expect("redo");
        let second = ended_partition_redo("m", "m", &blob, meta, Some(&kek)).expect("again");
        assert_eq!(first.files, second.files, "the re-encode is deterministic");
        assert_eq!(first.meta.ended_at, Some(9));
        assert!(
            first
                .files
                .iter()
                .all(|(name, bytes)| !INSTALL_WRITTEN.contains(&name.as_str())
                    && !is_encrypted(bytes).expect("sniff")),
            "every file is plaintext and the install writes the meta"
        );
        let plain = TsFlushedPartitionBlob {
            dir_name: "re".into(),
            meta_bytes: Vec::new(),
            files: first.files,
        };
        let read = read_backed_up_partition("m", &plain, None).expect("reads with no key");
        let ColumnData::Float64(values) = &read.columns[1] else {
            panic!("the value column reads as floats");
        };
        assert_eq!(values, &vec![1.5, 2.5]);
    }

    /// A plaintext partition reads with no key, whatever key this server
    /// holds. An encrypted one with no key here is refused.
    #[test]
    fn the_key_follows_the_files() {
        let kek = WalEncryptionKey::from_bytes(&[0x42u8; 32]).expect("key");
        let (plain, _) = backed_up(None);
        assert!(
            segment_key("m", &plain, Some(&kek))
                .expect("plain")
                .is_none()
        );
        let (sealed, _) = backed_up(Some(&kek));
        assert!(
            segment_key("m", &sealed, Some(&kek))
                .expect("sealed")
                .is_some()
        );
        assert!(segment_key("m", &sealed, None).is_err());
    }
}
