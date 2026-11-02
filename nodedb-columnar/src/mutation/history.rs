// SPDX-License-Identifier: Apache-2.0

//! Every stored version of an engine with its stamps: the rows a backup
//! restore reissues.

use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::error::ColumnarError;
use crate::materialize_rows::materialize_segment_rows;
use crate::pk_index::RowLocation;
use crate::versions::VersionCut;

use super::engine::MutationEngine;

/// One stored row version.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredVersion {
    /// The row's cells in schema column order.
    pub values: Vec<Value>,
    /// The row's cross-engine surrogate. `None` for a row stored without
    /// one.
    pub surrogate: Option<Surrogate>,
    /// The ordinal the version starts at.
    pub sys_from: i64,
    /// The ordinal a write ended the version at. `None` for an open version.
    pub sys_to: Option<i64>,
}

impl MutationEngine {
    /// Every version a read at some cut can see, ascending by `sys_from`.
    ///
    /// Covers the open rows and the ended rows kept as history, in the
    /// flushed segments and in the memtable. A purged row is left out: no
    /// read sees it. Versions with one `sys_from` keep their append order:
    /// flushed segments in flush order, then the memtable.
    ///
    /// `flushed_segments` holds the blob of every flushed segment, index `i`
    /// being segment id `i + 1`. `flushed_surrogates` is the surrogate
    /// sidecar parallel to it. `kek` decrypts an encrypted segment.
    ///
    /// Installing the result in order with
    /// [`MutationEngine::install_versions`] into an empty engine reproduces
    /// every read this engine answers.
    ///
    /// `Err` when a segment or a memtable row does not decode, or a row has
    /// no `sys_from` stamp.
    pub fn stored_versions(
        &self,
        flushed_segments: &[Vec<u8>],
        flushed_surrogates: &[Vec<Option<Surrogate>>],
        kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    ) -> Result<Vec<StoredVersion>, ColumnarError> {
        let mut versions = Vec::new();
        for (idx, blob) in flushed_segments.iter().enumerate() {
            let segment_id = idx as u64 + 1;
            let purged = self.purged_rows(segment_id);
            let surrogates = flushed_surrogates
                .get(idx)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            for row in materialize_segment_rows(blob, kek, &self.schema, &purged, surrogates)? {
                let location = RowLocation {
                    segment_id,
                    row_index: row.row,
                };
                versions.push(self.stored_version(location, row.values, row.surrogate)?);
            }
        }
        for scanned in self.scan_memtable_versions(VersionCut::All) {
            let (row_index, surrogate, values) = scanned?;
            let location = RowLocation {
                segment_id: self.memtable_segment_id,
                row_index,
            };
            versions.push(self.stored_version(location, values, surrogate)?);
        }
        versions.sort_by_key(|v| v.sys_from);
        Ok(versions)
    }

    /// The version stored at `location`, with its stamps.
    fn stored_version(
        &self,
        location: RowLocation,
        values: Vec<Value>,
        surrogate: Option<Surrogate>,
    ) -> Result<StoredVersion, ColumnarError> {
        let sys_from = self
            .sys_from_at(location)
            .ok_or_else(|| ColumnarError::Corruption {
                segment_id: Some(location.segment_id.to_string()),
                reason: format!("row {} has no sys_from stamp", location.row_index),
                offset: None,
            })?;
        Ok(StoredVersion {
            values,
            surrogate,
            sys_from,
            sys_to: self.sys_to_at(location),
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

    use super::*;
    use crate::delete_bitmap::DeleteBitmap;
    use crate::mutation::install::InstallRow;
    use crate::mutation::snapshot::ColumnarEngineSnapshot;
    use crate::pk_index::encode_pk;
    use crate::writer::{PROFILE_PLAIN, SegmentWriter};

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
        ])
        .expect("valid")
    }

    fn row(id: i64, name: &str) -> Vec<Value> {
        vec![Value::Integer(id), Value::String(name.into())]
    }

    /// Flush the memtable to a segment blob and its surrogate sidecar.
    fn flush(engine: &mut MutationEngine) -> (Vec<u8>, Vec<Option<Surrogate>>) {
        let surrogates = engine.memtable_surrogates().to_vec();
        let segment_id = engine.next_segment_id();
        let (schema, columns, row_count) = engine.memtable_mut().drain();
        let blob = SegmentWriter::new(PROFILE_PLAIN, crate::test_support::test_memory())
            .write_segment(&schema, &columns, row_count, None)
            .expect("write");
        engine.on_memtable_flushed(segment_id).expect("flush");
        (blob, surrogates)
    }

    /// The rows a read at `cut` sees, flushed and memtable, in a fixed order.
    fn rows_at(engine: &MutationEngine, blobs: &[Vec<u8>], cut: VersionCut) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        for (idx, blob) in blobs.iter().enumerate() {
            let segment_id = idx as u64 + 1;
            let decoded =
                materialize_segment_rows(blob, None, engine.schema(), &DeleteBitmap::new(), &[])
                    .expect("decode");
            for r in decoded {
                if engine.row_visible(segment_id, r.row, cut) {
                    rows.push(r.values);
                }
            }
        }
        for r in engine.scan_memtable_versions(cut) {
            rows.push(r.expect("read").2);
        }
        rows.sort_by_key(|r| format!("{r:?}"));
        rows
    }

    /// A backup of an engine with flushed history, purged rows and memtable
    /// rows round-trips through its snapshot, the reissued versions, and an
    /// install into an empty engine: every cut reads as before the backup.
    #[test]
    fn a_reissued_backup_restores_every_version() {
        let mut live = MutationEngine::new("t".into(), schema());
        live.insert_with_surrogate(&row(1, "a"), Surrogate(1), 10)
            .expect("put");
        live.insert_with_surrogate(&row(2, "x"), Surrogate(2), 15)
            .expect("put");
        live.update(&Value::Integer(1), &row(1, "b"), None, 20)
            .expect("overwrite");
        let (blob, surrogates) = flush(&mut live);
        live.delete(&Value::Integer(2), 25).expect("delete");
        live.update(&Value::Integer(1), &row(1, "c"), Some(Surrogate(1)), 30)
            .expect("overwrite");
        live.insert_with_surrogate(&row(3, "z"), Surrogate(3), 35)
            .expect("put");
        live.delete(&Value::Integer(3), 40).expect("delete");
        // The first version of key 1 ended at 20: the purge removes it.
        assert!(live.purge_versions_below(21) > 0);

        let blobs = vec![blob];
        let snap = live
            .export_snapshot(&blobs, std::slice::from_ref(&surrogates))
            .expect("export");
        let bytes = zerompk::to_msgpack_vec(&snap).expect("encode");
        let snap: ColumnarEngineSnapshot = zerompk::from_msgpack(&bytes).expect("decode");
        let (backup, backup_blobs, backup_surrogates) =
            MutationEngine::from_snapshot(snap).expect("from snapshot");
        let versions = backup
            .stored_versions(&backup_blobs, &backup_surrogates, None)
            .expect("versions");
        assert!(
            versions.windows(2).all(|w| w[0].sys_from <= w[1].sys_from),
            "ascending by sys_from"
        );
        assert!(
            !versions.iter().any(|v| v.values == row(1, "a")),
            "a purged version is not reissued"
        );

        let mut restored = MutationEngine::new("t".into(), schema());
        restored
            .install_versions(versions.iter().map(|v| InstallRow {
                values: &v.values,
                surrogate: v.surrogate,
                sys_from: v.sys_from,
                sys_to: v.sys_to,
                clone_copy: false,
            }))
            .expect("install");

        for at in [9, 10, 15, 19, 20, 24, 25, 30, 35, 39, 40, 50] {
            assert_eq!(
                rows_at(&restored, &[], VersionCut::At(at)),
                rows_at(&live, &blobs, VersionCut::At(at)),
                "cut {at}"
            );
        }
        assert_eq!(
            rows_at(&restored, &[], VersionCut::Current),
            vec![row(1, "c")]
        );
        assert_eq!(
            rows_at(&restored, &[], VersionCut::All),
            rows_at(&live, &blobs, VersionCut::All)
        );
        let bound = restored
            .pk_index()
            .get(&encode_pk(&Value::Integer(1)))
            .copied()
            .expect("bound");
        assert_eq!(restored.sys_from_at(bound), Some(30));
        assert_eq!(
            restored
                .memtable_surrogates()
                .get(bound.row_index as usize)
                .copied()
                .flatten(),
            Some(Surrogate(1))
        );
        let reissued: Vec<StoredVersion> = restored
            .stored_versions(&[], &[], None)
            .expect("versions")
            .into_iter()
            .map(|v| StoredVersion {
                surrogate: None,
                ..v
            })
            .collect();
        let original: Vec<StoredVersion> = versions
            .into_iter()
            .map(|v| StoredVersion {
                surrogate: None,
                ..v
            })
            .collect();
        assert_eq!(
            reissued, original,
            "a second backup reissues the same versions"
        );
    }
}
