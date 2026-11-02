// SPDX-License-Identifier: BUSL-1.1

//! The partition ends a timeseries `TRUNCATE` records.
//!
//! A TRUNCATE at ordinal `C` ends every current partition at `C`: its
//! `partition.meta` carries `ended_at = Some(C)`. Each rewrite of a meta is
//! one atomic rename. A truncate that fails part way puts back every meta it
//! changed, so the collection is either wholly ended at `C` or not at all.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::stamp::TsCollectionKey;
use crate::engine::timeseries::columnar_segment::write_partition_meta;
use crate::engine::timeseries::partition_registry::PartitionKey;

use super::paths::ts_collection_dir;

/// One partition a TRUNCATE ended, as its undo reverses it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) struct EndedPartition {
    /// The registry key of the partition.
    pub key: PartitionKey,
    /// The partition's directory name.
    pub dir_name: String,
}

impl CoreLoop {
    /// End every current partition of `key` at `ordinal`, on disk and in the
    /// registry. Returns the partitions ended and the rows they hold.
    ///
    /// `Err` when a meta does not write. Every meta written before the error
    /// is put back first. When that fails too, the error names both, and a
    /// restart replays the truncate whole.
    pub(in crate::data::executor) fn end_ts_partitions(
        &mut self,
        key: &TsCollectionKey,
        ordinal: i64,
    ) -> crate::Result<(Vec<EndedPartition>, u64)> {
        let current: Vec<(PartitionKey, String, u64)> = self
            .ts_registries
            .get(key)
            .map(|registry| {
                registry
                    .iter()
                    .filter(|(_, entry)| {
                        entry.meta.state != nodedb_types::timeseries::PartitionState::Deleted
                            && entry.meta.ended_at.is_none()
                    })
                    .map(|(partition_key, entry)| {
                        (
                            partition_key.clone(),
                            entry.dir_name.clone(),
                            entry.meta.row_count,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut ended = Vec::with_capacity(current.len());
        let mut rows = 0u64;
        for (partition_key, dir_name, row_count) in current {
            let partition = EndedPartition {
                key: partition_key,
                dir_name,
            };
            if let Err(e) = self.set_ts_partition_end(key, &partition, Some(ordinal)) {
                return Err(self.put_back_ends(key, &ended, e));
            }
            rows = rows.saturating_add(row_count);
            ended.push(partition);
        }
        Ok((ended, rows))
    }

    /// Clear the end of every partition in `ended`: the undo of
    /// [`Self::end_ts_partitions`]. Stops at the first meta that does not
    /// write.
    pub(in crate::data::executor) fn reopen_ts_partitions(
        &mut self,
        key: &TsCollectionKey,
        ended: &[EndedPartition],
    ) -> crate::Result<()> {
        for partition in ended {
            self.set_ts_partition_end(key, partition, None)?;
        }
        Ok(())
    }

    /// Put back the partitions a failed truncate ended, and return the error
    /// the truncate reports.
    fn put_back_ends(
        &mut self,
        key: &TsCollectionKey,
        ended: &[EndedPartition],
        cause: crate::Error,
    ) -> crate::Error {
        match self.reopen_ts_partitions(key, ended) {
            Ok(()) => cause,
            Err(put_back) => {
                tracing::error!(
                    core = self.core_id,
                    collection = %key.2,
                    error = %cause,
                    put_back_error = %put_back,
                    "timeseries truncate failed and left some partitions ended; a restart \
                     replays the truncate"
                );
                crate::Error::Storage {
                    engine: "timeseries".into(),
                    detail: format!(
                        "truncate of '{}' failed ({cause}) and putting back its partition \
                         ends failed ({put_back}); a restart replays the truncate",
                        key.2
                    ),
                }
            }
        }
    }

    /// Write `end` into the meta of `partition`, on disk then in the
    /// registry.
    fn set_ts_partition_end(
        &mut self,
        key: &TsCollectionKey,
        partition: &EndedPartition,
        end: Option<i64>,
    ) -> crate::Result<()> {
        let (database_id, tenant_id, collection) = key;
        let dir = ts_collection_dir(
            &self.data_dir,
            database_id.as_u64(),
            tenant_id.as_u64(),
            collection,
        )
        .join(&partition.dir_name);
        let registry = self
            .ts_registries
            .get_mut(key)
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "timeseries '{collection}': partition {} has no registry",
                    partition.dir_name
                ),
            })?;
        let entry = registry
            .get_mut(&partition.key)
            .filter(|entry| entry.dir_name == partition.dir_name)
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "timeseries '{collection}': the registry no longer names partition {}",
                    partition.dir_name
                ),
            })?;
        let mut meta = entry.meta.clone();
        meta.ended_at = end;
        write_partition_meta(&dir, &meta, self.segment_keks.ts_segment_kek.as_ref()).map_err(
            |e| crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!("write the end of partition {}: {e}", dir.display()),
            },
        )?;
        entry.meta.ended_at = end;
        Ok(())
    }
}
