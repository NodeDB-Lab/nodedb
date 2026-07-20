// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

#[cfg(test)]
use crate::types::{DatabaseId, TenantId};

use crate::wal::replay::SyncHwmReplayMaps;

use crate::types::Lsn;

use super::CoreLoop;

impl CoreLoop {
    pub fn core_id(&self) -> usize {
        self.core_id
    }

    pub fn pending_count(&self) -> usize {
        self.task_queue.len()
    }

    pub fn advance_watermark(&mut self, lsn: Lsn) {
        self.watermark = lsn;
    }

    /// Merge reconstructed sync HWM state from WAL replay into this core's gate.
    ///
    /// Called by `replay_all_wal` with the maps built by
    /// [`crate::wal::replay::replay_sync_hwm_records`], after
    /// `load_sync_hwm_checkpoint` has restored the gate from disk.
    ///
    /// Folds max-wins over the CURRENT maps rather than assigning them. The
    /// distinction is the whole reason the restore survives: the WAL below the
    /// checkpoint's LSN is deleted, so replay only ever sees the records ABOVE
    /// it, and assigning would discard every high-watermark that only the
    /// checkpoint still knows — resetting the gate to zero and re-admitting
    /// frames the producer had already had applied. Max-wins is also exactly the
    /// rule that built these maps
    /// ([`crate::wal::replay::sync_hwm::apply_sync_seq_advance`]), so a record already
    /// folded into the restored state re-folds to the same value and needs no
    /// replay floor.
    pub fn install_sync_hwm_maps(&mut self, maps: SyncHwmReplayMaps) {
        for (key, seq) in maps.sync_hwm {
            let entry = self.sync_hwm.entry(key).or_insert(0);
            *entry = (*entry).max(seq);
        }
        for (producer_id, epoch) in maps.producer_epoch_floor {
            let entry = self.producer_epoch_floor.entry(producer_id).or_insert(0);
            *entry = (*entry).max(epoch);
        }
    }

    /// Install the shared scan-quiesce registry. Called once by the
    /// server bootstrap in `main.rs` after `SharedState::open`.
    pub fn set_quiesce(
        &mut self,
        quiesce: std::sync::Arc<crate::bridge::quiesce::CollectionQuiesce>,
    ) {
        self.quiesce = Some(quiesce);
    }

    /// Acquire a scan guard for `(tid, collection)`. Returns `Ok(None)`
    /// if no quiesce registry is installed (e.g. in tests) — callers
    /// treat that as "scan unconditionally". Returns `Err(Response)`
    /// carrying a `NodeDbError::collection_draining` error code when
    /// a drain is in progress against the collection.
    pub(in crate::data::executor) fn acquire_scan_guard(
        &self,
        task: &crate::data::executor::task::ExecutionTask,
        tid: u64,
        collection: &str,
    ) -> Result<Option<crate::bridge::quiesce::ScanGuard>, crate::bridge::envelope::Response> {
        let Some(q) = self.quiesce.as_ref() else {
            return Ok(None);
        };
        match q.try_start_scan(task.request.database_id.as_u64(), tid, collection) {
            Ok(g) => Ok(Some(g)),
            Err(_) => Err(self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::CollectionDraining {
                    collection: collection.to_string(),
                },
            )),
        }
    }

    /// Install the encryption key used for vector checkpoint at-rest encryption.
    ///
    /// Called by the server bootstrap after opening the WAL key. When set,
    /// `checkpoint_vector_indexes` encrypts checkpoint files and
    /// `load_vector_checkpoints` refuses plaintext ones.
    pub fn set_vector_checkpoint_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.segment_keks.vector_checkpoint_kek = Some(kek);
    }

    /// Install the encryption key used for spatial checkpoint at-rest encryption.
    ///
    /// When set, `checkpoint_spatial_indexes` encrypts checkpoint files and
    /// `load_spatial_checkpoints` refuses plaintext ones.
    pub fn set_spatial_checkpoint_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.segment_keks.spatial_checkpoint_kek = Some(kek);
    }

    /// Install the encryption key used for columnar segment at-rest encryption.
    ///
    /// When set, columnar segment flushes produce AES-256-GCM encrypted SEGC
    /// envelopes and the segment reader refuses to load plaintext segments.
    pub fn set_columnar_segment_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.segment_keks.columnar_segment_kek = Some(kek);
    }

    /// Install the encryption key used for array segment at-rest encryption.
    ///
    /// When set, array segment flushes produce AES-256-GCM encrypted SEGA
    /// envelopes and the segment handle refuses to load plaintext segments.
    pub fn set_array_segment_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.array_engine.set_kek(kek.clone());
        self.segment_keks.array_segment_kek = Some(kek);
    }

    /// Returns the current SPSC drain batch size.
    ///
    /// Useful for observability and integration-level pressure tests that
    /// verify the governor correctly throttles the read depth.
    pub fn spsc_read_depth(&self) -> usize {
        self.spsc_read_depth
    }

    /// Returns whether new SPSC reads are suspended due to Emergency pressure.
    ///
    /// Useful for observability and integration-level pressure tests that
    /// verify the governor correctly gates the drain path.
    pub fn pressure_suspend_reads(&self) -> bool {
        self.pressure_suspend_reads
    }

    /// Returns the configured baseline SPSC drain depth (the value restored
    /// after pressure normalizes).  Exposed so integration tests can assert
    /// throttled depths relative to the baseline without hard-coding the value.
    pub fn spsc_read_depth_normal() -> usize {
        crate::data::executor::core_loop::pressure::SPSC_READ_DEPTH_NORMAL
    }

    /// Install the encryption key for timeseries columnar segment files.
    ///
    /// When set, `flush_ts_collection` wraps each output file in a `SEGT`
    /// AES-256-GCM envelope and readers refuse to load plaintext segment files.
    pub fn set_ts_segment_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.segment_keks.ts_segment_kek = Some(kek);
    }

    /// Install the shared quarantine registry.
    ///
    /// Called once by the server bootstrap after `SharedState::open`.
    pub fn set_quarantine_registry(
        &mut self,
        registry: std::sync::Arc<crate::storage::quarantine::QuarantineRegistry>,
    ) {
        self.inverted.set_quarantine_registry(Arc::clone(&registry));
        self.quarantine_registry = Some(registry);
    }

    /// Set the last timeseries ingest timestamp (for testing idle flush).
    pub fn set_last_ts_ingest(&mut self, value: Option<std::time::Instant>) {
        self.last_ts_ingest = value;
    }

    /// Test accessor: schema version for a strict-mode collection in `doc_configs`.
    ///
    /// Returns `None` if the collection is not registered on this core or is not
    /// in strict (Binary Tuple) storage mode.  Used by schema-visibility barrier
    /// integration tests to confirm every core has applied a schema ALTER.
    #[cfg(test)]
    pub fn schema_version_for_collection(
        &self,
        database_id: DatabaseId,
        tid: u64,
        collection: &str,
    ) -> Option<u32> {
        let key = (database_id, TenantId::new(tid), collection.to_string());
        let config = self.doc_configs.get(&key)?;
        match &config.storage_mode {
            nodedb_physical::physical_plan::StorageMode::Strict { schema } => Some(schema.version),
            nodedb_physical::physical_plan::StorageMode::Schemaless => None,
        }
    }

    /// Test accessor: row count in a columnar memtable.
    #[cfg(test)]
    pub fn columnar_memtable_row_count(&self, database_id: u64, tid: u64, collection: &str) -> u64 {
        let key = (
            nodedb_types::DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        self.columnar_memtables
            .get(&key)
            .map(|mt| mt.row_count())
            .unwrap_or(0)
    }

    /// Test accessor: total row count across all partitions in a timeseries registry.
    #[cfg(test)]
    pub fn ts_registry_row_count(&self, database_id: u64, tid: u64, collection: &str) -> u64 {
        let key = (
            nodedb_types::DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        self.ts_registries
            .get(&key)
            .map(|reg| {
                let range = nodedb_types::timeseries::TimeRange::new(0, i64::MAX);
                reg.query_partitions(&range)
                    .iter()
                    .map(|e| e.meta.row_count)
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Resolve the wall-clock instant (ms since epoch) for a TTL-bearing KV
    /// write's `expire_at_ms`. Precedence: the Control-Plane-resolved instant
    /// carried on `task` (so the durable WAL record and this live apply
    /// install the identical `expire_at_ms`) wins first; Calvin's
    /// `epoch_system_ms` is the deterministic-replay fallback (unchanged from
    /// before `resolved_now_ms` existed); a fresh wall-clock read is the last
    /// resort. Shared by every KV write handler that derives a TTL instant
    /// from a task carrying a Control-Plane-resolved one (`Put`, `Insert`,
    /// `InsertIfAbsent`, `InsertOnConflictUpdate`, `BatchPut`).
    pub(in crate::data::executor) fn kv_ttl_now_ms(
        &self,
        task: &crate::data::executor::task::ExecutionTask,
    ) -> u64 {
        task.resolved_now_ms()
            .or_else(|| self.epoch_system_ms.map(|ms| ms as u64))
            .unwrap_or_else(crate::engine::kv::current_ms)
    }

    /// Write a raw segment blob directly into the FTS LSM segment store for
    /// a given `(tenant, collection)`.
    ///
    /// This bypasses the memtable flush path and is intended for maintenance
    /// tests and bootstrapping code that need to pre-populate a known number
    /// of L0 segments without indexing documents. In production code,
    /// segments are written automatically when the FTS memtable crosses its
    /// flush threshold.
    pub fn fts_write_segment(
        &self,
        database_id: u64,
        tenant: crate::types::TenantId,
        collection: &str,
        segment_id: &str,
        data: &[u8],
    ) -> crate::Result<()> {
        use nodedb_fts::backend::FtsBackend;
        self.inverted.backend().write_segment(
            database_id,
            tenant.as_u64(),
            collection,
            segment_id,
            data,
        )
    }

    /// Return the list of FTS LSM segment IDs for a
    /// `(database, tenant, collection)`.
    ///
    /// Used by maintenance tests to verify that compaction reduced the
    /// segment count at a given level.
    pub fn fts_list_segments(
        &self,
        database_id: u64,
        tenant: crate::types::TenantId,
        collection: &str,
    ) -> crate::Result<Vec<String>> {
        use nodedb_fts::backend::FtsBackend;
        self.inverted
            .backend()
            .list_segments(database_id, tenant.as_u64(), collection)
    }
}
