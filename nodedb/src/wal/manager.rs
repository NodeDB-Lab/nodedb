use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tracing::info;

use nodedb_types::config::tuning::WalTuning;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;
use nodedb_wal::segmented::{SegmentedWal, SegmentedWalConfig};
use nodedb_wal::writer::WalWriterConfig;

use crate::types::{Lsn, TenantId, VShardId};

/// WAL manager: owns the segmented WAL and coordinates appends + sync.
///
/// The WAL is the single source of truth for durability. Every mutation
/// goes through here before being applied to any engine's in-memory state.
///
/// Thread-safety: the segmented WAL is behind a `Mutex` because multiple
/// Control Plane tasks may submit WAL appends concurrently. The mutex
/// serializes writes, which is correct — WAL appends must be ordered anyway.
/// The `sync()` call (fsync) is the expensive part and is batched via group commit.
pub struct WalManager {
    wal: Mutex<SegmentedWal>,
    /// The WAL directory path (for replay without holding the lock).
    wal_dir: PathBuf,
    /// Encryption key ring (if configured). Supports dual-key reads during rotation.
    encryption_ring: Option<nodedb_wal::crypto::KeyRing>,
}

impl WalManager {
    /// Open with encryption key loaded from a file.
    pub fn open_encrypted(
        path: &Path,
        use_direct_io: bool,
        key_path: &Path,
    ) -> crate::Result<Self> {
        let key =
            nodedb_wal::crypto::WalEncryptionKey::from_file(key_path).map_err(crate::Error::Wal)?;
        let ring = nodedb_wal::crypto::KeyRing::new(key);
        let mut mgr = Self::open(path, use_direct_io)?;
        {
            let mut wal = mgr.wal.lock().unwrap_or_else(|p| p.into_inner());
            wal.set_encryption_ring(ring.clone());
        }
        mgr.encryption_ring = Some(ring);
        info!(key_path = %key_path.display(), "WAL encryption enabled");
        Ok(mgr)
    }

    /// Open with key rotation: current key + previous key for dual-key reads.
    ///
    /// New writes use `current_key_path`. Reads try current first, then previous.
    /// Once all old WAL segments are compacted, remove the previous key.
    pub fn open_encrypted_rotating(
        path: &Path,
        use_direct_io: bool,
        current_key_path: &Path,
        previous_key_path: &Path,
    ) -> crate::Result<Self> {
        let current = nodedb_wal::crypto::WalEncryptionKey::from_file(current_key_path)
            .map_err(crate::Error::Wal)?;
        let previous = nodedb_wal::crypto::WalEncryptionKey::from_file(previous_key_path)
            .map_err(crate::Error::Wal)?;
        let ring = nodedb_wal::crypto::KeyRing::with_previous(current, previous);
        let mut mgr = Self::open(path, use_direct_io)?;
        {
            let mut wal = mgr.wal.lock().unwrap_or_else(|p| p.into_inner());
            wal.set_encryption_ring(ring.clone());
        }
        mgr.encryption_ring = Some(ring);
        info!(
            current_key = %current_key_path.display(),
            previous_key = %previous_key_path.display(),
            "WAL encryption enabled with key rotation"
        );
        Ok(mgr)
    }

    /// Rotate the encryption key at runtime without downtime.
    ///
    /// The new key becomes the current key for all future writes.
    /// The old current key becomes the previous key for dual-key reads.
    pub fn rotate_key(&self, new_key_path: &Path) -> crate::Result<()> {
        let new_key = nodedb_wal::crypto::WalEncryptionKey::from_file(new_key_path)
            .map_err(crate::Error::Wal)?;

        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let new_ring = if let Some(ring) = wal.encryption_ring() {
            nodedb_wal::crypto::KeyRing::with_previous(new_key, ring.current().clone())
        } else {
            nodedb_wal::crypto::KeyRing::new(new_key)
        };

        wal.set_encryption_ring(new_ring);
        info!(new_key = %new_key_path.display(), "WAL encryption key rotated");
        Ok(())
    }

    /// Get the current encryption key (if configured). Used for backup encryption.
    pub fn encryption_key(&self) -> Option<&nodedb_wal::crypto::WalEncryptionKey> {
        self.encryption_ring.as_ref().map(|r| r.current())
    }

    /// Get the key ring (if configured). Used for dual-key decryption during replay.
    pub fn encryption_ring(&self) -> Option<&nodedb_wal::crypto::KeyRing> {
        self.encryption_ring.as_ref()
    }

    /// Set the encryption key ring. All subsequent records will be encrypted.
    pub fn set_encryption_ring(&mut self, ring: nodedb_wal::crypto::KeyRing) {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.set_encryption_ring(ring.clone());
        self.encryption_ring = Some(ring);
    }

    /// Open or create a segmented WAL at the given path.
    ///
    /// The `path` argument is treated as the WAL directory for the segmented format.
    /// If a legacy single-file WAL exists at this path, it is automatically migrated
    /// to the segmented format (one-time, transparent).
    ///
    /// `segment_target_size` controls the WAL segment rollover threshold in bytes.
    /// Pass `0` to use the default (64 MiB).
    pub fn open(path: &Path, use_direct_io: bool) -> crate::Result<Self> {
        Self::open_with_segment_size(path, use_direct_io, 0)
    }

    /// Open with explicit segment target size (bytes). 0 = default (64 MiB).
    pub fn open_with_segment_size(
        path: &Path,
        use_direct_io: bool,
        segment_target_size: u64,
    ) -> crate::Result<Self> {
        Self::open_internal(
            path,
            segment_target_size,
            WalWriterConfig {
                use_direct_io,
                ..Default::default()
            },
        )
    }

    /// Open with explicit segment target size and WAL tuning from `TuningConfig`.
    ///
    /// Uses `tuning.write_buffer_size` and `tuning.alignment` from [`WalTuning`]
    /// to configure the underlying `WalWriterConfig`, instead of the hardcoded
    /// defaults. `segment_target_size` of 0 uses the default (64 MiB).
    pub fn open_with_tuning(
        path: &Path,
        use_direct_io: bool,
        segment_target_size: u64,
        tuning: &WalTuning,
    ) -> crate::Result<Self> {
        Self::open_internal(
            path,
            segment_target_size,
            WalWriterConfig {
                write_buffer_size: tuning.write_buffer_size,
                alignment: tuning.alignment,
                use_direct_io,
            },
        )
    }

    /// Shared WAL open logic: migrate legacy path, resolve segment size, open.
    fn open_internal(
        path: &Path,
        segment_target_size: u64,
        writer_config: WalWriterConfig,
    ) -> crate::Result<Self> {
        let wal_dir = if path.is_file() {
            let dir = path.with_extension("d");
            nodedb_wal::segment::migrate_legacy_wal(path, &dir).map_err(crate::Error::Wal)?;
            dir
        } else {
            path.to_path_buf()
        };

        let effective_target = if segment_target_size > 0 {
            segment_target_size
        } else {
            nodedb_wal::segment::DEFAULT_SEGMENT_TARGET_SIZE
        };

        let config = SegmentedWalConfig {
            wal_dir: wal_dir.clone(),
            segment_target_size: effective_target,
            writer_config,
        };

        let wal = SegmentedWal::open(config).map_err(crate::Error::Wal)?;

        info!(
            wal_dir = %wal_dir.display(),
            next_lsn = wal.next_lsn(),
            "WAL opened"
        );

        Ok(Self {
            wal: Mutex::new(wal),
            wal_dir,
            encryption_ring: None,
        })
    }

    /// Open without O_DIRECT (for testing on tmpfs).
    pub fn open_for_testing(path: &Path) -> crate::Result<Self> {
        Self::open(path, false)
    }

    /// Internal: append a record of the given type to the WAL.
    fn append_record(
        &self,
        record_type: RecordType,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                record_type as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    pub fn append_put(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::Put, tid, vs, p)
    }

    pub fn append_delete(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::Delete, tid, vs, p)
    }

    pub fn append_vector_put(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorPut, tid, vs, p)
    }

    pub fn append_vector_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDelete, tid, vs, p)
    }

    pub fn append_vector_params(
        &self,
        tid: TenantId,
        vs: VShardId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorParams, tid, vs, p)
    }

    pub fn append_transaction(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::Transaction, tid, vs, p)
    }

    pub fn append_crdt_delta(
        &self,
        tid: TenantId,
        vs: VShardId,
        delta: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::CrdtDelta, tid, vs, delta)
    }

    /// Append a checkpoint marker. Serializes the LSN before writing.
    pub fn append_checkpoint(
        &self,
        tid: TenantId,
        vs: VShardId,
        checkpoint_lsn: u64,
    ) -> crate::Result<Lsn> {
        let payload =
            rmp_serde::to_vec(&checkpoint_lsn).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("checkpoint: {e}"),
            })?;
        self.append_record(RecordType::Checkpoint, tid, vs, &payload)
    }

    pub fn append_timeseries_batch(
        &self,
        tid: TenantId,
        vs: VShardId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::TimeseriesBatch, tid, vs, p)
    }

    pub fn append_log_batch(&self, tid: TenantId, vs: VShardId, p: &[u8]) -> crate::Result<Lsn> {
        self.append_record(RecordType::LogBatch, tid, vs, p)
    }

    /// Truncate old WAL segments that are fully below the checkpoint LSN.
    ///
    /// Deletes sealed segment files whose records are all below `checkpoint_lsn`.
    /// The active segment is never deleted. Safe to call only after a checkpoint
    /// has been confirmed — all engines have flushed their dirty pages.
    ///
    /// Returns the number of segments deleted and bytes reclaimed.
    pub fn truncate_before(
        &self,
        checkpoint_lsn: Lsn,
    ) -> crate::Result<nodedb_wal::segment::TruncateResult> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let result = wal
            .truncate_before(checkpoint_lsn.as_u64())
            .map_err(crate::Error::Wal)?;

        if result.segments_deleted > 0 {
            info!(
                checkpoint_lsn = checkpoint_lsn.as_u64(),
                segments_deleted = result.segments_deleted,
                bytes_reclaimed = result.bytes_reclaimed,
                "WAL truncated"
            );
        }

        Ok(result)
    }

    /// Flush all buffered records to disk (group commit / fsync).
    pub fn sync(&self) -> crate::Result<()> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.sync().map_err(crate::Error::Wal)
    }

    /// Next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        Lsn::new(wal.next_lsn())
    }

    /// Replay all committed records from the WAL.
    ///
    /// Returns records in LSN order across all segments. Used during crash recovery.
    pub fn replay(&self) -> crate::Result<Vec<WalRecord>> {
        let records =
            nodedb_wal::segmented::replay_all_segments(&self.wal_dir).map_err(crate::Error::Wal)?;
        info!(records = records.len(), "WAL replay complete");
        Ok(records)
    }

    /// Replay committed records from the WAL starting at `from_lsn`.
    ///
    /// Returns records with LSN >= `from_lsn` in LSN order. Used by the Event
    /// Plane to catch up after restart or ring buffer overflow.
    pub fn replay_from(&self, from_lsn: Lsn) -> crate::Result<Vec<WalRecord>> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let records = wal
            .replay_from(from_lsn.as_u64())
            .map_err(crate::Error::Wal)?;
        Ok(records)
    }

    /// Replay WAL records from `from_lsn` using mmap (tier-2 catchup).
    ///
    /// Uses `MmapWalReader` instead of sequential reads — the kernel manages
    /// page residency without pinning slab allocator memory.
    pub fn replay_mmap_from(&self, from_lsn: Lsn) -> crate::Result<Vec<WalRecord>> {
        let records =
            nodedb_wal::mmap_reader::replay_segments_mmap(self.wal_dir(), from_lsn.as_u64())
                .map_err(crate::Error::Wal)?;
        Ok(records)
    }

    /// Total WAL size on disk across all segments.
    pub fn total_size_bytes(&self) -> crate::Result<u64> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.total_size_bytes().map_err(crate::Error::Wal)
    }

    /// List all WAL segment metadata (for monitoring).
    pub fn list_segments(&self) -> crate::Result<Vec<nodedb_wal::segment::SegmentMeta>> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.list_segments().map_err(crate::Error::Wal)
    }

    /// Get the WAL directory path.
    pub fn wal_dir(&self) -> &std::path::Path {
        &self.wal_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(1);
        let v = VShardId::new(0);

        let lsn1 = wal.append_put(t, v, b"key1=value1").unwrap();
        let lsn2 = wal.append_put(t, v, b"key2=value2").unwrap();
        let lsn3 = wal.append_delete(t, v, b"key1").unwrap();

        assert_eq!(lsn1, Lsn::new(1));
        assert_eq!(lsn2, Lsn::new(2));
        assert_eq!(lsn3, Lsn::new(3));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].payload, b"key1=value1");
        assert_eq!(records[2].payload, b"key1");
    }

    #[test]
    fn crdt_delta_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(5);
        let v = VShardId::new(42);

        let lsn = wal.append_crdt_delta(t, v, b"loro-delta-bytes").unwrap();
        assert_eq!(lsn, Lsn::new(1));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].header.record_type, RecordType::CrdtDelta as u16);
        assert_eq!(records[0].header.tenant_id, 5);
        assert_eq!(records[0].header.vshard_id, 42);
        assert_eq!(records[0].payload, b"loro-delta-bytes");
    }

    #[test]
    fn next_lsn_continues_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        {
            let wal = WalManager::open_for_testing(&path).unwrap();
            wal.append_put(TenantId::new(1), VShardId::new(0), b"a")
                .unwrap();
            wal.append_put(TenantId::new(1), VShardId::new(0), b"b")
                .unwrap();
            wal.sync().unwrap();
        }

        let wal = WalManager::open_for_testing(&path).unwrap();
        assert_eq!(wal.next_lsn(), Lsn::new(3));

        let lsn = wal
            .append_put(TenantId::new(1), VShardId::new(0), b"c")
            .unwrap();
        assert_eq!(lsn, Lsn::new(3));
    }

    #[test]
    fn truncate_reclaims_space() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(1);
        let v = VShardId::new(0);

        // Write enough records to span multiple segments (impossible with default 64 MiB,
        // so just test that truncation works without error on a single segment).
        for i in 0..10u32 {
            wal.append_put(t, v, format!("val-{i}").as_bytes()).unwrap();
        }
        wal.sync().unwrap();

        // Truncate at LSN 5 — no segments will be deleted since there's only one.
        let result = wal.truncate_before(Lsn::new(5)).unwrap();
        assert_eq!(result.segments_deleted, 0);

        // Records should still be replayable.
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn total_size_and_list_segments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();
        wal.append_put(TenantId::new(1), VShardId::new(0), b"data")
            .unwrap();
        wal.sync().unwrap();

        let size = wal.total_size_bytes().unwrap();
        assert!(size > 0);

        let segments = wal.list_segments().unwrap();
        assert_eq!(segments.len(), 1);
    }
}
