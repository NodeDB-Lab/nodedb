use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tracing::info;

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

    /// Open or create a segmented WAL at the given path.
    ///
    /// The `path` argument is treated as the WAL directory for the segmented format.
    /// If a legacy single-file WAL exists at this path, it is automatically migrated
    /// to the segmented format (one-time, transparent).
    pub fn open(path: &Path, use_direct_io: bool) -> crate::Result<Self> {
        // Determine the WAL directory.
        // If `path` is a file (legacy WAL), migrate it to a directory.
        // If `path` is a directory or doesn't exist, use it directly.
        let wal_dir = if path.is_file() {
            // Legacy single-file WAL detected. Migrate to segmented format.
            // Use path's parent + "wal_segments" as the new directory,
            // or just append "_segments" to the legacy path.
            let dir = path.with_extension("d");
            nodedb_wal::segment::migrate_legacy_wal(path, &dir)
                .map_err(crate::Error::Wal)?;
            dir
        } else {
            path.to_path_buf()
        };

        let config = SegmentedWalConfig {
            wal_dir: wal_dir.clone(),
            segment_target_size: nodedb_wal::segment::DEFAULT_SEGMENT_TARGET_SIZE,
            writer_config: WalWriterConfig {
                use_direct_io,
                ..Default::default()
            },
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

    /// Append a Put record to the WAL. Returns the assigned LSN.
    pub fn append_put(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                RecordType::Put as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    /// Append a Delete record to the WAL. Returns the assigned LSN.
    pub fn append_delete(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                RecordType::Delete as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    /// Append a Vector insert record. Returns the assigned LSN.
    pub fn append_vector_put(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                RecordType::VectorPut as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    /// Append a Vector delete record. Returns the assigned LSN.
    pub fn append_vector_delete(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                RecordType::VectorDelete as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    /// Append a Vector params record. Returns the assigned LSN.
    pub fn append_vector_params(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                RecordType::VectorParams as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    /// Append an atomic transaction record wrapping multiple sub-records.
    ///
    /// All sub-records share one CRC boundary and one fsync. On replay,
    /// either all sub-records apply or none.
    ///
    /// Payload: MessagePack-encoded `Vec<(u16, Vec<u8>)>` — each tuple is
    /// (record_type, sub-payload).
    pub fn append_transaction(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                RecordType::Transaction as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    /// Append a CRDT delta record. Returns the assigned LSN.
    pub fn append_crdt_delta(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        delta: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                RecordType::CrdtDelta as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                delta,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    /// Append a checkpoint marker to the WAL.
    ///
    /// Records the LSN at which a consistent checkpoint was taken.
    /// WAL segments before this LSN are safe to truncate after all
    /// engines have confirmed their dirty pages are flushed.
    pub fn append_checkpoint(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        checkpoint_lsn: u64,
    ) -> crate::Result<Lsn> {
        let payload =
            rmp_serde::to_vec(&checkpoint_lsn).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("checkpoint: {e}"),
            })?;
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                RecordType::Checkpoint as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                &payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
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
        let records = nodedb_wal::segmented::replay_all_segments(&self.wal_dir)
            .map_err(crate::Error::Wal)?;
        info!(records = records.len(), "WAL replay complete");
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
            wal.append_put(t, v, format!("val-{i}").as_bytes())
                .unwrap();
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
