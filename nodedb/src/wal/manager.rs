use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tracing::info;

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;
use nodedb_wal::writer::{WalWriter, WalWriterConfig};

use crate::types::{Lsn, TenantId, VShardId};

/// WAL manager: owns the writer and coordinates appends + sync.
///
/// The WAL is the single source of truth for durability. Every mutation
/// goes through here before being applied to any engine's in-memory state.
///
/// Thread-safety: the writer is behind a `Mutex` because multiple Control
/// Plane tasks may submit WAL appends concurrently. The mutex serializes
/// writes, which is correct — WAL appends must be ordered anyway. The
/// `sync()` call (fsync) is the expensive part and is batched via group commit.
pub struct WalManager {
    writer: Mutex<WalWriter>,
    wal_path: PathBuf,
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
            let mut writer = mgr.writer.lock().unwrap_or_else(|p| p.into_inner());
            writer.set_encryption_ring(ring.clone());
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
            let mut writer = mgr.writer.lock().unwrap_or_else(|p| p.into_inner());
            writer.set_encryption_ring(ring.clone());
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

        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let new_ring = if let Some(ring) = writer.encryption_ring() {
            // Current key becomes previous; new key becomes current.
            nodedb_wal::crypto::KeyRing::with_previous(new_key, ring.current().clone())
        } else {
            // First key — no previous.
            nodedb_wal::crypto::KeyRing::new(new_key)
        };

        writer.set_encryption_ring(new_ring);
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

    /// Open or create a WAL at the given path.
    pub fn open(path: &Path, use_direct_io: bool) -> crate::Result<Self> {
        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let config = WalWriterConfig {
            use_direct_io,
            ..Default::default()
        };

        let writer = WalWriter::open(path, config).map_err(crate::Error::Wal)?;

        info!(
            path = %path.display(),
            next_lsn = writer.next_lsn(),
            "WAL opened"
        );

        Ok(Self {
            writer: Mutex::new(writer),
            wal_path: path.to_path_buf(),
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
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = writer
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
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = writer
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
    ///
    /// Payload format: `[collection_len: 4B LE][collection: N bytes][dim: 4B LE][vector: dim*4 bytes f32 LE]`
    pub fn append_vector_put(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = writer
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
    ///
    /// Payload format: `[collection_len: 4B LE][collection: N bytes][vector_id: 4B LE]`
    pub fn append_vector_delete(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = writer
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
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = writer
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
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = writer
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
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = writer
            .append(
                RecordType::CrdtDelta as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                delta,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    /// Flush all buffered records to disk (group commit / fsync).
    /// Append a checkpoint marker to the WAL.
    ///
    /// Records the LSN at which a consistent checkpoint was taken.
    /// WAL entries before this LSN are safe to truncate after all
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
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = writer
            .append(
                nodedb_wal::record::RecordType::Checkpoint as u16,
                tenant_id.as_u32(),
                vshard_id.as_u16(),
                &payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    /// Truncate the WAL up to (but not including) the given LSN.
    ///
    /// Safe to call only after a checkpoint has been confirmed —
    /// all engines have flushed their dirty pages, and the checkpoint
    /// LSN has been persisted.
    ///
    /// Currently a no-op placeholder: actual truncation requires
    /// segment-level WAL file management (deleting old segment files
    /// while the writer continues in the active segment).
    pub fn truncate_before(&self, _checkpoint_lsn: Lsn) -> crate::Result<()> {
        // TODO: implement WAL segment file deletion.
        // For now, the WAL grows unbounded. Checkpointing reduces
        // the amount of replay needed on crash recovery even without
        // truncation — WAL replay starts from the checkpoint LSN
        // instead of the beginning.
        tracing::debug!(
            _checkpoint_lsn = _checkpoint_lsn.as_u64(),
            "WAL truncation requested (not yet implemented)"
        );
        Ok(())
    }

    pub fn sync(&self) -> crate::Result<()> {
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        writer.sync().map_err(crate::Error::Wal)
    }

    /// Next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        let writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        Lsn::new(writer.next_lsn())
    }

    /// Replay all committed records from the WAL.
    ///
    /// Returns records in LSN order. Used during crash recovery.
    pub fn replay(&self) -> crate::Result<Vec<WalRecord>> {
        let reader =
            nodedb_wal::reader::WalReader::open(&self.wal_path).map_err(crate::Error::Wal)?;
        let records: Vec<_> = reader
            .records()
            .collect::<nodedb_wal::Result<_>>()
            .map_err(crate::Error::Wal)?;

        info!(records = records.len(), "WAL replay complete");
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

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
        let path = dir.path().join("crdt.wal");

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
        let path = dir.path().join("reopen.wal");

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
}
