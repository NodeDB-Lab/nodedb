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
        let mgr = Self::open(path, use_direct_io)?;
        {
            let mut writer = mgr.writer.lock().unwrap();
            writer.set_encryption_key(key);
        }
        info!(key_path = %key_path.display(), "WAL encryption enabled");
        Ok(mgr)
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
        let mut writer = self.writer.lock().unwrap();
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
        let mut writer = self.writer.lock().unwrap();
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

    /// Append a CRDT delta record. Returns the assigned LSN.
    pub fn append_crdt_delta(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        delta: &[u8],
    ) -> crate::Result<Lsn> {
        let mut writer = self.writer.lock().unwrap();
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
    pub fn sync(&self) -> crate::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.sync().map_err(crate::Error::Wal)
    }

    /// Next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        let writer = self.writer.lock().unwrap();
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
