// SPDX-License-Identifier: BUSL-1.1

//! Watermark store: persists last-processed LSN per core to redb.
//!
//! The Event Plane tracks how far each core's consumer has progressed.
//! On restart, the consumer resumes from its persisted watermark —
//! replaying WAL entries from that LSN forward to reconstruct missed events.
//!
//! Stored in a dedicated redb file at `{data_dir}/event_plane/watermarks.redb`.

use std::path::{Path, PathBuf};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::debug;

use crate::types::Lsn;

/// redb table: core_id (as string) → last-processed LSN (as 8-byte LE u64).
const WATERMARKS: TableDefinition<&str, &[u8]> = TableDefinition::new("event_watermarks");

/// Persists Event Plane watermarks (last-processed LSN per core).
pub struct WatermarkStore {
    db: Database,
    path: PathBuf,
}

impl WatermarkStore {
    /// Open or create the watermark store at `{data_dir}/event_plane/watermarks.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;

        let path = dir.join("watermarks.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("open watermark db {}: {e}", path.display()),
        })?;

        // Ensure table exists.
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            txn.open_table(WATERMARKS)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        debug!(path = %path.display(), "watermark store opened");
        Ok(Self { db, path })
    }

    /// The directory holding this store: `{data_dir}/event_plane`. The Event
    /// Plane's other durable stores live beside it.
    pub fn dir(&self) -> &Path {
        self.path.parent().unwrap_or(Path::new("."))
    }

    /// Load the last-processed LSN for a given core. Returns `Lsn::ZERO` if
    /// no watermark has been persisted yet (first startup).
    pub fn load(&self, core_id: usize) -> crate::Result<Lsn> {
        let key = core_key(core_id);
        let txn = self.db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_read: {e}"),
        })?;
        let table = txn
            .open_table(WATERMARKS)
            .map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("open_table: {e}"),
            })?;

        match table.get(key.as_str()) {
            Ok(Some(guard)) => {
                let bytes = guard.value();
                if bytes.len() == 8 {
                    let arr: [u8; 8] = [
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ];
                    Ok(Lsn::new(u64::from_le_bytes(arr)))
                } else {
                    Ok(Lsn::ZERO)
                }
            }
            Ok(None) => Ok(Lsn::ZERO),
            Err(e) => Err(crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("get watermark core {core_id}: {e}"),
            }),
        }
    }

    /// Persist the last-processed LSN for a given core.
    pub fn save(&self, core_id: usize, lsn: Lsn) -> crate::Result<()> {
        let key = core_key(core_id);
        let value = lsn.as_u64().to_le_bytes();

        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(WATERMARKS)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            table
                .insert(key.as_str(), value.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("insert watermark core {core_id}: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit watermark core {core_id}: {e}"),
        })?;

        Ok(())
    }

    /// Load watermarks for all cores (0..num_cores).
    pub fn load_all(&self, num_cores: usize) -> crate::Result<Vec<Lsn>> {
        let mut watermarks = Vec::with_capacity(num_cores);
        for core_id in 0..num_cores {
            watermarks.push(self.load(core_id)?);
        }
        Ok(watermarks)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Build the redb key for a core. Using a short string key is simpler
/// and more debuggable than a raw byte key.
fn core_key(core_id: usize) -> String {
    format!("core:{core_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = WatermarkStore::open(dir.path()).unwrap();

        // First load returns ZERO.
        assert_eq!(store.load(0).unwrap(), Lsn::ZERO);
        assert_eq!(store.load(1).unwrap(), Lsn::ZERO);

        // Save and reload.
        store.save(0, Lsn::new(100)).unwrap();
        store.save(1, Lsn::new(200)).unwrap();
        assert_eq!(store.load(0).unwrap(), Lsn::new(100));
        assert_eq!(store.load(1).unwrap(), Lsn::new(200));

        // Overwrite.
        store.save(0, Lsn::new(500)).unwrap();
        assert_eq!(store.load(0).unwrap(), Lsn::new(500));
    }

    #[test]
    fn load_all_cores() {
        let dir = tempfile::tempdir().unwrap();
        let store = WatermarkStore::open(dir.path()).unwrap();
        store.save(0, Lsn::new(10)).unwrap();
        store.save(1, Lsn::new(20)).unwrap();

        let all = store.load_all(3).unwrap();
        assert_eq!(all, vec![Lsn::new(10), Lsn::new(20), Lsn::ZERO]);
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let store = WatermarkStore::open(dir.path()).unwrap();
            store.save(0, Lsn::new(42)).unwrap();
        }

        // Reopen from same path.
        let store = WatermarkStore::open(dir.path()).unwrap();
        assert_eq!(store.load(0).unwrap(), Lsn::new(42));
    }
}
