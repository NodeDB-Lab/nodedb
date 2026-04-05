//! Persistent Raft log storage backed by redb.
//!
//! Implements `nodedb_raft::LogStorage` with ACID durability via redb.
//! Each Raft group gets its own redb file at `{data_dir}/raft/group-{id}.redb`.
//!
//! Layout:
//! - `ENTRIES` table: key = log index (u64 big-endian), value = MessagePack LogEntry
//! - `META` table: key = name (&str), value = MessagePack-encoded metadata
//!   - "hard_state" → HardState { current_term, voted_for }
//!   - "snapshot_index" → u64
//!   - "snapshot_term" → u64

use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use tracing::{debug, info};

use nodedb_raft::message::LogEntry;
use nodedb_raft::state::HardState;
use nodedb_raft::storage::LogStorage;

/// Log entries: key = index (big-endian u64 for sorted iteration), value = MessagePack bytes.
const ENTRIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft.entries");

/// Metadata: key = name, value = MessagePack bytes.
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft.meta");

const KEY_HARD_STATE: &str = "hard_state";
const KEY_SNAPSHOT_INDEX: &str = "snapshot_index";
const KEY_SNAPSHOT_TERM: &str = "snapshot_term";

/// Persistent Raft log storage backed by redb.
pub struct RedbLogStorage {
    db: Database,
}

impl RedbLogStorage {
    /// Open or create storage at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| crate::ClusterError::Storage {
                detail: format!("create raft storage dir: {e}"),
            })?;
        }

        let db = Database::create(path).map_err(|e| crate::ClusterError::Storage {
            detail: format!("open raft storage: {e}"),
        })?;

        // Ensure tables exist.
        let write_txn = db.begin_write().map_err(|e| crate::ClusterError::Storage {
            detail: format!("init raft tables: {e}"),
        })?;
        {
            write_txn
                .open_table(ENTRIES)
                .map_err(|e| crate::ClusterError::Storage {
                    detail: format!("create entries table: {e}"),
                })?;
            write_txn
                .open_table(META)
                .map_err(|e| crate::ClusterError::Storage {
                    detail: format!("create meta table: {e}"),
                })?;
        }
        write_txn
            .commit()
            .map_err(|e| crate::ClusterError::Storage {
                detail: format!("commit raft init: {e}"),
            })?;

        info!(path = %path.display(), "raft log storage opened");

        Ok(Self { db })
    }
}

fn index_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

impl LogStorage for RedbLogStorage {
    fn append(&mut self, entries: &[LogEntry]) -> nodedb_raft::error::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let write_txn =
            self.db
                .begin_write()
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("write txn: {e}"),
                })?;
        {
            let mut table = write_txn.open_table(ENTRIES).map_err(|e| {
                nodedb_raft::error::RaftError::Storage {
                    detail: format!("open entries: {e}"),
                }
            })?;

            for entry in entries {
                let key = index_key(entry.index);
                let value = zerompk::to_msgpack_vec(entry).map_err(|e| {
                    nodedb_raft::error::RaftError::Storage {
                        detail: format!("serialize entry: {e}"),
                    }
                })?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|e| nodedb_raft::error::RaftError::Storage {
                        detail: format!("insert entry: {e}"),
                    })?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| nodedb_raft::error::RaftError::Storage {
                detail: format!("commit append: {e}"),
            })?;

        debug!(count = entries.len(), "raft log appended");
        Ok(())
    }

    fn truncate(&mut self, index: u64) -> nodedb_raft::error::Result<()> {
        let write_txn =
            self.db
                .begin_write()
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("write txn: {e}"),
                })?;
        {
            let mut table = write_txn.open_table(ENTRIES).map_err(|e| {
                nodedb_raft::error::RaftError::Storage {
                    detail: format!("open entries: {e}"),
                }
            })?;

            // Collect keys >= index to remove.
            let start = index_key(index);
            let keys_to_remove: Vec<[u8; 8]> = table
                .range(start.as_slice()..)
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("range: {e}"),
                })?
                .filter_map(|r| {
                    r.ok().map(|(k, _)| {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(k.value());
                        buf
                    })
                })
                .collect();

            for key in &keys_to_remove {
                table.remove(key.as_slice()).map_err(|e| {
                    nodedb_raft::error::RaftError::Storage {
                        detail: format!("remove: {e}"),
                    }
                })?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| nodedb_raft::error::RaftError::Storage {
                detail: format!("commit truncate: {e}"),
            })?;

        debug!(from_index = index, "raft log truncated");
        Ok(())
    }

    fn load_entries_after(&self, snapshot_index: u64) -> nodedb_raft::error::Result<Vec<LogEntry>> {
        let read_txn =
            self.db
                .begin_read()
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("read txn: {e}"),
                })?;
        let table =
            read_txn
                .open_table(ENTRIES)
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("open entries: {e}"),
                })?;

        // Start after snapshot_index (exclusive).
        let start = index_key(snapshot_index + 1);
        let mut entries = Vec::new();

        for result in
            table
                .range(start.as_slice()..)
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("range: {e}"),
                })?
        {
            let (_, value) = result.map_err(|e| nodedb_raft::error::RaftError::Storage {
                detail: format!("entry read: {e}"),
            })?;
            let entry: LogEntry = zerompk::from_msgpack(value.value()).map_err(|e| {
                nodedb_raft::error::RaftError::Storage {
                    detail: format!("deserialize entry: {e}"),
                }
            })?;
            entries.push(entry);
        }

        debug!(
            count = entries.len(),
            after = snapshot_index,
            "raft log loaded"
        );
        Ok(entries)
    }

    fn compact(&mut self, index: u64, term: u64) -> nodedb_raft::error::Result<()> {
        let write_txn =
            self.db
                .begin_write()
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("write txn: {e}"),
                })?;
        {
            // Remove entries <= index.
            let mut table = write_txn.open_table(ENTRIES).map_err(|e| {
                nodedb_raft::error::RaftError::Storage {
                    detail: format!("open entries: {e}"),
                }
            })?;

            let end = index_key(index + 1);
            let keys_to_remove: Vec<[u8; 8]> = table
                .range(..end.as_slice())
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("range: {e}"),
                })?
                .filter_map(|r| {
                    r.ok().map(|(k, _)| {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(k.value());
                        buf
                    })
                })
                .collect();

            for key in &keys_to_remove {
                table.remove(key.as_slice()).map_err(|e| {
                    nodedb_raft::error::RaftError::Storage {
                        detail: format!("remove: {e}"),
                    }
                })?;
            }

            // Save snapshot metadata.
            let mut meta =
                write_txn
                    .open_table(META)
                    .map_err(|e| nodedb_raft::error::RaftError::Storage {
                        detail: format!("open meta: {e}"),
                    })?;

            let idx_bytes = zerompk::to_msgpack_vec(&index).map_err(|e| {
                nodedb_raft::error::RaftError::Storage {
                    detail: format!("serialize: {e}"),
                }
            })?;
            let term_bytes = zerompk::to_msgpack_vec(&term).map_err(|e| {
                nodedb_raft::error::RaftError::Storage {
                    detail: format!("serialize: {e}"),
                }
            })?;

            meta.insert(KEY_SNAPSHOT_INDEX, idx_bytes.as_slice())
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("insert meta: {e}"),
                })?;
            meta.insert(KEY_SNAPSHOT_TERM, term_bytes.as_slice())
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("insert meta: {e}"),
                })?;
        }
        write_txn
            .commit()
            .map_err(|e| nodedb_raft::error::RaftError::Storage {
                detail: format!("commit compact: {e}"),
            })?;

        debug!(index, term, "raft log compacted");
        Ok(())
    }

    fn snapshot_metadata(&self) -> (u64, u64) {
        let Ok(read_txn) = self.db.begin_read() else {
            return (0, 0);
        };
        let Ok(table) = read_txn.open_table(META) else {
            return (0, 0);
        };

        let index = table
            .get(KEY_SNAPSHOT_INDEX)
            .ok()
            .flatten()
            .and_then(|v| zerompk::from_msgpack::<u64>(v.value()).ok())
            .unwrap_or(0);
        let term = table
            .get(KEY_SNAPSHOT_TERM)
            .ok()
            .flatten()
            .and_then(|v| zerompk::from_msgpack::<u64>(v.value()).ok())
            .unwrap_or(0);

        (index, term)
    }

    fn save_hard_state(&mut self, state: &HardState) -> nodedb_raft::error::Result<()> {
        let write_txn =
            self.db
                .begin_write()
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("write txn: {e}"),
                })?;
        {
            let mut table =
                write_txn
                    .open_table(META)
                    .map_err(|e| nodedb_raft::error::RaftError::Storage {
                        detail: format!("open meta: {e}"),
                    })?;

            let bytes = zerompk::to_msgpack_vec(state).map_err(|e| {
                nodedb_raft::error::RaftError::Storage {
                    detail: format!("serialize: {e}"),
                }
            })?;
            table
                .insert(KEY_HARD_STATE, bytes.as_slice())
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("insert: {e}"),
                })?;
        }
        write_txn
            .commit()
            .map_err(|e| nodedb_raft::error::RaftError::Storage {
                detail: format!("commit: {e}"),
            })?;

        debug!(
            term = state.current_term,
            voted_for = state.voted_for,
            "raft hard state saved"
        );
        Ok(())
    }

    fn load_hard_state(&self) -> nodedb_raft::error::Result<HardState> {
        let read_txn =
            self.db
                .begin_read()
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("read txn: {e}"),
                })?;
        let table =
            read_txn
                .open_table(META)
                .map_err(|e| nodedb_raft::error::RaftError::Storage {
                    detail: format!("open meta: {e}"),
                })?;

        match table.get(KEY_HARD_STATE) {
            Ok(Some(value)) => {
                let state: HardState = zerompk::from_msgpack(value.value()).map_err(|e| {
                    nodedb_raft::error::RaftError::Storage {
                        detail: format!("deserialize: {e}"),
                    }
                })?;
                Ok(state)
            }
            Ok(None) => Ok(HardState::default()),
            Err(e) => Err(nodedb_raft::error::RaftError::Storage {
                detail: format!("get hard state: {e}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (RedbLogStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-raft.redb");
        let storage = RedbLogStorage::open(&path).unwrap();
        (storage, dir)
    }

    #[test]
    fn append_and_load() {
        let (mut s, _dir) = open_temp();
        let entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                data: b"cmd-a".to_vec(),
            },
            LogEntry {
                term: 1,
                index: 2,
                data: b"cmd-b".to_vec(),
            },
            LogEntry {
                term: 2,
                index: 3,
                data: b"cmd-c".to_vec(),
            },
        ];
        s.append(&entries).unwrap();

        let loaded = s.load_entries_after(0).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].data, b"cmd-a");
        assert_eq!(loaded[2].term, 2);
    }

    #[test]
    fn truncate_removes_tail() {
        let (mut s, _dir) = open_temp();
        for i in 1..=5 {
            s.append(&[LogEntry {
                term: 1,
                index: i,
                data: vec![],
            }])
            .unwrap();
        }
        s.truncate(3).unwrap();
        let loaded = s.load_entries_after(0).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.last().unwrap().index, 2);
    }

    #[test]
    fn compact_removes_prefix() {
        let (mut s, _dir) = open_temp();
        for i in 1..=10 {
            s.append(&[LogEntry {
                term: 1,
                index: i,
                data: vec![],
            }])
            .unwrap();
        }
        s.compact(5, 1).unwrap();
        assert_eq!(s.snapshot_metadata(), (5, 1));
        let loaded = s.load_entries_after(5).unwrap();
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].index, 6);
    }

    #[test]
    fn hard_state_roundtrip() {
        let (mut s, _dir) = open_temp();
        let hs = HardState {
            current_term: 7,
            voted_for: 3,
        };
        s.save_hard_state(&hs).unwrap();
        let loaded = s.load_hard_state().unwrap();
        assert_eq!(loaded, hs);
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen-raft.redb");

        {
            let mut s = RedbLogStorage::open(&path).unwrap();
            s.append(&[LogEntry {
                term: 1,
                index: 1,
                data: b"durable".to_vec(),
            }])
            .unwrap();
            s.save_hard_state(&HardState {
                current_term: 3,
                voted_for: 1,
            })
            .unwrap();
        }

        let s = RedbLogStorage::open(&path).unwrap();
        let loaded = s.load_entries_after(0).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].data, b"durable");
        let hs = s.load_hard_state().unwrap();
        assert_eq!(hs.current_term, 3);
    }
}
