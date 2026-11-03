// SPDX-License-Identifier: BUSL-1.1

//! Persistent Raft log storage backed by redb.
//!
//! Implements `nodedb_raft::LogStorage` with ACID durability via redb.
//! Each Raft group gets its own redb file at `{data_dir}/raft/group-{id}.redb`.
//!
//! Layout:
//! - `ENTRIES` table: key = log index (u64 big-endian), value = MessagePack LogEntry
//! - `META` table: key = name (&str), value = MessagePack-encoded metadata
//!   - "hard_state" → HardState { current_term, voted_for, ordinal_floor }
//!   - "snapshot_index" → u64
//!   - "snapshot_term" → u64
//!   - "applied_index" → u64

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::{debug, info};

use nodedb_raft::message::LogEntry;
use nodedb_raft::state::HardState;
use nodedb_raft::storage::LogStorage;

use crate::raft_storage_ops::{LogOp, apply_in_txn};
use crate::wire_version::envelope::decode_versioned;

/// Log entries: key = index (big-endian u64 for sorted iteration), value = versioned-envelope bytes.
pub(crate) const ENTRIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft.entries");

/// Metadata: key = name, value = MessagePack bytes.
pub(crate) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft.meta");

pub(crate) const KEY_HARD_STATE: &str = "hard_state";
pub(crate) const KEY_SNAPSHOT_INDEX: &str = "snapshot_index";
pub(crate) const KEY_SNAPSHOT_TERM: &str = "snapshot_term";
pub(crate) const KEY_APPLIED_INDEX: &str = "applied_index";

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

impl RedbLogStorage {
    /// Apply `ops` in order inside one transaction. One commit, and so one
    /// group of fsyncs, makes the whole run durable.
    pub fn commit_ops(&mut self, ops: &[LogOp<'_>]) -> nodedb_raft::error::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| nodedb_raft::error::RaftError::Storage {
                detail: format!("write txn: {e}"),
            })?;
        for op in ops {
            apply_in_txn(&txn, op)?;
        }
        txn.commit()
            .map_err(|e| nodedb_raft::error::RaftError::Storage {
                detail: format!("commit: {e}"),
            })
    }
}

pub(crate) fn index_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

impl LogStorage for RedbLogStorage {
    fn append(&mut self, entries: &[LogEntry]) -> nodedb_raft::error::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.commit_ops(&[LogOp::Append(entries)])?;
        debug!(count = entries.len(), "raft log appended");
        Ok(())
    }

    fn truncate(&mut self, index: u64) -> nodedb_raft::error::Result<()> {
        self.commit_ops(&[LogOp::Truncate(index)])?;
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
            let entry: LogEntry = decode_versioned(value.value()).map_err(|e| {
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
        self.commit_ops(&[LogOp::Compact { index, term }])?;
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
        self.commit_ops(&[LogOp::HardState(state)])?;
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

    fn save_applied_index(&mut self, index: u64) -> nodedb_raft::error::Result<()> {
        self.commit_ops(&[LogOp::AppliedIndex(index)])?;
        debug!(index, "raft applied index saved");
        Ok(())
    }

    fn load_applied_index(&self) -> nodedb_raft::error::Result<u64> {
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

        match table.get(KEY_APPLIED_INDEX) {
            Ok(Some(value)) => {
                let index: u64 = zerompk::from_msgpack(value.value()).map_err(|e| {
                    nodedb_raft::error::RaftError::Storage {
                        detail: format!("deserialize: {e}"),
                    }
                })?;
                Ok(index)
            }
            // Absent on any group file written before the applied index
            // existed. Reporting 0 degrades to a full replay of the retained
            // log on the first boot after upgrade, and the key is written on
            // the first apply after that — self-correcting, never an error.
            Ok(None) => Ok(0),
            Err(e) => Err(nodedb_raft::error::RaftError::Storage {
                detail: format!("get applied index: {e}"),
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
            ordinal_floor: 1_234,
        };
        s.save_hard_state(&hs).unwrap();
        let loaded = s.load_hard_state().unwrap();
        assert_eq!(loaded, hs);
    }

    /// A group file written before the applied index existed has no key.
    /// Reporting 0 degrades to today's full replay rather than erroring.
    #[test]
    fn applied_index_absent_reads_zero() {
        let (s, _dir) = open_temp();
        assert_eq!(s.load_applied_index().unwrap(), 0);
    }

    #[test]
    fn applied_index_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("applied-index-raft.redb");

        {
            let mut s = RedbLogStorage::open(&path).unwrap();
            s.save_applied_index(9).unwrap();
        }

        let s = RedbLogStorage::open(&path).unwrap();
        assert_eq!(s.load_applied_index().unwrap(), 9);
    }

    /// LogEntry written and read back via the versioned envelope.
    #[test]
    fn versioned_roundtrip() {
        let (mut s, _dir) = open_temp();
        let entry = LogEntry {
            term: 3,
            index: 1,
            data: b"versioned-payload".to_vec(),
        };
        s.append(std::slice::from_ref(&entry)).unwrap();
        let loaded = s.load_entries_after(0).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], entry);
    }

    /// Bytes with the envelope marker but an unknown future version must
    /// return a Storage error, not silently decode as v1.
    #[test]
    fn version_mismatch_returns_error() {
        let (s, _dir) = open_temp();

        // Build an envelope with a future version number (9999).
        let inner = zerompk::to_msgpack_vec(&LogEntry {
            term: 1,
            index: 77,
            data: vec![],
        })
        .unwrap();

        // Construct the envelope manually: marker + version(9999) + inner_len + inner.
        let mut bad_bytes = Vec::new();
        bad_bytes.push(0xc1u8); // ENVELOPE_MARKER
        bad_bytes.extend_from_slice(&9999u16.to_be_bytes());
        bad_bytes.extend_from_slice(&(inner.len() as u32).to_be_bytes());
        bad_bytes.extend_from_slice(&inner);

        let key = index_key(77);
        let write_txn = s.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(ENTRIES).unwrap();
            table.insert(key.as_slice(), bad_bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();

        let err = s.load_entries_after(76).unwrap_err();
        match err {
            nodedb_raft::error::RaftError::Storage { detail } => {
                assert!(
                    detail.contains("deserialize"),
                    "expected 'deserialize' in detail: {detail}"
                );
            }
            other => panic!("expected Storage error, got: {other}"),
        }
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
                ordinal_floor: 0,
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

    /// A vote grant AND the log must survive a process restart. After a node
    /// grants a vote and persists its `HardState`, reopening the SAME redb
    /// path and calling `restore()` must (a) still refuse a second, different
    /// candidate in the same term — the split-brain / double-vote guard — and
    /// (b) still hold its durably-appended log entries rather than an empty
    /// log. Fails without the persist wiring (double-vote) or without the
    /// `restore()` log reload (empty log).
    #[test]
    fn vote_and_log_survive_restart() {
        use nodedb_raft::node::RaftConfig;
        use nodedb_raft::{AppendEntriesRequest, RaftNode, RequestVoteRequest};
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vote-restart-raft.redb");

        let config = || RaftConfig {
            node_id: 1,
            group_id: 7,
            peers: vec![2, 3],
            learners: vec![],
            observers: vec![],
            starts_as_learner: false,
            starts_as_observer: false,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            log_compaction_threshold: None,
        };

        const TERM: u64 = 5;

        {
            let storage = RedbLogStorage::open(&path).unwrap();
            let mut node = RaftNode::new(config(), storage);
            node.restore().unwrap();

            // Leader (node 2) replicates two entries at TERM: bumps the node's
            // term (become_follower) and durably appends the log entries.
            let ae = AppendEntriesRequest {
                term: TERM,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        term: TERM,
                        index: 1,
                        data: b"e1".to_vec(),
                    },
                    LogEntry {
                        term: TERM,
                        index: 2,
                        data: b"e2".to_vec(),
                    },
                ],
                leader_commit: 0,
                group_id: 7,
                round: 1,
                replicated_floor: 0,
            };
            assert!(node.handle_append_entries(&ae).success);
            node.persist_hard_state_if_dirty().unwrap();
            // Age the leader contact and end the boot fence, so the vote is
            // not refused as disruptive.
            node.leader_contact_at_override(
                std::time::Instant::now() - std::time::Duration::from_secs(1),
            );
            node.expire_boot_vote_fence();

            // Grant a vote to candidate 2 in TERM, then persist it durably.
            let rv = RequestVoteRequest {
                term: TERM,
                candidate_id: 2,
                last_log_index: 2,
                last_log_term: TERM,
                group_id: 7,
                transfer: false,
            };
            assert!(
                node.handle_request_vote(&rv).vote_granted,
                "first vote must be granted"
            );
            node.persist_hard_state_if_dirty().unwrap();
        }

        // Simulated crash: node + storage dropped. Reopen the SAME path.
        let storage = RedbLogStorage::open(&path).unwrap();
        let mut node = RaftNode::new(config(), storage);
        node.restore().unwrap();

        // The restarted node's boot fence would refuse any vote. End it, so
        // only the restored `voted_for` can refuse the second candidate.
        node.expire_boot_vote_fence();

        // Log-reload half: the durably-appended entries survived the restart.
        assert_eq!(node.last_log_index(), 2, "log entries must survive restart");
        // Vote-persistence half: the term survived.
        assert_eq!(node.current_term(), TERM);

        // A second, DIFFERENT candidate in the same term must be refused: the
        // restored voter has already voted for candidate 2 this term. (A reject
        // here — with the candidate's log fully up to date — proves the
        // restored `voted_for` is 2, not 0 or 3.)
        let rv2 = RequestVoteRequest {
            term: TERM,
            candidate_id: 3,
            last_log_index: 2,
            last_log_term: TERM,
            group_id: 7,
            transfer: false,
        };
        assert!(
            !node.handle_request_vote(&rv2).vote_granted,
            "restarted voter must not grant a second vote in the same term"
        );
        assert_eq!(node.current_term(), TERM);
    }

    #[test]
    fn one_commit_applies_a_run_of_writes_in_order() {
        let (mut s, _dir) = open_temp();
        let entries: Vec<LogEntry> = (1..=5)
            .map(|index| LogEntry {
                term: 1,
                index,
                data: vec![],
            })
            .collect();
        let hard_state = HardState {
            current_term: 3,
            voted_for: 2,
            ordinal_floor: 0,
        };
        s.commit_ops(&[
            LogOp::Append(&entries),
            LogOp::AppliedIndex(4),
            LogOp::HardState(&hard_state),
            LogOp::Compact { index: 2, term: 1 },
            LogOp::Truncate(5),
        ])
        .unwrap();

        let loaded = s.load_entries_after(0).unwrap();
        let indexes: Vec<u64> = loaded.iter().map(|e| e.index).collect();
        assert_eq!(indexes, vec![3, 4]);
        assert_eq!(s.snapshot_metadata(), (2, 1));
        assert_eq!(s.load_applied_index().unwrap(), 4);
        assert_eq!(s.load_hard_state().unwrap(), hard_state);
    }
}
