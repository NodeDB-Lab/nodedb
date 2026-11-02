// SPDX-License-Identifier: BUSL-1.1

//! Start a Raft group's log at a chosen point, with no prior log.
//!
//! A cluster restore rebuilds every group's state machine from backups and
//! then starts each group's log at the restored point: the snapshot marker
//! at `(snapshot_index, snapshot_term)`, the durable applied index there,
//! and the entries after it that every replica writes alike. The next boot
//! loads the file as it loads any group log.

use std::path::{Path, PathBuf};

use nodedb_raft::message::LogEntry;
use nodedb_raft::state::HardState;
use nodedb_raft::storage::LogStorage;

use crate::error::ClusterError;
use crate::raft_storage::RedbLogStorage;

/// Where group `group_id` keeps its log under `data_dir`.
pub fn group_log_path(data_dir: &Path, group_id: u64) -> PathBuf {
    data_dir.join(format!("raft/group-{group_id}.redb"))
}

/// The log a group starts from.
#[derive(Debug, Clone, Copy)]
pub struct GroupLogStart<'a> {
    /// Index the state machine holds every entry through.
    pub snapshot_index: u64,
    /// Term recorded for `snapshot_index`.
    pub snapshot_term: u64,
    /// The term the group's hard state starts in, with no vote cast.
    pub current_term: u64,
    /// Entries after `snapshot_index`, consecutive from `snapshot_index + 1`,
    /// not yet applied.
    pub entries: &'a [LogEntry],
}

fn refuse(path: &Path, why: String) -> ClusterError {
    ClusterError::Storage {
        detail: format!("start raft log {}: {why}", path.display()),
    }
}

/// Write group log `path` so it starts at `start`. Refuses a path that
/// exists: the start never merges into an earlier log.
pub fn start_group_log(path: &Path, start: &GroupLogStart<'_>) -> crate::Result<()> {
    if path.exists() {
        return Err(refuse(path, "a log already exists there".into()));
    }
    let mut expected = start.snapshot_index;
    let mut last_term = start.snapshot_term;
    for entry in start.entries {
        expected += 1;
        if entry.index != expected {
            return Err(refuse(
                path,
                format!("entry {} follows {}", entry.index, expected - 1),
            ));
        }
        if entry.term < last_term {
            return Err(refuse(
                path,
                format!(
                    "entry {} has term {} below {last_term}",
                    entry.index, entry.term
                ),
            ));
        }
        last_term = entry.term;
    }
    if start.current_term < last_term {
        return Err(refuse(
            path,
            format!(
                "current term {} is below the log's last term {last_term}",
                start.current_term
            ),
        ));
    }

    let mut storage = RedbLogStorage::open(path)?;
    storage.compact(start.snapshot_index, start.snapshot_term)?;
    storage.append(start.entries)?;
    storage.save_hard_state(&HardState {
        current_term: start.current_term,
        voted_for: 0,
        ordinal_floor: 0,
    })?;
    storage.save_applied_index(start.snapshot_index)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi_raft::MultiRaft;
    use crate::routing::RoutingTable;

    fn entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            data: index.to_le_bytes().to_vec(),
        }
    }

    #[test]
    fn a_started_log_reloads_at_its_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = group_log_path(dir.path(), 0);
        let entries = [entry(11, 7), entry(12, 7)];
        start_group_log(
            &path,
            &GroupLogStart {
                snapshot_index: 10,
                snapshot_term: 7,
                current_term: 9,
                entries: &entries,
            },
        )
        .unwrap();

        let storage = RedbLogStorage::open(&path).unwrap();
        assert_eq!(storage.snapshot_metadata(), (10, 7));
        assert_eq!(storage.load_entries_after(10).unwrap(), entries);
        assert_eq!(storage.load_hard_state().unwrap().current_term, 9);
        assert_eq!(storage.load_hard_state().unwrap().voted_for, 0);
        assert_eq!(storage.load_applied_index().unwrap(), 10);
        drop(storage);

        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();
        assert_eq!(mr.first_available_index(0), Some(11));
        assert_eq!(mr.log_term_at(0, 10), Some(7));
        assert_eq!(mr.log_term_at(0, 12), Some(7));
        assert_eq!(mr.log_term_at(0, 13), None);
    }

    fn start(entries: &[LogEntry], current_term: u64) -> GroupLogStart<'_> {
        GroupLogStart {
            snapshot_index: 5,
            snapshot_term: 3,
            current_term,
            entries,
        }
    }

    #[test]
    fn a_bad_start_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let gap = [entry(7, 3)];
        let falling = [entry(6, 2)];
        let behind = [entry(6, 4)];
        for (name, entries, term) in [
            ("gap", &gap[..], 3),
            ("falling", &falling[..], 3),
            ("behind", &behind[..], 3),
        ] {
            let path = dir.path().join(name);
            assert!(
                start_group_log(&path, &start(entries, term)).is_err(),
                "{name}"
            );
            assert!(!path.exists(), "{name}: nothing was written");
        }

        let path = group_log_path(dir.path(), 4);
        start_group_log(&path, &start(&[], 3)).unwrap();
        assert!(
            start_group_log(&path, &start(&[], 3)).is_err(),
            "an existing log is never overwritten"
        );
    }
}
