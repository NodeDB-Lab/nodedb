// SPDX-License-Identifier: BUSL-1.1

//! The Calvin sequencer group's Raft snapshot on this node.
//!
//! The sequencer state machine lives in memory and is rebuilt by applying
//! the sequencer log. A follower that installs a snapshot never applies the
//! entries it covers, so the snapshot carries the state they built (see
//! `nodedb_cluster::calvin::SequencerSnapshot`).
//!
//! - Send path: the leader captures its state machine at the group's applied
//!   index, on the Raft tick thread.
//! - Receive path: the follower writes the payload durably, then restores
//!   its state machine from it. The install is durable before Raft advances
//!   the group's log boundary, as every install is.
//! - Own compaction: the node writes its state machine's capture before it
//!   compacts its own log (see [`super::sequencer_compaction`]).
//! - Boot: a node whose sequencer log holds every entry after the kept
//!   snapshot restores the state machine from the file before the log
//!   applies.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use nodedb_cluster::calvin::{SEQUENCER_GROUP_ID, SequencerSnapshot, SequencerStateMachine};

/// The file an installed sequencer snapshot is kept in, under the data
/// directory's `calvin` directory.
const SNAPSHOT_FILE: &str = "sequencer.snapshot";

/// Captures, installs, and reloads the sequencer group's snapshot.
pub struct SequencerSnapshotStore {
    state_machine: Arc<Mutex<SequencerStateMachine>>,
    dir: PathBuf,
}

impl SequencerSnapshotStore {
    /// A store for `state_machine`, keeping its file under `data_dir`.
    pub fn new(
        state_machine: Arc<Mutex<SequencerStateMachine>>,
        data_dir: &std::path::Path,
    ) -> Self {
        Self {
            state_machine,
            dir: data_dir.join("calvin"),
        }
    }

    /// The state machine's snapshot at `applied_index`, encoded.
    pub fn capture(&self, applied_index: u64) -> crate::Result<Vec<u8>> {
        let snapshot = self
            .state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .capture_snapshot(applied_index);
        snapshot.encode().map_err(codec_error)
    }

    /// Install a received snapshot payload: record the install in `bases`,
    /// write the payload durably, then restore the state machine from it.
    /// Blocks on disk: call it off the async threads.
    ///
    /// The install skips entries this node's schedulers never received. The
    /// record is durable first, so every kept base it skips is whole no
    /// more, also after a crash.
    pub fn install(
        &self,
        bytes: &[u8],
        bases: &crate::control::state::CalvinBases,
        catalog: &crate::control::security::catalog::SystemCatalog,
    ) -> crate::Result<()> {
        let snapshot = SequencerSnapshot::decode(bytes).map_err(codec_error)?;
        bases.record_sequencer_install(catalog, snapshot.applied_index())?;
        self.persist(bytes)?;
        self.state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .restore_snapshot(snapshot);
        Ok(())
    }

    /// Write an encoded capture durably as this node's sequencer snapshot.
    /// Blocks on disk: call it off the async threads.
    pub fn persist(&self, bytes: &[u8]) -> crate::Result<()> {
        std::fs::create_dir_all(&self.dir).map_err(|e| storage_error(&self.dir, "create", e))?;
        nodedb_wal::segment::atomic_write_fsync(&self.dir, SNAPSHOT_FILE, bytes).map_err(|e| {
            crate::Error::Storage {
                engine: "calvin".into(),
                detail: format!(
                    "write the sequencer snapshot under {}: {e}",
                    self.dir.display()
                ),
            }
        })
    }

    /// Restore the state machine at boot from the kept snapshot when the
    /// sequencer log still holds every entry after it. Returns whether it
    /// restored.
    ///
    /// The group's log boundary is the index of the last snapshot the log
    /// adopted: an installed one, or this node's own compaction. Every own
    /// compaction writes the file at or above its boundary first, so the
    /// file holds the state whenever its index is at or above the boundary.
    /// The log then delivers only the entries after the file's index again:
    /// the state machine skips every entry at or below the index it
    /// restored.
    ///
    /// A boundary above 0 with no usable file leaves the entries through the
    /// boundary unrecoverable. The state machine then records its history as
    /// unknown, so its leader never seeds an epoch from partial state.
    pub fn restore_at_boot(
        &self,
        multi_raft: &nodedb_cluster::multi_raft::MultiRaft,
    ) -> crate::Result<bool> {
        let Ok((_, boundary, _)) = multi_raft.snapshot_metadata(SEQUENCER_GROUP_ID) else {
            return Ok(false);
        };
        let snapshot = self.read_kept()?;
        let mut state_machine = self.state_machine.lock().unwrap_or_else(|p| p.into_inner());
        match snapshot {
            Some(snapshot) if snapshot.applied_index() >= boundary => {
                state_machine.restore_snapshot(snapshot);
                Ok(true)
            }
            _ => {
                if boundary > 0 {
                    state_machine.mark_history_unknown();
                }
                Ok(false)
            }
        }
    }

    /// The kept snapshot, or `None` when no file exists.
    fn read_kept(&self) -> crate::Result<Option<SequencerSnapshot>> {
        let path = self.dir.join(SNAPSHOT_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(storage_error(&path, "read", e)),
        };
        SequencerSnapshot::decode(&bytes)
            .map(Some)
            .map_err(codec_error)
    }
}

fn codec_error(e: nodedb_cluster::ClusterError) -> crate::Error {
    crate::Error::Internal {
        detail: format!("sequencer snapshot: {e}"),
    }
}

fn storage_error(path: &std::path::Path, op: &str, e: std::io::Error) -> crate::Error {
    crate::Error::Storage {
        engine: "calvin".into(),
        detail: format!("{op} {}: {e}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_cluster::calvin::CalvinCompletionRegistry;

    use super::*;

    fn state_machine() -> Arc<Mutex<SequencerStateMachine>> {
        Arc::new(Mutex::new(SequencerStateMachine::new(
            HashMap::new(),
            CalvinCompletionRegistry::new_detached(),
        )))
    }

    /// An installed payload is kept durably and restores the state machine:
    /// its applied index becomes the state machine's. The kept bases it
    /// skips are whole no more.
    #[test]
    fn an_installed_snapshot_is_kept_and_restored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let leader = SequencerSnapshotStore::new(state_machine(), dir.path());
        let bytes = leader.capture(9).expect("capture");

        let catalog =
            crate::control::security::catalog::SystemCatalog::open(&dir.path().join("system.redb"))
                .expect("catalog");
        let bases = crate::control::state::CalvinBases::default();
        bases
            .record_kept(&catalog, &[(4, 3), (5, 10)])
            .expect("kept");

        let follower_sm = state_machine();
        let follower = SequencerSnapshotStore::new(Arc::clone(&follower_sm), dir.path());
        follower.install(&bytes, &bases, &catalog).expect("install");
        assert_eq!(bases.base(4), None, "inputs 3..=9 were skipped");
        assert!(bases.base(5).is_some());
        assert_eq!(catalog.load_calvin_sequencer_install().expect("load"), 9);
        assert_eq!(
            follower_sm
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .current_committed_index(),
            Some(9)
        );
        let kept = std::fs::read(dir.path().join("calvin").join(SNAPSHOT_FILE)).expect("kept");
        assert_eq!(kept, bytes);
    }
}
