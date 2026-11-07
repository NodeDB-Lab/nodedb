// SPDX-License-Identifier: BUSL-1.1

//! The metadata Raft log archiver.
//!
//! A restore rebuilds the metadata group from a base's catalog images plus
//! the metadata log entries after them. The log compacts, so every committed
//! entry is copied to cold storage first: the archiver sets the group's
//! compaction ceiling to the highest index it holds, and the log never
//! compacts past it. The same rule the WAL archiver enforces for WAL
//! segments.
//!
//! The archiver writes under the metadata timeline the catalog names, and
//! reads it on every pass: a node that joins a restored cluster learns the
//! timeline from the snapshot it installs.
//!
//! Each pass records the life's frontier: the newest stamp among the entries
//! it holds. A leader stamps each entry above every stamp before it (see
//! `MultiRaft::propose_stamped_metadata`), so every entry stamped at or below
//! the frontier is archived. On a log with no recent stamp, the leader's
//! archiver proposes an `ArchiveMark` entry, so the frontier keeps up with
//! the clock.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nodedb_cluster::multi_raft::MultiRaft;
use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, encode_entry, entry_stamp};
use tracing::{debug, warn};

use crate::control::pitr::NodeLife;
use crate::control::security::credential::CredentialStore;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;
use crate::storage::cold::ColdStorage;
use crate::storage::raft_log_archive::{
    ArchiveFrontier, ArchiveLife, ArchivedLogChunk, ArchivedLogEntry, chunk_key, fetch_chunk,
    frontier_key, list_chunks, put_chunk, put_frontier, seal_chunk,
};

/// How often the archiver copies new committed entries.
const ARCHIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Most entries one archive object holds.
const MAX_ENTRIES_PER_CHUNK: u64 = 4096;

/// The owner name of the archiver's ceiling on group 0's log compaction.
const ARCHIVE_CEILING: &str = "pitr_metadata_log_archive";

/// Copy group 0's committed log to cold storage, and bound its compaction
/// by what the archive holds. Starts nothing with PITR off, no cold storage,
/// or no WAL key: the archive is encrypted with the WAL key.
pub fn spawn_metadata_log_archiver(shared: &Arc<SharedState>, multi_raft: Arc<Mutex<MultiRaft>>) {
    let Some(life) = shared.pitr.life().cloned() else {
        return;
    };
    let Some(cold) = shared.cold_storage.clone() else {
        warn!("the metadata log is not archived: PITR is on, and this node has no cold storage");
        return;
    };
    let Some(wal_key) = shared.wal.encryption_key().cloned() else {
        warn!(
            "the metadata log is not archived: the archive needs the WAL key, and this node has none"
        );
        return;
    };
    // Nothing is archived yet, so the log compacts nothing until the first
    // pass copies it.
    let ceiling = Arc::new(AtomicU64::new(0));
    multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .set_compaction_ceiling(METADATA_GROUP_ID, ARCHIVE_CEILING, Arc::clone(&ceiling));
    let mut archiver = MetadataLogArchiver {
        cold,
        multi_raft,
        wal_key,
        ceiling,
        life,
        clock: Arc::clone(&shared.hlc_clock),
        credentials: Arc::clone(&shared.credentials),
        held: None,
    };
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "pitr_metadata_log_archive",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            loop {
                if let Err(e) = archiver.pass().await {
                    warn!(error = %e, "metadata log archive pass failed; compaction stays held");
                }
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tokio::time::sleep(ARCHIVE_INTERVAL) => {}
                }
            }
        },
    );
}

struct MetadataLogArchiver {
    cold: Arc<ColdStorage>,
    multi_raft: Arc<Mutex<MultiRaft>>,
    wal_key: nodedb_wal::crypto::WalEncryptionKey,
    ceiling: Arc<AtomicU64>,
    life: NodeLife,
    /// The node HLC. The leader stamps metadata entries from it.
    clock: Arc<nodedb_types::HlcClock>,
    /// The catalog, which names the metadata timeline.
    credentials: Arc<CredentialStore>,
    /// What the archive of the current timeline holds. `None` until read
    /// from the archive.
    held: Option<Held>,
}

/// What this life's archive of one timeline holds.
#[derive(Debug, Clone, Copy)]
struct Held {
    timeline: u64,
    /// Highest index archived.
    through: u64,
    /// Newest stamp among the archived entries.
    newest_stamp: Option<u64>,
    /// The frontier last written.
    frontier: Option<ArchiveFrontier>,
}

/// What a pass reads under the `MultiRaft` lock.
struct Batch {
    prev_term: u64,
    entries: Vec<nodedb_raft::message::LogEntry>,
}

impl MetadataLogArchiver {
    /// Archive every committed entry the archive lacks, record the frontier,
    /// and on an idle log propose the entry that moves it.
    async fn pass(&mut self) -> crate::Result<()> {
        let timeline = self.credentials.catalog().load_metadata_timeline()?;
        let incarnation = self.life.incarnation.as_str().to_string();
        let life = ArchiveLife {
            timeline,
            node_id: self.life.node_id,
            incarnation: &incarnation,
        };
        let mut held = match self.held.take() {
            Some(held) if held.timeline == timeline => held,
            _ => self.load_held(life).await?,
        };
        let drained = self.drain(life, &mut held).await;
        self.held = Some(held);
        drained?;
        self.mark_idle_log()
    }

    /// Read what this life's archive of `life.timeline` holds.
    async fn load_held(&self, life: ArchiveLife<'_>) -> crate::Result<Held> {
        let store = self.cold.object_store();
        let chunks = list_chunks(&store, self.cold.prefix(), life).await?;
        let Some(last) = chunks.last() else {
            return Ok(Held {
                timeline: life.timeline,
                through: 0,
                newest_stamp: None,
                frontier: None,
            });
        };
        let chunk = fetch_chunk(&store, &last.key, &self.wal_key).await?;
        Ok(Held {
            timeline: life.timeline,
            through: last.last,
            newest_stamp: newest_stamp(&chunk.entries),
            frontier: None,
        })
    }

    /// Archive every committed entry after `held.through`, then write the
    /// frontier when it moved.
    async fn drain(&self, life: ArchiveLife<'_>, held: &mut Held) -> crate::Result<()> {
        let prefix = self.cold.prefix();
        let store = self.cold.object_store();
        self.ceiling.store(held.through, Ordering::Release);
        while let Some(batch) = self.read_batch(held.through)? {
            let (Some(first), Some(last)) = (batch.entries.first(), batch.entries.last()) else {
                break;
            };
            let (first, last) = (first.index, last.index);
            let key = chunk_key(prefix, life, first, last);
            let entries: Vec<ArchivedLogEntry> = batch
                .entries
                .into_iter()
                .map(|entry| ArchivedLogEntry {
                    index: entry.index,
                    term: entry.term,
                    data: entry.data,
                })
                .collect();
            let batch_stamp = newest_stamp(&entries);
            let chunk = ArchivedLogChunk {
                group_id: METADATA_GROUP_ID,
                prev_term: batch.prev_term,
                entries,
            };
            let sealed = seal_chunk(&chunk, &key, &self.life.node_name(), &self.wal_key)?;
            put_chunk(&store, &key, sealed).await?;
            held.through = last;
            held.newest_stamp = held.newest_stamp.max(batch_stamp);
            self.ceiling.store(last, Ordering::Release);
            debug!(first, last, "metadata log entries archived");
        }
        let Some(stamped_through_ns) = held.newest_stamp else {
            return Ok(());
        };
        let frontier = ArchiveFrontier {
            through: held.through,
            stamped_through_ns,
        };
        if held.frontier != Some(frontier) {
            let key = frontier_key(prefix, life);
            put_frontier(
                &store,
                &key,
                frontier,
                &self.life.node_name(),
                &self.wal_key,
            )
            .await?;
            held.frontier = Some(frontier);
        }
        Ok(())
    }

    /// On the leader of a log whose newest stamp is older than one archive
    /// interval, propose an `ArchiveMark`. Its stamp moves the frontier to the
    /// clock once a later pass archives it.
    fn mark_idle_log(&self) -> crate::Result<()> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        if !mr.is_group_leader(METADATA_GROUP_ID) {
            return Ok(());
        }
        let now = self.clock.now().wall_ns;
        let interval = u64::try_from(ARCHIVE_INTERVAL.as_nanos()).unwrap_or(u64::MAX);
        if mr
            .newest_metadata_stamp()
            .is_some_and(|stamp| now.saturating_sub(stamp) < interval)
        {
            return Ok(());
        }
        let bytes =
            encode_entry(&MetadataEntry::ArchiveMark).map_err(|e| crate::Error::Internal {
                detail: format!("encode metadata archive mark: {e}"),
            })?;
        mr.propose_stamped_metadata(&bytes)
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("propose metadata archive mark: {e}"),
            })?;
        Ok(())
    }

    /// The committed entries after `archived`, at most one object's worth.
    /// `None` when there are none.
    fn read_batch(&self, archived: u64) -> crate::Result<Option<Batch>> {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let Some(first_available) = mr.first_available_index(METADATA_GROUP_ID) else {
            return Ok(None);
        };
        let mut lo = archived.saturating_add(1);
        if lo < first_available {
            // A log that starts at a snapshot holds nothing below it: this
            // life's archive starts there, and other lives hold the earlier
            // entries. Entries leaving after this life archived some mean a
            // snapshot install replaced them, and the archive has a hole.
            if archived > 0 {
                warn!(
                    archived_through = archived,
                    first_available,
                    "metadata log entries left the log unarchived; the archive has a hole"
                );
            }
            lo = first_available;
        }
        let Some(prev_term) = mr.log_term_at(METADATA_GROUP_ID, lo - 1) else {
            return Ok(None);
        };
        let hi = lo.saturating_add(MAX_ENTRIES_PER_CHUNK - 1);
        let entries = mr
            .read_committed_entries(METADATA_GROUP_ID, lo, hi)
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("read metadata log entries {lo}..={hi}: {e}"),
            })?;
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some(Batch { prev_term, entries }))
    }
}

/// The newest stamp among `entries`.
fn newest_stamp(entries: &[ArchivedLogEntry]) -> Option<u64> {
    entries
        .iter()
        .filter_map(|entry| entry_stamp(&entry.data))
        .max()
}
