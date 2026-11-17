// SPDX-License-Identifier: BUSL-1.1

//! The off-core half of a full-text rebuild: derive one collection's index
//! from a pinned snapshot into a scratch database.
//!
//! The job runs on its own OS thread and touches no core state. It reads
//! the snapshot the owning core pinned when it opened or renewed the write
//! journal. A first pass derives the whole collection:
//!
//! 1. Every stored version of every document replays at its own system
//!    time, through the per-version replay a snapshot restore uses.
//! 2. Every document the snapshot holds no version of replays each stored
//!    sync-frame version at its own ordinal, tombstones included.
//!
//! Step 2 keeps text a Lite client synced without a body this node stores:
//! a Lite columnar row lives in the columnar engine, and a schemaless row's
//! CRDT delta travels apart from its `FtsIndex` frame. A document with a
//! stored body takes its text from the body alone, as a local write does.
//!
//! A catch-up round follows a pass whose journal passed its bound. It
//! copies the ended pass's rows, minus the documents written during that
//! pass, and derives each of those again from its fresh snapshot.
//!
//! The scratch index commits every [`BUILD_BATCH_VERSIONS`] versions with
//! no fsync: it is disposable, and the owning core reads it in the same
//! process. Memory stays bounded by the batch and one bit per stored
//! document. `progress` counts every item the walk visits, replayed or
//! skipped, so a caller can tell a live rebuild from a stuck one.
//!
//! The core sets `cancel` when it abandons the rebuild. The job checks it
//! at each batch commit and each copied row, stops, and removes its scratch
//! files.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use redb::{Durability, WriteTransaction};

use super::rederive::{DocRef, rederive_doc};
use crate::data::executor::handlers::text_rebuild::replay::{
    ReplayTarget, TextVersion, replay_frame_into, replay_version_into,
};
use crate::engine::document::store::CollectionConfig;
use crate::engine::durability_gate::GatedDatabase;
use crate::engine::sparse::btree_versioned::version_walk;
use crate::engine::sparse::inverted::{
    FtsCatchUp, FtsRebuildTicket, FtsRebuilt, IndexDocScope, InvertedIndex,
};
use crate::types::TenantId;

/// Versions one scratch write transaction replays before it commits.
pub(in crate::data::executor) const BUILD_BATCH_VERSIONS: u64 = 1024;

/// What one pass of a rebuild derives.
pub(in crate::data::executor) enum BuildPass {
    /// The whole collection, from the rebuild's first snapshot.
    Full(FtsRebuildTicket),
    /// The documents written during the pass before, from a fresh snapshot.
    CatchUp(FtsCatchUp),
}

impl BuildPass {
    /// The snapshot this pass reads.
    pub(in crate::data::executor) fn ticket(&self) -> &FtsRebuildTicket {
        match self {
            Self::Full(ticket) => ticket,
            Self::CatchUp(catch_up) => &catch_up.ticket,
        }
    }
}

/// How a pass ended.
pub(in crate::data::executor) enum TextBuildEnd {
    /// The scratch database is ready to cut over.
    Built(FtsRebuilt),
    /// The core cancelled the rebuild. The job removed its scratch files.
    Cancelled,
}

/// One rebuild pass, ready to run on any thread.
pub(in crate::data::executor) struct TextBuildJob {
    pub pass: BuildPass,
    /// The declared text fields the rebuild indexes, sorted and deduped.
    pub text_fields: Vec<String>,
    /// The collection's registered config. The job reads only its storage
    /// mode, to decode stored bodies.
    pub config: Option<CollectionConfig>,
    /// The scratch database file. The job creates it, and removes it on an
    /// error or a cancel.
    pub scratch_path: PathBuf,
    pub governor: Arc<nodedb_mem::MemoryGovernor>,
    /// Items the walk visited so far, across every pass of the rebuild.
    pub progress: Arc<AtomicU64>,
    /// Set by the core when it abandons the rebuild.
    pub cancel: Arc<AtomicBool>,
}

impl TextBuildJob {
    /// Derive the pass into the scratch database and hand it to the
    /// cutover. The snapshot is released on return. A catch-up round
    /// removes the previous pass's scratch file in every case.
    pub(in crate::data::executor) fn run(self) -> crate::Result<TextBuildEnd> {
        let derived = self.derive();
        let TextBuildJob {
            pass,
            scratch_path,
            cancel,
            ..
        } = self;
        let ticket = match pass {
            BuildPass::Full(ticket) => ticket,
            BuildPass::CatchUp(FtsCatchUp {
                ticket, previous, ..
            }) => {
                let previous_path = previous.scratch_path().to_path_buf();
                drop(previous);
                remove_scratch(&previous_path);
                ticket
            }
        };
        if cancel.load(Ordering::Acquire) {
            drop(derived);
            remove_scratch(&scratch_path);
            return Ok(TextBuildEnd::Cancelled);
        }
        match derived {
            Ok(db) => Ok(TextBuildEnd::Built(ticket.finish(db, scratch_path))),
            Err(e) => {
                remove_scratch(&scratch_path);
                Err(e)
            }
        }
    }

    fn derive(&self) -> crate::Result<Arc<GatedDatabase>> {
        if let Some(parent) = self.scratch_path.parent() {
            std::fs::create_dir_all(parent).map_err(crate::Error::Io)?;
        }
        remove_scratch(&self.scratch_path);
        let db = Arc::new(GatedDatabase::create(&self.scratch_path).map_err(|e| {
            crate::Error::Storage {
                engine: "inverted".into(),
                detail: format!("rebuild scratch open: {e}"),
            }
        })?);
        let scratch = InvertedIndex::open(Arc::clone(&db), Arc::clone(&self.governor))?;
        self.pass.ticket().seed_scratch_config(&scratch)?;
        let target = ReplayTarget {
            inverted: &scratch,
            text_fields: &self.text_fields,
            config: self.config.as_ref(),
        };
        let mut batch = Batch {
            db: &db,
            open: None,
            in_batch: 0,
            progress: &self.progress,
            cancel: &self.cancel,
        };
        match &self.pass {
            BuildPass::Full(ticket) => {
                // No declared field: the rebuilt index holds no text.
                if !self.text_fields.is_empty() {
                    derive_all(ticket, target, &mut batch)?;
                }
            }
            BuildPass::CatchUp(catch_up) => {
                catch_up.carry_rows(&scratch, &|| {
                    self.progress.fetch_add(1, Ordering::Relaxed);
                    check_cancel(&self.cancel)
                })?;
                if !self.text_fields.is_empty() {
                    rederive_touched(catch_up, target, &mut batch)?;
                }
            }
        }
        batch.commit()?;
        Ok(db)
    }
}

/// Replay every stored version, then the frames of every document with no
/// stored version.
fn derive_all(
    ticket: &FtsRebuildTicket,
    target: ReplayTarget<'_>,
    batch: &mut Batch<'_>,
) -> crate::Result<()> {
    let (database_id, tid) = (ticket.database_id(), ticket.tid());
    let collection = ticket.collection();
    let mut stored = roaring::RoaringBitmap::new();
    let documents = version_walk::open_documents(ticket.snapshot())?;
    version_walk::for_each_collection_version(
        &documents,
        database_id,
        tid,
        collection,
        &mut |doc_id, sys_from, body| {
            let surrogate = doc_id.surrogate();
            stored.insert(surrogate.as_u32());
            replay_version_into(
                target,
                batch.txn()?,
                TextVersion {
                    database_id,
                    tid,
                    collection,
                    surrogate,
                    ordinal: sys_from,
                    body,
                },
            )?;
            batch.replayed(1)
        },
    )?;
    drop(documents);

    ticket.for_each_snapshot_frame(&mut |synced| {
        if stored.contains(synced.surrogate.as_u32()) {
            // A skipped frame still moves the walk.
            batch.walked();
            return Ok(());
        }
        replay_frame_into(
            target,
            batch.txn()?,
            IndexDocScope {
                database_id,
                tid: TenantId::new(tid),
                collection,
                surrogate: synced.surrogate,
                ordinal: synced.ordinal,
            },
            &synced.fields,
        )?;
        batch.replayed(1)
    })
}

/// Derive each document written during the pass before from the fresh
/// snapshot. The carried rows leave every one of them out.
fn rederive_touched(
    catch_up: &FtsCatchUp,
    target: ReplayTarget<'_>,
    batch: &mut Batch<'_>,
) -> crate::Result<()> {
    let ticket = &catch_up.ticket;
    for raw in &catch_up.touched {
        let doc = DocRef {
            database_id: ticket.database_id(),
            tid: ticket.tid(),
            collection: ticket.collection(),
            surrogate: nodedb_types::Surrogate::new(raw),
        };
        let replayed = rederive_doc(ticket.snapshot(), target, batch.txn()?, doc)?;
        // A document with nothing stored still moves the walk.
        batch.replayed(replayed.max(1))?;
    }
    Ok(())
}

/// The open scratch write transaction and its replay count.
struct Batch<'a> {
    db: &'a GatedDatabase,
    open: Option<WriteTransaction>,
    in_batch: u64,
    progress: &'a AtomicU64,
    cancel: &'a AtomicBool,
}

impl Batch<'_> {
    /// The open write transaction, begun with no fsync when none is open.
    fn txn(&mut self) -> crate::Result<&WriteTransaction> {
        let txn = match self.open.take() {
            Some(txn) => txn,
            None => {
                let mut txn = self.db.begin_write().map_err(scratch_err)?;
                txn.set_durability(Durability::None).map_err(scratch_err)?;
                txn
            }
        };
        let open: &WriteTransaction = self.open.insert(txn);
        Ok(open)
    }

    /// Count `n` replayed items and commit a full batch.
    fn replayed(&mut self, n: u64) -> crate::Result<()> {
        self.progress.fetch_add(n, Ordering::Relaxed);
        self.in_batch += n;
        if self.in_batch >= BUILD_BATCH_VERSIONS {
            self.commit()?;
        }
        Ok(())
    }

    /// Count one item the walk passed without replaying it.
    fn walked(&self) {
        self.progress.fetch_add(1, Ordering::Relaxed);
    }

    /// Commit the open write transaction, if any. A cancelled rebuild
    /// drops it and stops.
    fn commit(&mut self) -> crate::Result<()> {
        self.in_batch = 0;
        if let Err(e) = check_cancel(self.cancel) {
            self.open = None;
            return Err(e);
        }
        match self.open.take() {
            Some(txn) => txn.commit().map_err(scratch_err),
            None => Ok(()),
        }
    }
}

/// An error once the core cancelled the rebuild. It only stops the walk:
/// `TextBuildJob::run` reads the flag and answers `Cancelled`.
fn check_cancel(cancel: &AtomicBool) -> crate::Result<()> {
    if cancel.load(Ordering::Acquire) {
        return Err(crate::Error::ObjectNotInPrerequisiteState {
            object: "full-text rebuild".to_string(),
            detail: "cancelled by its core".to_string(),
        });
    }
    Ok(())
}

fn scratch_err(e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "inverted".into(),
        detail: format!("rebuild scratch write: {e}"),
    }
}

/// Remove a scratch database file. A file that does not exist is already
/// removed. Any other error files a report: the sweep at the next core open
/// removes the file.
pub(in crate::data::executor) fn remove_scratch(path: &std::path::Path) {
    use crate::data::executor::handlers::spill::cleanup::{SpillKind, remove_spill_file};
    // `remove_spill_file` files the report at the detection site.
    let _ = remove_spill_file(SpillKind::TextRebuild, path);
}
