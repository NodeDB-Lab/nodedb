// SPDX-License-Identifier: BUSL-1.1

//! The handles of one collection rebuild of the inverted index.
//!
//! [`FtsRebuildTicket`] pins a redb read snapshot of the whole sparse
//! database: the collection's stored document versions, its stored sync
//! frames, and its analyzer rows. `InvertedIndex::begin_rebuild` opens it on
//! the owning core, in the same step that opens the write journal, so the
//! snapshot and the journal meet with no gap. The ticket is `Send`: the
//! rebuild thread reads the snapshot and derives the collection's index
//! into a scratch database.
//!
//! [`FtsRebuilt`] carries the scratch database back to the owning core,
//! which cuts it over with `InvertedIndex::install_rebuild`.
//!
//! [`FtsCatchUp`] hands a pass whose journal passed its bound back to a
//! rebuild thread: a fresh snapshot, the scratch database of the pass, and
//! the documents written during the pass.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use redb::{Durability, ReadTransaction, ReadableDatabase as _};
use roaring::RoaringBitmap;

use super::core::InvertedIndex;
use super::errors::inverted_err;
use crate::engine::durability_gate::GatedDatabase;
use crate::engine::sparse::btree::storage_bytes::SPARSE_COUNTER;
use crate::engine::sparse::fts_redb::backend::rebuild_copy::{
    CollectionCopy, copy_collection_rows, copy_config_meta,
};
use crate::engine::storage_bytes::StorageDelta;

/// A pinned snapshot of one collection, ready to read on any thread.
pub struct FtsRebuildTicket {
    token: u64,
    database_id: u64,
    tid: u64,
    collection: String,
    txn: ReadTransaction,
    /// 0 for the first pass, then one per catch-up round.
    round: u32,
}

/// A rebuild pass whose journal passed its bound, ready for a catch-up
/// round on any thread.
pub struct FtsCatchUp {
    /// The fresh snapshot, pinned in the same step as the fresh journal.
    pub ticket: FtsRebuildTicket,
    /// The scratch database the ended pass derived.
    pub previous: FtsRebuilt,
    /// Every document written during the ended pass.
    pub touched: RoaringBitmap,
}

impl std::fmt::Debug for FtsCatchUp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtsCatchUp")
            .field("token", &self.ticket.token)
            .field("round", &self.ticket.round)
            .field("touched", &self.touched.len())
            .finish()
    }
}

/// A collection's index derived into a scratch database, ready to cut
/// over on the owning core.
pub struct FtsRebuilt {
    pub(super) token: u64,
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: String,
    pub(super) scratch: Arc<GatedDatabase>,
    scratch_path: PathBuf,
}

impl FtsRebuildTicket {
    pub(super) fn new(
        token: u64,
        database_id: u64,
        tid: u64,
        collection: String,
        txn: ReadTransaction,
        round: u32,
    ) -> Self {
        Self {
            token,
            database_id,
            tid,
            collection,
            txn,
            round,
        }
    }

    /// The token that ties this rebuild to its journal.
    pub fn token(&self) -> u64 {
        self.token
    }

    /// The pass this snapshot serves: 0 for the first, then one per
    /// catch-up round.
    pub fn round(&self) -> u32 {
        self.round
    }

    /// The database the collection belongs to.
    pub fn database_id(&self) -> u64 {
        self.database_id
    }

    /// The tenant the collection belongs to.
    pub fn tid(&self) -> u64 {
        self.tid
    }

    /// The collection this rebuild covers.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// The pinned snapshot: every row committed before the journal opened.
    pub fn snapshot(&self) -> &ReadTransaction {
        &self.txn
    }

    /// Bind the collection's analyzer, language and fuzzy rows from the
    /// snapshot into `scratch`, so it analyzes text as the live index does.
    pub fn seed_scratch_config(&self, scratch: &InvertedIndex) -> crate::Result<()> {
        let txn = scratch
            .backend()
            .db()
            .begin_write()
            .map_err(|e| inverted_err("rebuild scratch config txn", e))?;
        copy_config_meta(
            &self.txn,
            &txn,
            self.database_id,
            self.tid,
            &self.collection,
        )?;
        txn.commit()
            .map_err(|e| inverted_err("rebuild scratch config commit", e))
    }

    /// Release the snapshot and hand the scratch database at `scratch_path`
    /// to the cutover.
    pub fn finish(self, scratch: Arc<GatedDatabase>, scratch_path: PathBuf) -> FtsRebuilt {
        FtsRebuilt {
            token: self.token,
            database_id: self.database_id,
            tid: self.tid,
            collection: self.collection,
            scratch,
            scratch_path,
        }
    }
}

impl FtsCatchUp {
    /// Copy every row of the collection the ended pass derived into
    /// `scratch`, except the rows of the documents written during it. A
    /// catch-up round derives those again from the fresh snapshot.
    ///
    /// `on_row` runs once per copied row. Its error ends the copy.
    pub fn carry_rows(
        &self,
        scratch: &InvertedIndex,
        on_row: &dyn Fn() -> crate::Result<()>,
    ) -> crate::Result<()> {
        let previous = self
            .previous
            .scratch
            .begin_read()
            .map_err(|e| inverted_err("catch-up previous scratch read txn", e))?;
        let mut txn = scratch
            .backend()
            .db()
            .begin_write()
            .map_err(|e| inverted_err("catch-up scratch txn", e))?;
        // The scratch database is disposable and read in the same process.
        txn.set_durability(Durability::None)
            .map_err(|e| inverted_err("catch-up scratch durability", e))?;
        let mut delta = StorageDelta::default();
        copy_collection_rows(
            &previous,
            &txn,
            CollectionCopy {
                database_id: self.ticket.database_id,
                tid: self.ticket.tid,
                collection: &self.ticket.collection,
                skip: &self.touched,
                on_row,
            },
            &mut delta,
        )?;
        delta.apply(&txn, SPARSE_COUNTER)?;
        txn.commit()
            .map_err(|e| inverted_err("catch-up scratch commit", e))
    }
}

impl FtsRebuilt {
    /// The token that ties this rebuild to its journal.
    pub fn token(&self) -> u64 {
        self.token
    }

    /// The collection this rebuild covers.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// The file of the scratch database. The cutover's caller removes it
    /// once the rebuild is dropped.
    pub fn scratch_path(&self) -> &Path {
        &self.scratch_path
    }
}
