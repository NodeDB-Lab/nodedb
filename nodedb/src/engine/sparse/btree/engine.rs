// SPDX-License-Identifier: BUSL-1.1

//! The [`SparseEngine`] handle itself: database lifecycle (open / table
//! bootstrap) and the raw transaction + database accessors every other sparse
//! module builds on.

use std::path::Path;
use std::sync::Arc;

use redb::{Database, WriteTransaction};
use tracing::info;

use super::chain_head::CHAIN_HEADS;
use super::crdt_dead_letter::CRDT_DEAD_LETTERS;
use super::tables::{DOCUMENTS, INDEXES, redb_err};

/// redb-backed B-Tree storage engine for sparse/metadata queries.
pub struct SparseEngine {
    pub(in crate::engine::sparse) db: Arc<Database>,
}

impl SparseEngine {
    /// Open or create the sparse engine database at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| redb_err("open", e))?;

        let write_txn = db.begin_write().map_err(|e| redb_err("write txn", e))?;
        {
            let _ = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open documents table", e))?;
            let _ = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes table", e))?;
            // Created unconditionally, including for databases written before
            // chain heads were persisted: `load_chain_heads` runs at every boot
            // and a read transaction cannot open a table that does not exist.
            let _ = write_txn
                .open_table(CHAIN_HEADS)
                .map_err(|e| redb_err("open chain heads table", e))?;
            // Read transactions open it when a CRDT engine is created.
            let _ = write_txn
                .open_table(CRDT_DEAD_LETTERS)
                .map_err(|e| redb_err("open crdt dead letters table", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;

        info!(path = %path.display(), "sparse engine opened");

        let engine = Self { db: Arc::new(db) };
        engine.ensure_documents_versioned_table()?;
        engine.ensure_indexes_versioned_table()?;
        Ok(engine)
    }

    /// Begin a write transaction on the underlying database.
    pub fn begin_write(&self) -> crate::Result<WriteTransaction> {
        self.db
            .begin_write()
            .map_err(|e| redb_err("begin write txn", e))
    }

    /// Get the underlying database handle.
    pub fn db(&self) -> &Arc<Database> {
        &self.db
    }
}
