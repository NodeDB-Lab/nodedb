// SPDX-License-Identifier: BUSL-1.1

//! `ArrayEngine` — Data-Plane handle that owns every array's LSM store.
//!
//! The engine is `!Send` (`HashMap` of stores with no sync wrappers).
//! Persistence is owned by the Control Plane: SQL DDL/DML allocates the
//! WAL LSN before dispatch, and the engine only stamps the supplied LSN
//! into the memtable / segment manifest. Recovery routes through the
//! same shared `stamp_*` core.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::types::ArrayId;

use super::store::ArrayStore;

#[derive(Clone)]
pub struct ArrayEngineConfig {
    /// Root directory containing one subdirectory per array.
    pub root: PathBuf,
    /// Auto-flush when a memtable holds at least this many cells.
    pub flush_cell_threshold: usize,
    /// Optional at-rest encryption key for SEGA segment envelopes.
    pub(super) kek: Option<nodedb_wal::crypto::WalEncryptionKey>,
}

impl std::fmt::Debug for ArrayEngineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayEngineConfig")
            .field("root", &self.root)
            .field("flush_cell_threshold", &self.flush_cell_threshold)
            .field("kek", &self.kek.is_some())
            .finish()
    }
}

impl ArrayEngineConfig {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            flush_cell_threshold: 4096,
            kek: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ArrayEngineError {
    #[error(transparent)]
    Array(#[from] nodedb_array::ArrayError),
    #[error(transparent)]
    Store(#[from] super::store::catalog::ArrayStoreError),
    #[error(transparent)]
    Compaction(#[from] super::compaction::merger::CompactionError),
    #[error("array engine io: {detail}")]
    Io { detail: String },
    #[error("unknown array: {0}")]
    UnknownArray(String),
    #[error("array '{name}' open with different schema")]
    SchemaMismatch { name: String },
}

pub type ArrayEngineResult<T> = Result<T, ArrayEngineError>;

pub struct ArrayEngine {
    pub(super) cfg: ArrayEngineConfig,
    pub(super) arrays: HashMap<ArrayId, ArrayStore>,
}

impl ArrayEngine {
    pub fn new(cfg: ArrayEngineConfig) -> ArrayEngineResult<Self> {
        std::fs::create_dir_all(&cfg.root).map_err(|e| ArrayEngineError::Io {
            detail: format!("mkdir {:?}: {e}", cfg.root),
        })?;
        Ok(Self {
            cfg,
            arrays: HashMap::new(),
        })
    }

    pub fn config(&self) -> &ArrayEngineConfig {
        &self.cfg
    }

    /// Set the memtable cell count at which [`Self::needs_flush`] reports an
    /// array full.
    #[cfg(test)]
    pub(crate) fn set_flush_cell_threshold(&mut self, cells: usize) {
        self.cfg.flush_cell_threshold = cells;
    }

    /// Open or attach to an array.
    ///
    /// Idempotent: if the array is already open with the same
    /// `schema_hash`, this is a no-op so SQL read handlers can call
    /// `open_array` lazily on every request without losing state.
    /// If the array is open with a *different* schema_hash, returns
    /// [`ArrayEngineError::SchemaMismatch`].
    pub fn open_array(
        &mut self,
        id: ArrayId,
        schema: Arc<ArraySchema>,
        schema_hash: u64,
    ) -> ArrayEngineResult<()> {
        if let Some(existing) = self.arrays.get(&id) {
            if existing.schema_hash() == schema_hash {
                return Ok(());
            }
            return Err(ArrayEngineError::SchemaMismatch {
                name: id.name.clone(),
            });
        }
        let dir = array_dir(&self.cfg.root, &id);
        let tombstone = drop_tombstone_dir(&dir);
        if tombstone.exists() {
            return Err(ArrayEngineError::Io {
                detail: format!("array {id:?} has an unpurged drop tombstone at {tombstone:?}"),
            });
        }
        // The key goes in through the constructor: `ArrayStore::open` opens
        // every segment the manifest names, and an encrypted segment opened
        // without it is a typed error.
        let store = ArrayStore::open(dir, schema, schema_hash, self.cfg.kek.clone())?;
        self.arrays.insert(id, store);
        Ok(())
    }

    pub fn array_ids(&self) -> impl Iterator<Item = &ArrayId> {
        self.arrays.keys()
    }

    /// Install the at-rest encryption key for SEGA segment envelopes.
    ///
    /// Propagates the key to every currently-open `ArrayStore` and stores it
    /// so newly-opened arrays also receive it via `open_array`.
    pub fn set_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.cfg.kek = Some(kek.clone());
        for store in self.arrays.values_mut() {
            store.set_kek(kek.clone());
        }
    }

    /// Close an array store and atomically rename its directory to a
    /// deterministic tombstone. A tombstone is intentionally retained until
    /// catalog/surrogate deletion has committed on the Control Plane.
    pub fn stage_drop_array(&mut self, id: &ArrayId) -> ArrayEngineResult<()> {
        let _ = self.arrays.remove(id);
        let dir = array_dir(&self.cfg.root, id);
        let tombstone = drop_tombstone_dir(&dir);
        match (dir.exists(), tombstone.exists()) {
            (true, false) => std::fs::rename(&dir, &tombstone).map_err(|e| ArrayEngineError::Io {
                detail: format!("stage array drop {dir:?} -> {tombstone:?}: {e}"),
            }),
            (false, true) | (false, false) => Ok(()),
            (true, true) => Err(ArrayEngineError::Io {
                detail: format!(
                    "array drop has both live and tombstone directories: {dir:?}, {tombstone:?}"
                ),
            }),
        }
    }

    /// Restore a staged drop. Idempotent when this core had no array store.
    pub fn restore_drop_array(&mut self, id: &ArrayId) -> ArrayEngineResult<()> {
        let dir = array_dir(&self.cfg.root, id);
        let tombstone = drop_tombstone_dir(&dir);
        match (dir.exists(), tombstone.exists()) {
            (false, true) => std::fs::rename(&tombstone, &dir).map_err(|e| ArrayEngineError::Io {
                detail: format!("restore array drop {tombstone:?} -> {dir:?}: {e}"),
            }),
            (true, false) | (false, false) => Ok(()),
            (true, true) => Err(ArrayEngineError::Io {
                detail: format!(
                    "array restore has both live and tombstone directories: {dir:?}, {tombstone:?}"
                ),
            }),
        }
    }

    /// Permanently remove a staged drop tombstone. A live directory is an
    /// invariant violation: purging it could destroy a recreated array.
    pub fn purge_drop_array(&mut self, id: &ArrayId) -> ArrayEngineResult<()> {
        let dir = array_dir(&self.cfg.root, id);
        let tombstone = drop_tombstone_dir(&dir);
        if dir.exists() && tombstone.exists() {
            return Err(ArrayEngineError::Io {
                detail: format!("refusing to purge tombstone while live array exists: {dir:?}"),
            });
        }
        if tombstone.exists() {
            std::fs::remove_dir_all(&tombstone).map_err(|e| ArrayEngineError::Io {
                detail: format!("purge array tombstone {tombstone:?}: {e}"),
            })?;
        }
        Ok(())
    }

    /// Clear a deterministic tombstone left by a *finalized* prior DROP before
    /// an authorized CREATE opens the replacement store. The caller is
    /// responsible for proving catalog absence before installing the new entry;
    /// this method only enforces the filesystem half of that invariant.
    pub fn purge_finalized_drop_before_open(&mut self, id: &ArrayId) -> ArrayEngineResult<()> {
        self.purge_drop_array(id)
    }

    pub fn store(&self, id: &ArrayId) -> ArrayEngineResult<&ArrayStore> {
        self.arrays
            .get(id)
            .ok_or_else(|| ArrayEngineError::UnknownArray(format!("{:?}", id)))
    }

    pub fn store_mut(&mut self, id: &ArrayId) -> ArrayEngineResult<&mut ArrayStore> {
        self.arrays
            .get_mut(id)
            .ok_or_else(|| ArrayEngineError::UnknownArray(format!("{:?}", id)))
    }

    /// Drop superseded tile-versions older than `cutoff_system_ms` for the
    /// array named `array_id`.
    ///
    /// `tenant_id` and `database_id` identify the array's catalog namespace.
    ///
    /// Returns the number of tile-versions dropped. Returns `Ok(0)` when the
    /// array is not open (idempotent — array may have been dropped between
    /// schedule and execution).
    pub fn temporal_purge(
        &mut self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        array_id: &str,
        cutoff_system_ms: i64,
    ) -> ArrayEngineResult<u64> {
        let aid = ArrayId::in_database(tenant_id, database_id, array_id);
        // Idempotent: array may not be open on this core.
        if !self.arrays.contains_key(&aid) {
            return Ok(0);
        }

        let schema = {
            let store = self.store(&aid)?;
            store.schema().as_ref().clone()
        };

        let plan = {
            let store = self.store(&aid)?;
            super::purge::plan(store, cutoff_system_ms, &schema)?
        };

        if plan.segment_actions.is_empty() {
            return Ok(0);
        }

        let store = self.store_mut(&aid)?;
        let dropped = super::purge::execute(store, plan)?;
        Ok(dropped)
    }
}

fn drop_tombstone_dir(dir: &std::path::Path) -> PathBuf {
    let name = dir.file_name().unwrap_or_default().to_string_lossy();
    dir.with_file_name(format!(".{name}.drop-pending"))
}

/// Build the store directory for one array. Hex-encoding the name keeps
/// separators and traversal components out of the path.
pub(super) fn array_dir(root: &std::path::Path, id: &ArrayId) -> PathBuf {
    let encoded_name: String = id
        .name
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    root.join("arrays-v2")
        .join(format!("d{}", id.database_id.as_u64()))
        .join(format!("t{}", id.tenant_id.as_u64()))
        .join(encoded_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::array::test_support::{aid, schema};

    #[test]
    fn open_array_idempotent_for_same_hash() {
        use crate::engine::array::wal::ArrayPutCell;
        use nodedb_array::types::cell_value::value::CellValue;
        use nodedb_array::types::coord::value::CoordValue;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        // First open registers the store and a put stamps memtable state.
        e.open_array(aid(), schema(), 0xC0FFEE).unwrap();
        e.put_cells(
            &aid(),
            vec![ArrayPutCell {
                coord: vec![CoordValue::Int64(4), CoordValue::Int64(4)],
                attrs: vec![CellValue::Int64(99)],
                surrogate: nodedb_types::Surrogate::ZERO,
                system_from_ms: 0,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            }],
            1,
        )
        .unwrap();
        // Second open with the same hash must NOT reset state.
        e.open_array(aid(), schema(), 0xC0FFEE).unwrap();
        assert_eq!(
            e.store(&aid()).unwrap().memtable.stats().cell_count,
            1,
            "idempotent re-open must preserve memtable contents"
        );
    }

    #[test]
    fn open_array_rejects_different_hash() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), schema(), 0xC0FFEE).unwrap();
        let err = e.open_array(aid(), schema(), 0xDEADBEEF).unwrap_err();
        match err {
            ArrayEngineError::SchemaMismatch { name } => assert_eq!(name, "g"),
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }
    }

    #[test]
    fn staged_drop_restores_or_purges_without_exposing_stale_data() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let id = aid();
        let mut engine =
            ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        engine.open_array(id.clone(), schema(), 0xBEEF).unwrap();
        let live = array_dir(dir.path(), &id);
        let tombstone = drop_tombstone_dir(&live);

        engine.stage_drop_array(&id).unwrap();
        assert!(!live.exists());
        assert!(tombstone.exists());
        assert!(engine.open_array(id.clone(), schema(), 0xBEEF).is_err());

        engine.restore_drop_array(&id).unwrap();
        assert!(live.exists());
        assert!(!tombstone.exists());

        engine.stage_drop_array(&id).unwrap();
        // This is the authorized CREATE retry path after the prior DROP has
        // finalized its catalog deletion but its all-core purge was interrupted.
        engine.purge_finalized_drop_before_open(&id).unwrap();
        engine.open_array(id.clone(), schema(), 0xCAFE).unwrap();
        assert!(live.exists());
        assert!(!tombstone.exists());
    }

    #[test]
    fn reopen_loads_manifest_and_segments() {
        use crate::engine::array::test_support::put_one;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let aid = aid();
        {
            let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
            e.open_array(aid.clone(), schema(), 0xBEEF).unwrap();
            put_one(&mut e, 1, 1, 7, 1);
            e.flush(&aid, crate::types::replay_stamp::ReplayStamp::through(2))
                .unwrap();
        }
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid.clone(), schema(), 0xBEEF).unwrap();
        let m = e.store(&aid).unwrap().manifest();
        assert_eq!(m.segments.len(), 1);
        assert_eq!(
            m.replay,
            crate::types::replay_stamp::ReplayStamp::through(2)
        );
    }
}
