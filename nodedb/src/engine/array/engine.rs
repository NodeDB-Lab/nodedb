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

#[derive(Debug, Clone)]
pub struct ArrayEngineConfig {
    /// Root directory containing one subdirectory per array.
    pub root: PathBuf,
    /// Auto-flush when a memtable holds at least this many cells.
    pub flush_cell_threshold: usize,
}

impl ArrayEngineConfig {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            flush_cell_threshold: 4096,
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
        let store = ArrayStore::open(dir, schema, schema_hash)?;
        self.arrays.insert(id, store);
        Ok(())
    }

    pub fn array_ids(&self) -> impl Iterator<Item = &ArrayId> {
        self.arrays.keys()
    }

    /// Drop the per-core store for `id` and best-effort remove the
    /// on-disk segment directory. Called by the `DropArray` dispatch
    /// handler during `DROP ARRAY` broadcast so a follow-up
    /// `CREATE ARRAY` of the same name (potentially with a different
    /// schema) doesn't carry stale memtable / segment state.
    ///
    /// Idempotent: returns `Ok(())` when no store is open and no
    /// directory exists. Directory-removal errors are surfaced as
    /// `ArrayEngineError::Io` so the broadcast's status reflects the
    /// failure.
    pub fn drop_array(&mut self, id: &ArrayId) -> ArrayEngineResult<()> {
        let _ = self.arrays.remove(id);
        let dir = array_dir(&self.cfg.root, id);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(|e| ArrayEngineError::Io {
                detail: format!("rmdir {dir:?}: {e}"),
            })?;
        }
        Ok(())
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
}

pub(super) fn array_dir(root: &std::path::Path, id: &ArrayId) -> PathBuf {
    root.join(format!("t{}-{}", id.tenant_id.as_u32(), id.name))
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
    fn reopen_loads_manifest_and_segments() {
        use crate::engine::array::test_support::put_one;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let aid = aid();
        {
            let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
            e.open_array(aid.clone(), schema(), 0xBEEF).unwrap();
            put_one(&mut e, 1, 1, 7, 1);
            e.flush(&aid, 2).unwrap();
        }
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid.clone(), schema(), 0xBEEF).unwrap();
        let m = e.store(&aid).unwrap().manifest();
        assert_eq!(m.segments.len(), 1);
        assert!(m.durable_lsn > 0);
    }
}
