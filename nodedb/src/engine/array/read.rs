//! Read-path methods for `ArrayEngine`.

use nodedb_array::segment::{MbrQueryPredicate, TilePayload};
use nodedb_array::types::ArrayId;

use super::engine::{ArrayEngine, ArrayEngineResult};

impl ArrayEngine {
    pub fn scan_tiles(
        &self,
        id: &ArrayId,
        pred: &MbrQueryPredicate,
    ) -> ArrayEngineResult<Vec<TilePayload>> {
        Ok(self.store(id)?.scan_tiles(pred)?)
    }

    /// Like `scan_tiles` but pairs each tile with its Hilbert prefix for
    /// per-shard range filtering in distributed aggregate queries.
    pub fn scan_tiles_with_hilbert_prefix(
        &self,
        id: &ArrayId,
        pred: &MbrQueryPredicate,
    ) -> ArrayEngineResult<Vec<(u64, TilePayload)>> {
        Ok(self.store(id)?.scan_tiles_with_hilbert_prefix(pred)?)
    }
}
