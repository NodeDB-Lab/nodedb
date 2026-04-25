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
}
