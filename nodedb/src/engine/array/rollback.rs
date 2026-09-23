// SPDX-License-Identifier: BUSL-1.1

//! Withdraw a cell write from an array's memtable.
//!
//! Every cell write lands in the memtable tile its coordinate and system
//! time map to. [`ArrayEngine::snapshot_tiles`] copies those tiles before the
//! write, and [`ArrayEngine::restore_tiles`] puts them back. The write must
//! not flush in between: [`ArrayEngine::put_cells_unflushed`] and
//! [`ArrayEngine::delete_cells_unflushed`] stamp the memtable without the
//! threshold flush, and [`ArrayEngine::flush_if_full`] runs it afterwards.

use nodedb_array::tile::tile_id_for_cell;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::{ArrayId, TileId};

use super::engine::{ArrayEngine, ArrayEngineResult};
use super::memtable::TileBuffer;
use super::wal::{ArrayDeleteCell, ArrayPutCell};
use super::write::{stamp_delete_cells, stamp_put_cells};

/// The memtable tiles a write touches, as they were before it.
#[derive(Debug)]
pub struct ArrayTileSnapshot {
    tiles: Vec<(TileId, Option<TileBuffer>)>,
}

impl ArrayEngine {
    /// Copy the memtable tiles the cells at `cells` (coordinate and system
    /// time) map to.
    pub fn snapshot_tiles<'a>(
        &self,
        id: &ArrayId,
        cells: impl IntoIterator<Item = (&'a [CoordValue], i64)>,
    ) -> ArrayEngineResult<ArrayTileSnapshot> {
        let store = self.store(id)?;
        let schema = store.schema().clone();
        let mut tiles: Vec<(TileId, Option<TileBuffer>)> = Vec::new();
        for (coord, system_from_ms) in cells {
            let tile = tile_id_for_cell(&schema, coord, system_from_ms)?;
            if tiles.iter().any(|(seen, _)| *seen == tile) {
                continue;
            }
            tiles.push((tile, store.memtable.tile(&tile).cloned()));
        }
        Ok(ArrayTileSnapshot { tiles })
    }

    /// Put every tile of `snapshot` back.
    pub fn restore_tiles(
        &mut self,
        id: &ArrayId,
        snapshot: ArrayTileSnapshot,
    ) -> ArrayEngineResult<()> {
        let store = self.store_mut(id)?;
        for (tile, buffer) in snapshot.tiles {
            store.memtable.restore_tile(tile, buffer);
        }
        Ok(())
    }

    /// Stamp `cells` into the memtable without the threshold flush.
    pub fn put_cells_unflushed(
        &mut self,
        id: &ArrayId,
        cells: Vec<ArrayPutCell>,
        wal_lsn: u64,
    ) -> ArrayEngineResult<()> {
        if cells.is_empty() {
            return Ok(());
        }
        stamp_put_cells(self.store_mut(id)?, cells, wal_lsn)
    }

    /// Stamp tombstones for `cells` into the memtable without the threshold
    /// flush.
    pub fn delete_cells_unflushed(
        &mut self,
        id: &ArrayId,
        cells: Vec<ArrayDeleteCell>,
        wal_lsn: u64,
    ) -> ArrayEngineResult<()> {
        if cells.is_empty() {
            return Ok(());
        }
        stamp_delete_cells(self.store_mut(id)?, cells, wal_lsn)
    }

    /// Run the threshold flush the unflushed writes skipped.
    pub fn flush_if_full(&mut self, id: &ArrayId) -> ArrayEngineResult<()> {
        self.maybe_flush(id)
    }
}
