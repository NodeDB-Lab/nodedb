//! Write-path methods for `ArrayEngine`: put/delete cells with shared
//! memtable-stamping core. Recovery routes through `stamp_*` directly so
//! ordering matches the live dispatch path.

use nodedb_array::types::ArrayId;

use super::engine::{ArrayEngine, ArrayEngineResult};
use super::store::ArrayStore;
use super::wal::ArrayPutCell;

impl ArrayEngine {
    /// Stamps the memtable with a caller-supplied LSN. The Control Plane
    /// allocates the LSN at WAL-append time and the Data Plane just
    /// stamps; this is the only write path for the engine.
    pub fn put_cells(
        &mut self,
        id: &ArrayId,
        cells: Vec<ArrayPutCell>,
        wal_lsn: u64,
    ) -> ArrayEngineResult<()> {
        if cells.is_empty() {
            return Ok(());
        }
        stamp_put_cells(self.store_mut(id)?, cells, wal_lsn)?;
        self.maybe_flush(id)?;
        Ok(())
    }

    /// Stamps memtable tombstones (or GDPR erasure markers) with a
    /// caller-supplied LSN. Each cell in `cells` independently selects
    /// the sentinel byte via its `erasure` flag.
    pub fn delete_cells(
        &mut self,
        id: &ArrayId,
        cells: Vec<super::wal::ArrayDeleteCell>,
        wal_lsn: u64,
    ) -> ArrayEngineResult<()> {
        if cells.is_empty() {
            return Ok(());
        }
        stamp_delete_cells(self.store_mut(id)?, cells, wal_lsn)?;
        self.maybe_flush(id)?;
        Ok(())
    }

    /// GDPR-erase a single cell at `(array, coord, system_from_ms)`.
    ///
    /// Writes the `CELL_GDPR_ERASURE_SENTINEL` (`0xFE`) into the memtable as
    /// a new tile version at `system_from_ms`. Any read at
    /// `system_as_of >= system_from_ms` returns `CeilingResult::Erased` for
    /// this coord. The sentinel is physically removed by compaction once the
    /// tile version falls outside the array's `audit_retain_ms` horizon.
    ///
    /// This is equivalent to calling `delete_cells` with `erasure: true`
    /// on a single cell, exposed as a named method for clarity at call sites.
    pub fn gdpr_erase_cell(
        &mut self,
        id: &ArrayId,
        coord: Vec<nodedb_array::types::coord::value::CoordValue>,
        system_from_ms: i64,
        wal_lsn: u64,
    ) -> ArrayEngineResult<()> {
        use super::wal::ArrayDeleteCell;
        self.delete_cells(
            id,
            vec![ArrayDeleteCell {
                coord,
                system_from_ms,
                erasure: true,
            }],
            wal_lsn,
        )
    }

    pub(super) fn maybe_flush(&mut self, id: &ArrayId) -> ArrayEngineResult<()> {
        let stats = self.store(id)?.memtable.stats();
        if stats.cell_count >= self.cfg.flush_cell_threshold {
            // Auto-flush in the threshold path uses an LSN of 0 — the
            // caller's WAL-side flush record (if any) carries the real
            // watermark; auto-flushes are always followed by another
            // explicit Flush from the Control Plane in production.
            self.flush(id, 0)?;
        }
        Ok(())
    }
}

/// Shared memtable-stamping core for puts. Recovery routes through here
/// directly so write ordering is identical to the live dispatch path.
pub(crate) fn stamp_put_cells(
    store: &mut ArrayStore,
    cells: Vec<ArrayPutCell>,
    lsn: u64,
) -> ArrayEngineResult<()> {
    let schema = store.schema().clone();
    for c in cells {
        store.memtable.put_cell(
            &schema,
            super::memtable::PutCell {
                coord: c.coord,
                attrs: c.attrs,
                surrogate: c.surrogate,
                system_from_ms: c.system_from_ms,
                valid_from_ms: c.valid_from_ms,
                valid_until_ms: c.valid_until_ms,
                lsn,
            },
        )?;
    }
    Ok(())
}

pub(crate) fn stamp_delete_cells(
    store: &mut ArrayStore,
    cells: Vec<super::wal::ArrayDeleteCell>,
    lsn: u64,
) -> ArrayEngineResult<()> {
    let schema = store.schema().clone();
    for c in cells {
        if c.erasure {
            store
                .memtable
                .erase_cell(&schema, c.coord, c.system_from_ms, lsn)?;
        } else {
            store
                .memtable
                .delete_cell(&schema, c.coord, c.system_from_ms, lsn)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
    use crate::engine::array::test_support::{aid, put_one, schema};
    use nodedb_array::segment::MbrQueryPredicate;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use tempfile::TempDir;

    #[test]
    fn auto_flush_triggers_at_threshold() {
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 2;
        let mut e = ArrayEngine::new(cfg).unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();
        for i in 0..2 {
            put_one(&mut e, i, i, i, (i as u64) + 1);
        }
        assert!(!e.store(&aid()).unwrap().manifest().segments.is_empty());
    }

    #[test]
    fn put_cells_stamps_memtable_with_supplied_lsn() {
        let dir = TempDir::new().unwrap();
        let mut e = ArrayEngine::new(ArrayEngineConfig::new(dir.path().to_path_buf())).unwrap();
        e.open_array(aid(), schema(), 0xCAFE).unwrap();

        let cells = vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(3), CoordValue::Int64(5)],
            attrs: vec![CellValue::Int64(77)],
            surrogate: nodedb_types::Surrogate::ZERO,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }];
        e.put_cells(&aid(), cells, 42).unwrap();
        let seg = e.flush(&aid(), 43).unwrap().unwrap();
        assert_eq!(seg.flush_lsn, 43);

        let pred = MbrQueryPredicate::default();
        let tiles = e.scan_tiles(&aid(), &pred).unwrap();
        assert!(!tiles.is_empty());
    }
}
