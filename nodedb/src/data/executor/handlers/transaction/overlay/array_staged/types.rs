// SPDX-License-Identifier: BUSL-1.1

//! Shared value types for the ARRAY transaction staging overlay: the staged
//! cell put and the per-array staged-write state.

use std::collections::{HashMap, HashSet};

use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::Surrogate;

/// One staged cell address: the full coordinate tuple, exactly the key the
/// durable memtable / segment rows are addressed by.
pub(super) type CellKey = Vec<CoordValue>;

/// One staged cell put. Carries every field the durable
/// `crate::engine::array::wal::ArrayPutCell` carries (minus the coordinate,
/// which is the map key) so a same-transaction read reconstructs a real cell
/// row with its surrogate and bitemporal bounds intact.
#[derive(Debug, Clone, PartialEq)]
pub struct StagedCellPut {
    pub attrs: Vec<CellValue>,
    pub surrogate: Surrogate,
    pub system_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
}

impl StagedCellPut {
    pub(super) fn memory_size_estimate(&self) -> usize {
        let attrs: usize = self.attrs.iter().map(cell_value_size).sum();
        attrs + std::mem::size_of::<Surrogate>() + 3 * std::mem::size_of::<i64>()
    }
}

/// Staged cell mutations for a single array within one transaction.
#[derive(Debug, Default)]
pub(super) struct ArrayCollectionOverlay {
    /// Staged cell put-set: coordinate -> cell body.
    pub(super) pending_cells: HashMap<CellKey, StagedCellPut>,
    /// Staged cell delete-set (tombstones).
    pub(super) pending_tombstones: HashSet<CellKey>,
}

impl ArrayCollectionOverlay {
    pub(super) fn memory_size_estimate(&self) -> usize {
        let cells: usize = self
            .pending_cells
            .iter()
            .map(|(coord, put)| coord_size(coord) + put.memory_size_estimate())
            .sum();
        let tombstones: usize = self.pending_tombstones.iter().map(|c| coord_size(c)).sum();
        cells + tombstones
    }
}

fn coord_size(coord: &[CoordValue]) -> usize {
    coord
        .iter()
        .map(|c| match c {
            CoordValue::Int64(_) | CoordValue::Float64(_) | CoordValue::TimestampMs(_) => {
                std::mem::size_of::<i64>()
            }
            CoordValue::String(s) => s.len(),
        })
        .sum()
}

fn cell_value_size(v: &CellValue) -> usize {
    match v {
        CellValue::Int64(_) | CellValue::Float64(_) => std::mem::size_of::<i64>(),
        CellValue::String(s) => s.len(),
        CellValue::Bytes(b) => b.len(),
        CellValue::Null => 0,
    }
}
