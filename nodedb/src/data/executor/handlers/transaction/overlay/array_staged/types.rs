// SPDX-License-Identifier: BUSL-1.1

//! Shared value types for the ARRAY transaction staging overlay: the staged
//! cell put, the staged cell state, and the per-array staged-write state.

use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::Surrogate;
use zerompk::{FromMessagePack, Read, ToMessagePack, Write};

use crate::data::executor::handlers::spill::tiered::{SpillSpace, SpillWeight, TieredMap};

/// One staged cell address: the full coordinate tuple, exactly the key the
/// durable memtable / segment rows are addressed by.
pub(super) type CellKey = Vec<CoordValue>;

/// One staged cell put. Carries every field the durable
/// `crate::engine::array::wal::ArrayPutCell` carries (minus the coordinate,
/// which is the map key) so a same-transaction read reconstructs a real cell
/// row with its surrogate and bitemporal bounds intact.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StagedCellPut {
    pub attrs: Vec<CellValue>,
    pub surrogate: Surrogate,
    pub system_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
}

impl SpillWeight for StagedCellPut {
    fn spill_weight(&self) -> usize {
        let attrs: usize = self.attrs.iter().map(cell_value_size).sum();
        attrs + std::mem::size_of::<StagedCellPut>()
    }
}

/// One staged cell: a put or a tombstone. A put and a tombstone of one
/// coordinate replace each other, so a cell is never in both states.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CellSlot {
    Put(StagedCellPut),
    Tombstone,
}

/// A put spills as its cell, a tombstone as `nil`.
impl ToMessagePack for CellSlot {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            CellSlot::Put(put) => put.write(writer),
            CellSlot::Tombstone => writer.write_nil(),
        }
    }
}

impl<'a> FromMessagePack<'a> for CellSlot {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        const NIL: u8 = 0xc0;
        if reader.peek_marker()? == NIL {
            reader.read_nil()?;
            return Ok(CellSlot::Tombstone);
        }
        Ok(CellSlot::Put(StagedCellPut::read(reader)?))
    }
}

impl SpillWeight for CellSlot {
    fn spill_weight(&self) -> usize {
        match self {
            CellSlot::Put(put) => put.spill_weight(),
            CellSlot::Tombstone => std::mem::size_of::<CellSlot>(),
        }
    }
}

impl SpillWeight for CoordValue {
    fn spill_weight(&self) -> usize {
        match self {
            CoordValue::Int64(_) | CoordValue::Float64(_) | CoordValue::TimestampMs(_) => {
                std::mem::size_of::<i64>()
            }
            CoordValue::String(s) => s.len(),
        }
    }
}

/// Staged cell mutations for a single array within one transaction.
#[derive(Debug, Default)]
pub(super) struct ArrayCollectionOverlay {
    /// Staged cells by coordinate.
    pub(super) cells: TieredMap<CellKey, CellSlot>,
}

impl ArrayCollectionOverlay {
    pub(super) fn spill(&mut self, space: &mut SpillSpace) -> crate::Result<()> {
        self.cells.spill(space, "cell")
    }

    pub(super) fn rollback(&mut self, bound: u64, space: &mut SpillSpace) -> crate::Result<()> {
        self.cells.rollback(bound, space, "cell")
    }
}

fn cell_value_size(v: &CellValue) -> usize {
    match v {
        CellValue::Int64(_) | CellValue::Float64(_) => std::mem::size_of::<i64>(),
        CellValue::UInt8(_) | CellValue::Int8(_) => std::mem::size_of::<u8>(),
        CellValue::Int16(_) | CellValue::UInt16(_) => std::mem::size_of::<u16>(),
        CellValue::Int32(_) | CellValue::UInt32(_) | CellValue::Float32(_) => {
            std::mem::size_of::<u32>()
        }
        CellValue::String(s) => s.len(),
        CellValue::Bytes(b) => b.len(),
        CellValue::Null => 0,
    }
}
