//! Per-tile entry stored in the segment footer.
//!
//! Each entry locates a tile payload inside the segment file and
//! carries its [`TileMBR`] for predicate pushdown. The R-tree built by
//! [`super::super::mbr_index`] indexes these entries.

use serde::{Deserialize, Serialize};

use crate::tile::mbr::TileMBR;
use crate::types::TileId;

/// Discriminant for the on-disk tile payload variant.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
pub enum TileKind {
    Sparse = 0,
    Dense = 1,
}

/// One entry in the segment's per-tile table.
///
/// `offset` and `length` cover the **framed** block (length prefix +
/// payload + CRC). Readers feed the slice through
/// [`super::framing::BlockFraming::decode`] to validate and extract
/// the inner zerompk-encoded payload.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TileEntry {
    pub tile_id: TileId,
    pub kind: TileKind,
    pub offset: u64,
    pub length: u32,
    pub mbr: TileMBR,
}

impl TileEntry {
    pub fn new(tile_id: TileId, kind: TileKind, offset: u64, length: u32, mbr: TileMBR) -> Self {
        Self {
            tile_id,
            kind,
            offset,
            length,
            mbr,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_entry_round_trip_msgpack() {
        let mbr = TileMBR::new(0, 0);
        let e = TileEntry::new(TileId::snapshot(42), TileKind::Sparse, 100, 256, mbr);
        let bytes = zerompk::to_msgpack_vec(&e).unwrap();
        let d: TileEntry = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(d, e);
    }
}
