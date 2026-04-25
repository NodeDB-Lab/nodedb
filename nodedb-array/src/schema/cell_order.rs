//! Cell-order and tile-order strategies.

use serde::{Deserialize, Serialize};

/// Within-tile cell ordering strategy.
///
/// `Hilbert` is the default for ND locality — neighboring coordinates
/// in any dim project to neighboring positions in the linear tile
/// payload, which compresses well and preserves slice locality.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CellOrder {
    RowMajor,
    ColMajor,
    #[default]
    Hilbert,
    ZOrder,
}

/// Across-tile ordering. Hilbert keeps tile-id sort order coherent
/// with spatial locality, so range scans hit a contiguous run of tiles.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TileOrder {
    RowMajor,
    ColMajor,
    #[default]
    Hilbert,
    ZOrder,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_hilbert() {
        assert_eq!(CellOrder::default(), CellOrder::Hilbert);
        assert_eq!(TileOrder::default(), TileOrder::Hilbert);
    }
}
