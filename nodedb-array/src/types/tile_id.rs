// SPDX-License-Identifier: Apache-2.0

//! Tile identifier — `(hilbert_prefix, system_from_ms)`.
//!
//! The Hilbert prefix locates the tile in ND space. The suffix is the tile
//! version's system-time stamp. The writer picks its unit and every reader
//! compares in that unit: Origin stamps the system-time ordinal of the apply
//! (nanosecond precision, shared with every other engine), Lite stamps its
//! clock's milliseconds. A Ceiling read at cut `S` sees the newest version
//! with a stamp at or below `S`.

use serde::{Deserialize, Serialize};

/// Composite tile key. Lexicographic ordering of `(hilbert_prefix,
/// system_from_ms)` keeps newer tile versions adjacent to their
/// spatial parents, so the Ceiling resolver can reverse-scan a single
/// Hilbert range.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TileId {
    pub hilbert_prefix: u64,
    pub system_from_ms: i64,
}

impl TileId {
    pub fn new(hilbert_prefix: u64, system_from_ms: i64) -> Self {
        Self {
            hilbert_prefix,
            system_from_ms,
        }
    }

    /// Tile id for a non-bitemporal write (pass `system_from_ms = 0`).
    pub fn snapshot(hilbert_prefix: u64) -> Self {
        Self::new(hilbert_prefix, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_id_orders_by_prefix_then_time() {
        let a = TileId::new(10, 100);
        let b = TileId::new(10, 200);
        let c = TileId::new(11, 0);
        assert!(a < b);
        assert!(b < c);
    }
}
