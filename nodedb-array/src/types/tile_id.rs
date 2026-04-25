//! Tile identifier — `(hilbert_prefix, system_from_ms)`.
//!
//! The Hilbert prefix locates the tile in ND space; the system-time
//! suffix carries the bitemporal version (Tier 9). Until bitemporal
//! tiles land, callers pass `system_from_ms = 0`.

use serde::{Deserialize, Serialize};

/// Composite tile key. Lexicographic ordering of `(hilbert_prefix,
/// system_from_ms)` keeps newer tile versions adjacent to their
/// spatial parents, so the Ceiling resolver can reverse-scan a single
/// Hilbert range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

    /// Tile id for a non-bitemporal write — Tier 0 callers use this.
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
