//! Tile-aware array routing — maps `(array_name, coord)` → vShard id.
//!
//! A distributed Origin shards each array's op-log by tile: ops for tile T of
//! array A land on the vShard computed from the bytes `array_name || tile_id`.
//! This spreads a large array across 1 024 vShards without any coordination
//! beyond the deterministic hash already used for all other shard routing.
//!
//! # Key invariant
//!
//! Ops for the same (array, tile) always route to the same vShard. Ops for
//! different tiles of the same array *typically* route to different vShards
//! (with high probability for arrays whose tile grid is large enough to avoid
//! hash collisions on a 10-bit output).
//!
//! # Returned type
//!
//! Functions here return a raw `u16` vShard ID (0..1023). Callers in the
//! `nodedb` crate wrap this in `VShardId::new(…)`. `VShardId` lives in
//! `nodedb::types` which `nodedb-cluster` cannot depend on without creating
//! a circular dependency.
//!
//! # Fallbacks
//!
//! - Empty coord or tile_extents: fall back to [`vshard_from_collection`].
//! - Zero tile extent in any dimension: same fallback (no division by zero).
//! - `coord.len() != tile_extents.len()`: fallback.

/// Number of virtual shards — must match `VShardId::COUNT` in `nodedb::types`.
pub const VSHARD_COUNT: u16 = 1024;

// ─── Collection-level fallback hash ──────────────────────────────────────────

/// Compute a vShard ID from a collection (array) name alone.
///
/// Mirrors `VShardId::from_collection` in `nodedb::types::id` so callers
/// get consistent results when falling back.
pub fn vshard_from_collection(array_name: &str) -> u16 {
    let hash = array_name
        .as_bytes()
        .iter()
        .fold(0u16, |h, &b| h.wrapping_mul(31).wrapping_add(b as u16));
    hash % VSHARD_COUNT
}

/// Compute a vShard ID from an arbitrary byte key.
///
/// Mirrors `VShardId::from_key` in `nodedb::types::id`.
fn vshard_from_key(key: &[u8]) -> u16 {
    let mut h: u64 = 0;
    for &b in key {
        h = h.wrapping_mul(0x100000001B3).wrapping_add(b as u64);
    }
    (h % VSHARD_COUNT as u64) as u16
}

// ─── Tile-id computation ──────────────────────────────────────────────────────

/// Compute the tile identifier for a coordinate given tile extents.
///
/// Uses row-major ordering over the tile grid: each dimension's tile index
/// is `coord[i] / tile_extents[i]`, and the final `tile_id` multiplies those
/// indices by the product of the tile-grid dimensions for all *later* dimensions
/// (standard C-order / row-major strides).
///
/// Returns `None` when:
/// - `coord` and `tile_extents` have different lengths.
/// - Either slice is empty.
/// - Any `tile_extents[i]` is zero.
///
/// Callers that need a fallback should call [`vshard_for_array_coord`] directly,
/// which transparently falls back to collection-level routing in all error cases.
pub fn tile_id_of_coord(coord: &[u64], tile_extents: &[u64]) -> Option<u64> {
    if coord.is_empty() || tile_extents.is_empty() {
        return None;
    }
    if coord.len() != tile_extents.len() {
        return None;
    }
    if tile_extents.contains(&0) {
        return None;
    }

    // Compute per-dimension tile indices.
    let tile_indices: Vec<u64> = coord
        .iter()
        .zip(tile_extents.iter())
        .map(|(&c, &e)| c / e)
        .collect();

    // Compute row-major tile_id via stride accumulation from the last dimension.
    // stride[i] = product of tile widths for dims i+1..N.
    // We use the tile index itself as a conservative upper bound per dim, which
    // preserves uniqueness for monotonically growing coordinate spaces and is
    // sufficient as a hash pre-image.
    let n = tile_indices.len();
    let mut tile_id: u64 = 0;
    let mut stride: u64 = 1;

    for i in (0..n).rev() {
        tile_id = tile_id.wrapping_add(tile_indices[i].wrapping_mul(stride));
        stride = stride.wrapping_mul(tile_indices[i].wrapping_add(1).max(1));
    }

    Some(tile_id)
}

// ─── vShard routing ───────────────────────────────────────────────────────────

/// Compute the vShard ID for an array op at the given coordinate.
///
/// The shard key is `array_name_bytes || tile_id.to_le_bytes()`, hashed via
/// `vshard_from_key`.
///
/// Falls back to `vshard_from_collection(array_name)` when:
/// - `coord` / `tile_extents` are empty or mismatched.
/// - Any `tile_extents[i]` is zero.
///
/// Returns a raw `u16` in `0..1023`. Callers wrap it in `VShardId::new(…)`.
pub fn vshard_for_array_coord(array_name: &str, coord: &[u64], tile_extents: &[u64]) -> u16 {
    match tile_id_of_coord(coord, tile_extents) {
        Some(tile_id) => vshard_for_array_tile(array_name, tile_id),
        None => vshard_from_collection(array_name),
    }
}

/// Compute the vShard ID for an array tile by tile identifier.
///
/// Use this when the tile_id is already known (e.g. from the op-log key or
/// snapshot metadata) and coord re-derivation is unnecessary.
///
/// Returns a raw `u16` in `0..1023`. Callers wrap it in `VShardId::new(…)`.
pub fn vshard_for_array_tile(array_name: &str, tile_id: u64) -> u16 {
    let mut buf = Vec::with_capacity(array_name.len() + 8);
    buf.extend_from_slice(array_name.as_bytes());
    buf.extend_from_slice(&tile_id.to_le_bytes());
    vshard_from_key(&buf)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── tile_id_of_coord ─────────────────────────────────────────────────────

    #[test]
    fn tile_id_empty_coord_is_none() {
        assert!(tile_id_of_coord(&[], &[]).is_none());
    }

    #[test]
    fn tile_id_zero_extent_is_none() {
        assert!(tile_id_of_coord(&[5], &[0]).is_none());
    }

    #[test]
    fn tile_id_mismatched_lengths_is_none() {
        assert!(tile_id_of_coord(&[1, 2], &[4]).is_none());
    }

    #[test]
    fn tile_id_single_dim_same_tile() {
        // coords 0..9 with extent 10 all land in tile 0
        let t = tile_id_of_coord(&[9], &[10]).unwrap();
        assert_eq!(t, tile_id_of_coord(&[0], &[10]).unwrap());
        assert_eq!(t, 0);
    }

    #[test]
    fn tile_id_single_dim_different_tiles() {
        let t0 = tile_id_of_coord(&[0], &[10]).unwrap();
        let t1 = tile_id_of_coord(&[10], &[10]).unwrap();
        assert_ne!(
            t0, t1,
            "coords in different tiles must yield different tile_ids"
        );
    }

    #[test]
    fn tile_id_two_dim_row_major() {
        // extent = [4, 4]: tiles are 4×4 blocks
        // coord (0,0) and (0,3) share tile (0,0) → same tile_id
        let t00 = tile_id_of_coord(&[0, 0], &[4, 4]).unwrap();
        let t03 = tile_id_of_coord(&[0, 3], &[4, 4]).unwrap();
        assert_eq!(t00, t03, "same tile");

        // coord (0,4) is in tile (0,1) — different from (0,0)
        let t04 = tile_id_of_coord(&[0, 4], &[4, 4]).unwrap();
        assert_ne!(t00, t04, "different tile column");

        // coord (4,0) is in tile (1,0) — different from (0,0)
        let t40 = tile_id_of_coord(&[4, 0], &[4, 4]).unwrap();
        assert_ne!(t00, t40, "different tile row");

        // coord (4,4) is in tile (1,1) — different from (1,0) and (0,1)
        let t44 = tile_id_of_coord(&[4, 4], &[4, 4]).unwrap();
        assert_ne!(t40, t44);
        assert_ne!(t04, t44);
    }

    // ── vshard_from_collection ───────────────────────────────────────────────

    #[test]
    fn collection_fallback_is_deterministic() {
        let a = vshard_from_collection("prices");
        let b = vshard_from_collection("prices");
        assert_eq!(a, b);
        assert!(a < VSHARD_COUNT);
    }

    // ── vshard_for_array_coord ───────────────────────────────────────────────

    #[test]
    fn same_tile_gives_same_vshard() {
        // All coords within tile [0,10) map to the same shard.
        let s0 = vshard_for_array_coord("prices", &[0], &[10]);
        let s9 = vshard_for_array_coord("prices", &[9], &[10]);
        assert_eq!(s0, s9, "same tile → same shard");
    }

    #[test]
    fn different_tiles_likely_different_vshards() {
        // extent = 1 → every coord is its own tile.
        // With 256 distinct tiles we expect high shard diversity.
        let shards: Vec<u16> = (0u64..256)
            .map(|i| vshard_for_array_coord("matrix", &[i], &[1]))
            .collect();

        let unique: std::collections::HashSet<_> = shards.iter().copied().collect();
        assert!(
            unique.len() >= 200,
            "expected high shard diversity for 256 distinct tiles, got {}",
            unique.len()
        );
    }

    #[test]
    fn different_arrays_same_tile_different_shards() {
        let mut differs = 0usize;
        for t in 0u64..64 {
            let sa = vshard_for_array_tile("alpha", t);
            let sb = vshard_for_array_tile("beta", t);
            if sa != sb {
                differs += 1;
            }
        }
        assert!(
            differs >= 32,
            "expected most tiles to yield different shards for different arrays, got {differs}/64"
        );
    }

    #[test]
    fn fallback_on_empty_coord() {
        let s = vshard_for_array_coord("prices", &[], &[]);
        let expected = vshard_from_collection("prices");
        assert_eq!(s, expected, "empty coord must fall back to from_collection");
    }

    #[test]
    fn fallback_on_zero_extent() {
        let s = vshard_for_array_coord("prices", &[5], &[0]);
        let expected = vshard_from_collection("prices");
        assert_eq!(s, expected, "zero extent must fall back to from_collection");
    }

    #[test]
    fn fallback_on_mismatched_dims() {
        let s = vshard_for_array_coord("prices", &[1, 2], &[4]);
        let expected = vshard_from_collection("prices");
        assert_eq!(
            s, expected,
            "mismatched dims must fall back to from_collection"
        );
    }

    // ── vshard_for_array_tile ────────────────────────────────────────────────

    #[test]
    fn tile_fn_matches_coord_fn_for_same_tile() {
        let coord = &[7u64, 3];
        let extents = &[4u64, 4];
        let tile_id = tile_id_of_coord(coord, extents).unwrap();

        let from_coord = vshard_for_array_coord("mat", coord, extents);
        let from_tile = vshard_for_array_tile("mat", tile_id);
        assert_eq!(
            from_coord, from_tile,
            "vshard_for_array_coord and vshard_for_array_tile must agree"
        );
    }

    #[test]
    fn all_vshards_in_range() {
        for t in 0u64..1024 {
            let s = vshard_for_array_tile("arr", t);
            assert!(s < VSHARD_COUNT, "vshard {s} out of range for tile {t}");
        }
    }
}
