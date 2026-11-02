// SPDX-License-Identifier: BUSL-1.1

//! Name-only array routing for array-sync messages that carry no cell.
//!
//! A cell write never routes here. Every array cell routes to the vShard of
//! its tile key (`nodedb_types::array_route::array_vshard_for_tile`), the
//! rule SQL writes, Lite-sync writes and reads share.

/// Number of virtual shards.
pub const VSHARD_COUNT: u32 = nodedb_types::id::VShardId::COUNT;

/// Compute a vShard ID from an array name alone.
///
/// Used by `array_sync` messages that name an array and no cell, such as a
/// schema import. Uses the DJB multiply-31 hash of `VShardId::from_collection`
/// without the database scope.
pub fn array_vshard_for_name(array_name: &str) -> u32 {
    let hash = array_name
        .as_bytes()
        .iter()
        .fold(0u32, |h, &b| h.wrapping_mul(31).wrapping_add(b as u32));
    hash % VSHARD_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_routing_is_deterministic_and_in_range() {
        let a = array_vshard_for_name("prices");
        let b = array_vshard_for_name("prices");
        assert_eq!(a, b);
        assert!(a < VSHARD_COUNT);
    }
}
