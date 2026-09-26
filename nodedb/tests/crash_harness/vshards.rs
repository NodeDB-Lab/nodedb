// SPDX-License-Identifier: BUSL-1.1

//! Collection names that home on distinct vShards.

use nodedb_types::id::{DatabaseId, VShardId};

/// One collection name per prefix, `<prefix>_<n>`, each on a vShard no other
/// returned name homes on. A transaction that writes two of them spans two
/// vShards, so it commits through the Calvin scheduler.
pub fn names_on_distinct_vshards<const N: usize>(prefixes: [&str; N]) -> [String; N] {
    let mut taken: Vec<u32> = Vec::with_capacity(N);
    prefixes.map(|prefix| {
        let (name, vshard) = (0..512u32)
            .map(|i| {
                let name = format!("{prefix}_{i}");
                let vshard =
                    VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
                (name, vshard)
            })
            .find(|(_, vshard)| !taken.contains(vshard))
            .unwrap_or_else(|| panic!("no {prefix} name on a free vShard in 512 tries"));
        taken.push(vshard);
        name
    })
}

/// Data Raft groups a single-node server runs. The server maps vShard `v` to
/// group `1 + v % SINGLE_NODE_DATA_GROUPS`.
pub const SINGLE_NODE_DATA_GROUPS: u32 = 4;

/// The data Raft group a single-node server applies writes to `name` in.
pub fn single_node_data_group(name: &str) -> u32 {
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, name).as_u32();
    1 + vshard % SINGLE_NODE_DATA_GROUPS
}

/// One collection name per prefix, `<prefix>_<n>`, each applied in a data
/// group of a single-node server that no other returned name is applied in.
pub fn names_in_distinct_data_groups<const N: usize>(prefixes: [&str; N]) -> [String; N] {
    let mut taken: Vec<u32> = Vec::with_capacity(N);
    prefixes.map(|prefix| {
        let (name, group) = (0..512u32)
            .map(|i| {
                let name = format!("{prefix}_{i}");
                let group = single_node_data_group(&name);
                (name, group)
            })
            .find(|(_, group)| !taken.contains(group))
            .unwrap_or_else(|| panic!("no {prefix} name in a free data group in 512 tries"));
        taken.push(group);
        name
    })
}
