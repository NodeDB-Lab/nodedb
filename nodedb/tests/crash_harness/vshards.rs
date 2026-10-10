// SPDX-License-Identifier: BUSL-1.1

//! Collection names that home on distinct vShards.

use nodedb_types::id::DatabaseId;

/// One collection name per prefix, `<prefix>_<n>`, each on a vShard no other
/// returned name homes on. A transaction that writes two of them spans two
/// vShards, so it commits through the Calvin scheduler.
pub fn names_on_distinct_vshards<const N: usize>(prefixes: [&str; N]) -> [String; N] {
    let mut taken: Vec<u32> = Vec::with_capacity(N);
    prefixes.map(|prefix| {
        let (name, vshard) = (0..512u32)
            .map(|i| {
                let name = format!("{prefix}_{i}");
                let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                    .vshard()
                    .as_u32();
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
    let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32();
    1 + vshard % SINGLE_NODE_DATA_GROUPS
}

/// The data Raft groups a single-node server applies the cell writes of one
/// `INT64` dimension `[lo..hi]` array in.
///
/// An array cell write routes by its tile, never by the array's name. The
/// tile's key picks a bucket from its top `prefix_bits` bits, and the bucket
/// owns vShard `bucket * (1024 >> prefix_bits)`. At the default of 8 bits
/// every array vShard is a multiple of 4, so every cell of every array
/// applies in data group 1. Only `prefix_bits` of 9 or more spreads cells
/// over more than one group. `tile_extent` must match the array's
/// `TILE_EXTENTS`.
pub struct ArrayCellRoute {
    schema: nodedb_array::ArraySchema,
    prefix_bits: u8,
}

impl ArrayCellRoute {
    pub fn new(lo: i64, hi: i64, tile_extent: u64, prefix_bits: u8) -> Self {
        use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType};
        use nodedb_array::types::domain::{Domain, DomainBound};
        let schema = ArraySchemaBuilder::new("route")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(lo), DomainBound::Int64(hi)),
            ))
            .attr(AttrSpec::new("v", AttrType::Float64, true))
            .tile_extents(vec![tile_extent])
            .build()
            .expect("a one-dimension routing schema");
        Self {
            schema,
            prefix_bits,
        }
    }

    /// The data group the cell at `coord` applies in.
    pub fn group(&self, coord: i64) -> u32 {
        use nodedb_array::types::coord::value::CoordValue;
        let prefix = nodedb_array::cell_tile_prefix(&self.schema, &[CoordValue::Int64(coord)])
            .unwrap_or_else(|e| panic!("coordinate {coord} outside the routing schema: {e}"));
        let vshard =
            nodedb_cluster::distributed_array::array_vshard_for_tile(prefix, self.prefix_bits)
                .unwrap_or_else(|e| panic!("prefix_bits {}: {e}", self.prefix_bits));
        1 + vshard % SINGLE_NODE_DATA_GROUPS
    }
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
