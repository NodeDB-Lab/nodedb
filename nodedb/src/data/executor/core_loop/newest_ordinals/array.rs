// SPDX-License-Identifier: BUSL-1.1

//! The newest stored tile version of an array on one vShard.
//!
//! Every stored tile version carries its placement: the vShard whose write
//! stored it (`ArrayOp::Put::vshard_id`). A cell write and a read leg both
//! use the vShard the tile's key routes to. A core can host other vShards
//! too, and which ones depends on its core count. The placement keeps them
//! out, so every replica of the vShard answers alike.
//!
//! The placement is the record of the write, not a recomputed route. It
//! stays exact if an array's routing granularity changes.
//!
//! The placement travels in the replicated write and in its WAL record, so
//! every replica and every replay stores the same placement.

use nodedb_array::types::ArrayId;

use crate::types::VShardId;

use super::super::CoreLoop;

impl CoreLoop {
    /// The newest stored tile version, over the arrays `matches` accepts,
    /// of the tiles placed on `vshard`. An array with no catalog entry is
    /// mid-drop and counts nothing. `Err` when the array catalog does not
    /// read.
    pub(super) fn stored_array_tiles_newest(
        &self,
        vshard: VShardId,
        matches: impl Fn(&ArrayId) -> bool,
    ) -> crate::Result<Option<i64>> {
        let catalog = self
            .array_catalog
            .read()
            .map_err(|_| crate::Error::Internal {
                detail: "newest ordinal: array catalog lock poisoned".into(),
            })?;
        let placement = vshard.as_u32();
        let mut newest: Option<i64> = None;
        for id in self.array_engine.array_ids().filter(|id| matches(id)) {
            let Ok(store) = self.array_engine.store(id) else {
                continue;
            };
            // An array with no catalog entry is mid-drop: no read names it.
            if catalog.lookup_by_id(id).is_none() {
                continue;
            }
            newest = newest.max(store.newest_stored_tile_stamp_placed(placement));
        }
        Ok(newest)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::tile::cell_tile_prefix;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_types::array_route::array_vshard_for_tile;

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core, make_default_task};
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::TenantId;

    const PREFIX_BITS: u8 = 8;

    fn schema() -> nodedb_array::schema::ArraySchema {
        ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1023)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .expect("schema")
    }

    fn tile_vshard(x: i64) -> VShardId {
        let key = cell_tile_prefix(&schema(), &[CoordValue::Int64(x)]).expect("tile key");
        VShardId::new(array_vshard_for_tile(key, PREFIX_BITS).expect("route"))
    }

    /// Put the cell at `x` through the Data Plane, as a write of `placement`.
    fn put(core: &mut CoreLoop, id: &ArrayId, x: i64, placement: VShardId, lsn: u64) {
        let cells = vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(x)],
            attrs: vec![CellValue::Int64(x)],
            surrogate: nodedb_types::Surrogate::new(u32::try_from(x + 1).expect("small")),
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }];
        let bytes = zerompk::to_msgpack_vec(&cells).expect("cells");
        let response = core.handle_array_put(
            &make_default_task(),
            id,
            placement.as_u32(),
            &bytes,
            lsn,
            None,
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    }

    /// An open array, and two cells whose tiles route to two vShards, and a
    /// third vShard neither tile routes to.
    fn open_grid(core: &mut CoreLoop) -> (ArrayId, i64, i64, VShardId) {
        let id = ArrayId::new(TenantId::new(1), "grid");
        let schema_bytes = zerompk::to_msgpack_vec(&schema()).expect("schema");
        let opened =
            core.handle_array_open(&make_default_task(), &id, &schema_bytes, 7, PREFIX_BITS);
        assert_eq!(opened.status, Status::Ok, "{:?}", opened.error_code);
        let first = 0;
        let second = (1..256)
            .map(|tile| tile * 4)
            .find(|x| tile_vshard(*x) != tile_vshard(first))
            .expect("a tile on another vShard");
        let other = (0..VShardId::COUNT)
            .map(VShardId::new)
            .find(|v| *v != tile_vshard(first) && *v != tile_vshard(second))
            .expect("a vShard with no tile");
        (id, first, second, other)
    }

    /// Cluster placement: each tile write is placed on its tile's vShard. A
    /// core holding both vShards counts only the validated vShard's tiles.
    #[test]
    fn a_cluster_tile_counts_on_its_own_vshard_only() {
        let (mut core, _, _, _dir) = make_core();
        let (id, first, second, empty) = open_grid(&mut core);

        put(&mut core, &id, first, tile_vshard(first), 10);
        let all = |_: &ArrayId| true;
        let first_newest = core
            .stored_array_tiles_newest(tile_vshard(first), all)
            .expect("read")
            .expect("the first tile's vShard holds a version");
        assert_eq!(
            core.stored_array_tiles_newest(tile_vshard(second), all)
                .expect("read"),
            None
        );

        put(&mut core, &id, second, tile_vshard(second), 11);
        assert_eq!(
            core.stored_array_tiles_newest(tile_vshard(first), all)
                .expect("read"),
            Some(first_newest),
            "a write of another tile vShard leaves this one"
        );
        assert!(
            core.stored_array_tiles_newest(tile_vshard(second), all)
                .expect("read")
                > Some(first_newest)
        );
        assert_eq!(
            core.stored_array_tiles_newest(empty, all).expect("read"),
            None
        );
    }

    /// A tile counts on the vShard whose write stored it, not on the vShard
    /// its key routes to now. Two tiles routing to two vShards, both stored
    /// by writes of a third, count on the third only, in the memtable and
    /// after a flush.
    #[test]
    fn a_tile_counts_on_the_vshard_that_stored_it() {
        let (mut core, _, _, _dir) = make_core();
        let (id, first, second, placed) = open_grid(&mut core);
        let all = |_: &ArrayId| true;

        put(&mut core, &id, first, placed, 10);
        let snapshot = core
            .stored_array_tiles_newest(placed, all)
            .expect("read")
            .expect("the placing vShard holds the first tile");

        put(&mut core, &id, second, placed, 11);
        let after = core.stored_array_tiles_newest(placed, all).expect("read");
        assert!(
            after > Some(snapshot),
            "the second write is newer than the snapshot on the placing vShard"
        );
        for routed in [tile_vshard(first), tile_vshard(second)] {
            assert_eq!(
                core.stored_array_tiles_newest(routed, all).expect("read"),
                None,
                "a tile key's routed vShard holds no tile another vShard stored"
            );
        }

        core.flush_array(&id).expect("flush");
        assert_eq!(
            core.stored_array_tiles_newest(placed, all).expect("read"),
            after,
            "a flushed segment keeps every tile's placement"
        );
    }
}
