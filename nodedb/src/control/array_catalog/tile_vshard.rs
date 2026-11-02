// SPDX-License-Identifier: BUSL-1.1

//! The vShard an array cell's tile routes to, from the array's catalog row.
//!
//! Every array cell write routes by this one rule: SQL DML, Lite sync, and
//! restore. A read leg reads the tiles of one such vShard, so a write placed
//! by it is the write a read finds.

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_tile_prefix;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::array_route::array_vshard_for_tile;

use super::ArrayCatalogEntry;
use crate::types::VShardId;

/// The vShard that owns the tile of the cell at `coord` of `entry`'s array.
/// `Err` when the schema does not decode, the coordinate lies outside the
/// schema's domain, or the routing granularity is out of range.
pub(crate) fn cell_tile_vshard(
    entry: &ArrayCatalogEntry,
    coord: &[CoordValue],
) -> crate::Result<VShardId> {
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array '{}' schema: {e}", entry.name),
        })?;
    let tile_key = cell_tile_prefix(&schema, coord).map_err(|e| crate::Error::BadRequest {
        detail: format!("array '{}' cell {coord:?}: {e}", entry.name),
    })?;
    let vshard =
        array_vshard_for_tile(tile_key, entry.prefix_bits).map_err(|e| crate::Error::Internal {
            detail: format!("array '{}' routing: {e}", entry.name),
        })?;
    Ok(VShardId::new(vshard))
}

#[cfg(test)]
mod tests {
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::tile::TilePrefixMemo;
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_sql::types_array::ArrayCoordLiteral;
    use nodedb_types::Hlc;

    use super::*;
    use crate::control::planner::sql_plan_convert::array_convert::cell_vshard;
    use crate::types::{DatabaseId, TenantId};

    fn schema() -> ArraySchema {
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

    fn entry() -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::in_database(TenantId::new(1), DatabaseId::DEFAULT, "grid"),
            name: "grid".to_string(),
            schema_msgpack: zerompk::to_msgpack_vec(&schema()).expect("schema"),
            schema_hash: 7,
            created_at_ms: 0,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
            modification_hlc: Hlc::ZERO,
            incarnation: Hlc::ZERO,
        }
    }

    /// A Lite-sync cell routes to the vShard a SQL put of the same cell
    /// routes to, for every tile of the array.
    #[test]
    fn a_synced_cell_routes_as_a_sql_put_does() {
        let entry = entry();
        let schema = schema();
        let mut memo = TilePrefixMemo::new();
        let mut vshards = std::collections::BTreeSet::new();
        for x in (0..=1023).step_by(4) {
            let synced = cell_tile_vshard(&entry, &[CoordValue::Int64(x)]).expect("route");
            let put = cell_vshard(
                &[ArrayCoordLiteral::Int64(x)],
                &schema,
                entry.prefix_bits,
                &mut memo,
            )
            .expect("put route");
            assert_eq!(synced.as_u32(), put, "x = {x}");
            vshards.insert(put);
        }
        assert!(vshards.len() > 1, "the array's tiles span several vShards");
    }

    /// A cell outside the schema's domain has no route.
    #[test]
    fn a_cell_outside_the_domain_is_refused() {
        assert!(cell_tile_vshard(&entry(), &[CoordValue::Int64(5_000)]).is_err());
    }
}
