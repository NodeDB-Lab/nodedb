// SPDX-License-Identifier: BUSL-1.1

//! Tenant snapshot capture of the array engine.
//!
//! Every cell version of each of the tenant's arrays in the snapshot's
//! database is exported, grouped by the vShard its tile's key routes to.
//! A backup keeps each group on the source node of that vShard, so every
//! version is captured once.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_tile_prefix;
use nodedb_cluster::distributed_array::array_vshard_for_tile;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::array::export::ArrayCellVersion;
use crate::types::{ArrayCellsBlob, DatabaseId, TenantId};

fn capture_error(array: &str, detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("snapshot: array '{array}' failed to export: {detail}"),
    }
}

impl CoreLoop {
    /// Every cell version of `tenant`'s arrays in `database_id` on this core,
    /// one blob per `(array, vShard)`.
    pub(super) fn capture_arrays(
        &mut self,
        database_id: DatabaseId,
        tenant: TenantId,
    ) -> crate::Result<Vec<ArrayCellsBlob>> {
        let entries = {
            let mirror = self
                .array_catalog
                .read()
                .map_err(|_| crate::Error::Internal {
                    detail: "snapshot: array catalog lock poisoned".into(),
                })?;
            mirror.all_entries()
        };
        let mut blobs = Vec::new();
        for entry in entries {
            let id = &entry.array_id;
            if id.tenant_id != tenant || id.database_id != database_id {
                continue;
            }
            let schema: ArraySchema = zerompk::from_msgpack(&entry.schema_msgpack)
                .map_err(|e| capture_error(&entry.name, e))?;
            let schema = Arc::new(schema);
            let versions = self
                .array_engine
                .export_cell_versions(id, Arc::clone(&schema), entry.schema_hash)
                .map_err(|e| capture_error(&entry.name, e))?;
            for (vshard, cells) in by_vshard(&entry.name, &schema, entry.prefix_bits, versions)? {
                blobs.push(ArrayCellsBlob {
                    database_id: database_id.as_u64(),
                    tenant_id: tenant.as_u64(),
                    array: entry.name.clone(),
                    vshard,
                    cells: zerompk::to_msgpack_vec(&cells)
                        .map_err(|e| capture_error(&entry.name, e))?,
                });
            }
        }
        Ok(blobs)
    }
}

/// Group `versions` by the vShard a cell write of each routes to. A
/// tombstone or erasure of a coordinate outside the schema's domain has no
/// route: no cell ever lived there, so it is left out.
fn by_vshard(
    array: &str,
    schema: &ArraySchema,
    prefix_bits: u8,
    versions: Vec<ArrayCellVersion>,
) -> crate::Result<BTreeMap<u32, Vec<ArrayCellVersion>>> {
    let mut grouped: BTreeMap<u32, Vec<ArrayCellVersion>> = BTreeMap::new();
    for version in versions {
        let prefix = match cell_tile_prefix(schema, &version.coord) {
            Ok(prefix) => prefix,
            Err(_) if version.payload.is_none() => continue,
            Err(e) => {
                return Err(capture_error(
                    array,
                    format!("Hilbert prefix of a live cell: {e}"),
                ));
            }
        };
        let vshard = array_vshard_for_tile(prefix, prefix_bits)
            .map_err(|e| capture_error(array, format!("vShard of Hilbert prefix {prefix}: {e}")))?;
        grouped.entry(vshard).or_default().push(version);
    }
    Ok(grouped)
}

#[cfg(test)]
mod tests {
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::tile::cell_payload::CellPayload;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_types::{OPEN_UPPER, Surrogate};

    use super::*;

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1023)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![16])
            .build()
            .expect("schema")
    }

    fn live(x: i64) -> ArrayCellVersion {
        ArrayCellVersion {
            coord: vec![CoordValue::Int64(x)],
            system_from_ms: 1,
            payload: Some(CellPayload {
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                attrs: vec![CellValue::Int64(x)],
                surrogate: Surrogate::new(1),
            }),
            erased: false,
        }
    }

    /// Each version lands in the group of the vShard a live write of its
    /// coordinate routes to.
    #[test]
    fn versions_group_by_their_write_route() {
        let s = schema();
        let versions: Vec<ArrayCellVersion> = (0..1024).step_by(8).map(live).collect();
        let grouped = by_vshard("g", &s, 8, versions.clone()).expect("group");
        assert!(grouped.len() > 1, "the fixture spans several vShards");
        assert_eq!(
            grouped.values().map(Vec::len).sum::<usize>(),
            versions.len()
        );
        for (vshard, cells) in &grouped {
            for cell in cells {
                let prefix = cell_tile_prefix(&s, &cell.coord).expect("prefix");
                assert_eq!(array_vshard_for_tile(prefix, 8).expect("route"), *vshard);
            }
        }
    }

    #[test]
    fn an_unroutable_tombstone_is_left_out_and_a_live_cell_fails() {
        let s = schema();
        let tombstone = ArrayCellVersion {
            payload: None,
            ..live(5_000)
        };
        assert!(
            by_vshard("g", &s, 8, vec![tombstone])
                .expect("group")
                .is_empty()
        );
        assert!(by_vshard("g", &s, 8, vec![live(5_000)]).is_err());
    }
}
