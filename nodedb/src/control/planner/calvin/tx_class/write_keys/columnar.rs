// SPDX-License-Identifier: BUSL-1.1

//! Columnar, timeseries and array write keys.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{ArrayOp, ColumnarOp, TimeseriesOp};

use super::plan::{not_a_write, unsequenced};
use super::set::WriteKeys;

/// Add the write keys of columnar op `op` to `keys`.
pub(super) fn add_columnar_keys(keys: &mut WriteKeys, op: &ColumnarOp) -> crate::Result<()> {
    match op {
        // Every row it inserts, and the whole collection, so inserts append
        // to the collection in sequencer order.
        ColumnarOp::Insert {
            collection,
            surrogates,
            ..
        } => {
            keys.rows(collection.as_str(), surrogates.iter().map(|s| s.as_u32()));
            keys.whole_collection(collection.as_str());
        }
        // Predicate and primary-key writes carry no row surrogate.
        ColumnarOp::Update { collection, .. }
        | ColumnarOp::Delete { collection, .. }
        | ColumnarOp::ResolvedUpdate { collection, .. }
        | ColumnarOp::ResolvedDelete { collection, .. }
        | ColumnarOp::Truncate { collection, .. } => keys.whole_collection(collection.as_str()),
        ColumnarOp::Scan { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::ResolveDml { .. } => {
            return Err(not_a_write("a columnar read"));
        }
    }
    Ok(())
}

/// Add the write keys of timeseries op `op` to `keys`.
pub(super) fn add_timeseries_keys(keys: &mut WriteKeys, op: &TimeseriesOp) -> crate::Result<()> {
    match op {
        TimeseriesOp::Ingest { collection, .. } | TimeseriesOp::Truncate { collection, .. } => {
            keys.whole_collection(collection.as_str());
            Ok(())
        }
        TimeseriesOp::ResolveIngest(_) | TimeseriesOp::Scan { .. } => {
            Err(not_a_write("a timeseries read"))
        }
    }
}

/// Add the write keys of array op `op` to `keys`.
///
/// An array is tile-partitioned, and a cell write names the vShard its
/// cells' tiles live on. It locks the whole array on that vShard.
pub(super) fn add_array_keys(keys: &mut WriteKeys, op: &ArrayOp) -> crate::Result<()> {
    match op {
        ArrayOp::Put {
            array_id,
            vshard_id,
            ..
        }
        | ArrayOp::Delete {
            array_id,
            vshard_id,
            ..
        } => {
            let collection =
                nodedb_types::QualifiedCollection::new(array_id.database_id, &array_id.name);
            keys.array_cells(collection.as_str(), *vshard_id);
            Ok(())
        }
        // A flush writes every tile the node holds, and a transaction never
        // stages one: it runs outside any transaction.
        ArrayOp::Flush { .. } => Err(unsequenced(
            "an array flush writes every tile of the array on a node, and runs outside a \
             transaction",
        )),
        ArrayOp::OpenArray { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RekeyArray { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => Err(not_a_write("an array read or DDL op")),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::ArrayId;
    use nodedb_cluster::calvin::types::{EngineKeySet, SortedVec};
    use nodedb_types::{DatabaseId, QualifiedCollection, TenantId};

    use super::*;

    /// Array cell writes enlist the vShards their tiles live on, one key set
    /// per array. A flush is refused.
    #[test]
    fn array_cell_writes_lock_their_tile_vshards() {
        let array_id = ArrayId::new(TenantId::new(1), "grid");
        let mut keys = WriteKeys::default();
        for (vshard_id, delete) in [(40, false), (7, true), (40, true)] {
            let op = if delete {
                ArrayOp::Delete {
                    array_id: array_id.clone(),
                    coords_msgpack: Vec::new(),
                    wal_lsn: 0,
                    provenance: None,
                    vshard_id,
                }
            } else {
                ArrayOp::Put {
                    array_id: array_id.clone(),
                    cells_msgpack: Vec::new(),
                    wal_lsn: 0,
                    provenance: None,
                    vshard_id,
                }
            };
            add_array_keys(&mut keys, &op).expect("a cell write has keys");
        }
        let collection = QualifiedCollection::new(DatabaseId::DEFAULT, "grid").to_string();
        assert_eq!(
            keys.into_key_sets(),
            vec![EngineKeySet::Array {
                collection,
                vshards: SortedVec::new(vec![7, 40, 40]),
            }]
        );

        let flush = ArrayOp::Flush {
            array_id,
            wal_lsn: 0,
        };
        assert!(add_array_keys(&mut WriteKeys::default(), &flush).is_err());
    }
}
