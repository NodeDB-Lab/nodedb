// SPDX-License-Identifier: BUSL-1.1

//! Re-issue restored array cells as Calvin transactions.
//!
//! Every version of one coordinate goes into one unit, in system-time
//! order, and a unit never splits across two transactions. Each batch is a
//! `MetaOp::RestoreRedo` plan on the vShard the cells route to. Its
//! transaction locks the array on that vShard, as every array cell write
//! locks it. Its resolve reads the destination at the transaction's turn and
//! adds, for each coordinate the destination holds past its newest backed-up
//! version, that version once more above the destination's. Every version
//! keeps its backed-up system time. A live cell takes the destination's
//! surrogate for its coordinate, and every replica binds it before the batch
//! installs.

use std::collections::BTreeMap;

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::{RestoredArrayCells, RestoredIdentity, RestoredRedo};
use nodedb_types::CollectionKey;

use crate::control::state::SharedState;
use crate::engine::array::export::ArrayCellVersion;
use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};
use crate::types::{ArrayCellsBlob, TenantId, VShardId};
use crate::wal::ArrayRestoreRedo;

use super::super::target::DatabaseTarget;
use super::commit::{batch_units, commit_batch, in_context};

fn codec(array: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore: array '{array}': {e}"),
    }
}

/// Every version of one coordinate, in system-time order.
struct CoordUnit {
    /// The coordinate's key: its encoded coordinate tuple.
    pk: Vec<u8>,
    versions: Vec<ArrayCellVersion>,
    /// The unit's encoded size, for sizing the batches it goes into.
    bytes: usize,
}

impl CoordUnit {
    fn has_live_version(&self) -> bool {
        self.versions
            .iter()
            .any(|version| version.payload.is_some())
    }
}

/// Group `versions` of `array` into one unit per coordinate, in coordinate
/// key order.
fn coord_units(array: &str, versions: Vec<ArrayCellVersion>) -> crate::Result<Vec<CoordUnit>> {
    let mut units: BTreeMap<Vec<u8>, CoordUnit> = BTreeMap::new();
    for version in versions {
        let pk = zerompk::to_msgpack_vec(&version.coord).map_err(|e| codec(array, e))?;
        let bytes = zerompk::to_msgpack_vec(&version)
            .map_err(|e| codec(array, e))?
            .len();
        let unit = units.entry(pk.clone()).or_insert_with(|| CoordUnit {
            pk,
            versions: Vec::new(),
            bytes: 0,
        });
        unit.bytes += bytes;
        unit.versions.push(version);
    }
    let mut units: Vec<CoordUnit> = units.into_values().collect();
    for unit in &mut units {
        unit.versions.sort_by_key(|version| version.system_from_ms);
    }
    Ok(units)
}

/// The restore record of `batch`: every version at its own system time,
/// each live one under `surrogates[pk]`. Every version is placed on
/// `vshard`, the vShard the batch restores into. The source's placement
/// does not carry over.
fn restore_redo(
    array_id: &ArrayId,
    vshard: u32,
    batch: Vec<CoordUnit>,
    surrogates: &BTreeMap<Vec<u8>, nodedb_types::Surrogate>,
) -> crate::Result<ArrayRestoreRedo> {
    let mut puts = Vec::new();
    let mut deletes = Vec::new();
    for unit in batch {
        for version in unit.versions {
            match version.payload {
                Some(payload) => {
                    let surrogate = surrogates.get(&unit.pk).copied().ok_or_else(|| {
                        crate::Error::Internal {
                            detail: format!(
                                "restore: a live cell of array '{}' has no surrogate",
                                array_id.name
                            ),
                        }
                    })?;
                    puts.push(ArrayPutCell {
                        coord: version.coord,
                        attrs: payload.attrs,
                        surrogate,
                        system_from_ms: version.system_from_ms,
                        valid_from_ms: payload.valid_from_ms,
                        valid_until_ms: payload.valid_until_ms,
                    });
                }
                None => deletes.push(ArrayDeleteCell {
                    coord: version.coord,
                    system_from_ms: version.system_from_ms,
                    erasure: version.erased,
                }),
            }
        }
    }
    Ok(ArrayRestoreRedo {
        array_id: array_id.clone(),
        vshard_id: vshard,
        puts,
        deletes,
    })
}

/// Re-issue every cell version of `blobs` into `target.dest` as Calvin
/// transactions marked under the restore. Returns the versions re-issued.
pub(in crate::control::backup::restore) async fn reissue_array_cells(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    blobs: Vec<ArrayCellsBlob>,
) -> crate::Result<usize> {
    let tenant = TenantId::new(tenant_id);
    let mut reissued = 0usize;
    for blob in blobs {
        let versions: Vec<ArrayCellVersion> =
            zerompk::from_msgpack(&blob.cells).map_err(|e| codec(&blob.array, e))?;
        let vshard = VShardId::new(blob.vshard);
        super::super::durable::log_reissue_step(
            state,
            "array",
            &blob.array,
            vshard,
            versions.len(),
        );
        let array_id = ArrayId::in_database(tenant, target.dest, &blob.array);
        let key = CollectionKey::from_bare(target.dest, &blob.array);
        let units = coord_units(&blob.array, versions)?;
        for batch in batch_units(units, |unit| (unit.versions.len(), unit.bytes)) {
            // Every live coordinate's surrogate in one batch at the array's
            // home.
            let live: Vec<&[u8]> = batch
                .iter()
                .filter(|unit| unit.has_live_version())
                .map(|unit| unit.pk.as_slice())
                .collect();
            let assigned = if live.is_empty() {
                Vec::new()
            } else {
                crate::control::server::surrogate_exchange::assign_surrogates_routed(
                    state,
                    key,
                    tenant,
                    &live,
                    crate::types::TraceId::ZERO,
                )
                .await?
            };
            if assigned.len() != live.len() {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "restore: the surrogate assignment of array '{}' answered {} of {} cells",
                        blob.array,
                        assigned.len(),
                        live.len()
                    ),
                });
            }
            let identities: Vec<RestoredIdentity> = live
                .iter()
                .zip(&assigned)
                .map(|(pk, surrogate)| RestoredIdentity {
                    collection: blob.array.clone(),
                    pk_bytes: pk.to_vec(),
                    surrogate: surrogate.as_u32(),
                })
                .collect();
            let surrogates: BTreeMap<Vec<u8>, nodedb_types::Surrogate> =
                live.into_iter().map(<[u8]>::to_vec).zip(assigned).collect();
            let redo = restore_redo(&array_id, blob.vshard, batch, &surrogates)?;
            let versions = redo.puts.len() + redo.deletes.len();
            let plan = RestoredRedo {
                vshard: blob.vshard,
                rows_redo: Vec::new(),
                rows: Vec::new(),
                kv_keys: Vec::new(),
                edges: Vec::new(),
                node_labels: Vec::new(),
                arrays: vec![RestoredArrayCells {
                    array_id: array_id.clone(),
                    redo: redo.encode()?,
                }],
                columnar_keys: Vec::new(),
                vector_rows: Vec::new(),
                timeseries: Vec::new(),
                // The array names itself by `array_id`: the batch writes no
                // user collection.
                collections: Vec::new(),
                identities,
                restore_cut: target.restore_cut,
            };
            commit_batch(state, tenant, target.dest, target.restore_id, vec![plan])
                .await
                .map_err(|e| {
                    in_context(
                        e,
                        &format!("cells of array '{}' to vShard {}", blob.array, blob.vshard),
                    )
                })?;
            reissued += versions;
        }
    }
    Ok(reissued)
}

#[cfg(test)]
mod tests {
    use nodedb_array::tile::cell_payload::CellPayload;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::{OPEN_UPPER, Surrogate};

    use super::*;

    fn version(x: i64, stamp: i64, value: Option<i64>) -> ArrayCellVersion {
        ArrayCellVersion {
            coord: vec![CoordValue::Int64(x)],
            system_from_ms: stamp,
            payload: value.map(|v| CellPayload {
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                attrs: vec![CellValue::Int64(v)],
                surrogate: Surrogate::new(99),
            }),
            erased: false,
            placement: 0,
        }
    }

    /// Every version of one coordinate lands in one unit, in system-time
    /// order.
    #[test]
    fn a_coordinates_versions_form_one_ordered_unit() {
        let units = coord_units(
            "grid",
            vec![
                version(2, 30, Some(1)),
                version(1, 20, None),
                version(1, 10, Some(7)),
            ],
        )
        .expect("units");
        assert_eq!(units.len(), 2);
        for unit in &units {
            let stamps: Vec<i64> = unit.versions.iter().map(|v| v.system_from_ms).collect();
            let mut sorted = stamps.clone();
            sorted.sort();
            assert_eq!(stamps, sorted);
        }
        let x1 = units
            .iter()
            .find(|unit| unit.versions.len() == 2)
            .expect("x = 1");
        assert!(
            x1.has_live_version(),
            "a coordinate with a live version binds"
        );
    }

    /// The record keeps every stamp and the erasure flag, and every live
    /// version takes the destination's surrogate.
    #[test]
    fn the_restore_record_keeps_every_stamp() {
        let array_id = ArrayId::new(TenantId::new(1), "grid");
        let units = coord_units("grid", vec![version(1, 10, Some(7)), version(1, 20, None)])
            .expect("units");
        let surrogates = BTreeMap::from([(units[0].pk.clone(), Surrogate::new(5))]);
        let redo = restore_redo(&array_id, 9, units, &surrogates).expect("redo");
        assert_eq!(redo.vshard_id, 9);
        assert_eq!(redo.puts.len(), 1);
        assert_eq!(redo.puts[0].system_from_ms, 10);
        assert_eq!(redo.puts[0].surrogate, Surrogate::new(5));
        assert_eq!(redo.deletes.len(), 1);
        assert_eq!(redo.deletes[0].system_from_ms, 20);
        assert!(!redo.deletes[0].erasure);
    }
}
