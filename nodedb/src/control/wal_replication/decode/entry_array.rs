// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for the Raft-native array cell writes
//! (`ReplicatedWrite::ArrayCellPut` / `ArrayCellDelete`) — the cluster SQL DML
//! array path, distinct from the Lite-sync `ArrayOp` CRDT variant intercepted
//! upstream by the distributed applier.
//!
//! Delegated from `decode/entry.rs`'s grouped match arm. The reconstructed
//! `ArrayOp::Put` / `Delete` carries the cell/coord bytes verbatim with
//! `wal_lsn: 0` — the follower allocates its own WAL LSN at apply. `entry.rs`
//! binds each put cell's leader-assigned surrogate to its coord tuple
//! afterwards, so the same `(array, coord)` resolves to the same global
//! identity on every node.

use super::super::types::ReplicatedWrite;
use super::ctx::DecodeCtx;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::ArrayOp;
use nodedb_types::sync::wire::SyncProvenance;

pub(super) fn decode_arm(ctx: &DecodeCtx, write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::ArrayCellPut {
            array,
            cells_msgpack,
            provenance,
        } => cell_put(ctx, array, cells_msgpack, provenance),
        ReplicatedWrite::ArrayCellDelete {
            array,
            coords_msgpack,
            provenance,
        } => cell_delete(ctx, array, coords_msgpack, provenance),
        _ => Err(crate::Error::Internal {
            detail: "entry_array::decode_arm called with a non-array-cell ReplicatedWrite \
                variant (dispatch bug in decode/entry.rs's grouped array match arm)"
                .into(),
        }),
    }
}

/// Reconstruct `ArrayOp::Put`. `cells_msgpack` passes through verbatim; the
/// carried surrogate is already stamped in each cell.
fn cell_put(
    ctx: &DecodeCtx,
    array: &str,
    cells_msgpack: &[u8],
    provenance: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Array(ArrayOp::Put {
        array_id: ArrayId::in_database(ctx.tenant_id, ctx.database_id, array),
        cells_msgpack: cells_msgpack.to_vec(),
        wal_lsn: 0,
        provenance: decode_provenance(provenance)?,
    }))
}

/// Reconstruct `ArrayOp::Delete`. Deletes are keyed by exact coordinate and
/// carry no surrogate, so there is nothing to bind — `coords_msgpack` passes
/// through verbatim.
fn cell_delete(
    ctx: &DecodeCtx,
    array: &str,
    coords_msgpack: &[u8],
    provenance: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Array(ArrayOp::Delete {
        array_id: ArrayId::in_database(ctx.tenant_id, ctx.database_id, array),
        coords_msgpack: coords_msgpack.to_vec(),
        wal_lsn: 0,
        provenance: decode_provenance(provenance)?,
    }))
}

/// Decode replicated sync provenance bytes back into `SyncProvenance`. Absent
/// (`None`) is normal for locally-originated cluster writes.
fn decode_provenance(bytes: &Option<Vec<u8>>) -> crate::Result<Option<SyncProvenance>> {
    match bytes {
        None => Ok(None),
        Some(b) => zerompk::from_msgpack::<SyncProvenance>(b)
            .map(Some)
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array provenance decode: {e}"),
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::ReplicatedEntry;
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::Surrogate;

    /// Decide + encode in one call, so each test names only the plan it encodes.
    fn to_replicated_entry(
        tenant_id: TenantId,
        database_id: DatabaseId,
        vshard_id: VShardId,
        plan: &PhysicalPlan,
    ) -> crate::Result<Option<ReplicatedEntry>> {
        let write = crate::control::wal_replication::ReplicableWrite::decide_for_replication(plan)?;
        crate::control::wal_replication::encode::to_replicated_entry(
            tenant_id,
            database_id,
            vshard_id,
            &write,
        )
    }

    #[test]
    fn array_cell_put_roundtrips_and_carries_surrogate() {
        let tenant = TenantId::new(3);
        let vshard = VShardId::new(7);
        let array_id = ArrayId::new(tenant, "genome");

        // A cell carrying a real surrogate — the losslessness subject.
        let cell = ArrayPutCell {
            coord: vec![CoordValue::Int64(5), CoordValue::Int64(7)],
            attrs: vec![CellValue::Float64(42.0)],
            surrogate: Surrogate::new(9999),
            system_from_ms: 1,
            valid_from_ms: 1,
            valid_until_ms: i64::MAX,
        };
        let cells_msgpack = zerompk::to_msgpack_vec(&vec![cell]).unwrap();

        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: array_id.clone(),
            cells_msgpack: cells_msgpack.clone(),
            wal_lsn: 123,
            provenance: None,
        });

        // Must encode (no longer a replication gap).
        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("ArrayOp::Put must encode to a ReplicatedWrite");
        let bytes = entry.to_bytes();

        // Decode with no assigner (surrogate binding is a no-op) — the cells (and
        // thus the carried surrogate) must survive verbatim, wal_lsn resets to 0.
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("ArrayCellPut must decode to a plan");
        match decoded_plan {
            PhysicalPlan::Array(ArrayOp::Put {
                array_id: decoded_id,
                cells_msgpack: decoded_cells,
                wal_lsn,
                provenance,
            }) => {
                assert_eq!(
                    decoded_id, array_id,
                    "array id reconstructed from header tenant + name"
                );
                assert_eq!(
                    decoded_cells, cells_msgpack,
                    "cells (with surrogate) carried verbatim"
                );
                assert_eq!(wal_lsn, 0, "follower allocates its own wal_lsn at apply");
                assert!(provenance.is_none());
            }
            other => panic!("expected Array(Put), got {other:?}"),
        }
    }

    #[test]
    fn array_cell_delete_roundtrips_verbatim() {
        let tenant = TenantId::new(2);
        let vshard = VShardId::new(4);
        let array_id = ArrayId::new(tenant, "genome");

        let coords = vec![vec![CoordValue::Int64(1), CoordValue::Int64(2)]];
        let coords_msgpack = zerompk::to_msgpack_vec(&coords).unwrap();

        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: array_id.clone(),
            coords_msgpack: coords_msgpack.clone(),
            wal_lsn: 55,
            provenance: None,
        });

        let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .expect("ArrayOp::Delete must encode to a ReplicatedWrite");
        let bytes = entry.to_bytes();
        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("ArrayCellDelete must decode to a plan");
        match decoded_plan {
            PhysicalPlan::Array(ArrayOp::Delete {
                array_id: decoded_id,
                coords_msgpack: decoded_coords,
                wal_lsn,
                provenance,
            }) => {
                assert_eq!(decoded_id, array_id);
                assert_eq!(decoded_coords, coords_msgpack, "coords carried verbatim");
                assert_eq!(wal_lsn, 0);
                assert!(provenance.is_none());
            }
            other => panic!("expected Array(Delete), got {other:?}"),
        }
    }
}
