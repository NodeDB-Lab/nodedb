// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Array(ArrayOp)`.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::ArrayOp;

use crate::engine::array::wal::{
    ArrayDeleteCell, ArrayDeletePayload, ArrayPutCell, ArrayPutPayload, encode_delete_with_version,
    encode_put_with_version,
};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;

/// Append the WAL record for a single `ArrayOp`, returning the allocated LSN
/// for the cell write variants (`Some`) or `None` for every read / slice /
/// maintenance variant that carries no durable per-write effect on THIS path.
///
/// The match over [`ArrayOp`] is **exhaustive** (`wildcard_enum_match_arm` is
/// denied), so a future write variant cannot silently become non-durable.
pub(super) fn wal_append_array_op(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &ArrayOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        ArrayOp::Put {
            array_id,
            cells_msgpack,
            wal_lsn: _,
            provenance,
        } => {
            let cells = zerompk::from_msgpack::<Vec<ArrayPutCell>>(cells_msgpack).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal array put decode cells: {e}"),
                }
            })?;
            let payload = ArrayPutPayload {
                array_id: array_id.clone(),
                cells,
                provenance: provenance.clone(),
            };
            let bytes =
                encode_put_with_version(&payload).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal array put encode: {e}"),
                })?;
            Some(wal.append_array_put(tenant_id, vshard_id, database_id, &bytes)?)
        }
        ArrayOp::Delete {
            array_id,
            coords_msgpack,
            wal_lsn: _,
            provenance,
        } => {
            let cells =
                zerompk::from_msgpack::<Vec<ArrayDeleteCell>>(coords_msgpack).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal array delete decode cells: {e}"),
                    }
                })?;
            let payload = ArrayDeletePayload {
                array_id: array_id.clone(),
                cells,
                provenance: provenance.clone(),
            };
            let bytes =
                encode_delete_with_version(&payload).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal array delete encode: {e}"),
                })?;
            Some(wal.append_array_delete(tenant_id, vshard_id, database_id, &bytes)?)
        }
        // NotAWrite — reads / query ops / DDL that produces no engine mutation here.
        // `OpenArray` attaches a schema (catalog-durable); `DropArray` is a
        // per-core store-cache release broadcast after the durable catalog drop
        // (idempotent, no logical cell mutation); the rest are pure reads.
        ArrayOp::OpenArray { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::SurrogateBitmapScan { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RestoreArrayDrop { .. }
        | ArrayOp::PurgeArrayDrop { .. } => None,
        // DurableElsewhere — the flushed / compacted segment is rebuilt on replay
        // from the already-durable Put/Delete WAL records; Flush and Compact only
        // reorganize on-disk tile layout, creating no new logical cell.
        ArrayOp::Flush { .. } | ArrayOp::Compact { .. } => None,
    };
    Ok(appended)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::manager::WalManager;
    use nodedb_array::types::ArrayId;
    use nodedb_physical::physical_plan::PhysicalPlan;

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    fn has_record_of_type(wal: &WalManager, record_type: nodedb_wal::record::RecordType) -> bool {
        wal.sync().expect("sync wal");
        wal.replay().expect("read wal").into_iter().any(|r| {
            nodedb_wal::record::RecordType::from_raw(r.logical_record_type()) == Some(record_type)
        })
    }

    #[test]
    fn put_appends_array_put_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        // Empty cell batch is a valid encoding; the record shape is what matters.
        let cells: Vec<ArrayPutCell> = Vec::new();
        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "g"),
            cells_msgpack: zerompk::to_msgpack_vec(&cells).expect("encode cells"),
            wal_lsn: 0,
            provenance: None,
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "Put must produce a durable LSN");
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::ArrayPut
        ));
    }

    #[test]
    fn compact_appends_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Array(ArrayOp::Compact {
            array_id: ArrayId::new(TenantId::new(1), "g"),
            audit_retain_ms: None,
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_none(), "Compact must produce no durable LSN");
    }
}
