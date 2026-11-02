// SPDX-License-Identifier: BUSL-1.1

//! Array serializer for transaction resolve.
//!
//! Plan-driven, like the vector and columnar serializers: array batch writes
//! ride the buffered-plan path rather than a per-surrogate overlay post-image,
//! and their redo replay re-runs the engine's native cell batch
//! (`replay_array_wal`, dispatched from the redo reconstitute path, which
//! respects the array manifest's replay stamp). This module reads the
//! [`ArrayOp`] plan node directly and emits the SAME `RecordType::ArrayPut` /
//! `RecordType::ArrayDelete` sub-record the autocommit array path produces,
//! reusing its version-tagged encoders (`engine::array::wal`):
//!
//! * `Put` → `RecordType::ArrayPut`. The plan's `cells_msgpack` is spliced
//!   into the versioned payload by [`encode_put_spliced`], never decoded. The
//!   record is byte-identical to the autocommit `ArrayOp::Put` arm.
//! * `Delete` → `RecordType::ArrayDelete`, via [`encode_delete_spliced`] over
//!   the plan's `coords_msgpack`.
//!
//! ## Ops that emit nothing / raise a typed error
//!
//! Read and maintenance ops (`Slice`, `Project`, `Aggregate`, `Elementwise`,
//! `SurrogateBitmapScan`, `Flush`, `Compact`) carry no persisted logical
//! post-image and emit nothing. `OpenArray` / `DropArray` are catalog DDL with
//! no redo sub-record shape and raise a typed error, matching how the KV /
//! document serializers reject DDL.
//!
//! ## Determinism
//!
//! Emission is in plan order (a fixed `&[PhysicalPlan]`); each `Put` / `Delete`
//! maps to exactly one sub-record.

use nodedb_physical::physical_plan::ArrayOp;
use nodedb_wal::record::RecordType;

use crate::engine::array::wal::{encode_delete_spliced, encode_put_spliced};
use crate::wal::RedoSubRecord;

/// Append the redo sub-record for a single array plan op to `ops`.
///
/// `Put` / `Delete` serialize to their version-tagged `ArrayPut` / `ArrayDelete`
/// shape; read and maintenance ops emit nothing; catalog DDL raises a typed
/// error (see module docs).
pub(super) fn serialize_array_op(op: &ArrayOp, ops: &mut Vec<RedoSubRecord>) -> crate::Result<()> {
    match op {
        ArrayOp::Put {
            array_id,
            cells_msgpack,
            provenance,
            vshard_id,
            ..
        } => {
            let bytes = encode_put_spliced(array_id, *vshard_id, cells_msgpack, provenance)
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("array resolve put encode: {e}"),
                })?;
            ops.push(RedoSubRecord {
                record_type: RecordType::ArrayPut as u32,
                payload: bytes,
            });
            Ok(())
        }
        ArrayOp::Delete {
            array_id,
            coords_msgpack,
            provenance,
            vshard_id,
            ..
        } => {
            let bytes = encode_delete_spliced(array_id, *vshard_id, coords_msgpack, provenance)
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("array resolve delete encode: {e}"),
                })?;
            ops.push(RedoSubRecord {
                record_type: RecordType::ArrayDelete as u32,
                payload: bytes,
            });
            Ok(())
        }

        // Read / maintenance families: no persisted logical post-image. The
        // cells survive via their `ArrayPut` records; a flush or compaction is
        // re-derived on replay, so neither carries a redo sub-record.
        ArrayOp::Slice { .. }
        | ArrayOp::SurrogateBitmapScan { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::Flush { .. }
        | ArrayOp::Compact { .. } => Ok(()),

        // Catalog DDL: no row-level post-image and no redo sub-record shape.
        // Rejected like the KV / document index/DDL ops.
        ArrayOp::OpenArray { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RekeyArray { .. }
        | ArrayOp::PurgeArrayDrop { .. } => Err(crate::Error::PlanError {
            detail: "array OPEN/DROP (catalog DDL) is not supported in transaction resolve"
                .to_string(),
        }),
    }
}
