// SPDX-License-Identifier: BUSL-1.1

//! Array ops: a put binds every cell's coordinate tuple. The key is
//! `zerompk(coord)`, the same bytes the planner assigned under.

use nodedb_physical::physical_plan::ArrayOp;

use super::binder::IdentityBinder;
use crate::engine::array::wal::ArrayPutCell;

pub(super) fn bind(binder: &IdentityBinder<'_>, op: &mut ArrayOp) -> crate::Result<()> {
    match op {
        ArrayOp::Put {
            array_id,
            cells_msgpack,
            ..
        } => {
            let mut cells: Vec<ArrayPutCell> =
                zerompk::from_msgpack(cells_msgpack).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("array cell put decode: {e}"),
                })?;
            let mut rewritten = false;
            for cell in cells.iter_mut() {
                let pk_bytes = zerompk::to_msgpack_vec(&cell.coord).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("array coord pk encode: {e}"),
                    }
                })?;
                let bound = binder.resolve(&array_id.name, &pk_bytes, cell.surrogate)?;
                if bound != cell.surrogate {
                    cell.surrogate = bound;
                    rewritten = true;
                }
            }
            // First-wins can hand back an earlier binding; the cells must
            // carry it, or the tile stores a row under an identity no catalog
            // resolves.
            if rewritten {
                *cells_msgpack =
                    zerompk::to_msgpack_vec(&cells).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("array cell put re-encode: {e}"),
                    })?;
            }
            Ok(())
        }
        // Deletes are keyed by exact coordinate and carry no surrogate; the
        // rest read, reshape or maintain the array.
        ArrayOp::Delete { .. }
        | ArrayOp::OpenArray { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::Flush { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::SurrogateBitmapScan { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RestoreArrayDrop { .. }
        | ArrayOp::PurgeArrayDrop { .. } => Ok(()),
    }
}
