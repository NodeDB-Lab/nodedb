// SPDX-License-Identifier: BUSL-1.1

//! Prepare a committed `ReplicatedWrite::ArrayOp` entry: route its cell to
//! the array incarnation it writes, then apply it through the write funnel.

use crate::control::array_sync::ArrayOpTarget;
use crate::control::array_sync::raft_apply::{AppliedPosition, apply_array_op};
use crate::types::DatabaseId;

use super::context::{ApplyContext, FinishedApply};
use super::proposal_gate::EntryOutcome;
use super::start::Prepared;

/// The parts of a `ReplicatedWrite::ArrayOp` its apply takes.
pub(super) struct ArrayOpWrite {
    pub cell: super::array_cell_route::CellWrite,
    /// The vShard the entry was proposed to: the tile route of the cell.
    pub vshard: crate::types::VShardId,
    pub op_bytes: Vec<u8>,
    pub cell_surrogate: Option<u32>,
    pub provenance: Option<Vec<u8>>,
}

/// Prepare an array op. The op path submits through the write funnel, so
/// its redo is durable before it reports success. A failure breaks the
/// prefix: the entry must stay replayable.
pub(super) fn prepare_array_op<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry_database: DatabaseId,
    write: ArrayOpWrite,
) -> Prepared<'a> {
    Prepared::Exclusive(Box::pin(async move {
        let ArrayOpWrite {
            cell,
            vshard,
            op_bytes,
            cell_surrogate,
            provenance,
        } = write;
        let (_gate, database_id) = match super::array_cell_route::route_cell_write(
            ctx,
            pos,
            entry_database,
            &cell,
        )
        .await
        {
            Ok(routed) => routed,
            Err(finished) => return *finished,
        };
        let applied_ok = apply_array_op(
            ctx.state,
            ctx.tracker,
            pos,
            ArrayOpTarget {
                tenant_id: cell.tenant_id,
                database_id,
                array: &cell.array,
                vshard,
            },
            &op_bytes,
            cell_surrogate,
            provenance.as_deref(),
        )
        .await;
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome: EntryOutcome::Applied {
                durable: applied_ok,
                result: None,
            },
        }
    }))
}
