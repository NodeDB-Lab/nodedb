// SPDX-License-Identifier: BUSL-1.1

//! Apply path for a committed `ReplicatedWrite::SurrogateBind` entry.
//!
//! The leader of a key's collection home mints the key's surrogate by
//! proposing this entry (`surrogate_exchange::authority`). Every replica binds
//! each key first-wins, in Raft log order. A key an earlier entry of the log
//! already bound keeps that binding on every replica alike. The apply answers
//! each key's winner, in entry order, as its payload: msgpack `Vec<u32>`.
//! The binds land in the fsync-committed catalog before the apply reports.

use nodedb_types::{CollectionKey, Surrogate};

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::AppliedWrite;
use crate::types::{DatabaseId, TenantId};

use super::context::ApplyContext;
use super::proposal_gate::EntryOutcome;

/// One `SurrogateBind` entry: its collection and its `(pk, candidate)` keys.
pub(super) struct BindKeys<'e> {
    pub(super) tenant_id: TenantId,
    pub(super) database_id: DatabaseId,
    pub(super) collection: &'e str,
    pub(super) binds: &'e [(Vec<u8>, u32)],
}

/// Bind every key of the entry and resolve its waiter.
pub(super) fn apply_surrogate_bind(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: BindKeys<'_>,
) -> EntryOutcome {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
        ..
    } = pos;
    let result = bind_keys(&ctx, &entry).and_then(|winners| {
        zerompk::to_msgpack_vec(&winners).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("surrogate bind winners encode: {e}"),
        })
    });
    let durable = result.is_ok();
    ctx.tracker.complete(
        group_id,
        log_index,
        applied_key,
        result.map(AppliedWrite::new),
    );
    EntryOutcome::Applied {
        durable,
        result: None,
    }
}

/// Bind the entry's keys first-wins in one catalog transaction, in entry
/// order, and return the winner of every key, in entry order.
fn bind_keys(ctx: &ApplyContext<'_>, entry: &BindKeys<'_>) -> crate::Result<Vec<u32>> {
    let rows: Vec<(&[u8], Surrogate)> = entry
        .binds
        .iter()
        .map(|(pk, candidate)| (pk.as_slice(), Surrogate::new(*candidate)))
        .collect();
    let bound = ctx.state.surrogate_assigner.bind_many(
        CollectionKey::from_bare(entry.database_id, entry.collection),
        entry.tenant_id,
        &rows,
    )?;
    if bound.len() != rows.len() {
        return Err(crate::Error::Internal {
            detail: format!(
                "surrogate bind in '{}' answered {} of {} keys",
                entry.collection,
                bound.len(),
                rows.len()
            ),
        });
    }
    Ok(bound
        .into_iter()
        .map(|surrogate| surrogate.as_u32())
        .collect())
}
