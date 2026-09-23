// SPDX-License-Identifier: BUSL-1.1

//! Array DDL authorization and WAL redo append/stamp for the funnel.
//!
//! Array DDL conversion is intentionally read-only. Once a task has passed
//! authorization and admission, its durable catalog state installs
//! immediately before the Data-Plane dispatch; the mirror changes only after
//! the redb transaction commits. The DDL transition this creates must be
//! rolled back by every later phase that can fail before the Data Plane has
//! proved it applied the write — see [`rollback_on_err`].

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ddl::AuthorizedDdlTransition;
use crate::control::server::wal_dispatch::{self, WalAppendRequest};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

use super::super::params::WalDurability;

/// What the authorize-and-append phase produced: the DDL transition every
/// later phase must roll back on failure, the plan (stamped with its minted
/// LSN, if any), and the resolved durability values.
pub(super) struct WalAppendOutcome {
    pub ddl_transition: AuthorizedDdlTransition,
    pub plan: PhysicalPlan,
    pub wal_lsn: Option<Lsn>,
    pub resolved_now_ms: Option<u64>,
}

/// Roll `ddl_transition` back and convert `result`'s error, or pass a success
/// through unchanged. Every phase after DDL authorization that can fail calls
/// this instead of returning its error directly, so an authorized Array
/// CREATE/DROP/ALTER never survives a failure later in the sequence.
pub(super) fn rollback_on_err<T>(
    shared: &SharedState,
    ddl_transition: &AuthorizedDdlTransition,
    result: crate::Result<T>,
) -> crate::Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            let _ = ddl_transition.rollback(shared);
            Err(error)
        }
    }
}

/// Install the plan's Array DDL catalog transition, then make the write
/// durable: append its WAL redo record here (under the write-admission guard
/// the caller already holds) or take the LSN a caller supplied upstream.
///
/// Durability, under the guard, immediately before the enqueue: the LSN is
/// minted in the same order the request is about to be enqueued.
///
/// Writes the resolved LSN back into the plan itself. The envelope's
/// `wal_lsn` is where most engines read their committed version from, but the
/// array engine stamps its tile versions from the LSN carried in the plan
/// while replay stamps them from the record header — so the plan the Data
/// Plane is about to execute must name the record that reproduces it. This is
/// the only place that knows both, and it knows them for every caller: no
/// upstream path may allocate an LSN of its own and hope it matches.
pub(super) fn authorize_and_append(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    mut plan: PhysicalPlan,
    durability: WalDurability,
) -> crate::Result<WalAppendOutcome> {
    let ddl_transition = crate::control::array_catalog::ddl::apply_authorized_ddl(
        shared,
        tenant_id,
        database_id,
        &plan,
    )?;

    let (wal_lsn, resolved_now_ms) = match durability {
        WalDurability::AppendHere {
            now_override,
            apply_key,
        } => {
            let outcome = rollback_on_err(
                shared,
                &ddl_transition,
                shared.wal.with_apply_key(apply_key, || {
                    wal_dispatch::wal_append(WalAppendRequest {
                        wal: &shared.wal,
                        tenant_id,
                        vshard_id,
                        database_id,
                        plan: &plan,
                        credentials: None,
                        now_override,
                    })
                }),
            )?;
            (outcome.lsn, outcome.resolved_now_ms)
        }
        WalDurability::CallerSupplied {
            wal_lsn,
            resolved_now_ms,
        } => (wal_lsn, resolved_now_ms),
    };

    if let Some(lsn) = wal_lsn {
        wal_dispatch::stamp_minted_lsn(&mut plan, lsn);
    }

    Ok(WalAppendOutcome {
        ddl_transition,
        plan,
        wal_lsn,
        resolved_now_ms,
    })
}
