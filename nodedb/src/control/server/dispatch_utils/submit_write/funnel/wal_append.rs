// SPDX-License-Identifier: BUSL-1.1

//! WAL redo append/stamp for the funnel.
//!
//! Array DDL never reaches a core as a write: it runs through the replicated
//! catalog (`array_catalog::ddl`). The funnel refuses it.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::minted::{MintedRecords, RecordOwner};
use crate::control::server::wal_dispatch::{self, WalAppendRequest};
use crate::control::state::SharedState;
use crate::event::cdc::position::ChangePositionMarker;
use crate::types::Lsn;
use crate::wal::manager::NO_APPLY_KEY;

use super::super::params::WalDurability;

/// What the authorize-and-append phase produced: the plan (stamped with its
/// minted LSN, if any) and the resolved durability values.
pub(super) struct WalAppendOutcome {
    pub plan: PhysicalPlan,
    pub wal_lsn: Option<Lsn>,
    pub resolved_now_ms: Option<u64>,
    /// The origin of the write's record group, when it journals rows after
    /// apply. `wal_lsn` is its LSN.
    pub group_origin: Option<wal_dispatch::GroupOrigin>,
    /// The replicated position whose marker precedes the write's records.
    pub change_position: Option<crate::event::cdc::position::ReplicatedPosition>,
    /// The system-time ordinal the leader of the applied entry assigned.
    pub system_ordinal: Option<i64>,
}

/// Where and how [`authorize_and_append`] journals one write.
pub(super) struct AppendScope<'a> {
    pub owner: RecordOwner,
    /// The window the write's records join.
    pub minted: Option<&'a MintedRecords>,
    pub event_source: crate::event::EventSource,
    pub commit_hlc: Option<u64>,
    pub groups: bool,
}

/// Refuse array DDL, then make the write durable: append its WAL redo record
/// here (under the write-admission guard the caller already holds) or take
/// the LSN a caller supplied upstream.
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
/// upstream path can allocate an LSN of its own and hope it matches.
///
/// An `AppendHere` write appends through `minted`, so every record it writes
/// joins the write's outcome-floor window.
///
/// `commit_hlc` is the write's commit instant when it is known before the
/// append. Every record the write appends carries it. `None` stamps each
/// record with the node's HLC at its append.
///
/// `groups` is whether the write journals rows after apply. Such a write
/// appends its group's origin (see `wal_dispatch::append_group_origin`).
pub(super) fn authorize_and_append(
    shared: &SharedState,
    mut plan: PhysicalPlan,
    durability: WalDurability,
    scope: AppendScope<'_>,
) -> crate::Result<WalAppendOutcome> {
    let AppendScope {
        owner,
        minted,
        event_source,
        commit_hlc,
        groups,
    } = scope;
    let RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    } = owner;
    if crate::control::array_catalog::ddl::is_array_ddl(&plan) {
        return Err(crate::Error::Internal {
            detail: "array DDL reached the write funnel; it runs through the replicated \
                     array catalog"
                .into(),
        });
    }

    let (marked_position, system_ordinal) = match &durability {
        WalDurability::AppendHere {
            change_position,
            system_ordinal,
            ..
        } => (*change_position, *system_ordinal),
        WalDurability::CallerSupplied { .. } => (None, None),
    };
    let (wal_lsn, resolved_now_ms, group_origin) = match durability {
        WalDurability::AppendHere {
            now_override,
            apply_key,
            change_position,
            ..
        } => {
            // The marker precedes the redo in the WAL, so the fsync that makes
            // the redo durable makes the marker durable too.
            // The rows' system-time ordinals are fixed here, under the guard
            // and before the record, so the record carries them: the run the
            // applied entry's leader assigned, else a run of this node's clock.
            wal_dispatch::stamp_system_ordinals(
                &mut plan,
                &shared.wal.ordinal_clock(),
                system_ordinal,
            );
            if let Some(position) = change_position {
                let marker = ChangePositionMarker {
                    apply_key,
                    position,
                };
                stamped(shared.wal.appender(NO_APPLY_KEY), commit_hlc).append_change_position(
                    tenant_id,
                    vshard_id,
                    database_id,
                    &marker.to_bytes(),
                )?;
            }
            let request = WalAppendRequest {
                wal: stamped(
                    match minted {
                        Some(minted) => minted.appender(&shared.wal, apply_key),
                        None => shared.wal.appender(apply_key),
                    },
                    commit_hlc,
                ),
                event_source,
                tenant_id,
                vshard_id,
                database_id,
                plan: &plan,
                credentials: None,
                now_override,
            };
            let (lsn, resolved_now_ms, group_origin) = if groups {
                let appended = wal_dispatch::append_group_origin(request)?;
                (
                    Some(appended.origin.lsn),
                    appended.resolved_now_ms,
                    Some(appended.origin),
                )
            } else {
                let outcome = wal_dispatch::wal_append(request)?;
                (outcome.lsn, outcome.resolved_now_ms, None)
            };
            // Recorded before the enqueue, so it exists before any event of
            // the write reaches the Event Plane.
            if let (Some(position), Some(lsn)) = (change_position, lsn) {
                shared.cdc_router.positions().record(lsn.as_u64(), position);
            }
            (lsn, resolved_now_ms, group_origin)
        }
        WalDurability::CallerSupplied {
            wal_lsn,
            resolved_now_ms,
            ..
        } => (wal_lsn, resolved_now_ms, None),
    };

    if let Some(lsn) = wal_lsn {
        wal_dispatch::stamp_minted_lsn(&mut plan, lsn);
    }

    Ok(WalAppendOutcome {
        plan,
        wal_lsn,
        resolved_now_ms,
        group_origin,
        change_position: marked_position,
        system_ordinal,
    })
}

/// `appender`, carrying `commit_hlc` when it is known.
fn stamped(
    appender: crate::wal::manager::WalAppender<'_>,
    commit_hlc: Option<u64>,
) -> crate::wal::manager::WalAppender<'_> {
    match commit_hlc {
        Some(hlc) => appender.with_commit_hlc(hlc),
        None => appender,
    }
}
