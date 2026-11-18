// SPDX-License-Identifier: BUSL-1.1

//! WAL redo append/stamp for the funnel.
//!
//! Array DDL never reaches a core as a write: it runs through the replicated
//! catalog (`array_catalog::ddl`). The funnel refuses it.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::change_events::{EntryChanges, PendingChanges};
use crate::control::server::dispatch_utils::minted::{MintedRecords, RecordOwner};
use crate::control::server::wal_dispatch::{self, WalAppendRequest};
use crate::control::state::SharedState;
use crate::event::cdc::position::{ChangePositionMarker, MarkerChanges};
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
    /// The system-time ordinal the write stamps: the one the leader of the
    /// applied entry assigned, the one the caller's records carry, or the one
    /// the append reserved. Every record of the write carries it.
    pub system_ordinal: Option<i64>,
    /// The write's open ordinal reservation: it stays open until the write
    /// applied or failed, so no read cut at or above the ordinal closes
    /// before then (`read_cut::reservations`).
    pub reservation: Option<crate::control::read_cut::OrdinalReservation>,
    /// The change marker the append wrote ahead of the write's records,
    /// when the write applies a replicated entry.
    pub change_marker: Option<AppendedMarker>,
    /// The write's change set, staged at the change marker's LSN.
    pub changes: Option<PendingChanges>,
    /// The write's record is a stream marker (see
    /// `wal_dispatch::WalAppendOutcome::stream_marker`).
    pub stream_marker: bool,
}

/// A change marker the append wrote, and its LSN.
#[derive(Debug, Clone, Copy)]
pub(super) struct AppendedMarker {
    pub head: ChangePositionMarker,
    pub lsn: Lsn,
}

/// Where and how [`authorize_and_append`] journals one write.
pub(super) struct AppendScope<'a> {
    pub owner: RecordOwner,
    /// The window the write's records join.
    pub minted: Option<&'a MintedRecords>,
    pub event_source: crate::event::EventSource,
    pub commit_hlc: Option<u64>,
    pub groups: bool,
    /// The change set of a write that applies a replicated entry. Its rows
    /// ride the entry's change marker.
    pub changes: Option<EntryChanges>,
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
///
/// `changes` rides the entry's change marker, which precedes every record
/// of the write. The marker holds its place in the WAL until the entry's
/// group saves an applied floor at or above the entry (see
/// `change_stream::unjournaled`). A write with changes and no marker
/// position is refused before any record is appended.
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
        mut changes,
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

    let (marked_position, carried_ordinal) = match &durability {
        WalDurability::AppendHere {
            change_position,
            system_ordinal,
            ..
        } => (*change_position, *system_ordinal),
        WalDurability::CallerSupplied { system_ordinal, .. } => (None, *system_ordinal),
    };
    if changes.is_some() && marked_position.is_none() {
        return Err(crate::Error::Internal {
            detail: "internal invariant break: a replicated write's change set reached the WAL \
                     append with no change position to mark"
                .into(),
        });
    }
    let mut system_ordinal = carried_ordinal;
    let mut reservation = None;
    let mut change_marker = None;
    let mut pending_changes = None;
    let (wal_lsn, resolved_now_ms, group_origin, stream_marker) = match durability {
        WalDurability::AppendHere {
            now_override,
            apply_key,
            change_position,
            ..
        } => {
            // The marker precedes the redo in the WAL, so the fsync that makes
            // the redo durable makes the marker durable too.
            // The write's system-time ordinal is fixed here, under the guard
            // and before the record, so every record of the write carries it:
            // the one the applied entry's leader assigned, else one of this
            // node's clock.
            // The draw runs under the reservation registry's lock, so a read
            // cut closing here either sees the reservation or stamps below
            // the write. A carried ordinal is reserved here too: the leader's
            // own reservation ends when the apply starts.
            let clock = shared.wal.ordinal_clock();
            let (drawn, reserved) = shared.read_cut.reservations.reserve_with(|| {
                wal_dispatch::stamp_system_ordinals(&mut plan, &clock, carried_ordinal)
            });
            system_ordinal = drawn;
            reservation = reserved;
            // A session commit's versions take at least the reserved
            // ordinal, so each sits at or above an open reservation until it
            // installs. The record holds the raised stamps, so restart
            // replay installs the same versions.
            wal_dispatch::floor_redo_stamps(&mut plan, system_ordinal)?;
            if let Some(position) = change_position {
                let head = ChangePositionMarker {
                    apply_key,
                    position,
                };
                let tail = changes.as_ref().map_or(
                    MarkerChanges::Rows(Vec::new()),
                    EntryChanges::marker_changes,
                );
                let lsn = append_change_marker(shared, owner, commit_hlc, head, &tail)?;
                // The marker holds its place for the entry until a floor
                // covers it.
                shared.change_stream.unjournaled().note_marker(
                    position.group_id,
                    position.log_index,
                    shared.wal.floor_holds().hold(lsn),
                );
                change_marker = Some(AppendedMarker { head, lsn });
                pending_changes = changes.take().map(|changes| changes.at_marker(lsn));
            }
            let request = WalAppendRequest {
                wal: stamped(
                    match minted {
                        Some(minted) => minted.appender(&shared.wal, apply_key),
                        None => shared.wal.appender(apply_key),
                    },
                    commit_hlc,
                )
                .with_system_ordinal(system_ordinal),
                event_source,
                tenant_id,
                vshard_id,
                database_id,
                plan: &plan,
                credentials: None,
                now_override,
            };
            let (lsn, resolved_now_ms, group_origin, stream_marker) = if groups {
                let appended = wal_dispatch::append_group_origin(request)?;
                (
                    Some(appended.origin.lsn),
                    appended.resolved_now_ms,
                    Some(appended.origin),
                    appended.stream_marker,
                )
            } else {
                let outcome = wal_dispatch::wal_append(request)?;
                (
                    outcome.lsn,
                    outcome.resolved_now_ms,
                    None,
                    outcome.stream_marker,
                )
            };
            // Recorded before the enqueue, so it exists before any event of
            // the write reaches the Event Plane.
            if let (Some(position), Some(lsn)) = (change_position, lsn) {
                shared.cdc_router.positions().record(lsn.as_u64(), position);
            }
            (lsn, resolved_now_ms, group_origin, stream_marker)
        }
        WalDurability::CallerSupplied {
            wal_lsn,
            resolved_now_ms,
            ..
        } => (wal_lsn, resolved_now_ms, None, false),
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
        reservation,
        change_marker,
        changes: pending_changes,
        stream_marker,
    })
}

/// Append the change marker `head` with `tail`. Returns its LSN.
pub(super) fn append_change_marker(
    shared: &SharedState,
    owner: RecordOwner,
    commit_hlc: Option<u64>,
    head: ChangePositionMarker,
    tail: &MarkerChanges,
) -> crate::Result<Lsn> {
    let lsn = stamped(shared.wal.appender(NO_APPLY_KEY), commit_hlc).append_change_position(
        owner.tenant_id,
        owner.vshard_id,
        owner.database_id,
        &head.with_changes(tail)?,
    )?;
    Ok(lsn)
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
