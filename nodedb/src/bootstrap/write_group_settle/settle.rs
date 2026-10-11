// SPDX-License-Identifier: BUSL-1.1

//! Settle every record group a crash left broken, before any replay.
//!
//! A grouped write stores its write set beside its effects (see
//! `data::executor::core_loop::write_set_journal`). Boot compares the stored
//! write sets with the recovered WAL:
//!
//! - A stored write set whose origin the WAL holds gets the parts the WAL
//!   lacks, and the marker that cancels its origin when the write set says
//!   so and the WAL lacks it.
//! - A stored write set whose origin the crash cut journals the origin again
//!   from the stored append inputs, then every part. The effects are
//!   durable, so the WAL must name them.
//! - A broken group with no stored write set had no durable effect. Its
//!   origin, announcements, continuations and every part the WAL holds are
//!   cancelled. A committed
//!   transaction record is the exception: it always applies, so it stays,
//!   its group closes with one part with no rows, and replay runs its
//!   install.
//!
//! Restart replay, a point-in-time restore of the archived WAL, and the event
//! stream rebuilt from it then agree with the stores. Boot makes the appended
//! records durable before it drops the stored write sets.

use nodedb_wal::WalRecord;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::wal_dispatch::{
    GroupOrigin, WalAppendRequest, WriteSetTarget, append_group_origin, append_group_parts,
    append_planned_parts, plan_group_parts,
};
use crate::event::EventSource;
use crate::event::cdc::position::{ChangePositionMarker, ReplicatedPosition};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::{WalAppender, WalManager};
use crate::wal::{OriginAppend, WriteSetCapture};

use super::scan::WalGroups;
use super::stores::StoredWriteSets;

/// Settle every broken group of `records`, the stream replay read from
/// `wal`, against the write sets stored under `data_dir`. Returns whether it
/// appended a record, in which case the caller reads the stream again.
pub fn settle_write_groups(
    wal: &WalManager,
    data_dir: &std::path::Path,
    records: &[WalRecord],
) -> crate::Result<bool> {
    let stored = StoredWriteSets::read(data_dir)?;
    let appended = settle_against(wal, records, &stored.captures)?;
    if appended {
        wal.sync()?;
    }
    stored.clear()?;
    Ok(appended)
}

/// The settle itself, over write sets already read.
pub(crate) fn settle_against(
    wal: &WalManager,
    records: &[WalRecord],
    captures: &[WriteSetCapture],
) -> crate::Result<bool> {
    let scanned = WalGroups::scan(records)?;
    let wal_end = wal.next_lsn().as_u64();
    let mut appended = false;
    let mut captured = std::collections::HashSet::new();
    for capture in captures {
        captured.insert(capture.origin);
        appended |= if capture.origin >= wal_end {
            journal_cut_write(wal, capture)?
        } else {
            complete_group(wal, &scanned, capture)?
        };
    }
    for (&origin, scan) in &scanned.groups {
        if scan.is_whole() || captured.contains(&origin) {
            continue;
        }
        if scanned.committed.contains(&origin) {
            appended |= close_committed_group(wal, &scanned, origin, scan)?;
            continue;
        }
        let mut cancelled: Vec<(u64, super::scan::RecordHome)> = scan
            .seen
            .values()
            .copied()
            .chain(scan.members.iter().copied())
            .collect();
        if let Some(home) = scanned.present.get(&origin) {
            cancelled.push((origin, *home));
        }
        for (lsn, home) in cancelled {
            wal.appender(crate::wal::manager::NO_APPLY_KEY)
                .append_write_aborted(
                    home.tenant_id,
                    home.vshard_id,
                    home.database_id,
                    Lsn::new(lsn),
                )?;
            appended = true;
        }
    }
    Ok(appended)
}

/// Close the group of the committed transaction record at `origin` with one
/// part with no rows. The record's install never became durable, so replay
/// runs it for the first time. The edge tombstones of its deletes are its
/// own `EdgeDelete` sub-records, which replay writes at their ordinals.
fn close_committed_group(
    wal: &WalManager,
    scanned: &WalGroups,
    origin: u64,
    scan: &super::scan::GroupScan,
) -> crate::Result<bool> {
    let Some(home) = scanned.present.get(&origin) else {
        return Ok(false);
    };
    // A record whole at append takes no part. Replay applies the
    // continuations the WAL holds.
    if !scan.seen.is_empty() || scan.closed.is_some() {
        return Ok(false);
    }
    // The part carries the apply key, event source and commit instant of
    // the record it closes, as every part of a write does.
    let source = EventSource::from_wal_code(home.event_source).ok_or(crate::Error::Internal {
        detail: format!(
            "committed record at lsn {origin} names unknown event source code {}",
            home.event_source
        ),
    })?;
    wal.appender(home.apply_key)
        .with_event_source(source)
        .with_commit_hlc(home.commit_hlc)
        .append_write_group(
            home.tenant_id,
            home.vshard_id,
            home.database_id,
            &crate::wal::WriteGroupRecord {
                group: crate::wal::WriteGroup::part_of(origin, 1, 1),
                ops: Vec::new(),
                redo: None,
            },
        )?;
    Ok(true)
}

/// Append the parts of `capture`'s group the WAL lacks, and the marker that
/// cancels its origin when the write set cancels it and the WAL still
/// applies the origin.
fn complete_group(
    wal: &WalManager,
    scanned: &WalGroups,
    capture: &WriteSetCapture,
) -> crate::Result<bool> {
    let appender = part_appender(wal, &capture.origin_append)?;
    let target = target(capture, Lsn::new(capture.origin));
    let write_set = capture.write_set();
    let parts = plan_group_parts(&target, &write_set, wal.max_payload())?;
    let cancels_origin = parts.cancels_origin;
    let present = scanned.parts_present(capture.origin);
    let mut appended =
        append_planned_parts(appender, &target, parts, |part| present.contains(&part))?.is_some();
    if cancels_origin && scanned.present.contains_key(&capture.origin) {
        appender.append_write_aborted(
            target.tenant_id,
            target.vshard_id,
            target.database_id,
            target.origin.lsn,
        )?;
        appended = true;
    }
    Ok(appended)
}

/// Journal a write whose origin the crash cut: its change position marker,
/// its origin, then every part.
fn journal_cut_write(wal: &WalManager, capture: &WriteSetCapture) -> crate::Result<bool> {
    let inputs = &capture.origin_append;
    let appender = part_appender(wal, inputs)?;
    let plan = stored_plan(inputs)?;
    let home = target(capture, Lsn::ZERO);
    if let Some((epoch, group_id, log_index)) = inputs.change_position {
        let marker = ChangePositionMarker {
            apply_key: inputs.apply_key,
            position: ReplicatedPosition {
                epoch,
                group_id,
                log_index,
            },
        };
        // The marker's header names no apply key, as the funnel's does.
        let marker_appender = wal.appender(crate::wal::manager::NO_APPLY_KEY);
        let marker_appender = match inputs.commit_hlc {
            Some(hlc) => marker_appender.with_commit_hlc(hlc),
            None => marker_appender,
        };
        marker_appender.append_change_position(
            home.tenant_id,
            home.vshard_id,
            home.database_id,
            &marker.to_bytes(),
        )?;
    }
    let origin = append_group_origin(WalAppendRequest {
        wal: appender,
        event_source: event_source(inputs)?,
        tenant_id: home.tenant_id,
        vshard_id: home.vshard_id,
        database_id: home.database_id,
        plan: &plan,
        credentials: None,
        now_override: inputs.resolved_now_ms,
    })?
    .origin;
    append_group_parts(appender, target(capture, origin.lsn), &capture.write_set())?;
    Ok(true)
}

/// The plan a stored write set journals the origin of.
fn stored_plan(inputs: &OriginAppend) -> crate::Result<PhysicalPlan> {
    zerompk::from_msgpack(&inputs.plan).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("stored write set plan decode: {e}"),
    })
}

/// The appender every record of the write carried.
fn part_appender<'a>(wal: &'a WalManager, inputs: &OriginAppend) -> crate::Result<WalAppender<'a>> {
    let appender = wal
        .appender(inputs.apply_key)
        .with_event_source(event_source(inputs)?);
    Ok(match inputs.commit_hlc {
        Some(hlc) => appender.with_commit_hlc(hlc),
        None => appender,
    })
}

fn event_source(inputs: &OriginAppend) -> crate::Result<EventSource> {
    EventSource::from_wal_code(inputs.event_source).ok_or(crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!(
            "stored write set names unknown event source code {}",
            inputs.event_source
        ),
    })
}

fn target(capture: &WriteSetCapture, origin: Lsn) -> WriteSetTarget<'_> {
    WriteSetTarget {
        tenant_id: TenantId::new(capture.tenant_id),
        vshard_id: VShardId::new(capture.vshard_id),
        database_id: DatabaseId::new(capture.database_id),
        collection: &capture.collection,
        origin: GroupOrigin { lsn: origin },
    }
}
