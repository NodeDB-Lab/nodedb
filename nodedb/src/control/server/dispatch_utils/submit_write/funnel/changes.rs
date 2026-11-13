// SPDX-License-Identifier: BUSL-1.1

//! The change events a write owes its change feed.
//!
//! The change set is derived from the plan before the plan moves into the
//! request. A replicated write's rows ride its entry's change marker, which
//! the WAL append writes ahead of the write's records (see
//! [`super::wal_append`]). The response phase stages them once the write
//! applied.

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::server::dispatch_utils::change_events::{
    EntryChanges, extract_write_change_set,
};
use crate::control::server::dispatch_utils::minted::RecordOwner;
use crate::control::server::dispatch_utils::write_abort::write_definitely_not_applied;
use crate::control::state::SharedState;
use crate::event::cdc::position::MarkerChanges;
use crate::types::{Lsn, TenantId};

use super::super::params::ChangeFeedOwner;
use super::wal_append::{AppendedMarker, append_change_marker};

/// The change set `plan` owes `change_feed`. `staged` says the write is a
/// statement inside an open transaction.
///
/// Extraction is a pure match that clones out collection and document
/// identity, so a caller whose change feed is `Unowned` skips it. A write
/// that yields change events on a route no other replica applies is refused
/// before any record of it lands: every committed user write applies through
/// a replicated entry.
pub(super) fn entry_changes(
    change_feed: ChangeFeedOwner,
    staged: bool,
    plan: &PhysicalPlan,
    tenant_id: TenantId,
) -> crate::Result<Option<EntryChanges>> {
    match change_feed {
        // A staged write's rows are not committed: it yields no event yet.
        ChangeFeedOwner::LocalApply if staged => Ok(None),
        ChangeFeedOwner::LocalApply => {
            if extract_write_change_set(plan, tenant_id).is_empty() {
                return Ok(None);
            }
            Err(
                crate::control::change_stream::ChangeStreamError::UnreplicatedChange {
                    collection: plan
                        .named_collections()
                        .first()
                        .map(|collection| (*collection).to_owned())
                        .unwrap_or_default(),
                }
                .into(),
            )
        }
        ChangeFeedOwner::Replicated {
            group_id,
            log_index,
        } => Ok(Some(EntryChanges::of_entry(
            plan, tenant_id, group_id, log_index,
        ))),
        ChangeFeedOwner::Unowned => Ok(None),
    }
}

/// Withhold the change events of a replicated write that answered a
/// refusal and keeps its records: append a second marker of its entry that
/// says so. Returns its LSN, which the caller makes durable. `None` when the
/// write applied, or when its refusal cancels its records.
///
/// The live write stages no event for a refusal. Its records stay in the
/// WAL, so a restart delivers the entry again as a repeat. Without this
/// marker the restart stages the rows the first marker names.
pub(super) fn withhold_refused_changes(
    shared: &SharedState,
    owner: RecordOwner,
    commit_hlc: Option<u64>,
    marker: Option<AppendedMarker>,
    response: &Response,
) -> crate::Result<Option<Lsn>> {
    let Some(marker) = marker else {
        return Ok(None);
    };
    if response.status == Status::Ok {
        return Ok(None);
    }
    // The same verdict the outcome-floor window cancels the records on.
    let cancelled = response.write_set.is_empty()
        && response
            .error_code
            .as_deref()
            .is_some_and(write_definitely_not_applied);
    if cancelled {
        return Ok(None);
    }
    append_change_marker(
        shared,
        owner,
        commit_hlc,
        marker.head,
        &MarkerChanges::Withheld,
    )
    .map(Some)
}
