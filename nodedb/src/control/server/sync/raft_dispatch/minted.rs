// SPDX-License-Identifier: BUSL-1.1

//! Append a sync write's redo record under an outcome-floor window, and hand
//! the record to the dispatch that closes the window.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils::{MintedRecords, RecordOwner};
use crate::control::state::SharedState;
use crate::types::Lsn;
use crate::wal::manager::{NO_APPLY_KEY, WalAppender};

use super::authorize::authorize_sync_task;
use super::response::dispatch_sync_payload;

/// Open an outcome-floor window, then run `append` with an appender that
/// records every LSN it writes into the window. Returns the window's records
/// and the LSN `append` returned.
pub(crate) async fn append_under_window(
    shared: &SharedState,
    owner: RecordOwner,
    append: impl FnOnce(WalAppender<'_>) -> crate::Result<Option<Lsn>>,
) -> crate::Result<(MintedRecords, Option<Lsn>)> {
    let minted = MintedRecords::open(&shared.outcome_floor);
    let appended = append(minted.appender(&shared.wal, NO_APPLY_KEY));
    match appended {
        Ok(lsn) => Ok((minted, lsn)),
        Err(error) => {
            // Any record appended before the error never reaches a core.
            minted.cancel(&shared.wal, owner, 0).await?;
            Err(error)
        }
    }
}

/// Authorize `plan` and dispatch it with the records the caller appended. A
/// refused authorization reaches no core, so it cancels the records.
pub(crate) async fn authorize_and_dispatch_minted(
    shared: &SharedState,
    identity: Option<&AuthenticatedIdentity>,
    owner: RecordOwner,
    plan: PhysicalPlan,
    minted: MintedRecords,
) -> crate::Result<Vec<u8>> {
    let authorized = match authorize_sync_task(
        shared,
        identity,
        owner.tenant_id,
        owner.database_id,
        owner.vshard_id,
        plan,
    ) {
        Ok(authorized) => authorized,
        Err(error) => {
            minted.cancel(&shared.wal, owner, 0).await?;
            return Err(error);
        }
    };
    dispatch_sync_payload(shared, authorized, Some(minted)).await
}
