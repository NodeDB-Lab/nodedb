// SPDX-License-Identifier: BUSL-1.1

//! Crash recovery and idempotency checks for `MOVE TENANT`.
//!
//! On startup, the maintenance loop scans the journal for in-progress entries
//! and calls [`recover_all`] to resume or compensate each one.
//!
//! At handler entry time, [`tenant_already_in_target`] provides the idempotent
//! short-circuit: if a previously completed move is re-issued, the response
//! is `MOVE_TENANT_ALREADY_AT_TARGET`.
//!
//! The result is the protocol-neutral [`DdlResult`] / [`DdlError`].

use crate::control::security::catalog::{MovePhase, MoveTenantJournalEntry, SystemCatalog};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::support::status;
use super::entry::{DRAIN_TIMEOUT, SNAPSHOT_TIMEOUT};
use super::journal;
use super::{cutover, drain, snapshot};

/// Check whether the source database has already been emptied by a prior move.
///
/// Returns `true` if the source database has no active collection and no
/// array AND the target database has at least one of either — indicating a
/// previously completed move. Every check covers the whole database because
/// `MOVE TENANT` transfers the entire source database namespace atomically.
pub fn tenant_already_in_target(
    catalog: &SystemCatalog,
    _tenant_id: TenantId,
    source_db_id: DatabaseId,
    target_db_id: DatabaseId,
) -> crate::Result<bool> {
    let source_colls = catalog.load_all_collections(source_db_id)?;
    let active_in_source = source_colls.iter().any(|c| c.is_active);
    if active_in_source || !super::arrays::arrays_in(catalog, source_db_id)?.is_empty() {
        return Ok(false);
    }
    let target_colls = catalog.load_all_collections(target_db_id)?;
    Ok(target_colls.iter().any(|c| c.is_active)
        || !super::arrays::arrays_in(catalog, target_db_id)?.is_empty())
}

/// Resume or compensate a single in-progress journal entry.
///
/// Called both from handler entry (when a journal entry is found for the
/// same tenant at the start of a new `MOVE TENANT` invocation) and from
/// startup recovery.
pub async fn resume_or_compensate(
    state: &SharedState,
    catalog: &SystemCatalog,
    entry: MoveTenantJournalEntry,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = TenantId::new(entry.tenant_id);
    let source_db_id = DatabaseId::new(entry.source_db_id);
    let target_db_id = DatabaseId::new(entry.target_db_id);

    match entry.phase {
        MovePhase::Preflight | MovePhase::Drain => {
            // Journal recorded but drain never completed.
            // Compensate: remove journal, return error asking operator to retry.
            journal::delete_journal_entry_logged(catalog, tenant_id);
            Err(DdlError::move_tenant_drain_timeout(format!(
                "MOVE TENANT '{}' was interrupted during drain and has been rolled back; \
                 please retry the operation",
                entry.tenant_name
            )))
        }
        MovePhase::Snapshot => {
            // Drain completed but snapshot was interrupted.
            // Compensate: release drain, remove journal.
            drain::release(state, tenant_id, source_db_id).await;
            journal::delete_journal_entry_logged(catalog, tenant_id);
            Err(DdlError::move_tenant_snapshot_failed(format!(
                "MOVE TENANT '{}' was interrupted during snapshot and has been rolled back; \
                 please retry the operation",
                entry.tenant_name
            )))
        }
        MovePhase::Cutover => {
            // Snapshot succeeded but cutover was interrupted. Check if cutover
            // actually completed (idempotency: tenant can already be in target).
            let already_moved =
                tenant_already_in_target(catalog, tenant_id, source_db_id, target_db_id)
                    .map_err(|e| DdlError::from_error_in_context("idempotency check", &e))?;

            if already_moved {
                // Cutover succeeded but client crashed before reading the response.
                // Clean up the journal and return success.
                if let Some(ref key) = entry.temp_snapshot_key {
                    let _ = snapshot::delete_temp(state, key).await;
                }
                journal::delete_journal_entry_logged(catalog, tenant_id);
                state.audit_record(
                    crate::control::security::audit::AuditEvent::AdminAction,
                    Some(tenant_id),
                    &identity.username,
                    &format!(
                        "MOVE TENANT {} FROM {} TO {} recovered (cutover was already complete)",
                        entry.tenant_name, entry.source_db_name, entry.target_db_name
                    ),
                );
                return Ok(status("MOVE TENANT"));
            }

            // Cutover proposal did not apply. Re-run the drain, the snapshot
            // and the cutover: the capture needs the drain's write gate.
            let drain_holds = match drain::run(state, tenant_id, source_db_id, DRAIN_TIMEOUT).await
            {
                Ok(holds) => holds,
                Err(ref e) => {
                    journal::delete_journal_entry_logged(catalog, tenant_id);
                    return Err(DdlError::move_tenant_drain_timeout(e.message()).with_cause_of(e));
                }
            };
            let snapshot_result =
                snapshot::run(state, tenant_id, source_db_id, SNAPSHOT_TIMEOUT).await;
            let capture = match snapshot_result {
                Ok(capture) => capture,
                Err(ref e) => {
                    drain_holds.end().await;
                    journal::delete_journal_entry_logged(catalog, tenant_id);
                    return Err(DdlError::move_tenant_snapshot_failed(e.message()).with_cause_of(e));
                }
            };

            let cutover_result =
                cutover::run(state, tenant_id, source_db_id, target_db_id, capture).await;

            if let Err(ref e) = cutover_result {
                drain_holds.end().await;
                if let Some(ref key) = entry.temp_snapshot_key {
                    let _ = snapshot::delete_temp(state, key).await;
                }
                journal::delete_journal_entry_logged(catalog, tenant_id);
                return Err(DdlError::move_tenant_cutover_failed(e.message()).with_cause_of(e));
            }
            drain_holds.keep();

            if let Some(ref key) = entry.temp_snapshot_key {
                let _ = snapshot::delete_temp(state, key).await;
            }
            journal::delete_journal_entry_logged(catalog, tenant_id);
            state.audit_record(
                crate::control::security::audit::AuditEvent::AdminAction,
                Some(tenant_id),
                &identity.username,
                &format!(
                    "MOVE TENANT {} FROM {} TO {} recovered (cutover re-applied)",
                    entry.tenant_name, entry.source_db_name, entry.target_db_name
                ),
            );
            Ok(status("MOVE TENANT"))
        }
        MovePhase::Resumed => {
            // Move completed normally; journal entry is removed.
            // Clean it up now as a belt-and-suspenders measure.
            journal::delete_journal_entry_logged(catalog, tenant_id);
            Ok(status("MOVE TENANT"))
        }
    }
}

/// Scan the journal at startup and recover any in-progress entries.
///
/// Called once during server startup before accepting connections.
pub async fn recover_all(state: &SharedState) {
    let catalog = state.credentials.catalog();

    let entries = match journal::scan_all_journal_entries(catalog) {
        Ok(e) => e,
        Err(err) => {
            tracing::error!(
                error = %err,
                "move_tenant recovery: failed to scan journal; skipping"
            );
            return;
        }
    };

    for entry in entries {
        tracing::info!(
            tenant = entry.tenant_id,
            phase = ?entry.phase,
            "move_tenant recovery: found in-progress entry"
        );
        let tenant_id = TenantId::new(entry.tenant_id);
        let source_db_id = DatabaseId::new(entry.source_db_id);
        let target_db_id = DatabaseId::new(entry.target_db_id);

        match entry.phase {
            MovePhase::Preflight | MovePhase::Drain | MovePhase::Snapshot => {
                // Compensate: no data was moved; release drain, remove journal.
                drain::release(state, tenant_id, source_db_id).await;
                if let Some(ref key) = entry.temp_snapshot_key {
                    let _ = snapshot::delete_temp(state, key).await;
                }
                journal::delete_journal_entry_logged(catalog, tenant_id);
                tracing::info!(
                    tenant = entry.tenant_id,
                    "move_tenant recovery: compensated (no data moved)"
                );
            }
            MovePhase::Cutover => {
                // Check if cutover completed before crash. A catalog read
                // failure here is not silently swallowed — surface it in logs
                // and treat as "not moved" so we re-attempt cutover (which is
                // idempotent: the Raft proposal is rejected if already
                // applied).
                let source_left = catalog
                    .load_all_collections(source_db_id)
                    .and_then(|colls| {
                        Ok(colls.iter().all(|col| !col.is_active)
                            && super::arrays::arrays_in(catalog, source_db_id)?.is_empty())
                    });
                let already_moved = match source_left {
                    Ok(emptied) => emptied,
                    Err(err) => {
                        tracing::warn!(
                            tenant = entry.tenant_id,
                            source_db = entry.source_db_id,
                            error = %err,
                            "move_tenant recovery: failed to read source collections; \
                             treating as not-moved and will retry cutover"
                        );
                        false
                    }
                };

                if already_moved {
                    tracing::info!(
                        tenant = entry.tenant_id,
                        "move_tenant recovery: cutover already complete; cleaning journal"
                    );
                } else {
                    // Re-attempt the cutover behind a fresh drain. A failed
                    // drain ends its own drains and leaves the source intact.
                    if let Ok(drain_holds) =
                        drain::run(state, tenant_id, source_db_id, DRAIN_TIMEOUT).await
                    {
                        if let Ok(capture) =
                            snapshot::run(state, tenant_id, source_db_id, SNAPSHOT_TIMEOUT).await
                        {
                            let _ =
                                cutover::run(state, tenant_id, source_db_id, target_db_id, capture)
                                    .await;
                        }
                        drain_holds.end().await;
                    }
                }

                if let Some(ref key) = entry.temp_snapshot_key {
                    let _ = snapshot::delete_temp(state, key).await;
                }
                journal::delete_journal_entry_logged(catalog, tenant_id);
            }
            MovePhase::Resumed => {
                // Belt-and-suspenders cleanup.
                journal::delete_journal_entry_logged(catalog, tenant_id);
            }
        }
    }
}
