// SPDX-License-Identifier: BUSL-1.1

//! Top-level `MOVE TENANT` handler — wires the four phases together with the
//! durable journal and the compensation paths.
//!
//! See the module-level docs on [`super`] for the phase table.
//!
//! `require_superuser` is reused directly from `neutral::database::gate`
//! (audit on denial, same SQLSTATE, same message).

use std::time::Duration;

use nodedb_types::error::sqlstate;

use crate::control::security::catalog::{MovePhase, MoveTenantJournalEntry};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::super::database::gate::require_superuser;
use super::super::support::{ddl_err, status};
use super::journal;
use super::{cutover, drain, preflight, recovery, snapshot};

/// Drain timeout for phase 2: how long we wait for in-flight operations to
/// complete after revoking sessions before aborting the drain and rolling back.
pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Snapshot timeout passed to the backup orchestrator.
pub(crate) const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// Entry point for `MOVE TENANT <name> FROM <source_db> TO <target_db>`.
///
/// Required role: `Superuser`.
pub async fn handle_move_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_name: &str,
    from_db: &str,
    to_db: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    // The move re-issues rows and reclaims storage as it runs, and a ROLLBACK
    // cannot undo either. PostgreSQL refuses CREATE DATABASE here the same way.
    if crate::control::server::shared::session::ddl_buffer::is_active() {
        return Err(DdlError::from_error(&crate::Error::NotInTransactionBlock {
            statement: "MOVE TENANT".into(),
        }));
    }

    let catalog = state.credentials.catalog();

    // Resolve source database id first so the privilege gate can carry it
    // in the audit record. The gate runs BEFORE tenant / target lookup so an
    // unauthorized caller never learns whether the tenant or target db exists.
    let source_db_id = catalog
        .get_database_id_by_name(from_db)
        .map_err(|e| DdlError::from_error_in_context("catalog lookup", &e))?
        .ok_or_else(|| ddl_err("42P01", format!("source database '{from_db}' not found")))?;

    require_superuser(
        state,
        identity,
        Some(source_db_id),
        &format!("MOVE TENANT {tenant_name} FROM {from_db} TO {to_db}"),
    )?;

    // Resolve tenant by name (catalog stores all tenants; linear scan is acceptable
    // for administrative DDL operations).
    let tenant_record = catalog
        .load_all_tenants()
        .map_err(|e| DdlError::from_error_in_context("catalog lookup", &e))?
        .into_iter()
        .find(|t| t.name == tenant_name)
        .ok_or_else(|| ddl_err("42P01", format!("tenant '{tenant_name}' not found")))?;
    let tenant_id = TenantId::new(tenant_record.tenant_id);

    let target_db_id = catalog
        .get_database_id_by_name(to_db)
        .map_err(|e| DdlError::from_error_in_context("catalog lookup", &e))?
        .ok_or_else(|| ddl_err("42P01", format!("target database '{to_db}' not found")))?;

    // Idempotency: tenant already in target?
    if recovery::tenant_already_in_target(catalog, tenant_id, source_db_id, target_db_id)
        .map_err(|e| DdlError::from_error_in_context("idempotency check", &e))?
    {
        return Err(DdlError::move_tenant_already_at_target(
            nodedb_types::NodeDbError::move_tenant_already_at_target(tenant_name, to_db).message(),
        ));
    }

    // Check for an in-progress journal entry — resume or compensate.
    if let Some(entry) = journal::load_journal_entry(catalog, tenant_id)
        .map_err(|e| DdlError::from_error_in_context("journal read", &e))?
    {
        return recovery::resume_or_compensate(state, catalog, entry, identity).await;
    }

    // ── Phase 1: Pre-flight ───────────────────────────────────────────────────
    preflight::run(catalog, source_db_id, target_db_id, tenant_name, to_db)
        .map_err(|e| ddl_err(sqlstate::MOVE_TENANT_PREFLIGHT_FAILED, e.message()))?;

    // ── Write journal entry at Preflight phase ────────────────────────────────
    let journal_entry = MoveTenantJournalEntry {
        tenant_id: tenant_id.as_u64(),
        tenant_name: tenant_name.to_string(),
        source_db_id: source_db_id.as_u64(),
        source_db_name: from_db.to_string(),
        target_db_id: target_db_id.as_u64(),
        target_db_name: to_db.to_string(),
        phase: MovePhase::Drain,
        last_durable_lsn: state.wal.next_lsn().as_u64(),
        temp_snapshot_key: None,
    };
    journal::save_journal_entry(catalog, &journal_entry)
        .map_err(|e| DdlError::from_error_in_context("journal write", &e))?;

    // ── Phase 2: Drain ────────────────────────────────────────────────────────
    // The holds end the drain on every exit but a committed cutover, including
    // a dropped statement future.
    let drain_holds = match drain::run(state, tenant_id, source_db_id, DRAIN_TIMEOUT).await {
        Ok(holds) => holds,
        Err(ref e) => {
            // Compensate: remove journal entry so state is clean.
            journal::delete_journal_entry_logged(catalog, tenant_id);
            return Err(DdlError::move_tenant_drain_timeout(e.message()).with_cause_of(e));
        }
    };

    // Update journal to Snapshot phase.
    let journal_entry = journal_entry.with_phase(MovePhase::Snapshot);
    journal::save_journal_entry(catalog, &journal_entry)
        .map_err(|e| DdlError::from_error_in_context("journal update", &e))?;

    // ── Phase 3: Snapshot ─────────────────────────────────────────────────────
    let snapshot_result = snapshot::run(state, tenant_id, source_db_id, SNAPSHOT_TIMEOUT).await;
    let capture = match snapshot_result {
        Ok(capture) => capture,
        Err(ref e) => {
            // Compensate: release drain, remove journal.
            drain_holds.end().await;
            journal::delete_journal_entry_logged(catalog, tenant_id);
            return Err(DdlError::move_tenant_snapshot_failed(e.message()).with_cause_of(e));
        }
    };

    // Store snapshot key in journal for recovery.
    let temp_key = snapshot::temp_key(tenant_id);
    let journal_entry = journal_entry
        .with_phase(MovePhase::Cutover)
        .with_temp_snapshot_key(temp_key.clone());
    journal::save_journal_entry(catalog, &journal_entry)
        .map_err(|e| DdlError::from_error_in_context("journal update", &e))?;

    // ── Phase 4: Cutover ──────────────────────────────────────────────────────
    let cutover_result = cutover::run(state, tenant_id, source_db_id, target_db_id, capture).await;

    if let Err(ref e) = cutover_result {
        // Cutover failed: single-proposal failure is all-or-nothing; source intact.
        // Release drain; clean up snapshot; remove journal.
        drain_holds.end().await;
        let _ = snapshot::delete_temp(state, &temp_key).await;
        journal::delete_journal_entry_logged(catalog, tenant_id);
        return Err(DdlError::move_tenant_cutover_failed(e.message()).with_cause_of(e));
    }
    drain_holds.keep();

    // ── Phase 5: Resume ───────────────────────────────────────────────────────
    // Drain is already released by cutover (tenant now lives in target).
    // Mark journal complete and remove it.
    let _ = snapshot::delete_temp(state, &temp_key).await;
    journal::delete_journal_entry_logged(catalog, tenant_id);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("MOVE TENANT {tenant_name} FROM {from_db} TO {to_db} completed"),
    );

    Ok(status("MOVE TENANT"))
}
