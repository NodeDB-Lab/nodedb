// SPDX-License-Identifier: BUSL-1.1

//! Handlers for `BACKUP DATABASE <name> TO '<uri>'` and
//! `RESTORE DATABASE <name> FROM '<uri>' [FORCE] [DRY RUN]`.
//!
//! Gate: superuser, or the owner of the named database. A restore into a
//! database this cluster lacks needs superuser, since no owner exists yet.
//! The gate runs before the URI is read, so an unauthorized caller learns
//! nothing about the store. A bad URI fails with SQLSTATE 22023 before any
//! store is touched.

use nodedb_types::error::sqlstate;

use crate::control::backup::database;
use crate::control::backup::store::{BackupIoError, BackupObject, BackupUriError};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::backup_metering::{
    admit_backup_restore_quota, admit_restore_write_quota, meter_backup_restore,
    meter_restore_writes,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::{require_database_owner, require_superuser};
use super::support::ddl_err;

fn uri_error(e: &BackupUriError) -> DdlError {
    ddl_err(sqlstate::INVALID_PARAMETER_VALUE, e.to_string())
}

/// A path that leaves the local root at the open is 22023, as at resolve.
fn io_error(context: &str, e: BackupIoError) -> DdlError {
    match e {
        BackupIoError::Refused(refusal) => uri_error(&refusal),
        BackupIoError::Failed(error) => DdlError::from_error_in_context(context, &error),
    }
}

/// A backup or restore writes outside the transaction, and a ROLLBACK cannot
/// undo it.
fn refuse_in_transaction(statement: &str) -> Result<(), DdlError> {
    if crate::control::server::shared::session::ddl_buffer::is_active() {
        return Err(DdlError::from_error(&crate::Error::NotInTransactionBlock {
            statement: statement.into(),
        }));
    }
    Ok(())
}

/// Handle `BACKUP DATABASE <name> TO '<uri>'`.
///
/// Resolves the database first: an unknown name returns 3D000, not 42501.
pub async fn backup_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    uri: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    refuse_in_transaction("BACKUP DATABASE")?;
    let catalog = state.credentials.catalog();
    let db_id = match catalog.get_database_id_by_name(name) {
        Ok(Some(id)) => id,
        Ok(None) => {
            return Err(ddl_err(
                "3D000",
                format!("database '{name}' does not exist"),
            ));
        }
        Err(e) => {
            return Err(DdlError::from_error_in_context("catalog lookup failed", &e));
        }
    };
    require_database_owner(state, identity, db_id, &format!("BACKUP DATABASE {name}"))?;
    let object =
        BackupObject::resolve(uri, state.backup_storage.as_deref()).map_err(|e| uri_error(&e))?;
    let failed = |e: &crate::Error| DdlError::from_error_in_context("BACKUP DATABASE", e);

    // A spent hard quota of any tenant refuses the backup before it reads a
    // byte, as the tenant COPY backup does.
    let scope = RequestAuthScope::for_database(identity, state.auth_stores(), db_id)
        .map_err(|e| failed(&e))?;
    let tenants = database::database_tenants(state, db_id).map_err(|e| failed(&e))?;
    for &tenant_id in &tenants {
        admit_backup_restore_quota(state, &scope, tenant_id).map_err(|e| failed(&e))?;
    }

    let shared = state.self_arc().map_err(|e| failed(&e))?;
    let bytes = database::backup_database(&shared, db_id, name, &tenants)
        .await
        .map_err(|e| failed(&e))?;
    let size = bytes.len() as u64;
    object
        .put(bytes)
        .await
        .map_err(|e| io_error("BACKUP DATABASE", e))?;
    for &tenant_id in &tenants {
        meter_backup_restore(state, &scope, tenant_id, None);
    }
    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        None,
        &identity.username,
        &format!(
            "BACKUP DATABASE {name} TO '{}' wrote {size} bytes",
            object.uri()
        ),
    );
    Ok(vec![DdlResult::Status {
        command: "BACKUP DATABASE".into(),
        rows_affected: Some(size),
    }])
}

/// Handle `RESTORE DATABASE <name> FROM '<uri>' [FORCE] [DRY RUN]`.
///
/// The affected-row count is the number of rows the restore verified on the
/// destination, `0` for a DRY RUN.
pub async fn restore_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    uri: &str,
    force: bool,
    dry_run: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    refuse_in_transaction("RESTORE DATABASE")?;
    let action = format!("RESTORE DATABASE {name}");
    let db_id = state
        .credentials
        .catalog()
        .get_database_id_by_name(name)
        .map_err(|e| DdlError::from_error_in_context("catalog lookup failed", &e))?;
    match db_id {
        Some(db_id) => require_database_owner(state, identity, db_id, &action)?,
        None => require_superuser(state, identity, None, &action)?,
    }
    let object =
        BackupObject::resolve(uri, state.backup_storage.as_deref()).map_err(|e| uri_error(&e))?;

    let failed = |e: &crate::Error| DdlError::from_error_in_context("RESTORE DATABASE", e);

    let bytes = object
        .get()
        .await
        .map_err(|e| io_error("RESTORE DATABASE", e))?;
    let backup = database::open_database_backup(state, name, &bytes).map_err(|e| failed(&e))?;
    drop(bytes);

    // A spent hard backup quota of any restored tenant refuses the restore
    // before it reads further, as the tenant COPY restore admits.
    let scope = RequestAuthScope::for_database(
        identity,
        state.auth_stores(),
        db_id
            .or(identity.default_database)
            .unwrap_or(DatabaseId::DEFAULT),
    )
    .map_err(|e| failed(&e))?;
    for &tenant_id in backup.tenant_ids() {
        admit_backup_restore_quota(state, &scope, tenant_id).map_err(|e| failed(&e))?;
    }

    // Every tenant envelope passes its checks before any tenant writes. The
    // check lists each collection's rows, and a spent hard write quota on any
    // of them, on its tenant's marker, or on `*`, refuses the restore before
    // its first write. A DRY RUN writes nothing and admits no write quota.
    let shared = state.self_arc().map_err(|e| failed(&e))?;
    let checked = database::check_database(&shared, &backup, force)
        .await
        .map_err(|e| failed(&e))?;
    let restored = if dry_run {
        checked
    } else {
        for (tenant_id, stats) in &checked.tenants {
            admit_restore_write_quota(state, &scope, *tenant_id, &stats.collection_rows)
                .map_err(|e| failed(&e))?;
        }
        super::super::audit_intent::record_action_intent(
            state,
            identity,
            None,
            &format!("RESTORE DATABASE {name} FROM '{}'", object.uri()),
        )?;
        database::apply_database(&shared, &backup, force)
            .await
            .map_err(|e| failed(&e))?
    };
    // Charged on the success path, so the charge never refuses anything: the
    // backup quota with the row count the tenant COPY restore reports, and
    // the write quota with the rows the restore verified per collection.
    for (tenant_id, stats) in &restored.tenants {
        let rows =
            stats.documents + stats.kv_tables + stats.vectors + stats.timeseries + stats.edges;
        meter_backup_restore(state, &scope, *tenant_id, Some(rows as u64));
        if !dry_run {
            meter_restore_writes(state, &scope, *tenant_id, &stats.collection_rows);
        }
    }
    let stats = restored.total;
    if !dry_run {
        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            None,
            &identity.username,
            &format!(
                "RESTORE DATABASE {name} FROM '{}' verified {} rows",
                object.uri(),
                stats.verified_rows
            ),
        );
    }
    Ok(vec![DdlResult::Status {
        command: if dry_run {
            "RESTORE DATABASE DRY RUN".into()
        } else {
            "RESTORE DATABASE".into()
        },
        rows_affected: Some(stats.verified_rows),
    }])
}
