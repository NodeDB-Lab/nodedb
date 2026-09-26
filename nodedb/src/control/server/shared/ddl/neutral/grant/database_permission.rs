// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral handlers for database-level GRANT and REVOKE statements.
//!
//! ```sql
//! GRANT ALL ON DATABASE <name> TO <user>;
//! GRANT CREATE COLLECTION ON DATABASE <name> TO <user>;
//! GRANT SELECT ON DATABASE <name> TO <user>;
//! REVOKE ALL ON DATABASE <name> FROM <user>;
//! ```
//!
//! Ported from the pgwire `ddl::grant::database_permission` handlers. All
//! non-return logic (tenant-admin gate, catalog resolution of the database id
//! and grantee user record, `ALL` privilege expansion, catalog propose +
//! single-node fallback, and `audit_record`) is preserved verbatim; only the
//! result construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Grants are stored in `_system.database_grants`. They are also reflected
//! into the user's `accessible_databases` set — new grants add the database
//! to the set; all privileges revoked removes it.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{require_tenant_admin, status};

/// Handle `GRANT <privilege> ON DATABASE <name> TO <user>`.
///
/// Accepted privileges: `ALL`, `CREATE COLLECTION`, `SELECT`.
pub fn grant_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    privilege: &str,
    db_name: &str,
    grantee: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "GRANT ON DATABASE")?;

    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(db_name)
        .map_err(|e| DdlError::new("XX000", format!("catalog lookup: {e}")))?
        .ok_or_else(|| DdlError::new("42704", format!("database '{db_name}' does not exist")))?;

    // Resolve the target user_id from the grantee name, as this statement
    // sees it: a user created earlier in the transaction counts.
    let user_record = super::super::role_checks::visible_user(state, grantee)
        .ok_or_else(|| DdlError::new("42704", format!("user '{grantee}' does not exist")))?;

    let privileges: Vec<&str> = if privilege.eq_ignore_ascii_case("ALL") {
        vec!["ALL", "CREATE_COLLECTION", "SELECT"]
    } else {
        vec![privilege]
    };

    for priv_name in &privileges {
        let outcome = propose_catalog_entry(
            state,
            &CatalogEntry::PutDatabaseGrant {
                db_id: db_id.as_u64(),
                user_id: user_record.user_id,
                privilege: priv_name.to_string(),
            },
        )
        .map_err(|e| DdlError::new("XX000", format!("catalog propose: {e}")))?;

        if outcome.needs_local_apply() {
            catalog
                .put_database_grant(db_id, user_record.user_id, priv_name)
                .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        }
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("GRANT {} ON DATABASE {} TO {}", privilege, db_name, grantee),
    );

    Ok(status("GRANT"))
}

/// Handle `REVOKE <privilege> ON DATABASE <name> FROM <user>`.
pub fn revoke_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    privilege: &str,
    db_name: &str,
    grantee: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "REVOKE ON DATABASE")?;

    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(db_name)
        .map_err(|e| DdlError::new("XX000", format!("catalog lookup: {e}")))?
        .ok_or_else(|| DdlError::new("42704", format!("database '{db_name}' does not exist")))?;

    let user_record = super::super::role_checks::visible_user(state, grantee)
        .ok_or_else(|| DdlError::new("42704", format!("user '{grantee}' does not exist")))?;

    let privileges: Vec<&str> = if privilege.eq_ignore_ascii_case("ALL") {
        vec!["ALL", "CREATE_COLLECTION", "SELECT"]
    } else {
        vec![privilege]
    };

    for priv_name in &privileges {
        let outcome = propose_catalog_entry(
            state,
            &CatalogEntry::DeleteDatabaseGrant {
                db_id: db_id.as_u64(),
                user_id: user_record.user_id,
                privilege: priv_name.to_string(),
            },
        )
        .map_err(|e| DdlError::new("XX000", format!("catalog propose: {e}")))?;

        if outcome.needs_local_apply() {
            catalog
                .delete_database_grant(db_id, user_record.user_id, priv_name)
                .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        }
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "REVOKE {} ON DATABASE {} FROM {}",
            privilege, db_name, grantee
        ),
    );

    Ok(status("REVOKE"))
}
