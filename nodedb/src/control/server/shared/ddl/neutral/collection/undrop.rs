// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `UNDROP COLLECTION <name>` — restore a soft-deleted
//! collection.
//!
//! Valid only while the collection's retention window has not elapsed
//! (the redb row still exists with `is_active = false`). Flips
//! `is_active` back to `true` via a fresh `CatalogEntry::PutCollection`,
//! so the applier and every downstream cache observe the restore
//! through the normal catalog-change stream.
//!
//! Authorization matches `ALTER COLLECTION OWNER TO`: preserved owner,
//! superuser, or tenant_admin. If the preserved-owner user no longer
//! exists, only superuser / tenant_admin may undrop; the restore is
//! audit-logged with an `owner_user_missing` marker.
//!
//! Ported from the pgwire `ddl::collection::undrop` handler. The catalog /
//! permission / audit reads and the metadata proposal are preserved verbatim;
//! only the result construction changed from pgwire `Response` / `Tag` to the
//! protocol-neutral `DdlResult` / `DdlError`.

use nodedb_types::DatabaseId;

use crate::control::security::audit::{AuditEvent, UndropAuditDetail, UndropStage};
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

pub fn undrop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 3 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: UNDROP COLLECTION <name>".to_string(),
        });
    }

    let name_lower = parts[2].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    let catalog = state.credentials.catalog();

    // Look up the soft-deleted record. Three distinct failures:
    //   - row absent: retention already expired or never existed.
    //   - row present + active: nothing to undrop.
    //   - row present + inactive: candidate for restore.
    let mut stored = match catalog.get_collection(database_id, tenant_id.as_u64(), name) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return Err(DdlError {
                sqlstate: "42P01".to_string(),
                message: format!(
                    "collection '{name}' not found (retention window elapsed or never existed)"
                ),
            });
        }
        Err(e) => {
            return Err(DdlError {
                sqlstate: "XX000".to_string(),
                message: e.to_string(),
            });
        }
    };
    if stored.is_active {
        return Err(DdlError {
            sqlstate: "42P07".to_string(),
            message: format!("collection '{name}' is already active"),
        });
    }

    // Authorization: preserved owner OR admin.
    let preserved_owner = state.permissions.get_owner_in_database(
        "collection",
        database_id.as_u64(),
        tenant_id,
        name,
    );
    let is_preserved_owner = preserved_owner.as_deref() == Some(&identity.username);
    let is_admin = identity.is_superuser || identity.has_role(&Role::TenantAdmin);

    if !is_preserved_owner && !is_admin {
        return Err(DdlError {
            sqlstate: "42501".to_string(),
            message:
                "permission denied: only the preserved owner, superuser, or tenant_admin may UNDROP"
                    .to_string(),
        });
    }

    // If the preserved-owner user no longer exists, only admin may restore.
    let owner_user_missing = preserved_owner
        .as_deref()
        .is_some_and(|u| state.credentials.get_user(u).is_none());
    if owner_user_missing && !is_admin {
        return Err(DdlError {
            sqlstate: "42501".to_string(),
            message:
                "preserved-owner user no longer exists — only superuser or tenant_admin may UNDROP"
                    .to_string(),
        });
    }

    // Audit intent BEFORE the catalog mutation (symmetric with drop;
    // if we crash mid-propose, the audit record still captures that
    // an UNDROP was requested, with the owner-missing flag for
    // post-hoc investigation). Detail is a JSON-serialized
    // `UndropAuditDetail` so SIEM / compliance consumers filter on
    // `owner_user_missing` without string-scraping.
    let intent = UndropAuditDetail::new(name, UndropStage::Requested, owner_user_missing).to_json();
    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &intent,
    );

    // Propose the restore as a PutCollection through the metadata raft
    // group. Fresh entry carries `is_active = true` and the preserved
    // owner (already present on `stored`).
    stored.is_active = true;
    let entry =
        crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?;
    if log_index == 0 {
        // Single-node fallback: write directly.
        catalog
            .put_collection(database_id, &stored)
            .map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: e.to_string(),
            })?;
    }

    let completion = UndropAuditDetail::new(name, UndropStage::Completed, owner_user_missing)
        .with_log_index(log_index)
        .to_json();
    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &completion,
    );

    Ok(vec![DdlResult::Status {
        command: "UNDROP COLLECTION".to_string(),
        rows_affected: None,
    }])
}
