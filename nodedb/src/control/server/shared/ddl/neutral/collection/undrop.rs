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
//! exists, only superuser / tenant_admin can undrop; the restore is
//! audit-logged with an `owner_user_missing` marker.
//!
//! The catalog / permission / audit reads and the metadata proposal run
//! here. The result is the protocol-neutral `DdlResult` / `DdlError`.

use nodedb_types::DatabaseId;

use crate::control::security::audit::{AuditEvent, UndropAuditDetail, UndropStage};
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

pub async fn undrop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 3 {
        return Err(DdlError::new("42601", "syntax: UNDROP COLLECTION <name>"));
    }

    let name_lower = parse_ident_token(parts[2])?;
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
            return Err(DdlError::new(
                "42P01",
                format!(
                    "collection '{name}' not found (retention window elapsed or never existed)"
                ),
            ));
        }
        Err(e) => {
            return Err(DdlError::from_error(&e));
        }
    };
    if stored.is_active {
        return Err(DdlError::new(
            "42P07",
            format!("collection '{name}' is already active"),
        ));
    }

    // Authorization: preserved owner OR admin.
    let preserved_owner =
        state
            .permissions
            .get_owner("collection", database_id.as_u64(), tenant_id, name);
    let is_preserved_owner = preserved_owner.as_deref() == Some(&identity.username);
    let is_admin = identity.is_superuser || identity.has_role(&Role::TenantAdmin);

    if !is_preserved_owner && !is_admin {
        return Err(DdlError::new(
            "42501",
            "permission denied: only the preserved owner, superuser, or tenant_admin may UNDROP",
        ));
    }

    // If the preserved-owner user no longer exists, only admin can restore.
    let owner_user_missing = preserved_owner
        .as_deref()
        .is_some_and(|u| state.credentials.get_user(u).is_none());
    if owner_user_missing && !is_admin {
        return Err(DdlError::new(
            "42501",
            "preserved-owner user no longer exists — only superuser or tenant_admin may UNDROP",
        ));
    }

    // Best-effort operator trail, which carries the owner-missing flag. This
    // row is buffered and can be lost in a crash. The durable record is the
    // proposer's `ActionIntent` row, which syncs before the propose below and
    // refuses the statement if it fails. Detail is a JSON-serialized
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
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(stored));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error(&e))?;

    let completion = UndropAuditDetail::new(name, UndropStage::Completed, owner_user_missing)
        .with_log_index(outcome.log_index())
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
