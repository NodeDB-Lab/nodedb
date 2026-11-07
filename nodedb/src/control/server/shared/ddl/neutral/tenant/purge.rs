// SPDX-License-Identifier: BUSL-1.1

//! `PURGE TENANT <id|name> CONFIRM` — deletes ALL tenant data across every
//! engine on every replica. Superuser-only, requires the literal `CONFIRM`
//! keyword.
//!
//! The tenant reference accepts either a numeric id or a tenant name
//! (single-quoted optional), parallel to `CREATE TENANT <name>` and
//! `SHOW TENANT <name|id>`.
//!
//! The statement proposes one `ReplicatedWrite::TenantPurge` entry to each data
//! group. Every replica of a group applies it at one log position, so a write
//! of the group lands wholly before or wholly after the purge on every replica.
//!
//! A crash between two groups' entries leaves each group's replicas in step.
//! The purge is idempotent, so running the statement again finishes it.

use std::collections::BTreeMap;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::control::wal_replication::propose::propose_replicated_entry;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId};

use super::super::super::result::{DdlError, DdlResult};
use super::support::{ddl_err, resolve_tenant_ref, status, tenant_exists};

/// How long the statement waits for every group's purge to apply here.
const PURGE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

pub async fn purge_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can purge tenants",
        ));
    }

    if parts.len() < 4 {
        return Err(ddl_err("42601", "syntax: PURGE TENANT <id|name> CONFIRM"));
    }

    // Accept either a numeric id or a tenant name (mirrors CREATE/SHOW/DROP).
    let tenant_id = resolve_tenant_ref(state, parts[2])?
        .ok_or_else(|| ddl_err("42704", format!("tenant '{}' does not exist", parts[2])))?;
    let tid = tenant_id.as_u64();

    if tid == 0 {
        return Err(ddl_err("42501", "cannot purge system tenant (0)"));
    }

    // Existence gate, uniform across numeric ids and resolved names: refuse to
    // dispatch the destructive meta op for a tenant that does not exist.
    if !tenant_exists(state, tenant_id)? {
        return Err(ddl_err(
            "42704",
            format!("tenant '{}' does not exist", parts[2]),
        ));
    }

    if !parts[3].eq_ignore_ascii_case("CONFIRM") {
        return Err(ddl_err(
            "42601",
            "PURGE TENANT requires CONFIRM keyword to prevent accidental data destruction",
        ));
    }

    super::super::audit_intent::record_action_intent(
        state,
        identity,
        Some(tenant_id),
        &format!("PURGE TENANT {tid} CONFIRM — deleting all data across all engines"),
    )?;

    let purged = match tokio::time::timeout(
        PURGE_TIMEOUT,
        propose_to_every_group(state, tenant_id, database_id),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(crate::Error::Dispatch {
            detail: format!(
                "PURGE TENANT {tid} did not apply in every data group within {PURGE_TIMEOUT:?}"
            ),
        }),
    };
    match purged {
        Ok(()) => {
            state.audit_record(
                AuditEvent::AdminAction,
                Some(tenant_id),
                &identity.username,
                &format!("PURGE TENANT {tid} completed successfully"),
            );
            Ok(status("PURGE TENANT"))
        }
        Err(e) => Err(DdlError::from_error_in_context("purge failed", &e)),
    }
}

/// Each data group's vShards, by group. A group with no vShard holds no user
/// data, as the metadata group does.
pub(crate) fn data_groups(state: &SharedState) -> crate::Result<BTreeMap<u64, Vec<u32>>> {
    let routing = state
        .cluster_routing
        .as_ref()
        .ok_or_else(|| crate::Error::Internal {
            detail: "this node has no cluster routing table".into(),
        })?;
    let table = routing.read().map_err(|_| crate::Error::Internal {
        detail: "the cluster routing table lock is poisoned".into(),
    })?;
    Ok(table
        .group_ids()
        .into_iter()
        .filter_map(|group| {
            let vshards = table.vshards_for_group(group);
            (!vshards.is_empty()).then_some((group, vshards))
        })
        .collect())
}

/// Propose the purge to every data group and await each one's apply here.
async fn propose_to_every_group(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> crate::Result<()> {
    let collections: Vec<String> = state
        .credentials
        .catalog()
        .load_collections_for_tenant(database_id, tenant_id.as_u64())?
        .into_iter()
        .map(|collection| collection.name)
        .collect();
    let proposer = state.async_raft_proposer()?;
    let deadline = tokio::time::Instant::now() + PURGE_TIMEOUT;
    let proposals = data_groups(state)?.into_values().filter_map(|vshards| {
        let vshard_id = vshards.first().copied()?;
        let entry = ReplicatedEntry::new(
            tenant_id.as_u64(),
            database_id.as_u64(),
            vshard_id,
            ReplicatedWrite::TenantPurge {
                collections: collections.clone(),
                vshards,
            },
        );
        Some(propose_replicated_entry(state, proposer, entry, deadline))
    });
    for applied in futures::future::join_all(proposals).await {
        applied?;
    }
    Ok(())
}
