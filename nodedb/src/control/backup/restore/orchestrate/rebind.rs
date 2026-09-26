// SPDX-License-Identifier: BUSL-1.1

//! Surrogate rebinding and tombstoned-collection warnings for
//! [`super::restore_tenant`].

use std::sync::Arc;

use nodedb_types::Surrogate;

use crate::Error;
use crate::control::backup::snapshot_keys::extract_db_tenant_scoped_collection;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::parse_versioned_edge_key;
use crate::types::{DatabaseId, SurrogateBindEntry, TenantDataSnapshot, TenantId};

/// Bind every PK→surrogate identity the backup carries on this node, before
/// any re-issue, so a re-issued row keeps the surrogate it was stored under.
///
/// Binding is first-wins: a key this node already binds keeps its surrogate,
/// and the re-issue writes that row over it. Each bind also raises this
/// node's surrogate high-water mark past the backup's surrogate, so no later
/// allocation here reuses it. Every replica binds the identities a re-issued
/// write carries as it applies the write. Any bind error is fatal.
pub(super) fn rebind_surrogates(
    state: &Arc<SharedState>,
    binds: &[SurrogateBindEntry],
) -> Result<(), Error> {
    let database_id = crate::types::DatabaseId::DEFAULT;
    for e in binds {
        state.surrogate_assigner.bind(
            database_id,
            TenantId::new(e.tenant_id),
            &e.collection,
            &e.pk,
            Surrogate::new(e.surrogate),
        )?;
    }
    Ok(())
}

pub(super) fn warn_on_tombstoned_restores(
    state: &Arc<SharedState>,
    tenant_id: u64,
    merged: &TenantDataSnapshot,
    snapshot_watermark: u64,
) {
    let catalog = state.credentials.catalog();
    let Ok(tombstones) = catalog.load_wal_tombstones() else {
        return;
    };
    if tombstones.is_empty() {
        return;
    }

    for name in &restored_collection_names(tenant_id, merged) {
        let Some(purge_lsn) = tombstones.purge_lsn(DatabaseId::DEFAULT.as_u64(), tenant_id, name)
        else {
            continue;
        };
        if snapshot_watermark != 0 && snapshot_watermark >= purge_lsn {
            continue;
        }
        tracing::warn!(
            tenant_id,
            collection = %name,
            purge_lsn,
            snapshot_watermark,
            "RESTORE: bringing back a collection that was hard-deleted on this cluster"
        );
        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(TenantId::new(tenant_id)),
            "__restore",
            &format!(
                "restore resurrected tombstoned collection '{name}' \
                 (purge_lsn={purge_lsn}, snapshot_watermark={snapshot_watermark})"
            ),
        );
    }
}

/// Every collection the backup restores rows into, read from each section's
/// key in that section's own format.
fn restored_collection_names(
    tenant_id: u64,
    merged: &TenantDataSnapshot,
) -> std::collections::BTreeSet<String> {
    let mut names = std::collections::BTreeSet::new();
    let db_tenant_scoped: [&[(String, Vec<u8>)]; 5] = [
        &merged.documents,
        &merged.documents_versioned,
        &merged.indexes,
        &merged.vectors,
        &merged.timeseries,
    ];
    for section in db_tenant_scoped {
        for (key, _) in section {
            if let Some(name) = extract_db_tenant_scoped_collection(key, tenant_id) {
                names.insert(name.to_string());
            }
        }
    }
    // A KV table's key is its collection name.
    for (name, _) in &merged.kv_tables {
        names.insert(name.clone());
    }
    for (key, _) in &merged.edges {
        if let Some((name, ..)) = parse_versioned_edge_key(key) {
            names.insert(name.to_string());
        }
    }
    names
}

#[cfg(test)]
mod collection_name_tests {
    use super::*;

    #[test]
    fn every_section_names_its_collection() {
        let snap = TenantDataSnapshot {
            documents: vec![("0:7:users:0000002a".into(), vec![])],
            documents_versioned: vec![(
                "0:7:ledger:0000002a\x0000000000000000000001".into(),
                vec![],
            )],
            vectors: vec![("0:7:embeddings".into(), vec![])],
            kv_tables: vec![("sessions".into(), vec![])],
            edges: vec![(
                "follows\x00a\x00L\x00b\x0000000000000000000001".into(),
                vec![],
            )],
            ..Default::default()
        };
        let names: Vec<String> = restored_collection_names(7, &snap).into_iter().collect();
        assert_eq!(
            names,
            vec!["embeddings", "follows", "ledger", "sessions", "users"]
        );
    }

    #[test]
    fn another_tenants_key_names_nothing() {
        let snap = TenantDataSnapshot {
            documents: vec![("0:8:users:0000002a".into(), vec![])],
            ..Default::default()
        };
        assert!(restored_collection_names(7, &snap).is_empty());
    }
}
