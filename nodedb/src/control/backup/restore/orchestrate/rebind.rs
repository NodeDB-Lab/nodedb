// SPDX-License-Identifier: BUSL-1.1

//! Surrogate rebinding and tombstoned-collection warnings for
//! [`super::restore_tenant`].

use std::collections::BTreeSet;

use nodedb_types::{CollectionKey, Surrogate};

use crate::Error;
use crate::control::backup::snapshot_keys::{
    extract_db_scoped_collection, extract_db_tenant_scoped_collection,
};
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::parse_versioned_edge_key;
use crate::types::{SurrogateBindEntry, TenantDataSnapshot, TenantId};

use super::super::target::DatabaseTarget;

/// Bind every PK→surrogate identity the backup carries for one database on
/// this node, before any re-issue, so a re-issued row keeps the surrogate it
/// was stored under. Each bind names the bare collection, keyed in the
/// destination database.
///
/// Binding is first-wins: a key this node already binds keeps its surrogate,
/// and the re-issue writes that row over it. Each bind also raises this
/// node's surrogate high-water mark past the backup's surrogate, so no later
/// allocation here reuses it. Every replica binds the identities a re-issued
/// write carries as it applies the write. Any bind error is fatal.
pub(super) fn rebind_surrogates(
    state: &SharedState,
    target: DatabaseTarget,
    binds: &[SurrogateBindEntry],
) -> Result<(), Error> {
    for e in binds {
        if e.database_id != target.source.as_u64() {
            return Err(Error::Internal {
                detail: format!(
                    "invalid backup format: a surrogate bind of '{}' names database {}, but \
                     sits with database {}",
                    e.collection,
                    e.database_id,
                    target.source.as_u64()
                ),
            });
        }
    }
    // One catalog transaction per (tenant, collection) the backup binds.
    let mut groups: Vec<(u64, &str)> = Vec::new();
    for e in binds {
        if !groups.contains(&(e.tenant_id, e.collection.as_str())) {
            groups.push((e.tenant_id, e.collection.as_str()));
        }
    }
    for (tenant_id, collection) in groups {
        let rows: Vec<(&[u8], Surrogate)> = binds
            .iter()
            .filter(|e| e.tenant_id == tenant_id && e.collection == collection)
            .map(|e| (e.pk.as_slice(), Surrogate::new(e.surrogate)))
            .collect();
        state.surrogate_assigner.bind_many(
            CollectionKey::from_bare(target.dest, collection),
            TenantId::new(tenant_id),
            &rows,
        )?;
    }
    Ok(())
}

pub(super) fn warn_on_tombstoned_restores(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
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

    for name in &restored_collection_names(tenant_id, target, merged) {
        let Some(purge_lsn) =
            tombstones.purge_lsn(CollectionKey::from_bare(target.dest, name), tenant_id)
        else {
            continue;
        };
        if snapshot_watermark != 0 && snapshot_watermark >= purge_lsn {
            continue;
        }
        tracing::warn!(
            tenant_id,
            database_id = target.dest.as_u64(),
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
                "restore resurrected tombstoned collection '{name}' in database {} \
                 (purge_lsn={purge_lsn}, snapshot_watermark={snapshot_watermark})",
                target.dest.as_u64()
            ),
        );
    }
}

/// The bare catalog name of every collection the backup restores rows into
/// for one database, read from each section's key in that section's own
/// format. A name that does not resolve in the source database is skipped
/// here: the re-issue of its section refuses it.
fn restored_collection_names(
    tenant_id: u64,
    target: DatabaseTarget,
    merged: &TenantDataSnapshot,
) -> BTreeSet<String> {
    let mut stored: Vec<&str> = Vec::new();
    let db_tenant_scoped: [&[(String, Vec<u8>)]; 4] = [
        &merged.documents_versioned,
        &merged.indexes_versioned,
        &merged.vectors,
        &merged.timeseries,
    ];
    for section in db_tenant_scoped {
        for (key, _) in section {
            stored.extend(extract_db_tenant_scoped_collection(key, tenant_id));
        }
    }
    for (key, _) in merged.kv_tables.iter().chain(&merged.columnar_engines) {
        stored.extend(extract_db_scoped_collection(key, tenant_id));
    }
    for (key, _) in &merged.edges {
        if let Some((name, ..)) = parse_versioned_edge_key(key) {
            stored.push(name);
        }
    }
    stored
        .into_iter()
        .filter_map(|name| target.resolve(name).ok())
        .map(|name| name.bare)
        .collect()
}

#[cfg(test)]
mod collection_name_tests {
    use super::*;
    use crate::types::DatabaseId;

    const DEFAULT_TARGET: DatabaseTarget = DatabaseTarget {
        source: DatabaseId::DEFAULT,
        dest: DatabaseId::DEFAULT,
        restore_id: 0,
        restore_cut: i64::MAX,
    };

    #[test]
    fn every_section_names_its_collection() {
        let snap = TenantDataSnapshot {
            documents_versioned: vec![
                ("0:7:users:0000002a\x0000000000000000000001".into(), vec![]),
                ("0:7:ledger:0000002a\x0000000000000000000001".into(), vec![]),
            ],
            vectors: vec![("0:7:embeddings".into(), vec![])],
            kv_tables: vec![("0:7:sessions".into(), vec![])],
            edges: vec![(
                "follows\x00a\x00L\x00b\x0000000000000000000001".into(),
                vec![],
            )],
            ..Default::default()
        };
        let names: Vec<String> = restored_collection_names(7, DEFAULT_TARGET, &snap)
            .into_iter()
            .collect();
        assert_eq!(
            names,
            vec!["embeddings", "follows", "ledger", "sessions", "users"]
        );
    }

    /// A named database's sections name qualified collections. The warning
    /// reads the bare names the destination catalog keys its tombstones by.
    #[test]
    fn a_named_database_yields_bare_names() {
        let target = DatabaseTarget {
            source: DatabaseId::new(1025),
            dest: DatabaseId::new(1030),
            restore_id: 0,
            restore_cut: i64::MAX,
        };
        let snap = TenantDataSnapshot {
            documents_versioned: vec![(
                "1025:7:1025/users:0000002a\x0000000000000000000001".into(),
                vec![],
            )],
            kv_tables: vec![("1025:7:1025/sessions".into(), vec![])],
            ..Default::default()
        };
        let names: Vec<String> = restored_collection_names(7, target, &snap)
            .into_iter()
            .collect();
        assert_eq!(names, vec!["sessions", "users"]);
    }

    #[test]
    fn another_tenants_key_names_nothing() {
        let snap = TenantDataSnapshot {
            documents_versioned: vec![(
                "0:8:users:0000002a\x0000000000000000000001".into(),
                vec![],
            )],
            ..Default::default()
        };
        assert!(restored_collection_names(7, DEFAULT_TARGET, &snap).is_empty());
    }
}
