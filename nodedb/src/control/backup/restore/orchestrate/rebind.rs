// SPDX-License-Identifier: BUSL-1.1

//! Post-install surrogate rebinding and tombstoned-collection warnings for
//! [`super::restore_tenant`].

use std::sync::Arc;

use nodedb_types::Surrogate;

use crate::Error;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, SurrogateBindEntry, TenantDataSnapshot, TenantId};

/// Rebind every PK→surrogate identity carried in the backup into the
/// destination catalog so restored rows resolve by PK point-lookup.
///
/// No-op when the snapshot carried no bindings (e.g. an older backup created
/// before the surrogate-pk section existed) or when the node has no catalog.
/// Any catalog write failure is FATAL.
pub(super) fn rebind_surrogates(
    state: &Arc<SharedState>,
    binds: Vec<SurrogateBindEntry>,
) -> Result<(), Error> {
    if binds.is_empty() {
        return Ok(());
    }
    let catalog = state.credentials.catalog();
    let database_id = crate::types::DatabaseId::DEFAULT;
    for e in &binds {
        catalog.put_surrogate(
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

    let mut names = std::collections::BTreeSet::new();
    let sections: [&[(String, Vec<u8>)]; 6] = [
        &merged.documents,
        &merged.indexes,
        &merged.vectors,
        &merged.kv_tables,
        &merged.timeseries,
        &merged.edges,
    ];
    for section in sections {
        for (key, _) in section {
            if let Some(name) = collection_from_key(key) {
                names.insert(name.to_string());
            }
        }
    }

    for name in &names {
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

fn collection_from_key(key: &str) -> Option<&str> {
    let tail = key.split_once(':')?.1;
    tail.split([':', '\0']).next()
}

#[cfg(test)]
mod collection_key_tests {
    use super::collection_from_key;

    #[test]
    fn extracts_collection_with_colon_separator() {
        assert_eq!(collection_from_key("1:users:doc-1"), Some("users"));
    }

    #[test]
    fn extracts_collection_with_null_separator() {
        assert_eq!(collection_from_key("1:src\0label\0"), Some("src"));
    }

    #[test]
    fn vector_and_kv_key_shapes() {
        assert_eq!(collection_from_key("1:events"), Some("events"));
    }

    #[test]
    fn no_tenant_prefix_returns_none() {
        assert_eq!(collection_from_key("no_colon"), None);
    }
}
