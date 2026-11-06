// SPDX-License-Identifier: BUSL-1.1

//! Map every backed-up database to its destination database.
//!
//! The backup names each database by its source id and its name. The
//! destination database is the one of the same name. A database the
//! destination does not have is created with a fresh id: its descriptor
//! settings, its quota and the tenant's quota in it come from the backup.
//! Every entry is proposed through the metadata Raft group, exactly like
//! `CREATE DATABASE` and `ALTER ... SET QUOTA`, so every node learns it.
//!
//! Database grants are not carried: a grant names a user id of the source
//! cluster, and a tenant backup carries no users.

use std::collections::BTreeMap;

use nodedb_types::QuotaRecord;
use nodedb_types::backup_envelope::{DatabaseBlob, Envelope, SECTION_ORIGIN_DATABASES};

use crate::Error;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::{DatabaseDescriptor, DatabaseStatus};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::target::{DatabaseTarget, RestoreMark};

/// The destination database of every backed-up database, keyed by source id.
#[derive(Debug, Default)]
pub(super) struct DatabaseMap {
    targets: BTreeMap<u64, DatabaseTarget>,
    /// Databases the restore created on this cluster.
    created: usize,
}

impl DatabaseMap {
    /// Number of databases the restore created on this cluster.
    pub fn created(&self) -> usize {
        self.created
    }

    /// The target of the source database `source`, when it has one. A dry
    /// run leaves a database the destination lacks without a target.
    pub fn get(&self, source: u64) -> Option<DatabaseTarget> {
        self.targets.get(&source).copied()
    }

    /// The target of the source database `source`. A section that names a
    /// database the backup's database section does not list is malformed.
    pub fn target(&self, source: u64) -> Result<DatabaseTarget, Error> {
        self.get(source).ok_or_else(|| Error::Internal {
            detail: format!(
                "invalid backup format: a section names database {source}, which the \
                 backup's database section does not list"
            ),
        })
    }
}

/// Decode the database section of `env`. An envelope with rows but no
/// database section fails the restore when a row section names a database.
pub(super) fn decode_databases(env: &Envelope) -> Result<Vec<DatabaseBlob>, Error> {
    let mut databases = Vec::new();
    for section in &env.sections {
        if section.origin_node_id == SECTION_ORIGIN_DATABASES {
            let blobs: Vec<DatabaseBlob> =
                zerompk::from_msgpack(&section.body).map_err(|_| Error::Internal {
                    detail: "invalid backup format: database section is not decodable".into(),
                })?;
            databases.extend(blobs);
        }
    }
    Ok(databases)
}

/// Map every database of `blobs` to its destination. Unless `dry_run`,
/// create each database the destination lacks and restore the tenant's
/// quota in each database that has none. Every target carries `restore_id`.
pub(super) async fn resolve_databases(
    state: &SharedState,
    tenant_id: u64,
    blobs: &[DatabaseBlob],
    dry_run: bool,
    restore: RestoreMark,
) -> Result<DatabaseMap, Error> {
    let catalog = state.credentials.catalog();
    let mut map = DatabaseMap::default();
    for blob in blobs {
        let dest = match catalog.get_database_id_by_name(&blob.name)? {
            Some(id) => {
                require_writable(state, id, &blob.name)?;
                id
            }
            None if dry_run => continue,
            None => {
                map.created += 1;
                create_database(state, blob).await?
            }
        };
        if !dry_run && let Some(record) = &blob.tenant_quota {
            restore_tenant_quota(state, dest, TenantId::new(tenant_id), record).await?;
        }
        map.targets.insert(
            blob.database_id,
            DatabaseTarget {
                source: DatabaseId::new(blob.database_id),
                dest,
                restore_id: restore.id,
                restore_cut: restore.cut,
                event_source: restore.event_source,
            },
        );
    }
    Ok(map)
}

/// A restore writes rows, so the destination database must take writes.
pub(super) fn require_writable(
    state: &SharedState,
    id: DatabaseId,
    name: &str,
) -> Result<(), Error> {
    let status = state
        .credentials
        .catalog()
        .get_database(id)?
        .map(|descriptor| descriptor.status);
    match status {
        Some(DatabaseStatus::Active) => Ok(()),
        Some(DatabaseStatus::Deactivated | DatabaseStatus::Cloning | DatabaseStatus::Mirroring)
        | None => Err(Error::BadRequest {
            detail: format!(
                "restore refused: database '{name}' exists on this cluster but does not take \
                 writes (status {status:?}). Make it active or remove it, then retry the restore"
            ),
        }),
    }
}

/// Create the database `blob` describes under a fresh id, with its settings
/// and its quota. Returns the new id.
async fn create_database(state: &SharedState, blob: &DatabaseBlob) -> Result<DatabaseId, Error> {
    let source: DatabaseDescriptor =
        zerompk::from_msgpack(&blob.descriptor).map_err(|_| Error::Internal {
            detail: format!(
                "invalid backup format: descriptor of database '{}' is not decodable",
                blob.name
            ),
        })?;
    let id = crate::control::database::allocate_database_id(state).await?;
    // The restored database is a new, independent database: it is active,
    // and it is no clone or mirror of a database on this cluster.
    let descriptor = DatabaseDescriptor {
        id,
        name: blob.name.clone(),
        status: DatabaseStatus::Active,
        created_at_lsn: state.wal.next_lsn().as_u64(),
        parent_clone: None,
        mirror_origin: None,
        ..source
    };
    propose_catalog_entry_async(state, &CatalogEntry::PutDatabase(Box::new(descriptor))).await?;

    if let Some(record) = &blob.database_quota {
        restore_database_quota(state, id, record).await?;
    }
    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseCreated,
        None,
        Some(id),
        "__restore",
        &format!(
            "RESTORE created database '{}' (source id {})",
            blob.name, blob.database_id
        ),
    );
    Ok(id)
}

/// Install the backed-up quota of a database the restore created.
async fn restore_database_quota(
    state: &SharedState,
    id: DatabaseId,
    record: &QuotaRecord,
) -> Result<(), Error> {
    let catalog = state.credentials.catalog();
    catalog.check_database_quota(id, record, &state.quota_ceiling_snapshot())?;
    let entry = CatalogEntry::PutDatabaseQuota {
        db_id: id.as_u64(),
        record: Box::new(record.clone()),
    };
    propose_catalog_entry_async(state, &entry).await?;
    Ok(())
}

/// Install the tenant's backed-up quota in `id`, unless the destination
/// already sets one there.
async fn restore_tenant_quota(
    state: &SharedState,
    id: DatabaseId,
    tenant: TenantId,
    record: &QuotaRecord,
) -> Result<(), Error> {
    let catalog = state.credentials.catalog();
    if catalog.get_tenant_quota(id, tenant)?.is_some() {
        return Ok(());
    }
    catalog.check_tenant_quota(id, tenant, record)?;
    let entry = CatalogEntry::PutTenantQuota {
        db_id: id.as_u64(),
        tenant_id: tenant.as_u64(),
        record: Box::new(record.clone()),
    };
    propose_catalog_entry_async(state, &entry).await?;
    Ok(())
}
