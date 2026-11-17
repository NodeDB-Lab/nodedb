// SPDX-License-Identifier: BUSL-1.1

//! The tenant's databases and the metadata sections of its backup.
//!
//! A tenant's collections can live in any database. The backup covers every
//! database the tenant has a collection in. The metadata sections record, per
//! database:
//!
//! - the database itself: its descriptor, its quota and the tenant's quota in
//!   it (`SECTION_ORIGIN_DATABASES`);
//! - the catalog row of each of the tenant's collections in it
//!   (`SECTION_ORIGIN_CATALOG_ROWS`);
//! - the PK-to-surrogate binds of those collections
//!   (`SECTION_ORIGIN_SURROGATE_PK`);
//! - the WAL tombstones of the tenant's purged collections in it
//!   (`SECTION_ORIGIN_SOURCE_TOMBSTONES`);
//! - the catalog row of each of the tenant's arrays in it
//!   (`SECTION_ORIGIN_ARRAY_CATALOG`);
//! - every index of those collections, with its owner and a vector index's
//!   build parameters (`SECTION_ORIGIN_INDEX_RECORDS`).
//!
//! Every entry names its database by the source id. Restore maps each source
//! id to a destination database by name.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_types::backup_envelope::{
    ArrayCatalogBlob, DatabaseBlob, EnvelopeWriter, SECTION_ORIGIN_ARRAY_CATALOG,
    SECTION_ORIGIN_CATALOG_ROWS, SECTION_ORIGIN_DATABASES, SECTION_ORIGIN_INDEX_RECORDS,
    SECTION_ORIGIN_SOURCE_TOMBSTONES, SECTION_ORIGIN_SURROGATE_PK, SourceTombstoneEntry,
    StoredCollectionBlob, SurrogateBindBlob,
};

use crate::Error;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::security::catalog::{
    DatabaseDescriptor, IndexRecordState, StoredCollection, StoredIndexRecord, SystemCatalog,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// One database the tenant has collections in.
pub struct TenantDatabase {
    pub descriptor: DatabaseDescriptor,
    /// Every collection of the tenant in this database, soft-deleted ones
    /// included: UNDROP works after a restore of a backup taken during the
    /// retention window.
    pub collections: Vec<StoredCollection>,
    /// Every array of the tenant in this database.
    pub arrays: Vec<ArrayCatalogEntry>,
    /// Every index record of `collections`, except the ones a `DROP INDEX`
    /// is removing.
    pub indexes: Vec<StoredIndexRecord>,
}

impl TenantDatabase {
    pub fn id(&self) -> DatabaseId {
        self.descriptor.id
    }
}

/// Every database `tenant_id` has a collection or an array in, in
/// database-id order.
///
/// A collection whose database has no catalog entry fails the backup: the
/// restore cannot recreate that database, and its rows are lost.
///
/// A collection still delegating reads to a clone source fails the backup, as
/// [`refuse_unmaterialized_clone`] explains.
pub fn tenant_databases(state: &SharedState, tenant_id: u64) -> Result<Vec<TenantDatabase>, Error> {
    tenant_databases_in(state.credentials.catalog(), tenant_id)
}

fn tenant_databases_in(
    catalog: &SystemCatalog,
    tenant_id: u64,
) -> Result<Vec<TenantDatabase>, Error> {
    let mut by_database: BTreeMap<u64, (Vec<StoredCollection>, Vec<ArrayCatalogEntry>)> =
        BTreeMap::new();
    for coll in catalog.load_all_collections_across_databases()? {
        if coll.tenant_id == tenant_id {
            refuse_unmaterialized_clone(&coll)?;
            by_database
                .entry(coll.database_id.as_u64())
                .or_default()
                .0
                .push(coll);
        }
    }
    for array in catalog.load_all_arrays()? {
        if array.array_id.tenant_id.as_u64() == tenant_id {
            by_database
                .entry(array.array_id.database_id.as_u64())
                .or_default()
                .1
                .push(array);
        }
    }
    let mut databases = Vec::with_capacity(by_database.len());
    for (raw_id, (collections, arrays)) in by_database {
        let descriptor = catalog
            .get_database(DatabaseId::new(raw_id))?
            .ok_or_else(|| Error::Internal {
                detail: format!(
                    "backup: tenant {tenant_id} has collections or arrays in database {raw_id}, \
                         but the catalog has no entry for that database. Restore the \
                         database entry, then retry the backup"
                ),
            })?;
        let indexes = backed_up_indexes(catalog, raw_id, tenant_id, &collections)?;
        databases.push(TenantDatabase {
            descriptor,
            collections,
            arrays,
            indexes,
        });
    }
    Ok(databases)
}

/// The committed index records of `collections` in database `database_id`.
///
/// A `Dropping` record names an index a `DROP INDEX` is removing, so the
/// backup leaves it out. A record of a collection the backup does not carry
/// is left out too: its collection was created after the collection read.
fn backed_up_indexes(
    catalog: &SystemCatalog,
    database_id: u64,
    tenant_id: u64,
    collections: &[StoredCollection],
) -> Result<Vec<StoredIndexRecord>, Error> {
    let names: BTreeSet<&str> = collections.iter().map(|c| c.name.as_str()).collect();
    Ok(catalog
        .list_committed_index_records(database_id, tenant_id)?
        .into_iter()
        .filter(|record| {
            record.state != IndexRecordState::Dropping && names.contains(record.collection.as_str())
        })
        .collect())
}

/// Refuse to back up a collection whose rows still live in its clone source.
///
/// Its `cloned_from` names a source database id, a WAL LSN window, and a
/// surrogate ceiling. All three belong to this cluster only, so no restore
/// can translate them, and the collection's own storage lacks the delegated
/// rows. `ALTER DATABASE ... MATERIALIZE` copies the rows in and clears
/// `cloned_from`, after which the backup carries every row.
pub fn refuse_unmaterialized_clone(coll: &StoredCollection) -> Result<(), Error> {
    let Some(origin) = &coll.cloned_from else {
        return Ok(());
    };
    Err(Error::BadRequest {
        detail: format!(
            "collection '{}' in database {} is an unmaterialized clone of '{}' in database {}; \
             run ALTER DATABASE ... MATERIALIZE on its database, then retry",
            coll.name,
            coll.database_id.as_u64(),
            origin.source_collection,
            origin.source_database.as_u64()
        ),
    })
}

/// Push the metadata sections of `databases`, with `binds` from
/// [`surrogate_binds`]. A catalog read or encode error fails the backup: an
/// envelope without these sections restores rows into no database, rows a
/// point lookup cannot find, or a purged collection.
pub fn push_metadata_sections(
    state: &SharedState,
    tenant_id: u64,
    databases: &[TenantDatabase],
    binds: &[SurrogateBindBlob],
    writer: &mut EnvelopeWriter,
) -> Result<(), Error> {
    let catalog = state.credentials.catalog();
    let tenant = TenantId::new(tenant_id);

    let mut blobs = Vec::with_capacity(databases.len());
    for database in databases {
        blobs.push(DatabaseBlob {
            database_id: database.id().as_u64(),
            name: database.descriptor.name.clone(),
            descriptor: encode_section_part("database descriptor", &database.descriptor)?,
            database_quota: catalog.get_database_quota(database.id())?,
            tenant_quota: catalog.get_tenant_quota(database.id(), tenant)?,
        });
    }
    push_nonempty(writer, SECTION_ORIGIN_DATABASES, "databases", &blobs)?;

    let mut rows = Vec::new();
    for database in databases {
        for coll in &database.collections {
            rows.push(StoredCollectionBlob {
                database_id: database.id().as_u64(),
                name: coll.name.clone(),
                bytes: encode_section_part("catalog row", coll)?,
            });
        }
    }
    push_nonempty(writer, SECTION_ORIGIN_CATALOG_ROWS, "catalog rows", &rows)?;

    let mut arrays = Vec::new();
    for database in databases {
        for array in &database.arrays {
            arrays.push(ArrayCatalogBlob {
                database_id: database.id().as_u64(),
                name: array.name.clone(),
                bytes: encode_section_part("array catalog row", array)?,
            });
        }
    }
    push_nonempty(
        writer,
        SECTION_ORIGIN_ARRAY_CATALOG,
        "array catalog rows",
        &arrays,
    )?;

    let indexes = super::index_capture::index_blobs(state, tenant, databases)?;
    push_nonempty(
        writer,
        SECTION_ORIGIN_INDEX_RECORDS,
        "index records",
        &indexes,
    )?;

    // PK→surrogate identity map. This is DATA-derived per-node state that the
    // per-node engine sections do NOT carry (the Data-Plane snapshot handler
    // has no catalog access). Without it a restored node has documents but
    // cannot resolve PK point-lookups (`WHERE id=<pk>`).
    push_nonempty(writer, SECTION_ORIGIN_SURROGATE_PK, "surrogate pk", binds)?;

    let backed_up: BTreeSet<u64> = databases.iter().map(|d| d.id().as_u64()).collect();
    let mut tombs = Vec::new();
    for (database_id, tid, name, purge_lsn) in catalog.load_wal_tombstones()?.iter() {
        if tid == tenant_id && backed_up.contains(&database_id) {
            tombs.push(SourceTombstoneEntry {
                database_id,
                collection: name.to_string(),
                purge_lsn,
            });
        }
    }
    push_nonempty(
        writer,
        SECTION_ORIGIN_SOURCE_TOMBSTONES,
        "source tombstones",
        &tombs,
    )
}

/// Every PK→surrogate bind of the tenant's collections in `databases`, each
/// from a source node that holds it (see
/// [`super::bind_capture::capture_binds`]). No one node holds every bind when
/// nodes outnumber the replication factor.
pub async fn surrogate_binds(
    state: &SharedState,
    tenant_id: u64,
    databases: &[TenantDatabase],
) -> Result<Vec<SurrogateBindBlob>, Error> {
    let mut binds = Vec::new();
    for database in databases {
        let names: Vec<String> = database
            .collections
            .iter()
            .map(|coll| coll.name.clone())
            .collect();
        let captured =
            super::bind_capture::capture_binds(state, tenant_id, database.id(), &names).await?;
        binds.extend(captured.into_iter().map(|bind| SurrogateBindBlob {
            database_id: bind.database_id,
            tenant_id: bind.tenant_id,
            collection: bind.collection,
            pk: bind.pk,
            surrogate: bind.surrogate,
        }));
    }
    Ok(binds)
}

/// Encode one part of a section.
pub(super) fn encode_section_part<T: zerompk::ToMessagePack>(
    what: &str,
    value: &T,
) -> Result<Vec<u8>, Error> {
    zerompk::to_msgpack_vec(value).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("backup envelope ({what}): encode: {e}"),
    })
}

/// Encode `entries` and push them as the section `origin`, unless empty.
fn push_nonempty<T: zerompk::ToMessagePack>(
    writer: &mut EnvelopeWriter,
    origin: u64,
    what: &str,
    entries: &[T],
) -> Result<(), Error> {
    if entries.is_empty() {
        return Ok(());
    }
    let body = encode_section_part(what, &entries)?;
    writer
        .push_section(origin, body)
        .map_err(|e| Error::Internal {
            detail: format!("backup envelope ({what}): {e}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    /// A backup refuses a clone that still delegates to its source, and takes
    /// the same collection once it is materialized.
    #[test]
    fn backup_refuses_an_unmaterialized_clone() {
        let (_dir, catalog) = open_catalog();
        let mut coll = StoredCollection::stamped_for_test(1, "orders", "admin");
        coll.cloned_from = Some(nodedb_types::CloneOrigin {
            source_database: DatabaseId::new(1024),
            source_collection: "orders".into(),
            as_of_lsn: nodedb_types::Lsn::new(10),
            clone_created_at: nodedb_types::Lsn::new(11),
            kv_surrogate_ceiling: None,
        });
        catalog.put_collection(DatabaseId::DEFAULT, &coll).unwrap();

        let err = tenant_databases_in(&catalog, 1)
            .err()
            .expect("an unmaterialized clone fails the backup");
        assert!(matches!(err, Error::BadRequest { .. }), "{err}");

        coll.cloned_from = None;
        coll.clone_status = nodedb_types::CloneStatus::Materialized;
        catalog.put_collection(DatabaseId::DEFAULT, &coll).unwrap();
        assert_eq!(tenant_databases_in(&catalog, 1).unwrap().len(), 1);
    }
}
