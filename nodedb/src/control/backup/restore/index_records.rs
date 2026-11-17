// SPDX-License-Identifier: BUSL-1.1

//! RESTORE of the tenant's indexes: each index record, its owner, and a
//! vector index's build parameters.
//!
//! Every entry is proposed through the metadata group, as `CREATE INDEX`
//! proposes it, so every node's catalog lists and resolves the index. The
//! records land after the collection rows and before any row re-issues. A
//! full-text record therefore declares its fields when the restore registers
//! the collection, and every re-issued row indexes its text on every replica.
//!
//! A full-text record lands `Ready` only on a collection this restore
//! created: all of its rows then arrive through the re-issue. On a collection
//! the destination already held, rows the backup does not carry hold no
//! postings for the record's fields. That record lands `Building`, and the
//! text index rebuilds on every node once the rows re-issued.
//!
//! An index name the destination holds for the same kind on the same
//! collection takes the backup's definition, as a collection row does. A name
//! held by another collection or kind, or by an index under drop, refuses the
//! restore before it proposes anything. So do full-text records that
//! disagree on analysis once the backup's records replaced those of their
//! names.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_types::backup_envelope::{
    DatabaseBlob, Envelope, IndexRecordBlob, SECTION_ORIGIN_CATALOG_ROWS,
    SECTION_ORIGIN_INDEX_RECORDS, StoredCollectionBlob,
};
use nodedb_types::{Hlc, StoredVectorIndexParams};

use crate::Error;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::{
    IndexKind, IndexRecordState, StoredIndexRecord, SystemCatalog,
};
use crate::control::security::permission::prepare_owner;
use crate::control::server::shared::ddl::neutral::collection::index::text_build::build_text_index;
use crate::control::server::shared::ddl::neutral::collection::text_analysis::replacement_conflict;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::databases::DatabaseMap;
use super::index_apply::{CollectionRecords, propose_collection_records};

/// A full-text record the restore put `Building`, whose text index rebuilds
/// once every row re-issued.
pub(super) struct TextBuild {
    database_id: DatabaseId,
    collection: String,
    name: String,
}

/// One decoded entry of the index section.
struct BackedUpIndex {
    /// Source id of the index's database.
    source_database: u64,
    record: StoredIndexRecord,
    owner: Option<String>,
    vector_params: Option<StoredVectorIndexParams>,
}

/// Check every index entry against the envelope and the destination.
/// Proposes nothing.
///
/// Each entry must decode, belong to `tenant_id`, name a database the
/// envelope lists, and name a collection the envelope carries a row of. Its
/// name must be free in the destination database, or held by an index of
/// the same kind on the same collection that no `DROP INDEX` is removing.
pub(super) fn validate_index_records(
    catalog: &SystemCatalog,
    tenant_id: u64,
    env: &Envelope,
    databases: &[DatabaseBlob],
) -> Result<(), Error> {
    let carried = carried_collections(env)?;
    // Blobs of the same name land in the same destination database.
    let mut claimed: BTreeMap<(&str, String), (IndexKind, String)> = BTreeMap::new();
    // The backed-up full-text records of each collection, keyed by its
    // destination database. Only a database the destination holds is keyed.
    let mut full_text: BTreeMap<(u64, &str, String), Vec<StoredIndexRecord>> = BTreeMap::new();
    for index in decode_index_section(env, tenant_id)? {
        let Some(database) = databases
            .iter()
            .find(|blob| blob.database_id == index.source_database)
        else {
            return Err(invalid(format!(
                "index '{}' names database {}, which the backup's database section does not \
                 list",
                index.record.name, index.source_database
            )));
        };
        let record = &index.record;
        if !carried.contains(&(index.source_database, record.collection.clone())) {
            return Err(invalid(format!(
                "index '{}' names collection '{}', which the backup carries no catalog row of",
                record.name, record.collection
            )));
        }
        let key = (database.name.as_str(), record.name.clone());
        let shape = (record.kind, record.collection.clone());
        if let Some(other) = claimed.get(&key)
            && *other != shape
        {
            return Err(invalid(format!(
                "index '{}' is backed up twice for database '{}', on different collections or \
                 kinds",
                record.name, database.name
            )));
        }
        claimed.insert(key, shape);
        if let Some(dest) = catalog.get_database_id_by_name(&database.name)? {
            check_destination_holder(catalog, tenant_id, dest, &database.name, record)?;
            if record.kind == IndexKind::FullText {
                full_text
                    .entry((
                        dest.as_u64(),
                        database.name.as_str(),
                        record.collection.clone(),
                    ))
                    .or_default()
                    .push(index.record);
            }
        }
    }
    for ((dest, database_name, collection), records) in &full_text {
        check_destination_analysis(
            catalog,
            tenant_id,
            DatabaseId::new(*dest),
            database_name,
            collection,
            records,
        )?;
    }
    Ok(())
}

/// Refuse a destination index that holds `record`'s name and is not the
/// same index.
fn check_destination_holder(
    catalog: &SystemCatalog,
    tenant_id: u64,
    dest: DatabaseId,
    database_name: &str,
    record: &StoredIndexRecord,
) -> Result<(), Error> {
    let Some(holder) =
        catalog.get_committed_index_record(dest.as_u64(), tenant_id, &record.name)?
    else {
        return Ok(());
    };
    if holder.state == IndexRecordState::Dropping {
        return Err(Error::ObjectNotInPrerequisiteState {
            object: record.name.clone(),
            detail: format!(
                "restore: index '{}' on '{}' in database '{database_name}' is still being \
                 dropped; run DROP INDEX {} to finish the drop, then retry the restore",
                record.name, holder.collection, record.name
            ),
        });
    }
    check_holder_shape(&holder, record, database_name)
}

/// Refuse backed-up full-text `records` of `collection` whose analysis
/// disagrees with the destination collection's full-text records once the
/// restore applied them. One inverted index serves them all, so they
/// tokenize one way.
///
/// Each backed-up record replaces the destination record of its name, as
/// its apply does. A destination record of another name stays and counts.
fn check_destination_analysis(
    catalog: &SystemCatalog,
    tenant_id: u64,
    dest: DatabaseId,
    database_name: &str,
    collection: &str,
    records: &[StoredIndexRecord],
) -> Result<(), Error> {
    let held = catalog.list_committed_index_records_for_collection(
        dest.as_u64(),
        tenant_id,
        collection,
    )?;
    let incoming: Vec<&StoredIndexRecord> = records.iter().collect();
    match replacement_conflict(&held, &incoming, collection) {
        Some(conflict) => Err(Error::BadRequest {
            detail: format!(
                "restore: the backup's full-text indexes on '{collection}' in database \
                 '{database_name}' conflict: {conflict}"
            ),
        }),
        None => Ok(()),
    }
}

/// Refuse `holder`, the destination index of `record`'s name, unless it is
/// an index of the same kind on the same collection.
fn check_holder_shape(
    holder: &StoredIndexRecord,
    record: &StoredIndexRecord,
    database_name: &str,
) -> Result<(), Error> {
    if holder.kind != record.kind || holder.collection != record.collection {
        return Err(Error::BadRequest {
            detail: format!(
                "restore: the backup's {} index '{}' on '{}' collides with the {} index '{}' on \
                 '{}' in database '{database_name}'; drop or rename that index, then retry the \
                 restore",
                record.kind.display_type(),
                record.name,
                record.collection,
                holder.kind.display_type(),
                holder.name,
                holder.collection
            ),
        });
    }
    Ok(())
}

/// Propose every backed-up index into its destination database. Runs after
/// the collection rows are proposed and before any row re-issues.
///
/// `fresh` names every collection, by destination database id and name, that
/// the destination held no row of before this restore. Returns the
/// full-text records put `Building`, for [`build_text_indexes`].
pub(super) async fn apply_index_records(
    state: &SharedState,
    tenant_id: u64,
    env: &Envelope,
    databases: &DatabaseMap,
    fresh: &BTreeSet<(u64, String)>,
) -> Result<Vec<TextBuild>, Error> {
    // The records of each destination collection, proposed together.
    let mut collections: BTreeMap<(u64, String), Vec<StoredIndexRecord>> = BTreeMap::new();
    for index in decode_index_section(env, tenant_id)? {
        let dest = databases.target(index.source_database)?.dest;
        if let Some(params) = index.vector_params {
            restore_vector_params(state, dest, params).await?;
        }
        let mut record = index.record;
        record.database_id = dest.as_u64();
        if let Some(owner) = &index.owner {
            restore_owner(state, &record, owner).await?;
        }
        collections
            .entry((record.database_id, record.collection.clone()))
            .or_default()
            .push(record);
    }
    let mut builds = Vec::new();
    for ((dest, collection), records) in collections {
        let target = CollectionRecords {
            tenant_id,
            dest: DatabaseId::new(dest),
            collection: &collection,
            fresh: fresh.contains(&(dest, collection.clone())),
        };
        for name in propose_collection_records(state, &target, records).await? {
            builds.push(TextBuild {
                database_id: DatabaseId::new(dest),
                collection: collection.clone(),
                name,
            });
        }
    }
    Ok(builds)
}

/// Rebuild the text index of every record in `builds` on every node, then
/// flip the record `Ready`. Runs after every row re-issued.
pub(super) async fn build_text_indexes(
    state: &SharedState,
    tenant_id: u64,
    builds: Vec<TextBuild>,
) -> Result<(), Error> {
    for build in builds {
        build_text_index(
            state,
            TenantId::new(tenant_id),
            build.database_id,
            &build.collection,
            &build.name,
        )
        .await?;
    }
    Ok(())
}

/// Propose a vector index's build parameters into database `dest`, unless
/// the destination holds the same parameters.
async fn restore_vector_params(
    state: &SharedState,
    dest: DatabaseId,
    mut params: StoredVectorIndexParams,
) -> Result<(), Error> {
    params.database_id = dest.as_u64();
    // The propose stamp names the row's modification time.
    params.modification_hlc = Hlc::ZERO;
    let existing = state
        .credentials
        .catalog()
        .get_committed_vector_index_params(
            params.database_id,
            params.tenant_id,
            &params.collection,
            &params.field_name,
        )?;
    if let Some(mut existing) = existing {
        existing.modification_hlc = Hlc::ZERO;
        if encode("vector index params", &existing)? == encode("vector index params", &params)? {
            return Ok(());
        }
    }
    propose_catalog_entry_async(state, &CatalogEntry::PutVectorIndexParams(Box::new(params)))
        .await?;
    Ok(())
}

/// Propose `owner` as the owner of the index `record`, unless the ledger
/// already names it.
async fn restore_owner(
    state: &SharedState,
    record: &StoredIndexRecord,
    owner: &str,
) -> Result<(), Error> {
    let object_type = record.kind.owner_object_type();
    let tenant = TenantId::new(record.tenant_id);
    let held = state
        .permissions
        .get_owner(object_type, record.database_id, tenant, &record.name);
    if held.as_deref() == Some(owner) {
        return Ok(());
    }
    let stored = prepare_owner(object_type, record.database_id, tenant, &record.name, owner);
    propose_catalog_entry_async(state, &CatalogEntry::PutOwner(Box::new(stored))).await?;
    Ok(())
}

/// Every entry of the envelope's index sections, decoded. An entry of
/// another tenant, or one whose name or parameters disagree with its record,
/// is refused.
fn decode_index_section(env: &Envelope, tenant_id: u64) -> Result<Vec<BackedUpIndex>, Error> {
    let mut out = Vec::new();
    for section in env
        .sections
        .iter()
        .filter(|s| s.origin_node_id == SECTION_ORIGIN_INDEX_RECORDS)
    {
        let blobs = zerompk::from_msgpack::<Vec<IndexRecordBlob>>(&section.body)
            .map_err(|_| invalid("index-records section is not decodable".into()))?;
        for blob in blobs {
            out.push(decode_blob(blob, tenant_id)?);
        }
    }
    Ok(out)
}

fn decode_blob(blob: IndexRecordBlob, tenant_id: u64) -> Result<BackedUpIndex, Error> {
    let record = zerompk::from_msgpack::<StoredIndexRecord>(&blob.record)
        .map_err(|_| invalid(format!("index record '{}' is not decodable", blob.name)))?;
    if record.name != blob.name || record.tenant_id != tenant_id {
        return Err(invalid(format!(
            "index record '{}' names index '{}' of tenant {}, not tenant {tenant_id}",
            blob.name, record.name, record.tenant_id
        )));
    }
    let vector_params = blob
        .vector_params
        .map(|bytes| {
            zerompk::from_msgpack::<StoredVectorIndexParams>(&bytes).map_err(|_| {
                invalid(format!(
                    "vector parameters of index '{}' are not decodable",
                    blob.name
                ))
            })
        })
        .transpose()?;
    if let Some(params) = &vector_params
        && (record.kind != IndexKind::Vector
            || params.tenant_id != tenant_id
            || params.collection != record.collection
            || params.field_name != record.primary_field())
    {
        return Err(invalid(format!(
            "vector parameters of index '{}' name another index",
            blob.name
        )));
    }
    Ok(BackedUpIndex {
        source_database: blob.database_id,
        record,
        owner: blob.owner,
        vector_params,
    })
}

/// Every collection the envelope carries a catalog row of, by source
/// database id and name.
fn carried_collections(env: &Envelope) -> Result<BTreeSet<(u64, String)>, Error> {
    let mut carried = BTreeSet::new();
    for section in env
        .sections
        .iter()
        .filter(|s| s.origin_node_id == SECTION_ORIGIN_CATALOG_ROWS)
    {
        let blobs = zerompk::from_msgpack::<Vec<StoredCollectionBlob>>(&section.body)
            .map_err(|_| invalid("catalog-rows section is not decodable".into()))?;
        carried.extend(blobs.into_iter().map(|blob| (blob.database_id, blob.name)));
    }
    Ok(carried)
}

fn encode<T: zerompk::ToMessagePack>(what: &str, value: &T) -> Result<Vec<u8>, Error> {
    zerompk::to_msgpack_vec(value).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore ({what}): encode: {e}"),
    })
}

fn invalid(detail: String) -> Error {
    Error::Internal {
        detail: format!("invalid backup format: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_record(name: &str, analyzer: &str) -> StoredIndexRecord {
        StoredIndexRecord {
            database_id: 1,
            tenant_id: 1,
            name: name.into(),
            kind: IndexKind::FullText,
            collection: "docs".into(),
            fields: vec!["body".to_string()],
            is_active: true,
            state: IndexRecordState::Ready,
            analyzer: Some(analyzer.into()),
            fuzzy_default: None,
        }
    }

    fn check(catalog: &SystemCatalog, records: &[StoredIndexRecord]) -> Result<(), Error> {
        check_destination_analysis(catalog, 1, DatabaseId::new(1), "db", "docs", records)
    }

    /// The backup's records replace the destination records of their names.
    /// The check reads the collection as the restore leaves it, so records
    /// that replace every disagreeing record pass, and a destination record
    /// the restore leaves in place still counts.
    #[test]
    fn the_destination_is_checked_as_the_restore_leaves_it() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        catalog
            .put_index_record(&text_record("a", "german"))
            .unwrap();
        catalog
            .put_index_record(&text_record("b", "german"))
            .unwrap();

        let english = [text_record("a", "english"), text_record("b", "english")];
        check(&catalog, &english).unwrap();
        check(&catalog, &[text_record("a", "german")]).unwrap();

        let err = check(&catalog, &[text_record("a", "english")]).unwrap_err();
        assert!(err.to_string().contains("german"), "b stays german: {err}");

        catalog
            .put_index_record(&text_record("c", "german"))
            .unwrap();
        assert!(
            check(&catalog, &english).is_err(),
            "c stays german and the backup does not replace it"
        );
    }
}
