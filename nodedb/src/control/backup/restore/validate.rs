// SPDX-License-Identifier: BUSL-1.1

//! Whole-envelope validation for RESTORE TENANT.
//!
//! Every refusal the restore can raise from the envelope's own content runs
//! here, before the restore proposes anything. A refused envelope leaves the
//! destination catalog unchanged.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_types::backup_envelope::{
    DatabaseBlob, Envelope, SECTION_ORIGIN_CATALOG_ROWS, SECTION_ORIGIN_SOURCE_TOMBSTONES,
    SourceTombstoneEntry, StoredCollectionBlob,
};

use crate::Error;
use crate::control::backup::metadata::refuse_unmaterialized_clone;
use crate::control::backup::verify::expect::{Expectation, expect_envelope};
use crate::control::security::catalog::{DatabaseDescriptor, StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use crate::types::{TenantDataSnapshot, TenantId};
use nodedb_types::QuotaRecord;

use super::databases::{decode_databases, require_writable};
use super::sections::merge_sections;

/// An envelope every restore step accepts.
pub(super) struct ValidatedEnvelope {
    pub databases: Vec<DatabaseBlob>,
    /// Every data section, merged per source database.
    pub merged: BTreeMap<u64, TenantDataSnapshot>,
    /// What the destination must hold once every row is re-issued.
    pub expectation: Expectation,
}

/// Check the whole envelope against the destination. Proposes nothing.
pub(super) fn validate_envelope(
    state: &SharedState,
    tenant_id: u64,
    env: &Envelope,
) -> Result<ValidatedEnvelope, Error> {
    let catalog = state.credentials.catalog();
    let databases = decode_databases(env)?;
    validate_databases(state, catalog, TenantId::new(tenant_id), &databases)?;

    let listed: BTreeSet<u64> = databases.iter().map(|b| b.database_id).collect();
    validate_metadata_sections(env, &listed)?;

    let merged = merge_sections(&env.sections)?;
    if let Some(source) = merged.keys().find(|source| !listed.contains(source)) {
        return Err(unlisted("a data section", *source));
    }
    super::array_reissue::validate_array_rows(
        catalog,
        TenantId::new(tenant_id),
        env,
        &databases,
        &merged,
    )?;
    // The rows must match the counts and digests the backup recorded.
    let expectation = expect_envelope(state, tenant_id, env, &merged)?;
    Ok(ValidatedEnvelope {
        databases,
        merged,
        expectation,
    })
}

/// Every database the restore maps or creates, and every quota it installs.
fn validate_databases(
    state: &SharedState,
    catalog: &SystemCatalog,
    tenant: TenantId,
    blobs: &[DatabaseBlob],
) -> Result<(), Error> {
    let mut new_quotas = Vec::new();
    for blob in blobs {
        match catalog.get_database_id_by_name(&blob.name)? {
            Some(id) => {
                require_writable(state, id, &blob.name)?;
                if let Some(record) = &blob.tenant_quota
                    && catalog.get_tenant_quota(id, tenant)?.is_none()
                {
                    catalog.check_tenant_quota(id, tenant, record)?;
                }
            }
            None => {
                zerompk::from_msgpack::<DatabaseDescriptor>(&blob.descriptor).map_err(|_| {
                    Error::Internal {
                        detail: format!(
                            "invalid backup format: descriptor of database '{}' is not decodable",
                            blob.name
                        ),
                    }
                })?;
                if let Some(record) = &blob.tenant_quota {
                    // Blobs of the same name land in the same new database.
                    let others: Vec<&QuotaRecord> = blobs
                        .iter()
                        .filter(|other| {
                            other.name == blob.name && other.database_id != blob.database_id
                        })
                        .filter_map(|other| other.tenant_quota.as_ref())
                        .collect();
                    SystemCatalog::check_tenant_quota_in_new_database(
                        blob.database_quota.as_ref(),
                        &others,
                        record,
                    )?;
                }
                if let Some(record) = &blob.database_quota {
                    new_quotas.push(record);
                }
            }
        }
    }
    // The created databases' quotas count against the ceiling together.
    catalog.check_new_database_quotas(&new_quotas, &state.quota_ceiling_snapshot())?;
    Ok(())
}

/// Every catalog row and source tombstone decodes, names a listed database,
/// and is restorable.
fn validate_metadata_sections(env: &Envelope, listed: &BTreeSet<u64>) -> Result<(), Error> {
    for section in &env.sections {
        match section.origin_node_id {
            SECTION_ORIGIN_CATALOG_ROWS => {
                let blobs = zerompk::from_msgpack::<Vec<StoredCollectionBlob>>(&section.body)
                    .map_err(|_| Error::Internal {
                        detail: "invalid backup format: catalog-rows section is not decodable"
                            .into(),
                    })?;
                for blob in blobs {
                    if !listed.contains(&blob.database_id) {
                        return Err(unlisted("a catalog row", blob.database_id));
                    }
                    let coll =
                        zerompk::from_msgpack::<StoredCollection>(&blob.bytes).map_err(|_| {
                            Error::Internal {
                                detail: format!(
                                    "invalid backup format: catalog row of '{}' is not decodable",
                                    blob.name
                                ),
                            }
                        })?;
                    // A restored row mints its collection name in the destination.
                    crate::control::relation_name::check_relation_name("collection", &coll.name)?;
                    refuse_unmaterialized_clone(&coll)?;
                }
            }
            SECTION_ORIGIN_SOURCE_TOMBSTONES => {
                let tombs = zerompk::from_msgpack::<Vec<SourceTombstoneEntry>>(&section.body)
                    .map_err(|_| Error::Internal {
                        detail: "invalid backup format: source-tombstones section is not \
                                 decodable"
                            .into(),
                    })?;
                if let Some(t) = tombs.iter().find(|t| !listed.contains(&t.database_id)) {
                    return Err(unlisted("a source tombstone", t.database_id));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn unlisted(what: &str, source: u64) -> Error {
    Error::Internal {
        detail: format!(
            "invalid backup format: {what} names database {source}, which the backup's \
             database section does not list"
        ),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::backup_envelope::{
        EnvelopeMeta, SECTION_ORIGIN_DATABASES, Section, StoredCollectionBlob,
    };
    use nodedb_types::{CloneOrigin, DatabaseId, Lsn};

    use super::*;

    fn envelope(sections: Vec<Section>) -> Envelope {
        Envelope {
            meta: EnvelopeMeta {
                tenant_id: 1,
                source_vshard_count: 1,
                hash_seed: 0,
                snapshot_watermark: 0,
            },
            sections,
        }
    }

    fn databases_section(names: &[(u64, &str)]) -> Section {
        let blobs: Vec<DatabaseBlob> = names
            .iter()
            .map(|(id, name)| DatabaseBlob {
                database_id: *id,
                name: (*name).to_string(),
                descriptor: Vec::new(),
                database_quota: None,
                tenant_quota: None,
            })
            .collect();
        Section {
            origin_node_id: SECTION_ORIGIN_DATABASES,
            body: zerompk::to_msgpack_vec(&blobs).unwrap(),
        }
    }

    fn rows_section(rows: &[StoredCollection]) -> Section {
        let blobs: Vec<StoredCollectionBlob> = rows
            .iter()
            .map(|coll| StoredCollectionBlob {
                database_id: DatabaseId::DEFAULT.as_u64(),
                name: coll.name.clone(),
                bytes: zerompk::to_msgpack_vec(coll).unwrap(),
            })
            .collect();
        Section {
            origin_node_id: SECTION_ORIGIN_CATALOG_ROWS,
            body: zerompk::to_msgpack_vec(&blobs).unwrap(),
        }
    }

    /// A clone row anywhere in the envelope refuses it as a whole, before any
    /// earlier row is proposed: validation reads every section first.
    #[test]
    fn a_late_clone_row_refuses_the_whole_envelope() {
        let plain = StoredCollection::new(1, "plain", "admin");
        let mut clone = StoredCollection::new(1, "cloned", "admin");
        clone.cloned_from = Some(CloneOrigin {
            source_database: DatabaseId::new(1030),
            source_collection: "cloned".into(),
            as_of_lsn: Lsn::new(1),
            clone_created_at: Lsn::new(2),
            kv_surrogate_ceiling: None,
        });
        let env = envelope(vec![
            databases_section(&[(0, "default")]),
            rows_section(std::slice::from_ref(&plain)),
            rows_section(&[clone]),
        ]);
        let listed = BTreeSet::from([0]);
        assert!(matches!(
            validate_metadata_sections(&env, &listed),
            Err(Error::BadRequest { .. })
        ));

        let env = envelope(vec![
            databases_section(&[(0, "default")]),
            rows_section(&[plain]),
        ]);
        validate_metadata_sections(&env, &listed).expect("plain rows validate");
    }

    /// A row naming a database the envelope does not list refuses it.
    #[test]
    fn a_row_in_an_unlisted_database_refuses_the_envelope() {
        let env = envelope(vec![rows_section(&[StoredCollection::new(
            1, "orphan", "admin",
        )])]);
        let err = validate_metadata_sections(&env, &BTreeSet::new())
            .expect_err("an unlisted database refuses");
        assert!(err.to_string().contains("does not list"), "{err}");
    }
}
