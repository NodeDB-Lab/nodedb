// SPDX-License-Identifier: BUSL-1.1

//! A source database's catalog, as the entries that would create it.
//!
//! The mirror installs these inside its first `MirrorCut`, frozen as the source
//! stamped them. The filter is the mirror gate's own scope, so the image holds
//! exactly the objects a mirror follows:
//!
//! - Cluster objects stay out: a mirror's readers are its own principals.
//! - Reader state stays out: consumer groups and column statistics.
//! - Tombstones stay out: their LSNs name the source's WAL.

use nodedb_types::DatabaseId;

use crate::Error;
use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::security::catalog::SystemCatalog;

/// The entries that name one database, in the order a mirror installs them.
struct Image {
    database: DatabaseId,
    entries: Vec<CatalogEntry>,
}

impl Image {
    fn push(&mut self, entry: CatalogEntry) {
        if entry.mirror_scope().contains(&Some(self.database)) {
            self.entries.push(entry);
        }
    }

    fn push_all<T>(&mut self, rows: Vec<T>, entry: impl Fn(Box<T>) -> CatalogEntry) {
        for row in rows {
            self.push(entry(Box::new(row)));
        }
    }

    fn encode(&self) -> Result<Vec<Vec<u8>>, Error> {
        self.entries.iter().map(catalog_entry::encode).collect()
    }
}

/// `database`'s catalog, one encoded entry per object. Types and sequences come
/// before the collections that name them, and a collection before what hangs
/// off it.
pub(super) fn catalog_image(
    catalog: &SystemCatalog,
    database: DatabaseId,
) -> Result<Vec<Vec<u8>>, Error> {
    let id = database.as_u64();
    let mut image = Image {
        database,
        entries: Vec::new(),
    };
    image.push_all(
        catalog.load_all_custom_types()?,
        CatalogEntry::PutCustomType,
    );
    let sequences = catalog.load_sequences_in_database(id)?;
    for sequence in &sequences {
        if let Some(state) = catalog.get_sequence_state(id, sequence.tenant_id, &sequence.name)? {
            image.push(CatalogEntry::PutSequenceState(Box::new(state)));
        }
    }
    image.push_all(sequences, CatalogEntry::PutSequence);
    image.push_all(
        catalog.load_all_collections(database)?,
        CatalogEntry::PutCollection,
    );
    image.push_all(catalog.load_all_arrays()?, CatalogEntry::PutArray);
    image.push_all(
        catalog.list_index_records_in_database(id)?,
        CatalogEntry::PutIndexRecord,
    );
    image.push_all(catalog.load_all_owners()?, CatalogEntry::PutOwner);
    image.push_all(catalog.load_all_functions()?, CatalogEntry::PutFunction);
    image.push_all(catalog.load_all_procedures()?, CatalogEntry::PutProcedure);
    image.push_all(catalog.load_all_triggers()?, CatalogEntry::PutTrigger);
    image.push_all(catalog.load_all_schedules()?, CatalogEntry::PutSchedule);
    image.push_all(
        catalog.load_all_synonym_groups()?,
        CatalogEntry::PutSynonymGroup,
    );
    image.push_all(
        catalog.load_all_change_streams()?,
        CatalogEntry::PutChangeStream,
    );
    image.push_all(
        catalog.load_all_ep_topics()?,
        CatalogEntry::CreateTopicIfAbsent,
    );
    image.push_all(
        catalog.list_materialized_views_in_database(id)?,
        CatalogEntry::PutMaterializedView,
    );
    image.push_all(
        catalog.load_all_streaming_mvs()?,
        CatalogEntry::PutStreamingMaterializedView,
    );
    image.push_all(
        catalog.list_continuous_aggregates_in_database(id)?,
        CatalogEntry::PutContinuousAggregate,
    );
    image.push_all(
        catalog.load_all_retention_policies()?,
        CatalogEntry::PutRetentionPolicy,
    );
    image.push_all(catalog.load_all_alert_rules()?, CatalogEntry::PutAlertRule);
    image.push_all(catalog.load_all_checkpoints()?, CatalogEntry::PutCheckpoint);
    image.push_all(
        catalog.list_vector_models_in_database(id)?,
        CatalogEntry::PutVectorModel,
    );
    image.push_all(
        catalog.list_vector_index_params_in_database(id)?,
        CatalogEntry::PutVectorIndexParams,
    );
    image.push_all(catalog.load_all_rls_policies()?, CatalogEntry::PutRlsPolicy);
    image.push_all(
        catalog.load_all_redaction_policies()?,
        CatalogEntry::PutRedactionPolicy,
    );
    image.encode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredOwner;

    const DB: u64 = 1700;

    fn owner(database_id: u64, name: &str) -> StoredOwner {
        StoredOwner {
            database_id,
            object_type: "collection".to_string(),
            object_name: name.to_string(),
            tenant_id: 1,
            owner_username: "admin".to_string(),
        }
    }

    /// Another database's objects never leave.
    #[test]
    fn the_image_holds_only_its_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        catalog.put_owner(&owner(DB, "ours")).expect("put");
        catalog.put_owner(&owner(DB + 1, "theirs")).expect("put");

        let payloads = catalog_image(&catalog, DatabaseId::new(DB)).expect("image");
        let names: Vec<String> = payloads
            .iter()
            .map(
                |payload| match catalog_entry::decode(payload).expect("decode") {
                    CatalogEntry::PutOwner(owner) => owner.object_name,
                    other => panic!("an unexpected entry left: {other:?}"),
                },
            )
            .collect();
        assert_eq!(names, vec!["ours".to_string()]);
    }

    /// A cluster object is out of scope, so a push drops it.
    #[test]
    fn a_cluster_object_stays_out() {
        let mut image = Image {
            database: DatabaseId::new(DB),
            entries: Vec::new(),
        };
        image.push(CatalogEntry::DeleteRole {
            name: "reader".to_string(),
        });
        assert!(image.entries.is_empty());
    }
}
