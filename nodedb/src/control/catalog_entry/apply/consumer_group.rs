// SPDX-License-Identifier: BUSL-1.1

//! Apply consumer-group catalog entries to `SystemCatalog` redb.
//!
//! Writes only. The leader resolves the canonical stream name and reports a
//! duplicate before proposing, so apply carries no policy of its own.

use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::types::DatabaseId;

/// Apply a `PutConsumerGroupIfAbsent` entry. An existing row is kept, so a
/// re-delivered entry cannot overwrite a group's owner or creation time.
pub fn put_if_absent(def: &ConsumerGroupDef, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .put_consumer_group_if_absent(def)
        .map_err(|e| {
            catalog_err(
                &format!(
                    "put_consumer_group '{}' on stream '{}'",
                    def.name, def.stream_name
                ),
                e,
            )
        })
        .map(|_| ())
}

/// Apply a `DeleteConsumerGroup` entry. A missing row is not an error.
pub fn delete(
    database_id: u64,
    tenant_id: u64,
    stream_name: &str,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_consumer_group(DatabaseId::new(database_id), tenant_id, stream_name, name)
        .map_err(|e| {
            catalog_err(
                &format!("delete_consumer_group '{name}' on stream '{stream_name}'"),
                e,
            )
        })
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::catalog_entry::{apply, decode, encode};

    const DB: u64 = 0;
    const TENANT: u64 = 7;
    const GROUP: &str = "readers";
    const STREAM: &str = "topic:orders";

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn sample() -> ConsumerGroupDef {
        ConsumerGroupDef {
            database_id: DatabaseId::new(DB),
            tenant_id: TENANT,
            name: GROUP.to_string(),
            stream_name: STREAM.to_string(),
            owner: "admin".to_string(),
            created_at: 1_000,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    fn delete_entry() -> CatalogEntry {
        CatalogEntry::DeleteConsumerGroup {
            database_id: DB,
            tenant_id: TENANT,
            stream_name: STREAM.to_string(),
            name: GROUP.to_string(),
            target_hlc: nodedb_types::Hlc::new(7, 0),
        }
    }

    #[test]
    fn put_consumer_group_if_absent_roundtrips_through_codec() {
        let entry = CatalogEntry::PutConsumerGroupIfAbsent(Box::new(sample()));
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::PutConsumerGroupIfAbsent(def) => {
                assert_eq!(def.database_id, DatabaseId::new(DB));
                assert_eq!(def.tenant_id, TENANT);
                assert_eq!(def.name, GROUP);
                assert_eq!(def.stream_name, STREAM);
                assert_eq!(def.owner, "admin");
                assert_eq!(def.created_at, 1_000);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_consumer_group_roundtrips_through_codec() {
        let decoded = decode(&encode(&delete_entry()).unwrap()).unwrap();
        match decoded {
            CatalogEntry::DeleteConsumerGroup {
                database_id,
                tenant_id,
                stream_name,
                name,
                target_hlc,
            } => {
                assert_eq!(database_id, DB);
                assert_eq!(tenant_id, TENANT);
                assert_eq!(stream_name, STREAM);
                assert_eq!(name, GROUP);
                assert_eq!(target_hlc, nodedb_types::Hlc::new(7, 0));
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn apply_writes_and_removes_the_consumer_group_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(
            &CatalogEntry::PutConsumerGroupIfAbsent(Box::new(sample())),
            &catalog,
        )
        .unwrap();
        let stored = catalog.load_all_consumer_groups().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].name, GROUP);

        apply::apply_to(&delete_entry(), &catalog).unwrap();
        assert!(catalog.load_all_consumer_groups().unwrap().is_empty());
    }

    #[test]
    fn apply_put_keeps_the_existing_consumer_group_definition() {
        let (_dir, catalog) = open_catalog();
        put_if_absent(&sample(), &catalog).expect("apply initial put");

        let reowned = ConsumerGroupDef {
            owner: "someone-else".to_string(),
            created_at: 2_000,
            ..sample()
        };
        put_if_absent(&reowned, &catalog).expect("apply re-put");

        let stored = catalog.load_all_consumer_groups().unwrap();
        assert_eq!(stored.len(), 1, "create-only writes one row: {stored:?}");
        assert_eq!(
            stored[0].owner, "admin",
            "a re-delivered create must not overwrite the original definition"
        );
        assert_eq!(stored[0].created_at, 1_000);
    }

    #[test]
    fn deleting_an_absent_consumer_group_is_a_noop() {
        let (_dir, catalog) = open_catalog();
        delete(DB, TENANT, STREAM, "never-defined", &catalog).expect("delete absent group");
    }
}
