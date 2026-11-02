// SPDX-License-Identifier: BUSL-1.1

//! Consumer group metadata operations for the system catalog.

use redb::{ReadableDatabase, ReadableTable};

use super::types::{CONSUMER_GROUPS, SystemCatalog, catalog_err};
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::types::DatabaseId;

impl SystemCatalog {
    /// Store a consumer group definition under its unambiguous key.
    pub fn put_consumer_group(&self, def: &ConsumerGroupDef) -> crate::Result<()> {
        let key = group_key(def.database_id, def.tenant_id, &def.stream_name, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize consumer_group", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert consumer_group", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Insert a consumer group only when its durable identity is absent.
    ///
    /// The existence check and insertion share one redb write transaction, so
    /// concurrent CREATE requests cannot overwrite each other's definition.
    /// Returns `true` when the definition was inserted.
    pub fn put_consumer_group_if_absent(&self, def: &ConsumerGroupDef) -> crate::Result<bool> {
        let key = group_key(def.database_id, def.tenant_id, &def.stream_name, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize consumer_group", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let inserted = {
            let mut table = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            if table
                .get(key.as_str())
                .map_err(|e| catalog_err("get consumer_group", e))?
                .is_some()
            {
                false
            } else {
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| catalog_err("insert consumer_group", e))?;
                true
            }
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(inserted)
    }

    /// Read one committed consumer group by its durable identity.
    pub fn get_consumer_group(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
    ) -> crate::Result<Option<ConsumerGroupDef>> {
        let key = group_key(database_id, tenant_id, stream, group);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CONSUMER_GROUPS)
            .map_err(|e| catalog_err("open consumer_groups", e))?;
        let Some(value) = table
            .get(key.as_str())
            .map_err(|e| catalog_err("get consumer_group", e))?
        else {
            return Ok(None);
        };
        decode_consumer_group(value.value()).map(Some)
    }

    /// Delete a consumer group.
    pub fn delete_consumer_group(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
    ) -> crate::Result<bool> {
        let key = group_key(database_id, tenant_id, stream, group);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete consumer_group", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load every durable consumer group definition.
    pub fn load_all_consumer_groups(&self) -> crate::Result<Vec<ConsumerGroupDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CONSUMER_GROUPS)
            .map_err(|e| catalog_err("open consumer_groups", e))?;

        let mut groups = Vec::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range consumer_groups", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read consumer_group", e))?;
            groups.push(decode_consumer_group(value.value())?);
        }
        Ok(groups)
    }
}

fn group_key(database_id: DatabaseId, tenant_id: u64, stream: &str, group: &str) -> String {
    format!(
        "{}:{tenant_id}:{}:{stream}:{}:{group}",
        database_id.as_u64(),
        stream.len(),
        group.len()
    )
}

pub(super) fn decode_consumer_group(bytes: &[u8]) -> crate::Result<ConsumerGroupDef> {
    zerompk::from_msgpack(bytes).map_err(|e| catalog_err("deser consumer_group", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn group(database_id: DatabaseId, stream: &str, name: &str) -> ConsumerGroupDef {
        ConsumerGroupDef {
            tenant_id: 1,
            name: name.into(),
            stream_name: stream.into(),
            owner: "admin".into(),
            created_at: 0,
            database_id,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn groups_are_isolated_by_database() {
        let catalog = make_catalog();
        catalog
            .put_consumer_group(&group(DatabaseId::new(7), "orders", "analytics"))
            .unwrap();
        catalog
            .put_consumer_group(&group(DatabaseId::new(8), "orders", "analytics"))
            .unwrap();
        assert_eq!(catalog.load_all_consumer_groups().unwrap().len(), 2);
    }

    #[test]
    fn concurrent_put_if_absent_has_one_winner() {
        use std::sync::{Arc, Barrier};

        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(SystemCatalog::open(&dir.path().join("system.redb")).unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let first_catalog = Arc::clone(&catalog);
        let first_barrier = Arc::clone(&barrier);
        let first = std::thread::spawn(move || {
            first_barrier.wait();
            first_catalog.put_consumer_group_if_absent(&group(
                DatabaseId::new(7),
                "orders",
                "analytics",
            ))
        });
        let second_catalog = Arc::clone(&catalog);
        let second_barrier = Arc::clone(&barrier);
        let second = std::thread::spawn(move || {
            second_barrier.wait();
            second_catalog.put_consumer_group_if_absent(&group(
                DatabaseId::new(7),
                "orders",
                "analytics",
            ))
        });
        barrier.wait();
        assert_ne!(
            first.join().unwrap().unwrap(),
            second.join().unwrap().unwrap()
        );
        assert_eq!(catalog.load_all_consumer_groups().unwrap().len(), 1);
    }

    #[test]
    fn put_if_absent_preserves_first_definition() {
        let catalog = make_catalog();
        let def = group(DatabaseId::new(7), "orders", "analytics");
        assert!(catalog.put_consumer_group_if_absent(&def).unwrap());
        assert!(!catalog.put_consumer_group_if_absent(&def).unwrap());
        let groups = catalog.load_all_consumer_groups().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, def.name);
    }
}
