// SPDX-License-Identifier: BUSL-1.1

//! Change stream metadata operations for the system catalog.

use super::types::{CHANGE_STREAMS, SystemCatalog, catalog_err};
use crate::event::cdc::stream_def::ChangeStreamDef;
use crate::types::DatabaseId;
use redb::{ReadableDatabase, ReadableTable};

impl SystemCatalog {
    /// Store a change stream definition under its length-prefixed key.
    pub fn put_change_stream(&self, def: &ChangeStreamDef) -> crate::Result<()> {
        let key = stream_key(def.database_id, def.tenant_id, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize change_stream", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(CHANGE_STREAMS)
                .map_err(|e| catalog_err("open change_streams", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert change_stream", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Get a change stream by tenant_id + name.
    pub fn get_change_stream(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<ChangeStreamDef>> {
        let key = stream_key(database_id, tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CHANGE_STREAMS)
            .map_err(|e| catalog_err("open change_streams", e))?;
        table
            .get(key.as_str())
            .map_err(|e| catalog_err("get change_stream", e))?
            .map(|value| zerompk::from_msgpack(value.value()))
            .transpose()
            .map_err(|e| catalog_err("deser change_stream", e))
    }

    /// Delete a change stream. Returns true if it existed.
    pub fn delete_change_stream(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let key = stream_key(database_id, tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed = {
            let mut table = write_txn
                .open_table(CHANGE_STREAMS)
                .map_err(|e| catalog_err("open change_streams", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete change_stream", e))?
                .is_some()
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all change streams (all tenants).
    pub fn load_all_change_streams(&self) -> crate::Result<Vec<ChangeStreamDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CHANGE_STREAMS)
            .map_err(|e| catalog_err("open change_streams", e))?;

        let mut streams = Vec::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range change_streams", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read change_stream", e))?;
            let def = zerompk::from_msgpack::<ChangeStreamDef>(value.value())
                .map_err(|e| catalog_err("deser change_stream", e))?;
            streams.push(def);
        }
        Ok(streams)
    }
}

fn stream_key(database_id: DatabaseId, tenant_id: u64, name: &str) -> String {
    format!(
        "{:016x}/{:016x}/{:08x}/{}",
        database_id.as_u64(),
        tenant_id,
        name.len(),
        hex::encode(name)
    )
}

#[cfg(test)]
mod tests {
    use crate::control::security::catalog::types::SystemCatalog;
    use crate::event::cdc::stream_def::*;
    use crate::types::DatabaseId;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn sample_stream(name: &str, collection: &str) -> ChangeStreamDef {
        ChangeStreamDef {
            database_id: DatabaseId::new(7),
            tenant_id: 1,
            name: name.into(),
            collection: collection.into(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig::default(),
            compaction: CompactionConfig::default(),
            webhook: crate::event::webhook::WebhookConfig::default(),
            late_data: LateDataPolicy::default(),
            kafka: crate::event::kafka::KafkaDeliveryConfig::default(),
            owner: "admin".into(),
            created_at: 1000,
            subscriber_roles: Vec::new(),
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn put_get_roundtrip() {
        let cat = make_catalog();
        let def = sample_stream("orders_stream", "orders");
        cat.put_change_stream(&def).unwrap();

        let loaded = cat
            .get_change_stream(DatabaseId::new(7), 1, "orders_stream")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.name, "orders_stream");
        assert_eq!(loaded.collection, "orders");
    }

    #[test]
    fn delete_stream() {
        let cat = make_catalog();
        cat.put_change_stream(&sample_stream("s1", "c1")).unwrap();
        assert!(
            cat.delete_change_stream(DatabaseId::new(7), 1, "s1")
                .unwrap()
        );
        assert!(
            !cat.delete_change_stream(DatabaseId::new(7), 1, "s1")
                .unwrap()
        );
        assert!(
            cat.get_change_stream(DatabaseId::new(7), 1, "s1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn load_all() {
        let cat = make_catalog();
        cat.put_change_stream(&sample_stream("s1", "orders"))
            .unwrap();
        cat.put_change_stream(&sample_stream("s2", "users"))
            .unwrap();

        let all = cat.load_all_change_streams().unwrap();
        assert_eq!(all.len(), 2);
    }
}
