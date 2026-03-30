//! Change stream metadata operations for the system catalog.

use super::types::{CHANGE_STREAMS, SystemCatalog, catalog_err};
use crate::event::cdc::stream_def::ChangeStreamDef;

impl SystemCatalog {
    /// Store a change stream definition.
    ///
    /// Key format: `"{tenant_id}:{stream_name}"`.
    pub fn put_change_stream(&self, def: &ChangeStreamDef) -> crate::Result<()> {
        let key = stream_key(def.tenant_id, &def.name);
        let bytes =
            rmp_serde::to_vec(def).map_err(|e| catalog_err("serialize change_stream", e))?;
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
        tenant_id: u32,
        name: &str,
    ) -> crate::Result<Option<ChangeStreamDef>> {
        let key = stream_key(tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CHANGE_STREAMS)
            .map_err(|e| catalog_err("open change_streams", e))?;
        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let def: ChangeStreamDef = rmp_serde::from_slice(value.value())
                    .map_err(|e| catalog_err("deser change_stream", e))?;
                Ok(Some(def))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get change_stream", e)),
        }
    }

    /// Delete a change stream. Returns true if it existed.
    pub fn delete_change_stream(&self, tenant_id: u32, name: &str) -> crate::Result<bool> {
        let key = stream_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(CHANGE_STREAMS)
                .map_err(|e| catalog_err("open change_streams", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete change_stream", e))?
                .is_some();
        }
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
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range change_streams", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = rmp_serde::from_slice::<ChangeStreamDef>(value.value()) {
                streams.push(def);
            }
        }
        Ok(streams)
    }
}

fn stream_key(tenant_id: u32, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use crate::control::security::catalog::types::SystemCatalog;
    use crate::event::cdc::stream_def::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn sample_stream(name: &str, collection: &str) -> ChangeStreamDef {
        ChangeStreamDef {
            tenant_id: 1,
            name: name.into(),
            collection: collection.into(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig::default(),
            owner: "admin".into(),
            created_at: 1000,
        }
    }

    #[test]
    fn put_get_roundtrip() {
        let cat = make_catalog();
        let def = sample_stream("orders_stream", "orders");
        cat.put_change_stream(&def).unwrap();

        let loaded = cat.get_change_stream(1, "orders_stream").unwrap().unwrap();
        assert_eq!(loaded.name, "orders_stream");
        assert_eq!(loaded.collection, "orders");
    }

    #[test]
    fn delete_stream() {
        let cat = make_catalog();
        cat.put_change_stream(&sample_stream("s1", "c1")).unwrap();
        assert!(cat.delete_change_stream(1, "s1").unwrap());
        assert!(!cat.delete_change_stream(1, "s1").unwrap());
        assert!(cat.get_change_stream(1, "s1").unwrap().is_none());
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
