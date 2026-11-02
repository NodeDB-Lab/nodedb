// SPDX-License-Identifier: BUSL-1.1

//! Streaming MV metadata operations for the system catalog.

use redb::{ReadableDatabase, ReadableTable};

use crate::event::streaming_mv::StreamingMvDef;
use crate::types::DatabaseId;

use super::types::{STREAMING_MVS, SystemCatalog, catalog_err};

impl SystemCatalog {
    pub fn put_streaming_mv(&self, def: &StreamingMvDef) -> crate::Result<()> {
        let key = mv_key(def.database_id, def.tenant_id, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize streaming_mv", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(STREAMING_MVS)
                .map_err(|e| catalog_err("open streaming_mvs", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert streaming_mv", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn delete_streaming_mv(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let key = mv_key(database_id, tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(STREAMING_MVS)
                .map_err(|e| catalog_err("open streaming_mvs", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete streaming_mv", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Every streaming MV of one database, across every tenant.
    ///
    /// The key leads with `database_id`, but this scans every row and filters
    /// in memory so one code path serves every load.
    pub fn load_streaming_mvs_for_database(
        &self,
        database_id: DatabaseId,
    ) -> crate::Result<Vec<StreamingMvDef>> {
        Ok(self
            .load_all_streaming_mvs()?
            .into_iter()
            .filter(|def| def.database_id == database_id)
            .collect())
    }

    pub fn load_all_streaming_mvs(&self) -> crate::Result<Vec<StreamingMvDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(STREAMING_MVS)
            .map_err(|e| catalog_err("open streaming_mvs", e))?;
        let mut mvs = Vec::new();
        let range = table
            .range(..)
            .map_err(|e| catalog_err("range streaming_mvs", e))?;
        for entry in range {
            let (_, value) = entry.map_err(|e| catalog_err("read streaming_mv", e))?;
            let def = zerompk::from_msgpack::<StreamingMvDef>(value.value())
                .map_err(|e| catalog_err("deser streaming_mv", e))?;
            mvs.push(def);
        }
        Ok(mvs)
    }
}

fn mv_key(database_id: DatabaseId, tenant_id: u64, name: &str) -> String {
    format!("{}:{tenant_id}:{name}", database_id.as_u64())
}
