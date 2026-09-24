// SPDX-License-Identifier: BUSL-1.1

//! Catalog operations for durable vector-index parameters.
//!
//! Stores `CREATE VECTOR INDEX` build parameters in
//! `_system.vector_index_params`. Key format:
//! `"{database_id}:{tenant_id}:{collection}:{field_name}"`.

use nodedb_types::StoredVectorIndexParams;
use redb::{ReadableDatabase, ReadableTable};

use super::types::{SystemCatalog, VECTOR_INDEX_PARAMS, catalog_err};

impl SystemCatalog {
    /// Store vector index parameters for a collection/field.
    pub fn put_vector_index_params(&self, entry: &StoredVectorIndexParams) -> crate::Result<()> {
        let key = vector_index_params_key(
            entry.database_id,
            entry.tenant_id,
            &entry.collection,
            &entry.field_name,
        );
        let bytes = zerompk::to_msgpack_vec(entry)
            .map_err(|e| catalog_err("serialize vector index params", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(VECTOR_INDEX_PARAMS)
                .map_err(|e| catalog_err("open vector_index_params", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert vector index params", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Load vector index parameters for a specific collection/field, with
    /// this connection's uncommitted transactional DDL merged in.
    pub fn get_vector_index_params(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field_name: &str,
    ) -> crate::Result<Option<StoredVectorIndexParams>> {
        let committed =
            self.get_committed_vector_index_params(database_id, tenant_id, collection, field_name)?;
        Ok(
            crate::control::catalog_overlay::resolve_vector_index_params(
                database_id,
                tenant_id,
                collection,
                field_name,
                committed,
            ),
        )
    }

    /// Committed-only read, bypassing the transaction DDL overlay.
    pub fn get_committed_vector_index_params(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field_name: &str,
    ) -> crate::Result<Option<StoredVectorIndexParams>> {
        let key = vector_index_params_key(database_id, tenant_id, collection, field_name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(VECTOR_INDEX_PARAMS)
            .map_err(|e| catalog_err("open vector_index_params", e))?;

        match table
            .get(key.as_str())
            .map_err(|e| catalog_err("get vector index params", e))?
        {
            Some(value) => zerompk::from_msgpack(value.value())
                .map(Some)
                .map_err(|e| catalog_err("deser vector index params", e)),
            None => Ok(None),
        }
    }

    /// Remove the parameters of one vector index. Returns whether an entry
    /// was present.
    ///
    /// Without this the build parameters outlive `DROP INDEX`: the name is
    /// freed but the collection's vector slot stays occupied, so a re-CREATE
    /// is refused as a duplicate and the next boot re-seeds an index the user
    /// dropped.
    pub fn delete_vector_index_params(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field_name: &str,
    ) -> crate::Result<bool> {
        let key = vector_index_params_key(database_id, tenant_id, collection, field_name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(VECTOR_INDEX_PARAMS)
                .map_err(|e| catalog_err("open vector_index_params", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove vector index params", e))?
                .is_some()
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(removed)
    }

    /// List every vector index parameter row of one database, across tenants.
    ///
    /// The scan is bounded to the database's key range, so a node holding many
    /// databases reads only the rows it returns.
    pub fn list_vector_index_params_in_database(
        &self,
        database_id: u64,
    ) -> crate::Result<Vec<StoredVectorIndexParams>> {
        let lower = format!("{database_id}:");
        let upper = format!("{database_id};");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(VECTOR_INDEX_PARAMS)
            .map_err(|e| catalog_err("open vector_index_params", e))?;

        let mut entries = Vec::new();
        for item in table
            .range(lower.as_str()..upper.as_str())
            .map_err(|e| catalog_err("range vector index params", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read vector index params", e))?;
            let entry: StoredVectorIndexParams = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser vector index params", e))?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// List all vector index parameter entries across all tenants.
    pub fn list_all_vector_index_params(&self) -> crate::Result<Vec<StoredVectorIndexParams>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        list_all_vector_index_params_in(&read_txn)
    }
}

/// Body of [`SystemCatalog::list_all_vector_index_params`], over an already-open
/// read transaction so the read-only catalog handle can reuse it verbatim.
pub(super) fn list_all_vector_index_params_in(
    read_txn: &redb::ReadTransaction,
) -> crate::Result<Vec<StoredVectorIndexParams>> {
    let table = read_txn
        .open_table(VECTOR_INDEX_PARAMS)
        .map_err(|e| catalog_err("open vector_index_params", e))?;

    let mut entries = Vec::new();
    for item in table
        .range(..)
        .map_err(|e| catalog_err("range vector index params", e))?
    {
        let (_, value) = item.map_err(|e| catalog_err("read vector index params", e))?;
        let entry: StoredVectorIndexParams = zerompk::from_msgpack(value.value())
            .map_err(|e| catalog_err("deser vector index params", e))?;
        entries.push(entry);
    }
    Ok(entries)
}

fn vector_index_params_key(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    field_name: &str,
) -> String {
    format!("{database_id}:{tenant_id}:{collection}:{field_name}")
}
