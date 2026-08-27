// SPDX-License-Identifier: BUSL-1.1

//! Catalog operations for durable vector-index parameters.
//!
//! Stores `CREATE VECTOR INDEX` build parameters in
//! `_system.vector_index_params`. Key format: `"{tenant_id}:{collection}:{field_name}"`.

use nodedb_types::StoredVectorIndexParams;
use redb::{ReadableDatabase, ReadableTable};

use super::types::{SystemCatalog, VECTOR_INDEX_PARAMS, catalog_err};

/// Decode a stored vector-index entry, tolerating the legacy 11-field shape
/// written before `database_id` existed (field order is the zerompk array
/// format, so `database_id` is simply appended last). Legacy entries decode
/// to `database_id = 0` (`DatabaseId::DEFAULT`) — the historical assumption
/// of the seed/rebuild paths.
fn decode_vector_index_params(bytes: &[u8]) -> crate::Result<StoredVectorIndexParams> {
    if let Ok(e) = zerompk::from_msgpack::<StoredVectorIndexParams>(bytes) {
        return Ok(e);
    }
    // Legacy 11-field tuple: (tenant, collection, field, dim, metric, m,
    // ef_construction, index_type, pq_m, ivf_cells, ivf_nprobe).
    let legacy: (u64, String, String, usize, String, usize, usize, String, usize, usize, usize) =
        zerompk::from_msgpack(bytes)
            .map_err(|e| catalog_err("deser legacy vector index params", e))?;
    Ok(StoredVectorIndexParams {
        tenant_id: legacy.0,
        collection: legacy.1,
        field_name: legacy.2,
        dim: legacy.3,
        metric: legacy.4,
        m: legacy.5,
        ef_construction: legacy.6,
        index_type: legacy.7,
        pq_m: legacy.8,
        ivf_cells: legacy.9,
        ivf_nprobe: legacy.10,
        database_id: 0,
    })
}

impl SystemCatalog {
    /// Store vector index parameters for a collection/field.
    pub fn put_vector_index_params(&self, entry: &StoredVectorIndexParams) -> crate::Result<()> {
        let key = vector_index_params_key(entry.tenant_id, &entry.collection, &entry.field_name);
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

    /// Load vector index parameters for a specific collection/field.
    pub fn get_vector_index_params(
        &self,
        tenant_id: u64,
        collection: &str,
        field_name: &str,
    ) -> crate::Result<Option<StoredVectorIndexParams>> {
        let key = vector_index_params_key(tenant_id, collection, field_name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(VECTOR_INDEX_PARAMS)
            .map_err(|e| catalog_err("open vector_index_params", e))?;

        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let entry = decode_vector_index_params(value.value())?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get vector index params", e)),
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
        tenant_id: u64,
        collection: &str,
        field_name: &str,
    ) -> crate::Result<bool> {
        let key = vector_index_params_key(tenant_id, collection, field_name);
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
        let entry = decode_vector_index_params(value.value())?;
        entries.push(entry);
    }
    Ok(entries)
}

fn vector_index_params_key(tenant_id: u64, collection: &str, field_name: &str) -> String {
    format!("{tenant_id}:{collection}:{field_name}")
}
