// SPDX-License-Identifier: BUSL-1.1

//! The newest stored version of one row, through the engine that keys it.

use nodedb_types::StorageKey;

use crate::engine::graph::edge_store::EdgeRef;
use crate::types::{DatabaseId, KeyRepr, TenantId};

use super::super::CoreLoop;

impl CoreLoop {
    /// Whether the rows of `collection` are document rows a surrogate
    /// keys: no columnar, timeseries, or array store holds the collection
    /// on this core.
    pub(in crate::data::executor) fn rows_keyed_as_documents(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
    ) -> bool {
        let owner = (db, tenant, collection.to_string());
        !(self.columnar_engines.contains_key(&owner)
            || self.columnar_memtables.contains_key(&owner)
            || self.ts_registries.contains_key(&owner)
            || self
                .array_engine
                .array_ids()
                .any(|id| id.tenant_id == tenant && id.database_id == db && id.name == collection))
    }

    /// The newest stored version of the row `key` names, live or tombstone:
    /// `Some(None)` for a row with no version. `None` when no keyed lookup
    /// reads the row's engine, and the caller takes the collection's newest
    /// version instead. `Err` when the store does not read.
    pub(in crate::data::executor) fn row_newest(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: &KeyRepr,
    ) -> crate::Result<Option<Option<i64>>> {
        match key {
            KeyRepr::Surrogate(surrogate) => {
                if !self.rows_keyed_as_documents(db, tenant, collection) {
                    return Ok(None);
                }
                self.sparse
                    .versioned_latest_sys_from(
                        db.as_u64(),
                        tenant.as_u64(),
                        collection,
                        &StorageKey::for_surrogate(nodedb_types::Surrogate::new(*surrogate)),
                    )
                    .map(Some)
            }
            KeyRepr::KvKey(kv_key) => Ok(Some(self.kv_engine.key_newest_ordinal(
                db.as_u64(),
                tenant.as_u64(),
                collection,
                kv_key,
            ))),
            KeyRepr::Edge { src, label, dst } => self
                .edge_store
                .latest_version_ordinal(EdgeRef::new(db, tenant, collection, src, label, dst))
                .map(Some),
        }
    }
}
