// SPDX-License-Identifier: BUSL-1.1

//! Current-state scan of bitemporal document collections.
//!
//! Bitemporal collections keep every write on the versioned sparse table, so
//! the plain [`CoreLoop::scan_collection`] path (which reads the non-versioned
//! namespace) returns zero rows. This module reads the newest live version per
//! `doc_id` from the versioned table and normalizes each body to the same
//! standard-msgpack shape `scan_collection` produces, so downstream consumers
//! (streaming aggregation, etc.) are format-agnostic to the temporal storage.

use super::scan_normalize::sparse_row_to_doc;
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Current-state scan of a bitemporal document collection: reads the newest
    /// live version per `doc_id` and normalizes each body to standard msgpack
    /// via [`sparse_row_to_doc`], producing rows byte-identical in shape to
    /// [`CoreLoop::scan_collection`] (schemaless normalized from possibly-legacy
    /// JSON, strict decoded from Binary Tuple, `id` injected).
    pub(in crate::data::executor) fn scan_collection_versioned_current(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let docs = self.sparse.versioned_scan_as_of(
            crate::engine::sparse::btree_versioned::VersionedScanParams {
                database_id: did,
                tenant: tid,
                coll: collection,
                sys_cutoff_ms: None,
                valid_at_ms: None,
                limit,
            },
            &|_| true,
        )?;
        let strict_schema = self.strict_schema_for(
            crate::types::DatabaseId::new(did),
            crate::types::TenantId::new(tid),
            collection,
        );

        let mut normalized = Vec::with_capacity(docs.len());
        for (id, raw) in docs {
            normalized.push(sparse_row_to_doc(&id, &raw, strict_schema.as_ref()));
        }
        Ok(normalized)
    }
}
