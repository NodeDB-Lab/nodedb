// SPDX-License-Identifier: BUSL-1.1

//! The rows a spatial read refines: each row's current version, or its
//! version visible at a system-time cut.
//!
//! A document collection keeps its rows in the sparse store, keyed by the
//! hex surrogate the R-tree reverse map records. A columnar-family
//! collection keeps them in columnar, keyed by the user `id` column. Both
//! forms normalise to standard msgpack maps.

use std::collections::HashMap;

use nodedb_columnar::VersionCut;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::row_shape::sparse_row_to_doc;
use crate::engine::sparse::btree_versioned::VersionedScanParams;

/// The bodies of one spatial read's candidates, read lazily: the sparse
/// store per row, and a columnar collection once for the whole read.
pub(super) struct CandidateBodies<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// The system-time ordinal the read sees. `None` reads the current
    /// version.
    pub at: Option<i64>,
    /// The columnar id → document map, built on the first candidate the
    /// sparse store does not hold.
    pub columnar: Option<HashMap<String, Vec<u8>>>,
}

impl CoreLoop {
    /// The rows of `collection` a full spatial scan reads: every row at its
    /// current version, or at its version visible at `at`. Reads at most
    /// `limit` rows.
    ///
    /// A timeseries collection reads the rows whose `_ts_system` ordinal is
    /// at or below `at`.
    pub(super) fn spatial_scan_rows(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        limit: usize,
        at: Option<i64>,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let Some(at) = at else {
            return self.scan_collection(database_id, tid, collection, limit);
        };
        if self.is_timeseries_collection(database_id, tid, collection) {
            return self.scan_timeseries_docs_at(database_id, tid, collection, limit, Some(at));
        }
        let engine_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        if self.columnar_engines.contains_key(&engine_key) {
            return self.scan_columnar(database_id, tid, collection, limit, VersionCut::At(at));
        }
        let rows = self.sparse.versioned_scan_as_of(
            VersionedScanParams {
                database_id,
                tenant: tid,
                coll: collection,
                sys_cutoff: Some(at),
                valid_at_ms: None,
                limit,
            },
            &|_: &nodedb_types::StorageKey, _: &[u8]| true,
            &|| false,
        )?;
        let format = self.sparse_body_format(engine_key.0, engine_key.1, collection);
        let identity_column = self.identity_column(database_id, tid, collection);
        Ok(rows
            .into_iter()
            .map(|(key, raw)| {
                sparse_row_to_doc(&key, &raw, format.as_format_ref(), &identity_column)
            })
            .collect())
    }

    /// The body of candidate `doc_id` the read of `bodies` sees, normalised
    /// to a msgpack map. `None` when the row has no version the read sees.
    ///
    /// A document id that parses as a storage key is read from the sparse
    /// store. Any other id, or one the sparse store does not hold, is a
    /// columnar-family row's `id`, read from the columnar map. The R-tree
    /// indexes the rows of these two stores only.
    pub(super) fn spatial_candidate_body(
        &self,
        bodies: &mut CandidateBodies<'_>,
        doc_id: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        let CandidateBodies {
            database_id,
            tid,
            collection,
            at,
            ..
        } = *bodies;
        if let Some(key) = nodedb_types::StorageKey::parse(doc_id) {
            let raw = match at {
                // At its current version, so a bitemporal collection resolves.
                None => self.current_row_body(database_id, tid, collection, &key)?,
                Some(at) => self.sparse.versioned_get_as_of(
                    database_id,
                    tid,
                    collection,
                    &key,
                    Some(at),
                    None,
                )?,
            };
            if let Some(raw) = raw {
                let format = self.sparse_body_format(
                    nodedb_types::DatabaseId::new(database_id),
                    crate::types::TenantId::new(tid),
                    collection,
                );
                let identity_column = self.identity_column(database_id, tid, collection);
                let (_, doc) =
                    sparse_row_to_doc(&key, &raw, format.as_format_ref(), &identity_column);
                return Ok(Some(doc));
            }
        }
        if bodies.columnar.is_none() {
            let cut = at.map_or(VersionCut::Current, VersionCut::At);
            let rows = self.scan_columnar(database_id, tid, collection, usize::MAX, cut)?;
            bodies.columnar = Some(rows.into_iter().collect());
        }
        Ok(bodies
            .columnar
            .as_ref()
            .and_then(|rows| rows.get(doc_id))
            .cloned())
    }
}
