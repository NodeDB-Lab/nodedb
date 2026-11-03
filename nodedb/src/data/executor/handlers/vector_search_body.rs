// SPDX-License-Identifier: BUSL-1.1

//! The row bodies a vector search attaches to its hits, at the search's cut.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Fetch the document body via the sparse engine (keyed by
    /// surrogate-hex) and attach it to the hit. Used both by the RLS path
    /// (the Control Plane evaluates the predicate against `body`) and by
    /// the slow-path SELECT (the Control Plane response translator flattens
    /// the body's fields into the hit JSON so payload columns surface to
    /// the client). When `attach == false`, or the hit carries no surrogate
    /// binding, the hit is returned unchanged. A storage read error fails
    /// the call: a hit without its body would skip the RLS predicate check.
    ///
    /// The bytes are normalized to a standard msgpack map through the shared
    /// sparse-body normalizer, resolved from the collection's registered kind.
    /// A vector-primary collection's rows are `zerompk` TAGGED sidecars and a
    /// classic collection's are ordinary document bodies; the two are
    /// indistinguishable byte-wise, and both the RLS filter evaluator and the
    /// Control-Plane flatten read the attached body as one shape.
    ///
    /// `at` is the system-time ordinal a search at a cut reads: the body is
    /// the row's version visible there. `None` attaches the current version.
    #[inline]
    pub(in crate::data::executor) fn attach_body(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        attach: bool,
        at: Option<i64>,
        mut hit: super::super::response_codec::VectorSearchHit,
    ) -> crate::Result<super::super::response_codec::VectorSearchHit> {
        if !attach {
            return Ok(hit);
        }
        let Some(key) = hit.id.storage_key() else {
            return Ok(hit);
        };
        if let Some(bytes) = self.row_body_at(database_id, tid, collection, &key, at)? {
            let format = self.sparse_body_format(
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection,
            );
            // Owned: the hit outlives `bytes`, which is the storage read's
            // local buffer.
            hit.body = Some(
                crate::data::executor::scan_normalize::sparse_body_to_msgpack(
                    &bytes,
                    format.as_format_ref(),
                )
                .into_owned(),
            );
        }
        Ok(hit)
    }

    /// The stored body of `key` at `at`, or its current version for `None`.
    /// A current read takes the row at its current version, so a bitemporal
    /// collection attaches.
    fn row_body_at(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        key: &nodedb_types::StorageKey,
        at: Option<i64>,
    ) -> crate::Result<Option<Vec<u8>>> {
        match at {
            None => self.current_row_body(database_id, tid, collection, key),
            Some(ordinal) => Ok(self.sparse.versioned_get_as_of(
                database_id,
                tid,
                collection,
                key,
                Some(ordinal),
                None,
            )?),
        }
    }

    /// [`Self::attach_body`] for a candidate the residual filter has yet to
    /// pass. A strict body decodes only `columns`, the fields the filter
    /// reads, so a candidate the filter drops never pays for its other
    /// columns. Any other body format attaches whole.
    pub(in crate::data::executor) fn attach_filter_body(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        columns: &[String],
        at: Option<i64>,
        mut hit: super::super::response_codec::VectorSearchHit,
    ) -> crate::Result<super::super::response_codec::VectorSearchHit> {
        let format = self.sparse_body_format(
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection,
        );
        let crate::data::executor::sparse_body_format::SparseBodyFormat::Strict(schema) = &format
        else {
            return self.attach_body(database_id, tid, collection, true, at, hit);
        };
        let Some(key) = hit.id.storage_key() else {
            return Ok(hit);
        };
        if let Some(bytes) = self.row_body_at(database_id, tid, collection, &key, at)? {
            hit.body = crate::data::executor::strict_format::binary_tuple_columns_to_msgpack(
                &bytes, schema, columns,
            );
        }
        Ok(hit)
    }
}
