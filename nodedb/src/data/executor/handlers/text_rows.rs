// SPDX-License-Identifier: BUSL-1.1

//! Row sets of a full-text read: the current version of each base row, and
//! the rows the residual WHERE filters and RLS admit before ranking.
//!
//! Hydration, eligibility, and the score scan read rows through the same
//! functions here, so the three can never disagree about which version of a
//! row is current or how a predicate sees it.

use std::ops::ControlFlow;

use nodedb_types::{StorageKey, Surrogate, SurrogateBitmap};

use crate::bridge::scan_filter::{ScanFilter, decode_scan_filters};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::row_shape::sparse_row_to_doc;
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::btree_versioned::VersionedScanParams;
use crate::engine::sparse::scan_stop::never_stop;
use crate::types::TenantId;

/// Rows read per page of a bitemporal collection's current versions.
const BITEMPORAL_PAGE_ROWS: usize = 1024;

/// The row predicates a full-text read applies before ranking: the residual
/// WHERE filters and the RLS filters.
pub(in crate::data::executor) struct TextRowGate {
    filters: Vec<ScanFilter>,
    rls: Vec<ScanFilter>,
}

impl TextRowGate {
    /// Decode the serialized `Vec<ScanFilter>` of both predicate sets.
    pub(in crate::data::executor) fn new(
        filters: &[u8],
        rls_filters: &[u8],
    ) -> crate::Result<Self> {
        Ok(Self {
            filters: decode_scan_filters(filters, "text search filters")?,
            rls: decode_scan_filters(rls_filters, "text search RLS filters")?,
        })
    }

    /// Whether every row passes: no residual filter and no RLS filter.
    pub(in crate::data::executor) fn is_open(&self) -> bool {
        self.filters.is_empty() && self.rls.is_empty()
    }

    /// Whether the row image passes every residual filter and every RLS
    /// filter. A residual filter that fails to evaluate fails the query. An
    /// RLS filter that fails to evaluate denies the row: RLS fails closed and
    /// never tells an erroring row from a missing one.
    pub(in crate::data::executor) fn admits(&self, image: &[u8]) -> crate::Result<bool> {
        if !ScanFilter::all_match_binary(&self.filters, image)? {
            return Ok(false);
        }
        Ok(self.rls.is_empty() || ScanFilter::all_match_binary(&self.rls, image).unwrap_or(false))
    }
}

/// The msgpack image filters and RLS read for a row: the normalized body
/// with `id` injected from the storage key, the same image the base document
/// scan matches and returns.
pub(in crate::data::executor) fn text_row_image(
    key: &StorageKey,
    body: &[u8],
    format: SparseBodyFormatRef<'_>,
) -> Vec<u8> {
    sparse_row_to_doc(key, body, format).1
}

impl CoreLoop {
    /// The current version of one base row: the versioned table's current
    /// version for a bitemporal collection, the document table otherwise.
    pub(in crate::data::executor) fn text_base_body(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        key: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        if self.is_bitemporal(database_id, tid, collection) {
            self.sparse
                .versioned_get_current(database_id, tid, collection, key)
        } else {
            self.sparse.get(database_id, tid, collection, key)
        }
    }

    /// Visit the current version of every base row of `collection`, from the
    /// tables [`Self::text_base_body`] reads, until `f` breaks. A bitemporal
    /// collection is read a page at a time, so at most one page of rows is in
    /// memory.
    pub(in crate::data::executor) fn for_each_text_base_row<F>(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        mut f: F,
    ) -> crate::Result<()>
    where
        F: FnMut(&StorageKey, &[u8]) -> crate::Result<ControlFlow<()>>,
    {
        if !self.is_bitemporal(database_id, tid, collection) {
            return self
                .sparse
                .scan_documents_while(database_id, tid, collection, f);
        }
        let mut after: Option<StorageKey> = None;
        loop {
            let page = self.sparse.versioned_scan_as_of_after(
                VersionedScanParams {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    sys_cutoff_ms: None,
                    valid_at_ms: None,
                    limit: BITEMPORAL_PAGE_ROWS,
                },
                after.as_ref(),
                &|_, _| true,
                &never_stop,
            )?;
            let full = page.len() >= BITEMPORAL_PAGE_ROWS;
            for (key, body) in &page {
                if f(key, body)?.is_break() {
                    return Ok(());
                }
            }
            match page.into_iter().next_back() {
                Some((key, _)) if full => after = Some(key),
                _ => return Ok(()),
            }
        }
    }

    /// Surrogates of `collection` whose row passes `gate`, judged before
    /// ranking. Staged rows of the issuing transaction are judged on their
    /// staged body. A staged tombstone or TRUNCATE removes base rows. `None`
    /// when the gate is open.
    pub(in crate::data::executor) fn text_eligible_rows(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        gate: &TextRowGate,
    ) -> crate::Result<Option<SurrogateBitmap>> {
        if gate.is_open() {
            return Ok(None);
        }
        let database_id = task.request.database_id;
        let tenant = TenantId::new(tid);
        let format = self.sparse_body_format(database_id, tenant, collection);
        let admits = |key: &StorageKey, bytes: &[u8]| -> crate::Result<bool> {
            gate.admits(&text_row_image(key, bytes, format.as_format_ref()))
        };
        let coll_key = (database_id, tenant, collection.to_string());
        let overlay = match task.request.txn_id {
            Some(txn_id) => {
                // Read-your-own-writes refreshes the lease (see the reaper).
                self.touch_overlay(txn_id);
                self.txn_overlays.get(&txn_id)
            }
            None => None,
        };

        let mut eligible = SurrogateBitmap::new();
        if overlay.is_none_or(|o| o.base_visible(&coll_key)) {
            self.for_each_text_base_row(database_id.as_u64(), tid, collection, |key, bytes| {
                if admits(key, bytes)? {
                    eligible.insert(key.surrogate());
                }
                Ok(ControlFlow::Continue(()))
            })?;
        }
        if let Some(overlay) = overlay {
            for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
                let surrogate = Surrogate::new(surrogate);
                let keep = match staged {
                    Staged::Put(body) => admits(&StorageKey::for_surrogate(surrogate), body)?,
                    Staged::Tombstone => false,
                };
                if keep {
                    eligible.insert(surrogate);
                } else {
                    eligible.remove(surrogate);
                }
            }
        }
        Ok(Some(eligible))
    }
}

/// The rows both the plan's prefilter and the gated rows admit.
pub(in crate::data::executor) fn combine_eligible(
    prefilter: Option<&SurrogateBitmap>,
    eligible: Option<SurrogateBitmap>,
) -> Option<SurrogateBitmap> {
    match (prefilter, eligible) {
        (Some(prefilter), Some(eligible)) => Some(prefilter.intersect(&eligible)),
        (Some(prefilter), None) => Some(prefilter.clone()),
        (None, eligible) => eligible,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msgpack(value: serde_json::Value) -> Vec<u8> {
        nodedb_types::json_to_msgpack_or_empty(&value)
    }

    fn id_filter(id: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "id".into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value: nodedb_types::Value::String(id.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).unwrap()
    }

    /// A row stored without an `id` field matches `id = '<identity>'`: the
    /// image carries the identity the storage key holds.
    #[test]
    fn row_image_carries_the_key_identity() {
        let key = StorageKey::for_surrogate(Surrogate::new(42));
        let body = msgpack(serde_json::json!({ "body": "rust" }));
        let image = text_row_image(&key, &body, SparseBodyFormatRef::Document);
        let identity = key.to_identity();
        let gate = TextRowGate::new(&id_filter(identity.as_str()), &[]).unwrap();
        assert!(gate.admits(&image).unwrap());
        let other = TextRowGate::new(&id_filter("nope"), &[]).unwrap();
        assert!(!other.admits(&image).unwrap());
    }

    /// RLS naming `id` reads the injected identity too.
    #[test]
    fn rls_on_id_reads_the_key_identity() {
        let key = StorageKey::for_surrogate(Surrogate::new(7));
        let body = msgpack(serde_json::json!({ "body": "rust" }));
        let image = text_row_image(&key, &body, SparseBodyFormatRef::Document);
        let identity = key.to_identity();
        let gate = TextRowGate::new(&[], &id_filter(identity.as_str())).unwrap();
        assert!(!gate.is_open());
        assert!(gate.admits(&image).unwrap());
    }
}
