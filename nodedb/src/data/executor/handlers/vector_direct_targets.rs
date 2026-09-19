// SPDX-License-Identifier: BUSL-1.1

//! Resolve the rows a vector-primary `DELETE` / `UPDATE` targets.
//!
//! Point keys arrive as surrogates the Control Plane bound from the primary
//! key. A predicate arrives as serialized `ScanFilter`s and is evaluated
//! here against every payload sidecar row, decoded through the vector
//! sidecar format — the same converter `SELECT` on the collection uses, so
//! `WHERE owner = 'x'` matches the same rows on a write as on a read.

use nodedb_physical::physical_plan::VectorWriteTargets;
use nodedb_types::{StorageKey, Surrogate};
use redb::{ReadableDatabase, ReadableTable};

use crate::bridge::envelope::ErrorCode;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scan_normalize::sparse_row_to_doc;
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;

impl CoreLoop {
    /// The surrogates `targets` names, in a deterministic order.
    ///
    /// Point targets keep statement order with duplicates removed. Predicate
    /// targets come back in sparse-store key order. A malformed filter
    /// payload is an error, never an empty predicate: a silent decode
    /// failure would turn a `WHERE` into a whole-collection write.
    pub(in crate::data::executor) fn resolve_vector_direct_targets(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        targets: &VectorWriteTargets,
    ) -> Result<Vec<Surrogate>, ErrorCode> {
        match targets {
            VectorWriteTargets::Surrogates(surrogates) => {
                let mut seen = std::collections::HashSet::with_capacity(surrogates.len());
                Ok(surrogates
                    .iter()
                    .copied()
                    .filter(|s| *s != Surrogate::ZERO && seen.insert(*s))
                    .collect())
            }
            VectorWriteTargets::Predicate(filter_bytes) => {
                let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
                    Vec::new()
                } else {
                    zerompk::from_msgpack(filter_bytes).map_err(|e| ErrorCode::Internal {
                        detail: format!(
                            "vector-primary predicate write on '{collection}': filter decode: {e}"
                        ),
                    })?
                };
                self.scan_vector_sidecar_matches(database_id, tid, collection, &filters)
            }
        }
    }

    /// Every sidecar row of `collection` that `filters` matches, as the
    /// surrogate its storage key carries.
    fn scan_vector_sidecar_matches(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &[ScanFilter],
    ) -> Result<Vec<Surrogate>, ErrorCode> {
        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, collection);
        let end = format!("{prefix}\u{ffff}");
        let storage_err = |what: &str, e: &dyn std::fmt::Display| ErrorCode::Internal {
            detail: format!("vector-primary sidecar scan on '{collection}': {what}: {e}"),
        };

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| storage_err("read txn", &e))?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| storage_err("open table", &e))?;
        let range = table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| storage_err("range", &e))?;

        let mut out = Vec::new();
        for entry in range {
            let (key_guard, value_guard) = entry.map_err(|e| storage_err("iterate", &e))?;
            let full_key = key_guard.value();
            let Some(rest) = full_key.strip_prefix(&prefix) else {
                continue;
            };
            let key = StorageKey::parse(rest).ok_or_else(|| {
                ErrorCode::from(crate::engine::sparse::btree::invalid_storage_key_err(
                    crate::engine::sparse::btree::KeyedTable::Documents,
                    collection,
                    rest,
                ))
            })?;
            if !filters.is_empty() {
                let (_id, mp) = sparse_row_to_doc(
                    &key,
                    value_guard.value(),
                    SparseBodyFormatRef::VectorSidecar,
                );
                // A predicate that divides by zero fails the statement, the
                // same as it fails the equivalent `SELECT`.
                match ScanFilter::all_match_binary(filters, &mp) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(_) => return Err(ErrorCode::DivisionByZero),
                }
            }
            out.push(key.surrogate());
        }
        Ok(out)
    }
}
