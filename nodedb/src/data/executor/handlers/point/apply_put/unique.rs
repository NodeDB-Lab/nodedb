// SPDX-License-Identifier: BUSL-1.1

//! UNIQUE-constraint check for `apply_point_put`.

/// Reject the write if any `unique: true` index already holds one of the
/// incoming document's extracted values under a *different* `document_id`.
///
/// Runs before `apply_secondary_indexes_in_txn` so the caller's write
/// transaction is still clean — rejection does not roll anything back.
/// Same-id re-puts (idempotent overwrites) are allowed through; we only
/// reject when another row owns the value.
/// Parameters for [`check_unique_constraints`].
pub(in crate::data::executor) struct UniqueCheck<'a> {
    pub sparse: &'a crate::engine::sparse::btree::SparseEngine,
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub doc: &'a serde_json::Value,
    pub document_id: &'a crate::engine::document::store::StorageKey,
    pub paths: &'a [crate::engine::document::store::IndexPath],
    /// Bitemporal collections keep secondary-index entries in the versioned
    /// index only; the uniqueness probe must read that index, not the empty
    /// plain one.
    pub bitemporal: bool,
}

pub(in crate::data::executor) fn check_unique_constraints(c: UniqueCheck<'_>) -> crate::Result<()> {
    use crate::engine::document::store::extract_index_values;

    let UniqueCheck {
        sparse,
        database_id,
        tid,
        collection,
        doc,
        document_id,
        paths,
        bitemporal,
    } = c;

    let doc_engine = crate::engine::document::store::DocumentEngine::new(sparse, database_id, tid);
    for path in paths {
        if !path.unique {
            continue;
        }
        // A partial UNIQUE index only applies to rows the predicate
        // accepts; rows outside the predicate's scope are not part of
        // the uniqueness domain. Skipping the check here mirrors the
        // skip in `apply_secondary_indexes_in_txn` so the two paths
        // agree on which rows the index governs.
        if let Some(ref p) = path.predicate
            && !p.evaluate_json(doc)
        {
            continue;
        }
        for raw in extract_index_values(doc, &path.path, path.is_array) {
            let needle = if path.case_insensitive {
                raw.to_lowercase()
            } else {
                raw
            };
            let existing: Vec<crate::engine::document::store::StorageKey> = doc_engine
                .index_lookup(collection, &path.path, &needle, bitemporal)
                .unwrap_or_default();
            if existing.iter().any(|id| id != document_id) {
                return Err(crate::Error::RejectedConstraint {
                    collection: collection.to_string(),
                    constraint: "unique".to_string(),
                    detail: format!(
                        "unique index '{}' violation on field '{}' (value '{}')",
                        path.name, path.path, needle
                    ),
                });
            }
        }
    }
    Ok(())
}
