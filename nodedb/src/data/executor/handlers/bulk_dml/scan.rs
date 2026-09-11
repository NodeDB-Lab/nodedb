// SPDX-License-Identifier: BUSL-1.1

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable};

impl CoreLoop {
    /// Scan documents in a collection matching the given filters.
    ///
    /// Returns the storage keys of all matching documents.
    pub(in crate::data::executor) fn scan_matching_documents(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &[ScanFilter],
    ) -> crate::Result<Vec<StorageKey>> {
        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open table: {e}"),
            })?;

        // Resolve the strict schema ONCE for the whole scan rather than
        // per row: `strict_aware_matcher` captures it in the closure so the
        // `doc_configs` lookup doesn't repeat for every row in `range`.
        let matches = self.strict_aware_matcher(database_id, tid, collection, filters);

        let mut ids = Vec::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let key = entry.0.value();
                let value_bytes = entry.1.value();
                if let Some(doc_id) = key.strip_prefix(&prefix)
                    && matches(doc_id, value_bytes)?
                {
                    let doc_id = StorageKey::parse(doc_id).ok_or_else(|| {
                        crate::engine::sparse::btree::invalid_storage_key_err(
                            "DOCUMENTS",
                            collection,
                            doc_id,
                        )
                    })?;
                    ids.push(doc_id);
                }
            }
        }
        Ok(ids)
    }
}

/// Convert the carried OLLP predicted surrogate set into the sorted list of
/// storage keys to apply the bulk mutation to.
///
/// This is the determinism anchor for multi-replica OLLP: every replica —
/// leader and follower — mutates EXACTLY this set, derived from the leader's
/// verified prediction carried in the plan, rather than from a per-replica
/// local scan (which can differ when a follower's redb snapshot lags). Output
/// is sorted ascending by surrogate so the apply order is identical on every
/// replica.
pub(in crate::data::executor) fn ollp_predicted_doc_ids(predicted: &[u32]) -> Vec<StorageKey> {
    let mut surrogates: Vec<u32> = predicted.to_vec();
    surrogates.sort_unstable();
    surrogates
        .into_iter()
        .map(|s| StorageKey::for_surrogate(nodedb_types::Surrogate::new(s)))
        .collect()
}

/// Compute the sorted list of surrogates from scanned storage keys.
///
/// Feeds the OLLP verification comparison on both sides: Data Plane and
/// Control Plane pre-exec.
pub(in crate::data::executor) fn ollp_actual_surrogates(doc_ids: &[StorageKey]) -> Vec<u32> {
    let mut surrogates: Vec<u32> = doc_ids.iter().map(|k| k.surrogate().as_u32()).collect();
    surrogates.sort_unstable();
    surrogates
}

/// True when the live `matching_ids` surrogate set equals the carried
/// `predicted` set. Both sides are sorted before comparison, so the result is
/// deterministic on every replica. Shared by the bulk-DML apply handlers and
/// the Calvin active-stage OLLP verifier so the `actual == predicted` guard
/// lives in exactly one place.
pub(in crate::data::executor) fn ollp_surrogates_match(
    matching_ids: &[StorageKey],
    predicted: &[u32],
) -> bool {
    let actual = ollp_actual_surrogates(matching_ids);
    let mut predicted_sorted: Vec<u32> = predicted.to_vec();
    predicted_sorted.sort_unstable();
    actual == predicted_sorted
}

/// True when the recomputed `actual` implicit-edge set equals the carried
/// `predicted` edge set. Both sides are sorted via `OllpPredictedEdge`'s
/// derived `Ord`, matching the surrogate-set comparison's determinism contract.
pub(in crate::data::executor) fn ollp_edges_match(
    mut actual: Vec<nodedb_physical::physical_plan::OllpPredictedEdge>,
    predicted: &[nodedb_physical::physical_plan::OllpPredictedEdge],
) -> bool {
    actual.sort_unstable();
    let mut predicted_sorted = predicted.to_vec();
    predicted_sorted.sort_unstable();
    actual == predicted_sorted
}
