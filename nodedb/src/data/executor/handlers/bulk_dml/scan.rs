// SPDX-License-Identifier: BUSL-1.1

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Scan documents in a collection matching the given filters.
    ///
    /// Returns document IDs of all matching documents.
    pub(in crate::data::executor) fn scan_matching_documents(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &[ScanFilter],
    ) -> crate::Result<Vec<String>> {
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
                if matches(value_bytes)
                    && let Some(doc_id) = key.strip_prefix(&prefix)
                {
                    ids.push(doc_id.to_string());
                }
            }
        }
        Ok(ids)
    }
}

/// Compute the sorted list of surrogates from scanned document IDs.
///
/// Document storage keys are 8-character hex-encoded u32 surrogates
/// (see `engine::document::store::key`). Ids that cannot be parsed are
/// silently skipped — they represent legacy non-surrogate documents that
/// do not participate in OLLP verification.
///
/// The output is sorted ascending, matching the contract expected by the
/// OLLP verification comparison on both sides (Data Plane and Control
/// Plane pre-exec).
/// Convert the carried OLLP predicted surrogate set into the sorted list of
/// document storage keys (8-char hex doc-ids) to apply the bulk mutation to.
///
/// This is the determinism anchor for multi-replica OLLP: every replica —
/// leader and follower — mutates EXACTLY this set, derived from the leader's
/// verified prediction carried in the plan, rather than from a per-replica
/// local scan (which can differ when a follower's redb snapshot lags). Output
/// is sorted ascending by surrogate so the apply order is identical on every
/// replica (`surrogate_to_doc_id` is monotonic in the surrogate, so sorting the
/// surrogates sorts the doc-ids).
pub(in crate::data::executor) fn ollp_predicted_doc_ids(predicted: &[u32]) -> Vec<String> {
    let mut surrogates: Vec<u32> = predicted.to_vec();
    surrogates.sort_unstable();
    surrogates
        .into_iter()
        .map(|s| {
            crate::engine::document::store::surrogate_to_doc_id(nodedb_types::Surrogate::new(s))
        })
        .collect()
}

pub(in crate::data::executor) fn ollp_actual_surrogates(doc_ids: &[String]) -> Vec<u32> {
    let mut surrogates: Vec<u32> = doc_ids
        .iter()
        .filter_map(|id| {
            if id.len() == 8 {
                u32::from_str_radix(id, 16).ok()
            } else {
                None
            }
        })
        .collect();
    surrogates.sort_unstable();
    surrogates
}

/// True when the live `matching_ids` surrogate set equals the carried
/// `predicted` set. Both sides are sorted before comparison, so the result is
/// deterministic on every replica. Shared by the bulk-DML apply handlers and
/// the Calvin active-stage OLLP verifier so the `actual == predicted` guard
/// lives in exactly one place.
pub(in crate::data::executor) fn ollp_surrogates_match(
    matching_ids: &[String],
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
