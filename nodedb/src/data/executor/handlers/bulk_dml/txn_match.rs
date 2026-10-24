// SPDX-License-Identifier: BUSL-1.1

//! The rows a predicate write matches inside a transaction, and the check of
//! a carried prediction against them.
//!
//! A session stage and a Calvin stage both read BASE with the transaction's
//! overlay folded in. A Calvin overlay holds the plans staged before this
//! one, so the check runs at the plan's own turn in the transaction. BASE is
//! the current version of each row, read by [`CoreLoop::current_rows_where`].

use nodedb_physical::physical_plan::OllpPredictedEdge;
use nodedb_types::StorageKey;

use super::admission::implicit_edge_of;
use super::scan::{ollp_edges_match, ollp_surrogates_match};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::filter_match::HeldEvalError;
use crate::types::{DatabaseId, TenantId, TxnId};

/// The rows a bulk plan predicts it writes: the matched surrogates, and the
/// implicit edges of the matched edge documents when the plan carries them.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct TxnPrediction<'a> {
    pub surrogates: &'a [u32],
    pub edges: Option<&'a [OllpPredictedEdge]>,
}

impl<'a> TxnPrediction<'a> {
    /// The prediction a plan carries. `None` when it carries no surrogate set.
    pub(in crate::data::executor) fn of(
        surrogates: &'a Option<Vec<u32>>,
        edges: &'a Option<Vec<OllpPredictedEdge>>,
    ) -> Option<Self> {
        surrogates.as_deref().map(|surrogates| Self {
            surrogates,
            edges: edges.as_deref(),
        })
    }

    /// Whether `rows` are exactly the predicted rows, carrying exactly the
    /// predicted implicit edges.
    pub(in crate::data::executor) fn holds(&self, rows: &[(StorageKey, Vec<u8>)]) -> bool {
        let keys: Vec<StorageKey> = rows.iter().map(|(key, _)| *key).collect();
        if !ollp_surrogates_match(&keys, self.surrogates) {
            return false;
        }
        let Some(predicted) = self.edges else {
            return true;
        };
        let actual = rows
            .iter()
            .filter_map(|(key, body)| implicit_edge_of(*key, body))
            .collect();
        ollp_edges_match(actual, predicted)
    }
}

/// Decode a bulk plan's serialized filters. Empty bytes match every row.
pub(in crate::data::executor) fn decode_bulk_filters(
    filter_bytes: &[u8],
) -> crate::Result<Vec<ScanFilter>> {
    if filter_bytes.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(filter_bytes).map_err(|e| crate::Error::Serialization {
        format: "msgpack".to_string(),
        detail: format!("bulk predicate filters: {e}"),
    })
}

impl CoreLoop {
    /// Each `(row key, body)` that `filters` matches in the collection
    /// `coll_key` names, as transaction `txn_id` sees it.
    pub(in crate::data::executor) fn txn_matching_rows(
        &self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        filters: &[ScanFilter],
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        let (database_id, tenant, collection) = coll_key;
        let (database_id, tid) = (database_id.as_u64(), tenant.as_u64());
        let collection = collection.as_str();
        let crs = self.crs_context();
        let raw_matches = self.strict_aware_matcher(database_id, tid, collection, filters, &crs);
        let held = HeldEvalError::default();
        let matches = |row_key: &StorageKey, body: &[u8]| held.check(|| raw_matches(row_key, body));
        let mut rows = self.current_rows_where(database_id, tid, collection, &matches)?;
        self.merge_overlay_into_scan(txn_id, coll_key, &mut rows, &matches);
        held.finish(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Surrogate;

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    fn edge_body(from: &str, to: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "_from".to_string(),
            nodedb_types::Value::String(from.to_string()),
        );
        obj.insert(
            "_to".to_string(),
            nodedb_types::Value::String(to.to_string()),
        );
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(obj)).expect("encode")
    }

    fn edge(surrogate: u32, from: &str, to: &str) -> OllpPredictedEdge {
        OllpPredictedEdge {
            surrogate,
            from: from.to_string(),
            to: to.to_string(),
            label: None,
        }
    }

    #[test]
    fn prediction_holds_on_exactly_the_predicted_rows() {
        let rows = vec![(key(2), edge_body("a", "b")), (key(1), edge_body("c", "d"))];
        let surrogates = Some(vec![1, 2]);
        let edges = Some(vec![edge(1, "c", "d"), edge(2, "a", "b")]);
        let prediction = TxnPrediction::of(&surrogates, &edges).expect("prediction");
        assert!(prediction.holds(&rows));
    }

    #[test]
    fn prediction_fails_on_an_extra_row() {
        let rows = vec![(key(1), edge_body("c", "d")), (key(3), edge_body("e", "f"))];
        let surrogates = Some(vec![1]);
        let prediction = TxnPrediction::of(&surrogates, &None).expect("prediction");
        assert!(!prediction.holds(&rows));
    }

    #[test]
    fn prediction_fails_on_a_moved_implicit_edge() {
        let rows = vec![(key(1), edge_body("c", "z"))];
        let surrogates = Some(vec![1]);
        let edges = Some(vec![edge(1, "c", "d")]);
        let prediction = TxnPrediction::of(&surrogates, &edges).expect("prediction");
        assert!(!prediction.holds(&rows));
    }

    #[test]
    fn a_plan_without_surrogates_carries_no_prediction() {
        assert!(TxnPrediction::of(&None, &None).is_none());
    }
}
