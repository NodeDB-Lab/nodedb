// SPDX-License-Identifier: BUSL-1.1

//! ANN tuning shared by the vector search handlers.
//!
//! Resolves the beam width and rerank oversample one search reads, so the
//! search body works in plain numbers.

use nodedb_vector::rerank::{DEFAULT_OVERSAMPLE, SearchBreadth, recall_scale};
use tracing::debug;

/// Resolved ANN tuning for a single search call.
#[derive(Debug)]
pub(super) struct ResolvedAnnOptions {
    /// Beam width and rerank oversample after any explicit override and the
    /// recall target.
    pub breadth: SearchBreadth,
}

/// Distil `VectorAnnOptions` into the breadth the search reads.
///
/// `ef_search` takes the override or `base_ef_search`. `oversample` takes the
/// override or `DEFAULT_OVERSAMPLE`. A `target_recall` above 0.80 widens
/// both through `recall_scale`. A `target_recall` outside `(0.0, 1.0]` is a
/// data exception.
pub(super) fn apply_ann_options(
    core_id: usize,
    collection: &str,
    base_ef_search: usize,
    ann: &nodedb_types::VectorAnnOptions,
) -> crate::Result<ResolvedAnnOptions> {
    if ann.query_dim.is_some() || ann.meta_token_budget.is_some() {
        debug!(
            core = core_id,
            %collection,
            query_dim = ?ann.query_dim,
            meta_token_budget = ?ann.meta_token_budget,
            "ann_options received"
        );
    }
    let ef_search = ann.ef_search_override.unwrap_or(base_ef_search);
    let oversample = ann.oversample.unwrap_or(DEFAULT_OVERSAMPLE).max(1);
    let (ef_search, oversample) = recall_scale(ann.target_recall, ef_search, oversample)
        .map_err(nodedb_vector::error::VectorError::from)?;
    Ok(ResolvedAnnOptions {
        breadth: SearchBreadth {
            ef_search,
            oversample,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(ann: &nodedb_types::VectorAnnOptions, base_ef: usize) -> SearchBreadth {
        apply_ann_options(0, "docs", base_ef, ann)
            .expect("valid ann options")
            .breadth
    }

    #[test]
    fn apply_ann_options_defaults_oversample_to_three() {
        let breadth = resolve(&nodedb_types::VectorAnnOptions::default(), 40);
        assert_eq!(breadth.ef_search, 40);
        assert_eq!(breadth.oversample, 3);
        assert_eq!(breadth.oversample, DEFAULT_OVERSAMPLE);
    }

    #[test]
    fn explicit_overrides_win_over_the_defaults() {
        let ann = nodedb_types::VectorAnnOptions {
            ef_search_override: Some(128),
            oversample: Some(5),
            ..Default::default()
        };
        let breadth = resolve(&ann, 40);
        assert_eq!(breadth.ef_search, 128);
        assert_eq!(breadth.oversample, 5);
    }

    #[test]
    fn target_recall_widens_beam_and_oversample() {
        let ann = nodedb_types::VectorAnnOptions {
            ef_search_override: Some(100),
            oversample: Some(3),
            target_recall: Some(0.90),
            ..Default::default()
        };
        let breadth = resolve(&ann, 40);
        // Scale 3.0 at recall 0.90: ef 100 × 3, oversample ⌈3 × √3⌉.
        assert_eq!((breadth.ef_search, breadth.oversample), (300, 6));
    }

    #[test]
    fn target_recall_out_of_range_is_a_data_exception() {
        let ann = nodedb_types::VectorAnnOptions {
            target_recall: Some(0.0),
            ..Default::default()
        };
        let err = apply_ann_options(0, "docs", 40, &ann).expect_err("0.0 is out of range");
        assert!(
            matches!(err, crate::Error::DataException { .. }),
            "got {err:?}"
        );
    }
}
