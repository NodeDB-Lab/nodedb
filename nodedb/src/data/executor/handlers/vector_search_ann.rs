//! ANN tuning helpers shared by the vector search handler.
//!
//! Carved out of `vector_search.rs` so the hot-path body can stay focused
//! on HNSW/IVF dispatch and so the quantization-mismatch matrix lives
//! next to the variant definitions it bridges.

use tracing::debug;

/// Resolved ANN tuning state for a single search call. Distilled from the
/// caller's `VectorAnnOptions` so the search body works in plain numbers.
pub(super) struct ResolvedAnnOptions {
    /// Effective ef_search after applying any explicit override.
    pub ef_search: usize,
    /// Oversample multiplier (≥ 1) for fetch breadth.
    pub oversample: usize,
}

/// Distil `VectorAnnOptions` into the runtime values the search path needs
/// and emit a debug trace for the forward-looking knobs (query_dim,
/// meta_token_budget, target_recall, quantization) that the index features
/// will consume when they land.
pub(super) fn apply_ann_options(
    core_id: usize,
    collection: &str,
    base_ef_search: usize,
    ann: &nodedb_types::VectorAnnOptions,
) -> ResolvedAnnOptions {
    if ann.query_dim.is_some()
        || ann.meta_token_budget.is_some()
        || ann.target_recall.is_some()
        || ann.quantization.is_some()
    {
        debug!(
            core = core_id,
            %collection,
            query_dim = ?ann.query_dim,
            meta_token_budget = ?ann.meta_token_budget,
            target_recall = ?ann.target_recall,
            quantization = ?ann.quantization,
            "ann_options received"
        );
    }
    ResolvedAnnOptions {
        ef_search: ann.ef_search_override.unwrap_or(base_ef_search),
        oversample: ann.oversample.unwrap_or(1).max(1) as usize,
    }
}

/// Whether a SQL-requested quantization hint matches the quantization the
/// index was actually built with. Exhaustive over `VectorQuantization` so
/// adding a variant is a compile-time error rather than a silent "mismatch"
/// warning.
///
/// `Ternary` and `Opq` are SQL-level scaffolding for future codecs that are
/// not yet persisted as `VectorIndexQuantization` variants — they always
/// report as "no match" so the caller's warning path fires until index-side
/// support lands.
pub(super) fn quantization_matches(
    requested: nodedb_types::VectorQuantization,
    actual: nodedb_types::VectorIndexQuantization,
) -> bool {
    use nodedb_types::VectorIndexQuantization as Idx;
    use nodedb_types::VectorQuantization as Req;
    match requested {
        Req::None => matches!(actual, Idx::None),
        Req::Sq8 => matches!(actual, Idx::Sq8),
        Req::Pq => matches!(actual, Idx::Pq),
        Req::Binary => matches!(actual, Idx::Binary),
        Req::RaBitQ => matches!(actual, Idx::RaBitQ),
        Req::Bbq => matches!(actual, Idx::Bbq),
        // Not yet representable on the index side; treat as a mismatch
        // so callers get the warning and proceed with the actual codec.
        Req::Ternary | Req::Opq => false,
    }
}
