// SPDX-License-Identifier: Apache-2.0

//! Vector ANN option types for SqlPlan.

use crate::types_array;

/// Cross-engine prefilter for `SqlPlan::VectorSearch`: the array slice
/// runs first, its matching cells' surrogates form a bitmap that gates
/// the HNSW candidate set.
#[derive(Debug, Clone)]
pub struct ArrayPrefilter {
    /// Array name (resolved against the catalog).
    pub array_name: String,
    /// Slice predicate (per-dim ranges).
    pub slice: types_array::ArraySliceAst,
}

/// Secondary-index prefilter for `SqlPlan::VectorSearch`: an equality on an
/// indexed field. The index lookup runs first and its rows' surrogates form
/// a bitmap that gates the candidate set, so the search ranks only matching
/// rows and decodes no other body.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexPrefilter {
    /// Canonical index field path (`$.agent_id`).
    pub field: String,
    /// The literal to look up, lowercased for a case-insensitive index.
    pub value: crate::types::SqlValue,
}

/// Knobs the vector planner exposes via SQL.
///
/// All fields default to `None` / sensible defaults, in which case the
/// executor falls back to the collection's configured quantization and
/// `ef_search` heuristic.
///
/// Parsed from an optional JSON-string third argument to
/// `vector_distance(field, query, '{"quantization":"rabitq","oversample":3}')`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VectorAnnOptions {
    pub quantization: Option<VectorQuantization>,
    pub oversample: Option<u8>,
    pub query_dim: Option<u32>,
    pub meta_token_budget: Option<u8>,
    /// Override `ef_search`; falls back to `2 * top_k` when None.
    pub ef_search_override: Option<usize>,
    /// Target recall used with the cost model to escalate from coarse to
    /// fine quantization.
    pub target_recall: Option<f32>,
}

/// Quantization choices exposed at SQL level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorQuantization {
    None,
    Sq8,
    Pq,
    RaBitQ,
    Bbq,
    Binary,
    Ternary,
    Opq,
}

impl VectorAnnOptions {
    /// Convert into the runtime mirror used by `nodedb-types`.
    pub fn to_runtime(&self) -> nodedb_types::VectorAnnOptions {
        nodedb_types::VectorAnnOptions {
            quantization: self.quantization.map(|q| match q {
                VectorQuantization::None => nodedb_types::VectorQuantization::None,
                VectorQuantization::Sq8 => nodedb_types::VectorQuantization::Sq8,
                VectorQuantization::Pq => nodedb_types::VectorQuantization::Pq,
                VectorQuantization::RaBitQ => nodedb_types::VectorQuantization::RaBitQ,
                VectorQuantization::Bbq => nodedb_types::VectorQuantization::Bbq,
                VectorQuantization::Binary => nodedb_types::VectorQuantization::Binary,
                VectorQuantization::Ternary => nodedb_types::VectorQuantization::Ternary,
                VectorQuantization::Opq => nodedb_types::VectorQuantization::Opq,
            }),
            oversample: self.oversample,
            query_dim: self.query_dim,
            meta_token_budget: self.meta_token_budget,
            ef_search_override: self.ef_search_override,
            target_recall: self.target_recall,
        }
    }
}

impl VectorQuantization {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Sq8 => "sq8",
            Self::Pq => "pq",
            Self::RaBitQ => "rabitq",
            Self::Bbq => "bbq",
            Self::Binary => "binary",
            Self::Ternary => "ternary",
            Self::Opq => "opq",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "sq8" => Some(Self::Sq8),
            "pq" => Some(Self::Pq),
            "rabitq" => Some(Self::RaBitQ),
            "bbq" => Some(Self::Bbq),
            "binary" => Some(Self::Binary),
            "ternary" => Some(Self::Ternary),
            "opq" => Some(Self::Opq),
            _ => None,
        }
    }
}
