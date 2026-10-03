// SPDX-License-Identifier: Apache-2.0

//! Vector search and hybrid search scalar function registrations.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta, SearchTrigger};

use super::super::helpers::{m, no_trigger};

pub(super) fn vector_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "vector_distance",
            Scalar,
            2,
            3,
            SearchTrigger::VectorSearch,
            Some(ColumnType::Float64),
            arg_types::VECTOR_DISTANCE_ARGS,
        ),
        m(
            "vector_cosine_distance",
            Scalar,
            2,
            3,
            SearchTrigger::VectorSearch,
            Some(ColumnType::Float64),
            arg_types::VECTOR_DISTANCE_ARGS,
        ),
        m(
            "vector_neg_inner_product",
            Scalar,
            2,
            3,
            SearchTrigger::VectorSearch,
            Some(ColumnType::Float64),
            arg_types::VECTOR_DISTANCE_ARGS,
        ),
        m(
            "multi_vector_search",
            Scalar,
            1,
            2,
            SearchTrigger::MultiVectorSearch,
            Some(ColumnType::Float64),
            arg_types::MULTI_VECTOR_SEARCH_ARGS,
        ),
        m(
            "multi_vector_score",
            Scalar,
            3,
            3,
            no_trigger(),
            Some(ColumnType::Float64),
            arg_types::MULTI_VECTOR_SCORE_ARGS,
        ),
        m(
            "sparse_score",
            Scalar,
            3,
            3,
            SearchTrigger::SparseSearch,
            Some(ColumnType::Float64),
            arg_types::SPARSE_SCORE_ARGS,
        ),
        m(
            "bm25_score",
            Scalar,
            2,
            2,
            SearchTrigger::TextSearch,
            Some(ColumnType::Float64),
            arg_types::BM25_SCORE_ARGS,
        ),
        m(
            "search_score",
            Scalar,
            2,
            2,
            SearchTrigger::TextSearch,
            Some(ColumnType::Float64),
            arg_types::SEARCH_SCORE_ARGS,
        ),
        // Options are named (`mode => 'or'`, `fuzzy => true`), not positional.
        m(
            "text_match",
            Scalar,
            2,
            2,
            SearchTrigger::TextMatch,
            None,
            arg_types::TEXT_MATCH_ARGS,
        ),
        // `SEARCH(field, 'query text')` — alias for WHERE-clause FTS search.
        // Routes the same as `text_match` through the TextMatch WHERE-search path.
        m(
            "search",
            Scalar,
            2,
            2,
            SearchTrigger::TextMatch,
            None,
            arg_types::TEXT_MATCH_ARGS,
        ),
        m(
            "rrf_score",
            Scalar,
            2,
            6,
            SearchTrigger::HybridSearch,
            Some(ColumnType::Float64),
            arg_types::RRF_SCORE_ARGS,
        ),
        m(
            "graph_score",
            Scalar,
            2,
            usize::MAX,
            SearchTrigger::GraphSearch,
            Some(ColumnType::Float64),
            arg_types::GRAPH_SCORE_ARGS,
        ),
    ]
}
