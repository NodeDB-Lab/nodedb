// SPDX-License-Identifier: Apache-2.0

//! Shared query-execution utilities used by every engine: BM25 scoring,
//! text analysis (tokenization / normalization / stop words), expression
//! evaluation helpers, MessagePack scan readers, and rank-fusion (RRF).
//!
//! This crate is consumed by `nodedb` (server), `nodedb-lite` (embedded),
//! and `nodedb-fts` (full-text overlay). Functions are pure / `Send + Sync`
//! and free of I/O — actual data fetch happens in the calling crate.

pub mod agg_key;
pub mod cast;
pub mod chunk_text;
pub mod expr;
pub mod expr_parse;
pub mod functions;
pub mod fusion;
pub mod geo_functions;
pub mod json_expr;
pub mod json_ops;
pub mod metadata_filter;
pub mod msgpack_scan;
pub mod numeric_sum;
pub mod partition_hash;
pub mod scan_filter;
pub mod simd_agg;
pub mod simd_agg_i64;
pub mod simd_filter;
pub mod text_search;
pub mod ts_functions;
pub mod value_ops;
pub mod window;

pub use chunk_text::{ChunkError, ChunkStrategy, TextChunk, chunk_text};
pub use expr::{BinaryOp, CastType, ComputedColumn, EvalError, SqlExpr};
pub use fusion::{
    DEFAULT_RRF_K, FusedResult, RankedResult, reciprocal_rank_fusion,
    reciprocal_rank_fusion_linear, reciprocal_rank_fusion_weighted,
};
pub use json_expr::{compare_json, eval_expr_on_json};
pub use numeric_sum::ExactSum;
pub use partition_hash::{partition_hash, partition_hash_seeded};
pub use scan_filter::ScanFilter;
pub use window::{
    FrameBound, WindowError, WindowFrame, WindowFuncSpec, evaluate_window_functions,
    evaluate_window_functions_value,
};
