// SPDX-License-Identifier: Apache-2.0

//! Full-text search engine shared by Origin, Lite, and WASM: BMW-optimized
//! BM25 with 128-doc block pruning, 16 Snowball stemmers, 27-language stop
//! words, CJK bigram tokenization (always available) plus optional
//! dictionary segmentation (lindera/jieba/icu) wired in unconditionally,
//! posting compression (delta + variable-width bitpack + SIMD unpack),
//! LSM segment store, fuzzy match, synonyms, highlighting, n-gram /
//! edge-ngram, and hybrid vector+text fusion.
//!
//! FTS is a cross-engine *overlay* — it indexes text fields from any
//! collection without owning the row storage. Analyzer choice is made per
//! collection at DDL time via `WITH (analyzer='...')`.

pub mod analyzer;
pub mod backend;
pub mod block;
pub mod bm25;
pub mod codec;
pub mod document_text;
pub mod fuzzy;
pub mod highlight;
pub mod index;
pub mod lsm;
mod mem_scope;
pub mod posting;
pub mod scope;
pub mod search;
#[cfg(test)]
mod test_support;

pub use analyzer::{
    AnalyzerRegistry, EdgeNgramAnalyzer, KeywordAnalyzer, LanguageAnalyzer, NgramAnalyzer,
    SimpleAnalyzer, StandardAnalyzer, SynonymMap, TextAnalyzer, analyze,
};
pub use backend::FtsBackend;
pub use block::{CompactPosting, PostingBlock};
pub use document_text::DocumentText;
pub use fuzzy::{fuzzy_discount, fuzzy_match, levenshtein, max_distance_for_length};
pub use index::{FtsIndex, FtsIndexError, MAX_INDEXABLE_SURROGATE, SynonymGroupRecord};
pub use nodedb_types::Surrogate;
pub use posting::{Bm25Params, MatchOffset, Posting, QueryMode, TextSearchResult};
pub use scope::IndexScope;
pub use search::bm25_search::FtsSearchParams;
pub use search::doc_scorer::{DocScore, DocScorer};
pub use search::query_parser::{InvalidQuery, ParsedQuery};
pub use search::query_terms::TextQuery;
pub use search::staged::{StagedDoc, StagedView};
