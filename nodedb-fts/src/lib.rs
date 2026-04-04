pub mod analyzer;
pub mod backend;
pub mod block;
pub mod bm25;
pub mod codec;
pub mod fuzzy;
pub mod highlight;
pub mod index;
pub mod posting;
pub mod search;

pub use analyzer::{
    AnalyzerRegistry, EdgeNgramAnalyzer, KeywordAnalyzer, LanguageAnalyzer, NgramAnalyzer,
    SimpleAnalyzer, StandardAnalyzer, SynonymMap, TextAnalyzer, analyze,
};
pub use backend::FtsBackend;
pub use block::{CompactPosting, PostingBlock};
pub use codec::DocIdMap;
pub use fuzzy::{fuzzy_discount, fuzzy_match, levenshtein, max_distance_for_length};
pub use index::FtsIndex;
pub use posting::{Bm25Params, MatchOffset, Posting, QueryMode, TextSearchResult};
