// SPDX-License-Identifier: Apache-2.0

//! Full-text search operations dispatched to the Data Plane.

use nodedb_types::text_search::QueryMode;
use nodedb_types::{QualifiedCollection, SurrogateBitmap};

/// One per-row BM25 score column: `bm25_score(field, query)` under `alias`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TextScoreSpec {
    /// `None` reads the whole-document index.
    pub field: Option<String>,
    pub query: String,
    /// Boolean combination of the query terms.
    pub mode: QueryMode,
    /// Fuzzy (Levenshtein) fallback for a term with no exact posting.
    pub fuzzy: bool,
    /// Output column the score lands in. A row the scoped index holds but
    /// the query does not match carries `0.0` there. A row the index does
    /// not hold carries `null`.
    pub alias: String,
}

/// The order a bounded score scan keeps its best rows in: one score column.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ScoreScanOrder {
    /// Alias of the score column the rows are ordered by.
    pub alias: String,
    pub ascending: bool,
    /// Whether `null` scores sort before every number.
    pub nulls_first: bool,
}

/// The row bound of a score scan.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ScoreScanBound {
    /// Rows the scan returns at most.
    pub rows: usize,
    /// `Some`: the scan returns the first `rows` rows in this order. `None`:
    /// any `rows` admitted rows.
    pub order: Option<ScoreScanOrder>,
}

/// Full-text search physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TextOp {
    /// BM25 full-text search on the inverted index.
    Search {
        collection: QualifiedCollection,
        /// Field index the query reads. `None` reads the whole-document index.
        field: Option<String>,
        query: String,
        /// Hits returned, best first. `usize::MAX` returns every match.
        top_k: usize,
        /// Boolean combination of the query terms.
        mode: QueryMode,
        /// Enable fuzzy matching (Levenshtein) for typo tolerance.
        fuzzy: bool,
        /// Pre-computed bitmap of eligible surrogates (from prefilter evaluation).
        /// `None` = no prefilter; all postings are eligible.
        prefilter: Option<SurrogateBitmap>,
        /// Residual WHERE predicates (serialized `Vec<ScanFilter>`). They
        /// restrict candidates before ranking, so `top_k` counts only rows
        /// that satisfy them.
        filters: Vec<u8>,
        /// RLS filters (serialized `Vec<ScanFilter>`). Like `filters`, they
        /// restrict candidates before ranking, so `top_k` counts only rows
        /// the policy admits.
        rls_filters: Vec<u8>,
        /// Score columns injected into each hit.
        scores: Vec<TextScoreSpec>,
    },

    /// Every row `filters` admit, each with its score columns.
    ///
    /// The physical plan for `bm25_score(field, term)` with no `text_match`:
    /// every admitted row is present. A row the score's index holds but its
    /// query does not match carries `0.0`. A row the index does not hold
    /// carries `null`.
    BM25ScoreScan {
        collection: QualifiedCollection,
        /// Residual WHERE predicates (serialized `Vec<ScanFilter>`).
        filters: Vec<u8>,
        /// RLS filters (serialized `Vec<ScanFilter>`). A row that fails one
        /// is dropped.
        rls_filters: Vec<u8>,
        /// Score columns injected into each row.
        scores: Vec<TextScoreSpec>,
        /// The query's LIMIT pushed into the scan. `None` returns every
        /// admitted row. The relational tail still applies its own ORDER BY
        /// and LIMIT over the rows returned.
        bound: Option<ScoreScanBound>,
    },

    /// Exact phrase search: all terms must appear consecutively in the document.
    ///
    /// Unlike `Search` (BM25 scoring), phrase search returns only documents
    /// where the query terms appear as an exact contiguous sequence. Scoring
    /// is positional: documents with the phrase closer to the start rank higher.
    PhraseSearch {
        collection: QualifiedCollection,
        /// Field index the phrase reads. `None` reads the whole-document index.
        field: Option<String>,
        /// Ordered sequence of terms to match as a phrase.
        terms: Vec<String>,
        /// Hits returned, best first. `usize::MAX` returns every match.
        top_k: usize,
        /// Pre-computed bitmap of eligible surrogates (from prefilter evaluation).
        prefilter: Option<nodedb_types::SurrogateBitmap>,
        /// Residual WHERE predicates (serialized `Vec<ScanFilter>`), applied
        /// before ranking.
        filters: Vec<u8>,
        /// RLS filters (serialized `Vec<ScanFilter>`), applied before
        /// ranking.
        rls_filters: Vec<u8>,
        /// Score columns injected into each hit.
        scores: Vec<TextScoreSpec>,
    },

    /// Hybrid search: vector similarity + BM25 text, fused via RRF.
    HybridSearch {
        collection: QualifiedCollection,
        /// Vector column the vector leg searches.
        vector_field: String,
        query_vector: Vec<f32>,
        /// Field index the text leg reads. `None` reads the whole-document index.
        text_field: Option<String>,
        query_text: String,
        /// Residual WHERE predicates (serialized `Vec<ScanFilter>`). They
        /// restrict both legs before fusion.
        filters: Vec<u8>,
        top_k: usize,
        ef_search: usize,
        /// Boolean combination of the text leg's query terms.
        mode: QueryMode,
        fuzzy: bool,
        /// Weight for vector results in RRF (0.0–1.0). Default: 0.5.
        vector_weight: f32,
        filter_bitmap: Option<SurrogateBitmap>,
        /// RLS post-fusion filters.
        rls_filters: Vec<u8>,
        /// SELECT-list alias the response should use for the RRF score
        /// column. `None` means the executor uses the fixed internal name
        /// `rrf_score`. Set by the planner from the SELECT alias for the
        /// `rrf_score(...)` call.
        score_alias: Option<String>,
    },

    /// Index a document into the inverted FTS index.
    ///
    /// Used by the sync path when a Lite client sends an `FtsIndex` frame.
    /// Origin assigns a surrogate for `(collection, doc_id)` on the Control
    /// Plane before dispatch; `surrogate` is the pre-assigned value.
    FtsIndexDoc {
        collection: QualifiedCollection,
        /// Pre-assigned global surrogate for `(collection, doc_id)`.
        surrogate: nodedb_types::Surrogate,
        /// `(field, text)` per top-level string field. Empty removes the
        /// document from every index.
        fields: Vec<(String, String)>,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    },

    /// Remove a document from the inverted FTS index.
    ///
    /// Used by the sync path when a Lite client sends an `FtsDelete` frame.
    FtsDeleteDoc {
        collection: QualifiedCollection,
        /// The global surrogate `(collection, doc_id)` is bound to. `None`
        /// when the key's home binds none: the delete removes nothing and
        /// still commits its sync provenance.
        surrogate: Option<nodedb_types::Surrogate>,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    },

    /// Three-source hybrid search: vector + BM25 text + graph BFS, fused via weighted RRF.
    ///
    /// Extends `HybridSearch` with an optional graph BFS leg. The graph leg
    /// performs a BFS from `graph_seed_id` up to `graph_depth` hops, filtering
    /// edges by `graph_edge_label` when set. All three ranked lists are passed
    /// to `reciprocal_rank_fusion_weighted` with per-source k-constants.
    HybridSearchTriple {
        collection: QualifiedCollection,
        /// Vector column the vector leg searches.
        vector_field: String,
        query_vector: Vec<f32>,
        /// Field index the text leg reads. `None` reads the whole-document index.
        text_field: Option<String>,
        query_text: String,
        /// Residual WHERE predicates (serialized `Vec<ScanFilter>`). They
        /// restrict every leg before fusion.
        filters: Vec<u8>,
        /// Node id used as the BFS seed for the graph leg.
        graph_seed_id: String,
        /// Maximum BFS depth from the seed node.
        graph_depth: usize,
        /// Edge label filter for graph BFS. `None` = all edges.
        graph_edge_label: Option<String>,
        top_k: usize,
        ef_search: usize,
        /// Boolean combination of the text leg's query terms.
        mode: QueryMode,
        fuzzy: bool,
        /// Per-source RRF k constants: (vector_k, text_k, graph_k).
        rrf_k: (f64, f64, f64),
        filter_bitmap: Option<SurrogateBitmap>,
        /// RLS post-fusion filters.
        rls_filters: Vec<u8>,
        /// SELECT-list alias for the fused RRF score column.
        score_alias: Option<String>,
    },

    /// Bind a collection's per-collection FTS analyzer.
    ///
    /// Persists to backend metadata (`InvertedIndex::set_collection_analyzer`)
    /// so `InvertedIndex::analyze_for_collection` resolves it for every
    /// subsequent tokenization of the collection's text — forward indexing,
    /// the in-transaction staged-write overlay, and query-time scoring alike.
    /// Config-only, single-node, non-WAL-durable — the same dispatch shape
    /// `VectorOp::SetParams` uses for `CREATE VECTOR INDEX`.
    SetTextConfig {
        collection: QualifiedCollection,
        /// Analyzer name, already checked against
        /// `nodedb_fts::index::analyzer_config::analyzer_exists` by the DDL
        /// layer (e.g. "standard", "english", "japanese"). `None` leaves the
        /// collection's current analyzer in place.
        analyzer_name: Option<String>,
        /// Whether searches over this collection fall back to fuzzy matching
        /// by default. `None` leaves the current setting in place.
        fuzzy_default: Option<bool>,
    },
}
