# Full-Text Search

NodeDB's full-text search engine provides Block-Max WAND (BMW) optimized BM25 ranking with 27-language support, CJK bigram tokenization, fuzzy matching, and native hybrid fusion with vector search — all in a single query. The engine is implemented in the shared `nodedb-fts` crate, used identically by Origin (server), Lite (embedded), and WASM.

## When to Use

- Text search across documents (articles, products, logs)
- Search-as-you-type with fuzzy matching
- Multilingual content search (including CJK, Arabic, Hindi)
- Hybrid retrieval: combine keyword matching with semantic similarity
- Per-field weighted scoring (title vs body)

## Key Features

- **Block-Max WAND (BMW) scoring** — Two-level skip optimization: WAND pivot selection across terms + block-level pruning via precomputed upper bounds. Skips entire 128-doc blocks that can't beat the current top-k threshold.
- **BM25 ranking** — Standard relevance scoring with term frequency, inverse document frequency, and SmallFloat fieldnorm-aware length normalization (4x space reduction).
- **16 Snowball stemmers** — Arabic, Danish, Dutch, English, Finnish, French, German, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Turkish.
- **27-language stop words** — Per-language sorted stop word lists (always compiled in, ~50KB total). Covers: English, German, French, Spanish, Italian, Portuguese, Dutch, Swedish, Norwegian, Danish, Finnish, Romanian, Russian, Turkish, Hungarian, Czech, Polish, Greek, Arabic, Hebrew, Hindi, Chinese, Japanese, Korean, Thai, Vietnamese, Indonesian.
- **CJK tokenization** — Overlapping character bigrams for Chinese/Japanese/Korean (always available, zero dictionary dependency). Optional feature-gated dictionary segmentation: `lang-ja` (lindera/IPADIC), `lang-zh` (jieba-rs), `lang-ko` (lindera/ko-dic), `lang-th` (icu_segmenter).
- **Korean Hangul decomposition** — Decomposes Hangul syllables into Jamo components for morphological matching.
- **AND-first with OR fallback** — Tries AND semantics first; if zero results, falls back to OR with a coverage penalty (`matched_terms / total_terms`).
- **Phrase proximity boost** — Consecutive query tokens at consecutive positions get up to 3x score boost.
- **Per-collection analyzer binding** — Each collection can configure its analyzer and language. Applied consistently at both index time and query time, eliminating mismatch bugs.
- **Field-aware BM25** — Weighted multi-field scoring: `final_score = Σ(weight_i × bm25(field_i))`. Title matches score higher than body matches.
- **Fuzzy matching** — Levenshtein distance-based matching with adaptive thresholds (1 edit for 4-6 chars, 2 for 7+).
- **Synonyms** — Define synonym groups for query-time expansion.
- **Highlighting** — Return matched terms with byte offsets or wrapped in custom tags.
- **N-gram and edge-ngram** — Partial-word matching for autocomplete and substring search.
- **Posting compression** — Delta-encoded, variable-width bitpacked doc IDs and frequencies with SIMD-accelerated unpacking (SSE2 on x86_64, NEON on AArch64, scalar fallback).
- **LSM storage** — In-memory memtable with configurable spill thresholds, immutable compressed segments, level-based compaction (8×8 tiering), and parallel index build.
- **Hybrid vector fusion** — Reciprocal Rank Fusion (RRF) merges BM25 text results with vector similarity in one query.

## Examples

```sql
-- Create a full-text index
CREATE COLLECTION articles TYPE document;
CREATE SEARCH INDEX ON articles FIELDS title, body
    ANALYZER 'english'
    FUZZY true;

-- Basic search
SELECT title, bm25_score(body, 'distributed database rust') AS score
FROM articles
WHERE text_match(body, 'distributed database rust')
ORDER BY score DESC
LIMIT 20;

-- Fuzzy search (handles typos)
SELECT title FROM articles
WHERE text_match(title, 'databse', { fuzzy: true, distance: 2 });

-- Hybrid search: BM25 + vector similarity (RRF fusion)
SELECT title, rrf_score() AS score
FROM articles
WHERE text_match(body, 'distributed systems')
  AND embedding <-> $query_vec
LIMIT 10;

-- Highlighting
SELECT title, search_highlight(body, 'graph traversal') AS snippet
FROM articles
WHERE text_match(body, 'graph traversal')
LIMIT 10;

-- Search with synonyms
CREATE SYNONYM GROUP db_terms AS ('database', 'db', 'datastore');

SELECT title FROM articles
WHERE text_match(body, 'db performance');
-- Also matches "database performance" and "datastore performance"

-- Per-collection analyzer (German)
ALTER COLLECTION german_articles SET text_analyzer = 'german';

-- CJK search (bigram tokenization automatic for CJK codepoints)
SELECT title FROM articles
WHERE text_match(body, '全文検索');
```

## Analyzers

Full-text indexes use analyzers to process text. An analyzer is a pipeline: Unicode normalization → lowercase → split → stop word removal → stemming. CJK text is automatically routed to the bigram tokenizer regardless of the configured analyzer.

| Analyzer                        | What it does                                                                                 |
| ------------------------------- | -------------------------------------------------------------------------------------------- |
| `standard`                      | English pipeline: NFD normalize, lowercase, split, English stop words, Snowball English stem |
| `simple`                        | Lowercase + whitespace split. No stemming or stop words.                                     |
| `keyword`                       | Entire input as a single lowercase token.                                                    |
| `cjk_bigram`                    | CJK bigram tokenization for all CJK scripts.                                                 |
| `ngram` / `ngram:2:4`           | Character n-grams (configurable min:max).                                                    |
| `edge_ngram` / `edge_ngram:2:5` | Prefix-anchored n-grams for autocomplete.                                                    |

### Language-Specific Analyzers

Set via `ALTER COLLECTION ... SET text_analyzer = '<name>'`:

| Languages (Snowball stemming)                                                                                                                    | Codes                                                                                          |
| ------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| Arabic, Danish, Dutch, English, Finnish, French, German, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Turkish | `ar`, `da`, `nl`, `en`, `fi`, `fr`, `de`, `hu`, `it`, `no`, `pt`, `ro`, `ru`, `es`, `sv`, `tr` |

| Languages (stop words only, no stemmer)                     | Codes                                    |
| ----------------------------------------------------------- | ---------------------------------------- |
| Czech, Greek, Hebrew, Hindi, Indonesian, Polish, Vietnamese | `cs`, `el`, `he`, `hi`, `id`, `pl`, `vi` |

| CJK Languages (bigram default, dictionary segmentation feature-gated) | Codes | Feature gate               |
| --------------------------------------------------------------------- | ----- | -------------------------- |
| Chinese                                                               | `zh`  | `lang-zh` (jieba-rs)       |
| Japanese                                                              | `ja`  | `lang-ja` (lindera/IPADIC) |
| Korean                                                                | `ko`  | `lang-ko` (lindera/ko-dic) |
| Thai                                                                  | `th`  | `lang-th` (icu_segmenter)  |

Without the feature gate, CJK/Thai text falls back to character bigrams (high recall, zero dictionary dependency).

## Internals

### Query Algorithm: Block-Max WAND

Posting lists are split into 128-document blocks. Each block stores precomputed metadata: `block_max_tf` and `block_min_fieldnorm`. During scoring:

1. **WAND pivot selection**: Terms sorted by current doc_id. Accumulate per-term `max_score` until sum exceeds the k-th best score. The pivot doc_id is selected.
2. **Block-level pruning**: Before decompressing a block, compute upper bound from block metadata. Skip if it can't beat the current threshold.
3. **Top-k min-heap**: Fixed-capacity binary heap. Threshold = heap root when full.

Documents are scored using u32 integer IDs throughout (assigned via DocIdMap). String doc IDs are resolved only for the final top-k results (deferred resolution).

### Posting Compression

- **Delta encoding**: Sorted doc IDs stored as deltas (first absolute, rest as differences).
- **Variable-width bitpacking**: Deltas and frequencies packed at the minimum required bit width. 3-byte header: `[count: u16][bit_width: u8]`.
- **SIMD unpack**: Runtime dispatch to SSE2 (x86_64) or NEON (AArch64). Processes 4 values per iteration. Scalar fallback on all platforms.
- **SmallFloat fieldnorms**: Document lengths quantized to 1 byte (3-bit mantissa + exponent). 4x space reduction, <25% error dampened by BM25's `b=0.75`.

### LSM Architecture

Writes accumulate in an in-memory memtable (`HashMap<String, Vec<CompactPosting>>`). When the memtable exceeds a configurable threshold (default 32M posting entries or 100K unique terms), it is flushed to an immutable compressed segment stored via the backend. Queries merge the active memtable with all persisted segments.

Segments use level-based compaction (8 levels, 8 segments per level). When a level fills, all segments are N-way merged into one segment at the next level.

## Hybrid Search

The most powerful feature is combining BM25 with vector search. This handles the common RAG problem where keyword matching catches exact terms that embeddings miss, and embeddings catch semantic meaning that keywords miss.

NodeDB fuses both result sets using Reciprocal Rank Fusion (RRF) — no application-level merging needed. The fusion happens inside the query engine in a single pass.

## Related

- [Vector Search](vectors.md) — Hybrid vector + text fusion
- [Graph](graph.md) — Graph context can further refine search results (GraphRAG)

[Back to docs](README.md)
