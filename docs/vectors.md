# Vector Search

NodeDB's vector engine is built for production semantic search — not vectors shoehorned through a SQL planner. It uses a custom HNSW index with multiple quantization levels and hardware-accelerated distance math.

## When to Use

- Semantic search over embeddings (text, images, audio)
- RAG pipelines for AI agents
- Recommendation systems
- Similarity matching at scale

## Key Features

- **Indexes**: HNSW (in-memory hierarchical) + Vamana / DiskANN (SSD-resident flat-beam, billion-scale on one node). The cost-model planner picks based on collection size + workload signals.
- **Quantization frontier**:
  - **SQ8** — Scalar quantization to 8-bit. ~4× memory reduction, minimal recall loss.
  - **PQ** — Classic product quantization codec. ~4-8× / ~16 bytes/vector for very large indexes.
  - **OPQ** — PQ + learned random rotation; minor accuracy bump over plain PQ.
  - **Binary** — Sign-bit Hamming for ultra-cold tiers (no rerank).
  - **Ternary** — BitNet 1.58 trit-pack `{-1, 0, +1}` with cold/hot pack and AVX-512 popcnt.
  - **RaBitQ** — 1-bit quantization with `O(1/√D)` error bound (SIGMOD 2024). Frontier 1-bit primary path.
  - **BBQ** — Centroid-asymmetric 1-bit + 14-byte corrective factors + oversample rerank.
- **Filtered traversal**: NaviX adaptive-local (VLDB 2025) — per-hop switch between standard / directed / blind heuristics by local selectivity. Replaces classic ACORN-1.
- **Workload routing**: SIEVE — pre-built specialized HNSW subindices for stable predicates (e.g. `tenant_id`); planner-routed filtered queries.
- **Multi-vector**: MetaEmbed Matryoshka multivec + ColBERT MaxSim + budgeted PLAID (ICLR 2026); `meta_token_budget` query option drives test-time scaling.
- **Adaptive-dim**: Matryoshka coarse-to-fine on first-N dims of MRL embeddings via `query_dim`.
- **Streaming updates**: SPFresh + LIRE topology-aware local rebalancing (SOSP 2023) — no full-rebuild stalls.
- **Adaptive pre-filtering**: Roaring Bitmap with automatic pre / post / brute-force strategy selection by selectivity.
- **Distance metrics**: L2, cosine, negative inner product, Hamming/Jaccard for binary.
- **Cross-engine fusion**: Combine with graph ([GraphRAG](graph.md)), BM25 ([hybrid](full-text-search.md)), spatial ([geo-aware](spatial.md)), array slices, or document predicates — all via roaring-bitmap intersection of cross-engine surrogate IDs.

## Examples

```sql
-- Create a collection with a vector index (vector as side-index on a document collection)
CREATE COLLECTION articles;
CREATE VECTOR INDEX idx_articles_embedding ON articles METRIC cosine DIM 384;

-- Insert documents with embeddings
INSERT INTO articles {
    title: 'Understanding Transformers',
    embedding: [0.12, -0.34, 0.56, ...]
};

-- Nearest neighbor search
SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, -0.2, ...], 10);

-- Filtered vector search (adaptive pre-filtering kicks in)
SELECT title, vector_distance(embedding, ARRAY[0.1, 0.3, -0.2, ...]) AS score
FROM articles
WHERE category = 'machine-learning'
  AND id IN (
    SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, -0.2, ...], 10)
  );

-- Hybrid vector + full-text search (RRF fusion)
SELECT title, rrf_score() AS score
FROM articles
WHERE id IN (
    SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, ...], 10)
  )
  AND text_match(body, 'transformer attention mechanism')
LIMIT 10;
```

## ANN Tuning (Named Arguments)

`vector_distance(field, query, name => value, ...)` accepts a closed set of typed named arguments. Validation is strict — unknown names, duplicate keys, positional 3rd args, or `=` instead of `=>` all return typed errors.

| Argument            | Type   | Notes                                                                                       |
| ------------------- | ------ | ------------------------------------------------------------------------------------------- |
| `quantization`      | string | `none`, `sq8`, `pq`, `binary`, `ternary`, `rabitq`, `bbq`, `opq`                            |
| `oversample`        | u8     | Candidates fetched before rerank. Default `3`. Final rerank set = `oversample × ef_search`. |
| `query_dim`         | u32    | Coarse-to-fine on first-N dims of Matryoshka embeddings.                                    |
| `meta_token_budget` | u8     | MetaEmbed multivec MaxSim / PLAID budget.                                                   |
| `ef_search`         | u32    | HNSW / Vamana beam width. Default `64`.                                                     |
| `target_recall`     | f32    | Adaptive recall target — cost-model planner picks `oversample` and `ef_search` to hit it.   |

Set `target_recall` and let the planner do the rest unless you need a hard latency ceiling.

## Sparse Vectors

For learned sparse representations (SPLADE, uniCOIL, BM25-style term weights), declare a dimensionless `SPARSEVECTOR` column. An inverted index over the non-zero dimensions is maintained automatically on every document write.

```sql
CREATE TABLE sparse_docs (id TEXT PRIMARY KEY, terms SPARSEVECTOR);

-- Values are '{dimension: weight}' string literals
INSERT INTO sparse_docs (id, terms) VALUES ('a', '{3: 1.0, 7: 1.0}');

-- Dot-product top-k; LIMIT sets k
SELECT id FROM sparse_docs
ORDER BY sparse_score(terms, '{3: 1.0, 7: 0.5}') DESC
LIMIT 3;
```

`SPARSEVECTOR` carries no fixed dimensionality — only non-zero `(dimension, weight)` pairs are stored and indexed. Documents sharing no dimension with the query are excluded by the inverted index rather than scored at zero. `sparse_score` ranks by descending dot product; it is a standalone surface, separate from the dense `vector_distance` path and the RRF hybrid fusion.

## Vector-Primary Collections

By default, vectors are an _index_ attached to a column on a normal collection — the document store is the source of truth. For pure-vector workloads (RAG corpora, recommendation memory, embedding stores) flip a collection into vector-primary mode where the vector index is the primary access path and the document store is a metadata sidecar:

```sql
CREATE COLLECTION corpus (
    id UUID DEFAULT gen_uuid_v7(),
    embedding FLOAT[384],
    title TEXT,
    tenant_id UUID,
    created_at TIMESTAMP DEFAULT now()
) WITH (
    primary='vector',
    vector_field='embedding',
    dim=384,
    metric='cosine',
    quantization='rabitq',
    storage_dtype='bf16',
    m=32,
    ef_construction=200,
    payload_indexes=['tenant_id', 'created_at']
);
```

`primary='vector'` is purely an access-path hint — not a different engine. Cross-engine queries, CRDT sync, SQL semantics all keep working. `payload_indexes` are per-field equality / range / boolean indexes over the metadata sidecar for filtered ANN (replaces Pinecone metadata filters).

`storage_dtype` controls the precision of raw HNSW vector storage (independent of quantization). Accepted values: `F32` (default, 4 bytes/dimension), `F16` (2 bytes/dimension, IEEE half), `BF16` (2 bytes/dimension, bfloat16 — same exponent range as F32, better dynamic range for embeddings, preferred). Using `BF16` or `F16` halves resident vector bytes, reducing memory pressure on large indexes with a small accuracy cost. The HNSW graph structure and search quality remain unaffected.

Default `primary='document'` is unchanged: classic `CREATE VECTOR INDEX ON ...` syntax continues to work.

## Quantization Selection

| Codec     | Bits/dim | Recall (typ.) | Best For                                                 |
| --------- | -------- | ------------- | -------------------------------------------------------- |
| `none`    | 32       | 100%          | Small index (< 1M vectors), latency not critical         |
| `sq8`     | 8        | ~99%          | Balanced default for medium index sizes                  |
| `pq`      | ~2       | ~95%          | Large memory-bound indexes; classic Product Quantization |
| `opq`     | ~2       | ~96%          | PQ + learned random rotation                             |
| `rabitq`  | 1        | ~97%          | Frontier 1-bit with `O(1/√D)` error bound                |
| `bbq`     | 1        | ~98%          | Centroid-asymmetric 1-bit + 14-byte corrective           |
| `binary`  | 1        | ~85%          | Hamming-only, ultra-cold tiers                           |
| `ternary` | 1.58     | ~96%          | BitNet `{-1, 0, +1}` cold/hot pack                       |

## How It Works

Vectors are indexed in the HNSW graph at full precision to maintain structural integrity. During search, the engine traverses quantized copies of the graph for speed, then re-ranks the top candidates against full-precision vectors for accuracy.

When a query includes metadata filters (e.g., `WHERE category = 'ml'`), the engine builds a Roaring Bitmap of matching document IDs and selects the optimal filtering strategy automatically — if the filter is highly selective, it pre-filters before HNSW traversal; if broad, it post-filters after.

## Temporal Vector Search

The Vector engine is an index. It points at records; it does not carry temporal columns itself. To query embeddings at a point in time, attach a Vector index to a Document collection that has `bitemporal = true`. The Document collection holds the embedding payload and temporal columns; the Vector index returns candidate IDs; `AS OF` filtering happens at the Document layer.

See [Bitemporal Queries — Bitemporal Vector Search](bitemporal.md#bitemporal-vector-search) for a worked example.

## Related

- [Graph](graph.md) — GraphRAG combines vector similarity with graph traversal
- [Full-Text Search](full-text-search.md) — Hybrid vector + BM25 fusion
- [Spatial](spatial.md) — Hybrid spatial-vector search
- [Bitemporal Queries](bitemporal.md) — Temporal composition pattern

[Back to docs](README.md)
