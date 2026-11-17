# RAG Pipeline Patterns

Retrieval-Augmented Generation (RAG) uses vector search to find relevant context, then feeds it to an LLM. NodeDB handles the retrieval half — store chunks, search semantically, return context. The LLM call happens in your application.

## Basic RAG

Store pre-chunked documents with embeddings. Search by vector similarity, return top-K chunks as LLM context.

```sql
-- 1. Create a collection for document chunks
CREATE COLLECTION chunks TYPE document;
CREATE VECTOR INDEX idx_chunks_embedding ON chunks METRIC cosine DIM 1536;

-- 2. Insert chunks (your app chunks and embeds externally)
INSERT INTO chunks {
    id: 'chunk-001',
    content: 'Raft consensus requires a quorum of nodes to agree...',
    embedding: [0.12, -0.34, 0.56, ...],
    source_doc: 'design-doc-v3.pdf',
    page: 12,
    token_count: 342
};

-- 3. Retrieve relevant chunks
SELECT id, content, source_doc, page, vector_distance(embedding, $query_embedding) AS score
FROM chunks
WHERE embedding <-> $query_embedding
LIMIT 5;
-- Your app sends these chunks as context to the LLM
```

**Embedding model choice is yours.** NodeDB stores whatever vectors you provide — OpenAI, Cohere, local GGUF, any dimension. Track which model produced which vectors with [embedding model metadata](../vectors.md):

```sql
ALTER COLLECTION chunks SET VECTOR METADATA ON embedding (
    model = 'text-embedding-3-large',
    dimensions = 1536
);
```

## Hybrid RAG (Vector + BM25)

Vector search finds semantically similar chunks. BM25 finds keyword matches. Combining both via RRF catches what either alone would miss — especially for domain-specific terminology that embedding models may not handle well.

```sql
-- The BM25 leg reads only fields a search index declares.
CREATE SEARCH INDEX ON chunks FIELDS content;

-- Hybrid retrieval: vector similarity + keyword match
SELECT id, content, source_doc, rrf_score(
    vector_distance(embedding, $query_embedding),
    bm25_score(content, $user_question),
    60, 60
) AS score
FROM chunks
WHERE tenant_id = $tenant
ORDER BY score DESC
LIMIT 10;
```

**Tuning guidance:**

- Start with equal weights (k=60 for both). This is the standard RRF smoothing constant.
- If your queries are mostly natural language questions, bias toward vector (lower k for vector = higher weight).
- If your queries contain exact terms (error codes, function names, product SKUs), bias toward BM25.
- For technical documentation, hybrid with equal weights typically outperforms either signal alone by 10-15% on MRR.

**With the new sparse vector support**, you can add SPLADE-style learned sparse retrieval as a third signal:

```sql
-- Uses the search index on chunks (content) declared above.
SELECT id, content, rrf_score(
    vector_distance(embedding, $dense_query),
    bm25_score(content, $text_query),
    60, 60
) AS score
FROM chunks
ORDER BY score DESC
LIMIT 10;
-- sparse_score can be added as a third RRF input when SPLADE embeddings are available
```

## Filtered RAG

Real applications always filter by metadata — tenant, category, date range, access level. NodeDB evaluates filters during HNSW traversal, not after, so filtered search is almost as fast as unfiltered.

```sql
-- Filter by category + date range, then vector search
SELECT id, content, vector_distance(embedding, $query_embedding) AS score
FROM chunks
WHERE category = 'engineering'
  AND created_at > '2025-01-01'
  AND embedding <-> $query_embedding
LIMIT 10;

-- Multi-tenant: tenant_id is enforced at the WAL level
SELECT id, content, vector_distance(embedding, $query_embedding) AS score
FROM chunks
WHERE tenant_id = $tenant
  AND source_doc = 'api-reference'
  AND embedding <-> $query_embedding
LIMIT 10;
```

The adaptive pre-filtering engine automatically selects the best strategy:

- **Pre-filter** (<50% of docs filtered out): bitmap during HNSW traversal
- **Post-filter** (50-95% filtered): over-fetch k\*10, then filter
- **Brute-force** (>95% filtered): skip HNSW, scan matching IDs only

## Parent-Document Retrieval

Store small chunks for precise embedding, but return the parent document (or a larger context window) to the LLM. This gives the LLM enough context to generate coherent answers.

```sql
-- Schema: each chunk references its parent document
INSERT INTO chunks {
    id: 'chunk-042',
    content: 'The consensus algorithm ensures...',
    embedding: [0.12, ...],
    parent_doc_id: 'doc-007',
    chunk_index: 3
};

-- Store parent documents separately (full text)
INSERT INTO documents {
    id: 'doc-007',
    title: 'Distributed Consensus Design',
    full_text: '... entire document ...',
    summary: 'Architecture document covering Raft consensus...'
};

-- Search chunks, then look up parent docs
-- Step 1: find relevant chunks
SELECT id, parent_doc_id, vector_distance(embedding, $query_embedding) AS score
FROM chunks
WHERE embedding <-> $query_embedding
LIMIT 5;

-- Step 2: fetch parent documents (your app does the join)
SELECT id, title, full_text
FROM documents
WHERE id IN ('doc-007', 'doc-012', ...);
```

**Alternative: store surrounding chunks.** If you want a sliding window around the matched chunk:

```sql
-- Get matched chunk + neighbors (same parent, adjacent chunk_index)
SELECT c2.content
FROM chunks c1
JOIN chunks c2 ON c1.parent_doc_id = c2.parent_doc_id
WHERE c1.id = $matched_chunk_id
  AND c2.chunk_index BETWEEN c1.chunk_index - 1 AND c1.chunk_index + 1
ORDER BY c2.chunk_index;
```

## Conversational RAG

For multi-turn conversations, use the conversation history to improve retrieval. Embed the full conversation context (not just the latest message) and retrieve chunks relevant to the ongoing discussion.

```sql
-- Store conversation turns with embeddings
CREATE COLLECTION conversations TYPE strict (
    id         STRING PRIMARY KEY,
    session_id STRING NOT NULL,
    role       STRING NOT NULL,
    content    STRING NOT NULL,
    embedding  VECTOR(1536),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert each turn
INSERT INTO conversations VALUES (
    $turn_id, $session_id, 'user', $user_message, $turn_embedding, NOW()
);

-- Retrieve: search all chunks using a conversation-aware embedding
-- (Your app creates $context_embedding from the full conversation history,
--  not just the last message. This improves retrieval relevance significantly.)
SELECT id, content, vector_distance(embedding, $context_embedding) AS score
FROM chunks
WHERE embedding <-> $context_embedding
LIMIT 10;

-- Also retrieve recent conversation turns for the LLM's context window
SELECT role, content
FROM conversations
WHERE session_id = $session
ORDER BY created_at DESC
LIMIT 10;
```

**Tip:** Embed the concatenation of the last 3-5 turns (not just the latest question). This gives the embedding model conversation context, which dramatically improves retrieval for follow-up questions like "What about the second one?" or "Can you explain that further?"
