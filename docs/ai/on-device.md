# On-Device AI

NodeDB-Lite runs the full engine suite — vector search, BM25, graph traversal, KV — embedded in your app. Same HNSW index, same query API, sub-millisecond latency, zero network calls. Available as a native library (iOS/Android), WASM module (browsers), and desktop library.

## Local Vector Search

NodeDB-Lite uses the same HNSW implementation as the server. Queries run in-process with no serialization overhead.

```sql
-- Same API as the server
CREATE COLLECTION notes TYPE document;
CREATE VECTOR INDEX idx_notes_embedding ON notes METRIC cosine DIM 384;

INSERT INTO notes {
    id: 'note-001',
    content: 'Meeting notes from sprint review...',
    embedding: [0.12, -0.34, 0.56, ...]
};

-- Search locally — sub-ms latency on modern phones
SEARCH notes USING VECTOR(embedding, $query_embedding, 5);
```

**Performance characteristics:**

- 10K vectors, 384 dims: < 1ms search on iPhone 15 / Pixel 8
- 100K vectors, 384 dims: 2-5ms search
- 1M vectors: use SQ8 quantization to fit in mobile RAM (384 dims _ 1M _ 1 byte = 384 MB)

## Offline RAG

Run a complete RAG pipeline locally without any network access. Your app embeds text using a local model (e.g., ONNX Runtime, MLC-LLM) and stores chunks in NodeDB-Lite.

```sql
-- Store chunks with locally-computed embeddings
INSERT INTO knowledge {
    id: $chunk_id,
    content: $chunk_text,
    embedding: $local_embedding,
    source: 'user-manual.pdf',
    page: 42
};

-- Offline search — works in airplane mode
SELECT content, source, page, vector_distance(embedding, $query_embedding) AS score
FROM knowledge
WHERE embedding <-> $query_embedding
LIMIT 5;

-- Hybrid offline search: vector + keyword
-- The keyword leg reads only fields a search index declares.
CREATE SEARCH INDEX ON knowledge FIELDS content;

SELECT content, rrf_score(
    vector_distance(embedding, $query_embedding),
    bm25_score(content, $keywords),
    60, 60
) AS score
FROM knowledge
ORDER BY score DESC
LIMIT 5;
```

**Local embedding models:** Use ONNX Runtime Mobile (all-MiniLM-L6-v2, 22MB) or MLC-LLM for on-device inference. NodeDB-Lite doesn't generate embeddings — it stores and searches them.

## CRDT Sync

NodeDB-Lite uses Loro CRDTs for bidirectional sync with a NodeDB Origin server. New embeddings computed locally sync to the cloud when connectivity returns. Cloud updates (new documents, re-embedded vectors) sync back to the device.

```
Device (NodeDB-Lite)              Cloud (NodeDB Origin)
    |                                    |
    |-- local insert (offline) --------->|  (queued)
    |-- local insert (offline) --------->|  (queued)
    |                                    |
    |====== connectivity restored =======|
    |                                    |
    |-- CRDT delta sync (bidirectional)->|
    |<- CRDT delta sync (bidirectional)--|
    |                                    |
    |-- new cloud docs synced locally -->|
```

```sql
-- On device: insert locally (works offline)
INSERT INTO shared_notes {
    id: $note_id,
    content: $text,
    embedding: $embedding,
    author: $user_id
};

-- Sync happens automatically via WebSocket when online.
-- Conflicts are resolved by CRDT merge rules (last-writer-wins by default,
-- or custom conflict policies per collection).

-- On device: query includes both local and synced data
SELECT content, vector_distance(embedding, $query_embedding) AS score
FROM shared_notes
WHERE embedding <-> $query_embedding
LIMIT 10;
```

**Sync is incremental.** Only deltas are transferred — not full snapshots. A device that's been offline for a week catches up by replaying only the changes, not the entire dataset.

## WASM Deployment

NodeDB-Lite compiles to WebAssembly. Run semantic search in the browser with no backend.

```javascript
// Load NodeDB-Lite WASM module
import { NodeDB } from "nodedb-lite-wasm";

const db = await NodeDB.open("my-search-app");

// Create collection and index
await db.exec(`CREATE COLLECTION docs TYPE document`);
await db.exec(
  `CREATE VECTOR INDEX idx_docs_embedding ON docs METRIC cosine DIM 384`,
);

// Load a static snapshot (pre-built dataset)
await db.loadSnapshot("/data/docs-snapshot.ndb");

// Search in the browser — no backend needed
const results = await db.query(
  `SEARCH docs USING VECTOR(embedding, ARRAY[${queryVector.join(",")}], 10)`,
);
```

**Use cases:**

- Documentation search widgets (embed in any website)
- Demo applications (show vector search without a server)
- Privacy-sensitive search (data never leaves the browser)
- Offline-capable progressive web apps

**Static snapshots:** Pre-build a dataset on your server, export as a snapshot file, host on a CDN. The WASM module loads the snapshot at startup — no live database connection needed.

## Privacy Model

All data stays on the device by default. Sync is opt-in and explicit.

- **Local-only collections** never leave the device. No sync, no telemetry, no cloud storage.
- **Synced collections** replicate via CRDT to the Origin server. You control which collections sync and which stay local.
- **Encryption at rest:** NodeDB-Lite supports AES-256-GCM encryption for the local database file. The key is managed by your app (iOS Keychain, Android Keystore, or WASM Web Crypto).

```sql
-- Create a local-only collection (never synced)
CREATE COLLECTION private_notes TYPE document;

-- Create a synced collection (replicates to cloud)
CREATE COLLECTION shared_docs TYPE document WITH sync = true;
```

There is no "phone home" behavior. NodeDB-Lite is a library embedded in your app — it makes no network calls unless your app explicitly initiates sync.
