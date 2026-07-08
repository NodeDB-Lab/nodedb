# Graph Engine

NodeDB's graph engine uses a native CSR (Compressed Sparse Row) adjacency index with interned node IDs (`u32`) and labels (`u32`) — not recursive JOINs pretending to be a graph. At 1 billion edges, CSR uses ~10 GB vs ~60 GB for naive adjacency lists (6x improvement). Sub-millisecond multi-hop traversals, 13 native algorithms, Cypher-subset pattern matching, and GraphRAG fusion — all in the same process as every other engine.

---

## Storage Model

Edges are persisted in a redb B-Tree with forward and reverse indexes, both keyed by a `(tenant_id, composite)` tuple:

```
Forward Index (EDGES table):
  Key:   (tenant_id: u32, "src\x00edge_label\x00dst")
  Value: edge properties (MessagePack)

Reverse Index (REVERSE_EDGES table):
  Key:   (tenant_id: u32, "dst\x00edge_label\x00src")
  Value: [] (existence check only)
```

Tenant isolation is structural: the tenant id is a first-class key component, not a lexical prefix on node names. Node names in the composite portion are user-visible strings. Both tables update atomically in a single write transaction, and the null-byte separator enables prefix scans for outbound traversal within a tenant.

At query time the in-memory CSR is partitioned by tenant (`ShardedCsrIndex` = one `CsrIndex` per tenant); algorithms and traversals run against a single tenant's partition:

```
CsrIndex (per tenant):
  out_offsets: Vec<u32>   [num_nodes + 1]    — offset into target array per node
  out_targets: Vec<u32>   [num_edges]        — destination node IDs (contiguous)
  out_labels:  Vec<u32>   [num_edges]        — edge labels (parallel array)
  out_weights: Vec<f64>   [num_edges]        — optional, allocated only when weighted

  in_offsets / in_targets / in_labels / in_weights — symmetric for inbound
```

Each `CsrIndex` gets a unique partition tag at construction. Public APIs that return dense node indices hand out `LocalNodeId { id, partition_tag }` — using a node id from one partition with another partition's API panics at the boundary.

Writes go to a mutable buffer and become visible immediately. Compaction merges the buffer into the dense CSR arrays via double-buffered swap when the buffer exceeds 10% of the dense size.

---

## Edge Operations

### Insert Edge

```sql
GRAPH INSERT EDGE IN 'edges' FROM 'users:alice' TO 'users:bob' TYPE 'KNOWS';

-- With properties (JSON string form):
GRAPH INSERT EDGE IN 'edges' FROM 'users:alice' TO 'users:bob' TYPE 'KNOWS'
  PROPERTIES '{"since": 2020, "weight": 0.9}';
-- Object literal form (equivalent):
GRAPH INSERT EDGE IN 'edges' FROM 'users:alice' TO 'users:bob' TYPE 'KNOWS'
  PROPERTIES { since: 2020, weight: 0.9 };
```

The `IN '<collection>'` clause is **required** — edges are overlays on a named document collection, not a global namespace. Statements without `IN` fail to parse.

### Delete Edge

```sql
GRAPH DELETE EDGE IN 'edges' FROM 'users:alice' TO 'users:bob' TYPE 'KNOWS';
```

Removes both forward and reverse index entries atomically. Cascading delete is available for all edges touching a node.

---

## Traversal Queries

### Traverse (Multi-Hop BFS)

```sql
GRAPH TRAVERSE FROM 'users:alice' DEPTH 3;
GRAPH TRAVERSE FROM 'users:alice' DEPTH 2 LABEL 'follows' DIRECTION out;
```

Breadth-first search from a start node. Returns discovered nodes at each depth level.

| Parameter   | Default | Description            |
| ----------- | ------- | ---------------------- |
| `DEPTH`     | 2       | Maximum hop count      |
| `LABEL`     | (any)   | Filter by edge label   |
| `DIRECTION` | out     | `in`, `out`, or `both` |

### Neighbors (1-Hop)

```sql
GRAPH NEIGHBORS OF 'users:bob';
GRAPH NEIGHBORS OF 'users:bob' LABEL 'follows' DIRECTION both;
```

Returns immediate neighbors only — no hop expansion.

### Shortest Path

```sql
GRAPH PATH FROM 'users:alice' TO 'users:charlie';
GRAPH PATH FROM 'users:alice' TO 'users:charlie' MAX_DEPTH 5 LABEL 'knows';
```

Cross-core BFS path finding. Returns an ordered list of node IDs, or empty array if no path exists within `MAX_DEPTH` (default 10).

---

## MATCH Pattern Queries (Cypher Subset)

NodeDB embeds a Cypher-subset pattern engine. MATCH queries arrive through any protocol — pgwire, HTTP, or native MessagePack — and are parsed, optimized, and dispatched to the Data Plane like any other query.

### Syntax

**Node bindings:**

```
(a)            -- named variable, any label
(a:Person)     -- named variable, label filter
(:Person)      -- anonymous node, label filter
()             -- anonymous, any label
```

**Edge bindings with direction:**

```
-[:KNOWS]->        -- outbound
<-[:KNOWS]-        -- inbound
-[:KNOWS]-         -- undirected (both directions)
-[r:KNOWS]->       -- named edge binding
```

**Variable-length paths:**

```
-[:KNOWS*1..3]->   -- 1 to 3 hops (BFS expansion)
-[:KNOWS*2..2]->   -- exactly 2 hops
-[:FOLLOWS*]->     -- unlimited hops
```

**Clauses:**

| Clause                           | Description                                                              |
| -------------------------------- | ------------------------------------------------------------------------ |
| `MATCH`                          | Required. Pattern to match against the graph.                            |
| `OPTIONAL MATCH`                 | LEFT JOIN semantics — preserves rows with no match.                      |
| `WHERE`                          | Filter on bound variables. Supports `=`, `!=`, `<`, `<=`, `>`, `>=`.     |
| `WHERE NOT EXISTS { MATCH ... }` | Anti-join — exclude rows matching a sub-pattern.                         |
| `RETURN`                         | Project bindings. Supports aliases (`AS`). Default: all bound variables. |
| `ORDER BY`                       | Sort results. `ASC` or `DESC`.                                           |
| `LIMIT`                          | Cap result count.                                                        |

Multiple comma-separated patterns in one `MATCH` clause act as self-joins.

### Examples

**Friend-of-friend:**

```sql
MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person)
WHERE a.name = 'Alice'
RETURN b.name, c.name;
```

**Variable-length recommendations:**

```sql
MATCH (u:User)-[:follows*2..3]->(recommended:User)
WHERE u.id = 'you'
RETURN DISTINCT recommended.id
LIMIT 10;
```

**Multi-pattern query (self-join):**

```sql
MATCH (a:Person)-[:works_at]->(company:Company),
      (a)-[:located_in]->(city:City),
      (company)-[:headquartered_in]->(hq_city:City)
WHERE city.id = hq_city.id
RETURN a.name, company.name;
```

**Anti-join (negative pattern):**

```sql
MATCH (a:User)-[:follows]->(b:User)
WHERE NOT EXISTS { MATCH (b)-[:blocked_by]->(a) }
RETURN a.id, b.id;
```

**OPTIONAL MATCH:**

```sql
MATCH (a:Person)-[:knows]->(b:Person)
OPTIONAL MATCH (b)-[:works_at]->(c:Company)
RETURN a.name, b.name, c.name;
```

**Fraud detection pattern:**

```sql
MATCH (a:Account)-[:transfers_to]->(mid:Account)-[:transfers_to]->(b:Account)
WHERE a.risk_score > 0.7 AND b.risk_score > 0.7
RETURN a.id, b.id, count(*) AS paths
ORDER BY paths DESC
LIMIT 20;
```

### Execution

1. **Parse** — Tokenizes Cypher syntax into AST (`MatchQuery`)
2. **Optimize** — Reorders triples by selectivity (bind tightly-connected patterns first)
3. **Serialize** — MessagePack the AST for SPSC transport
4. **Execute** — Data Plane resolves bindings against CSR, expands variable-length paths via BFS, applies WHERE predicates, projects RETURN columns
5. **Return** — Results come back as pgwire rows

---

## Algorithms

All algorithms run directly on the CSR index. SIMD-accelerated where applicable (rank scatter, L1 norm delta, fill operations).

### Running Algorithms

```sql
GRAPH ALGO PAGERANK ON 'social_graph' DAMPING 0.85 ITERATIONS 20 TOLERANCE 1e-7;
GRAPH ALGO PAGERANK ON 'social_graph' DAMPING 0.85 PERSONALIZATION {"alice": 1.0, "bob": 0.5};
GRAPH ALGO WCC ON 'knowledge_graph';
GRAPH ALGO SSSP ON 'routes' FROM 'city:chicago';
GRAPH ALGO COMMUNITY ON 'products' ITERATIONS 10 RESOLUTION 1.0;
GRAPH ALGO BETWEENNESS ON 'network' SAMPLE 500;
GRAPH ALGO KCORE ON 'collaboration';
GRAPH ALGO TRIANGLES ON 'social' MODE global;
GRAPH ALGO DIAMETER ON 'web';
```

### Algorithm Reference

| Algorithm             | What it computes                                                             | Key Parameters                                                                             |
| --------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| **PageRank**          | Node importance via incoming link structure; biased by personalization seeds | `DAMPING` (0.85), `ITERATIONS` (20), `TOLERANCE` (1e-7), `PERSONALIZATION` (optional JSON) |
| **WCC**               | Weakly connected components (union-find with path compression)               | —                                                                                          |
| **Label Propagation** | Community detection via iterative label spreading                            | `ITERATIONS` (10)                                                                          |
| **LCC**               | Local clustering coefficient — how tightly neighbors connect                 | —                                                                                          |
| **SSSP**              | Single-source shortest path (Dijkstra, rejects negative weights)             | `FROM` (required)                                                                          |
| **Betweenness**       | Bridge nodes with high traffic (Brandes' algorithm)                          | `SAMPLE` (optional, for approximation)                                                     |
| **Closeness**         | How close a node is to all others (inverse distance sum)                     | `SAMPLE` (optional)                                                                        |
| **Harmonic**          | Like closeness, but handles disconnected graphs                              | `SAMPLE` (optional)                                                                        |
| **Degree**            | Connection count per node                                                    | `DIRECTION` (in/out/both)                                                                  |
| **Louvain**           | Community detection via modularity optimization                              | `ITERATIONS` (10), `RESOLUTION` (1.0)                                                      |
| **Triangles**         | Triangle count (per-node or global)                                          | `MODE` (global/per_node)                                                                   |
| **Diameter**          | Longest shortest path in the graph                                           | —                                                                                          |
| **k-Core**            | Coreness decomposition (peeling algorithm)                                   | —                                                                                          |

---

## GraphRAG (Vector + Graph Fusion)

Combines vector similarity search with graph traversal in a single query pipeline — zero network hops between engines.

### How It Works

```
Query Vector
    |
    v
[1] Vector Search (HNSW) --- top-K semantically similar nodes (seed set)
    |
    v
[2] Graph Expansion (BFS) -- expand seeds along edges to depth N
    |
    v
[3] RRF Fusion ------------- merge vector rank + graph hop distance
    |
    v
Final top-N results with (node_id, rrf_score, vector_rank, vector_distance, hop_distance)
```

### SQL Interface

```sql
GRAPH RAG FUSION ON entities
  QUERY $embedding
  VECTOR_FIELD 'embedding'
  VECTOR_TOP_K 50
  EXPANSION_DEPTH 2
  EDGE_LABEL 'related_to'
  DIRECTION both
  FINAL_TOP_K 10
  RRF_K (60.0, 35.0)
  MAX_VISITED 1000;
```

| Parameter         | Description                                                                                                          |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| `QUERY`           | Query embedding vector                                                                                               |
| `VECTOR_FIELD`    | Name of the embedding field on the target collection (optional — defaults to the collection's declared vector field) |
| `VECTOR_TOP_K`    | Number of seed nodes from vector search                                                                              |
| `EXPANSION_DEPTH` | BFS hop count from seeds                                                                                             |
| `EDGE_LABEL`      | Optional edge type filter during expansion                                                                           |
| `DIRECTION`       | `in`, `out`, or `both` for BFS                                                                                       |
| `FINAL_TOP_K`     | Final result count after fusion                                                                                      |
| `RRF_K`           | Weighting constants `(vector_k, graph_k)` for RRF scoring                                                            |
| `MAX_VISITED`     | Memory budget cap — BFS stops early if exceeded                                                                      |

The response includes a truncation flag if the memory budget forced early termination.

### Step-by-Step Pattern (Application-Level)

```sql
-- 1. Seed retrieval
SELECT id, content FROM chunks
WHERE embedding <-> $query_embedding
LIMIT 10;

-- 2. Graph expansion from seeds
MATCH (c:Chunk)-[:mentions]->(e:Entity)-[:related_to*1..3]->(related:Entity)
WHERE c.id IN ('chunk-042', 'chunk-017')
RETURN DISTINCT related.id, related.description;

-- 3. Assemble expanded context for LLM
```

---

## Distributed Execution (BSP)

In multi-shard deployments, graph algorithms and pattern matching execute via Bulk Synchronous Parallel (BSP) coordination. There is no separate distributed syntax — the same `GRAPH ALGO` and `MATCH` statements run single-node or distributed transparently; the coordinator picks the BSP pipeline when the graph is partitioned. Distributed PageRank (including `PERSONALIZATION`), WCC, and cross-shard MATCH are supported; results are globally correct (e.g., PageRank sums to ≈1.0 across shards).

### Distributed PageRank

Each shard maintains a local rank vector, out-degree array, and dangling node mask. Cross-shard edges are tracked as "ghost edges." Each superstep:

1. **Scatter** — distribute rank to outbound edges
2. **Boundary exchange** — package contributions for ghost edges, send to target shards
3. **Gather** — add incoming contributions from other shards
4. **Convergence check** — L1 norm delta across all shards
5. Repeat until convergence or max iterations

### Distributed WCC

Union-find with cross-shard label propagation:

1. Each shard computes local WCC
2. Boundary edges send component labels to target shards
3. Target shards adopt the lexicographically smaller label
4. Repeat until no labels change (guaranteed convergence)

### Distributed Pattern Matching

Scatter-gather with continuations:

1. Coordinator broadcasts MATCH query to all shards
2. Each shard executes the pattern on its local CSR
3. Ghost edges produce `PatternContinuation` messages (partial bindings + remaining pattern + start node)
4. Target shards resume from continuations
5. Coordinator collects completed rows and new continuations
6. Repeat until no pending continuations or max rounds reached

### Variable-Length Expansion Caps

Variable-length `[*min..max]` MATCH expansion is bounded by operational knobs in the `[tuning.graph]` section of `config.toml`:

| Knob                 | Default | Effect                                                                                                  |
| -------------------- | ------- | -------------------------------------------------------------------------------------------------------- |
| `varlen_max_results` | 100000  | Cap on results from one variable-length expansion. On overflow, truncates at the hop boundary and pages via a resume cursor — no row is silently dropped. |
| `varlen_max_frontier`| 100000  | Cap on the live per-hop frontier; overflow pages via resume.                                              |
| `max_depth`          | 10      | Maximum traversal depth.                                                                                  |
| `max_visited`        | 100000  | Memory budget on visited nodes.                                                                           |

Resume cursors work both locally and across shards, so a capped expansion continues from where it stopped instead of failing.

---

## Bitemporal Support

Graph edges support both system time and valid time, enabling relationship timelines and historical traversal.

```sql
-- Insert an edge that becomes valid in the future (scheduled relationship)
GRAPH INSERT EDGE IN 'edges' FROM 'user:alice' TO 'project:rocket'
  TYPE 'assigned_to'
  PROPERTIES { assigned_date: '2026-05-01' }
  VALID_TIME '2026-05-01T00:00:00Z';

-- Query the graph as it existed at a past moment
GRAPH TRAVERSE FROM 'user:alice' DEPTH 2
AS OF SYSTEM TIME 1700000000000;

-- Query relationships that were valid at a specific date
MATCH (u:User)-[:assigned_to]->(p:Project)
WHERE u.id = 'alice'
AS OF VALID TIME 1746086400000;  -- May 1, 2026
```

This is useful for organizational hierarchies, historical relationship analysis, and compliance audits. See [Bitemporal](bitemporal.md) for detailed examples.

---

## Statistics

`SHOW GRAPH STATS` gives a persistence-rooted readout of a graph collection's
node, edge, and label counts. It bypasses the in-memory CSR cache and reads
the durable edge-store counter table, so it reflects what was persisted —
useful for distinguishing a failed write from a failed read when a traversal
returns an empty result.

### Syntax

```sql
SHOW GRAPH STATS '<collection>'  [VERBOSE]  [AS OF SYSTEM TIME <ms>];
SHOW GRAPH STATS                  [VERBOSE]  [AS OF SYSTEM TIME <ms>];
```

Both `VERBOSE` and `AS OF SYSTEM TIME` are optional and order-insensitive.
Omitting the collection name returns a tenant-wide aggregate, one row per
collection that has (or has ever had) graph edges.

### Compact form (default)

```sql
SHOW GRAPH STATS 'social';
```

| collection | node_count | edge_count | distinct_label_count | labels                                     |
| ---------- | ---------- | ---------- | -------------------- | ------------------------------------------ |
| social     | 14218      | 41902      | 5                    | `[{"label":"follows","count":21504}, ...]` |

The `labels` column is a JSON array — easy for drivers to parse without a
second round-trip.

### VERBOSE form

```sql
SHOW GRAPH STATS 'social' VERBOSE;
```

| collection | label      | edge_count |
| ---------- | ---------- | ---------- |
| social     | follows    | 21504      |
| social     | likes      | 12008      |
| social     | reports_to | 5802       |
| social     | blocks     | 1488       |
| social     | knows      | 1100       |

One row per `(collection, label)` pair — SQL-idiomatic, sortable, filterable.

### Historical snapshot

```sql
SHOW GRAPH STATS 'social' AS OF SYSTEM TIME 1700000000000;
```

When `AS OF SYSTEM TIME` is provided, the stats reflect the live edges at
that system-time cutoff. The live-snapshot path is O(1) (it reads a cached
summary row); the historical path falls back to a prefix scan of the
edge store and is proportional to the number of edges in the collection.

### Semantics

- **Persistence-rooted.** Counts come from a counter table maintained
  atomically inside the same redb transaction as each edge put/delete.
  The CSR cache is never consulted — a divergence between the CSR and the
  edge store will still produce the durable answer.
- **Live by default.** Soft-deleted (tombstoned) and GDPR-erased edges
  are not counted unless `AS OF SYSTEM TIME` requests a snapshot from
  before their deletion.
- **Cross-vShard aggregation.** Origin fans the read out to every Data
  Plane core that holds a partition of the collection, aggregates per-core
  results, and re-derives `distinct_label_count` from the merged label set
  (labels are not partition-disjoint across cores).
- **Hard error on partial failure.** If any vShard fails or times out the
  call returns a structured error — no silent truncation.
- **Multigraph.** A pair `(src, dst)` with two different labels counts as
  two edges, one node-pair, two distinct labels.
- **Self-loops.** `(x, label, x)` counts as one edge, one distinct node.

### Errors

| Condition                                       | sqlstate | message                              |
| ----------------------------------------------- | -------- | ------------------------------------ |
| Collection does not exist                       | `42P01`  | `collection '<name>' not found`      |
| Collection is deactivated (in retention window) | `42P01`  | `collection '<name>' is deactivated` |
| Catalog unavailable                             | `XX000`  | `catalog not available`              |
| Partial vShard failure during dispatch          | `58000`  | `graph stats dispatch failed: ...`   |

---

## Related

- [Bitemporal](bitemporal.md) — Cross-engine temporal queries and audit trails
- [Vector Search](vectors.md) — GraphRAG combines graph traversal with vector similarity
- [Full-Text Search](full-text-search.md) — BM25 results can be re-ranked using graph context
- [GraphRAG Patterns](ai/graphrag.md) — Detailed retrieval-augmented generation with graphs

[Back to docs](README.md)
