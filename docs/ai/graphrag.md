# GraphRAG Patterns

GraphRAG augments vector retrieval with knowledge graph traversal. Instead of returning only the chunks that match a query embedding, you also traverse relationships to discover contextually relevant entities and documents that vector similarity alone would miss.

NodeDB runs both vector search and graph traversal in the same process, on the same snapshot — zero network hops between engines.

## Entity Extraction and Graph Storage

First, build a knowledge graph from your documents. Entity extraction happens in your application (using an LLM or NER model). NodeDB stores the entities and relationships.

```sql
-- Create collections for the knowledge graph
CREATE COLLECTION entities TYPE strict (
    id          STRING PRIMARY KEY,
    name        STRING NOT NULL,
    entity_type STRING NOT NULL,
    description STRING,
    embedding   VECTOR(1536)
);

CREATE COLLECTION chunks TYPE document;

-- Insert extracted entities
INSERT INTO entities VALUES (
    'entity-raft', 'Raft Consensus', 'algorithm',
    'A consensus algorithm for managing a replicated log',
    $entity_embedding
);

-- Insert relationships using graph edges
GRAPH INSERT EDGE FROM 'chunks:chunk-042' TO 'entities:entity-raft' TYPE 'mentions';
GRAPH INSERT EDGE FROM 'entities:entity-raft' TO 'entities:entity-paxos' TYPE 'related_to'
  PROPERTIES '{"weight": 0.85}';

-- Bulk entity relationships from extraction results
-- (Your app extracts entities and emits these statements)
GRAPH INSERT EDGE FROM 'entities:entity-raft' TO 'entities:entity-leader-election' TYPE 'related_to';
GRAPH INSERT EDGE FROM 'entities:entity-raft' TO 'entities:entity-log-replication' TYPE 'related_to';
```

## Seed Retrieval + Graph Expansion

The core GraphRAG pattern: vector search finds seed chunks, then graph traversal expands to related entities and their connected chunks.

```sql
-- Step 1: Vector retrieval gets seed chunks
SELECT id, content, vector_distance() AS score
FROM chunks
WHERE embedding <-> $query_embedding
LIMIT 10;

-- Step 2: Graph expansion from seed chunks
-- Find entities mentioned by seed chunks, then follow relationships
MATCH (c:Chunk)-[:mentions]->(e:Entity)-[:related_to*1..3]->(related:Entity)
WHERE c.id IN ('chunk-042', 'chunk-017', 'chunk-089')
RETURN DISTINCT related.id, related.name, related.description,
       COUNT(*) AS connection_strength
ORDER BY connection_strength DESC
LIMIT 20;

-- Step 3: Find additional chunks that mention the discovered entities
MATCH (related:Entity)<-[:mentions]-(neighbor:Chunk)
WHERE related.id IN ('entity-paxos', 'entity-leader-election', ...)
  AND neighbor.id NOT IN ('chunk-042', 'chunk-017', 'chunk-089')
RETURN DISTINCT neighbor.id, neighbor.content
LIMIT 10;
```

**Context assembly:** Your application combines:

1. Seed chunks (from vector search) — directly relevant
2. Entity descriptions (from graph) — background knowledge
3. Neighbor chunks (from graph expansion) — contextually related

This combined context gives the LLM a much richer understanding than vector search alone.

## Community Summarization

Use Louvain clustering to discover communities of related entities, then generate per-community summaries. This enables "global" queries that require understanding of broad topics, not just specific chunks.

```sql
-- Run Louvain community detection on the entity relationship graph
GRAPH ALGO LOUVAIN ON related_to ITERATIONS 10;
-- Returns: node_id, community_id for each entity connected by related_to edges

-- Store community assignments
-- (Your app reads the algo output and updates entities)
INSERT INTO entity_communities {
    entity_id: 'entity-raft',
    community_id: 7,
    community_label: 'Distributed Consensus'
};

-- Query: for a broad topic, retrieve community summaries
-- Step 1: find relevant entities via vector search
SELECT id, name, vector_distance() AS score
FROM entities
WHERE embedding <-> $query_embedding
LIMIT 5;

-- Step 2: find which communities those entities belong to
SELECT DISTINCT ec.community_id, ec.community_label
FROM entity_communities ec
WHERE ec.entity_id IN ('entity-raft', 'entity-paxos', ...);

-- Step 3: retrieve all entities in those communities for summarization
SELECT e.name, e.description
FROM entities e
JOIN entity_communities ec ON e.id = ec.entity_id
WHERE ec.community_id IN (7, 12);
-- Your app generates a summary of each community using the LLM,
-- then uses those summaries as high-level context for the final answer.
```

## Entity Disambiguation

When extracting entities, the same real-world concept may appear with different names ("JS", "JavaScript", "ECMAScript"). Use graph patterns to find candidates for resolution.

```sql
-- Find potential duplicates: entities with similar embeddings
SELECT e1.id, e1.name, e2.id AS candidate_id, e2.name AS candidate_name,
       vector_distance() AS similarity
FROM entities e1, entities e2
WHERE e1.embedding <-> e2.embedding
  AND e1.id != e2.id
  AND e1.entity_type = e2.entity_type
LIMIT 50;

-- Check if candidates share neighbors (strong signal for same entity)
MATCH (e1:Entity)-[:related_to]->(shared:Entity)<-[:related_to]-(e2:Entity)
WHERE e1.id = 'entity-js' AND e2.id = 'entity-javascript'
RETURN COUNT(shared) AS shared_neighbors;
-- High shared_neighbors count → likely the same entity

-- After disambiguation, merge by redirecting edges
-- (Your app decides which pairs to merge based on similarity + shared neighbors)
-- Delete the duplicate's edges and redirect to the canonical entity
GRAPH DELETE EDGE FROM 'entities:entity-js' TO 'entities:entity-dom-api' LABEL 'related_to';
INSERT INTO related_to { from: 'entities:entity-javascript', to: 'entities:entity-dom-api' };
```

## Tips

- **Graph depth:** Start with 1-2 hops for expansion. Beyond 3 hops, noise increases faster than signal.
- **Edge weights:** Use weighted edges (`weight` property) to encode relationship strength. Graph algorithms and traversal can use these for ranking.
- **Incremental updates:** When new documents are added, extract entities and edges incrementally. Use [CDC change streams](../real-time.md) to trigger entity extraction pipelines automatically.
- **Latency:** Since vector search and graph traversal run in the same process, the total latency is the sum of both operations — typically 5-20ms for seed retrieval + 1-5ms for graph expansion. No network round-trips between databases.
