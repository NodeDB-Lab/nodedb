# CDC for Inference Triggers

NodeDB's Event Plane delivers change events within microseconds of commit. Use change streams and triggers to build reactive AI pipelines: auto-embed new documents, re-index knowledge graphs on entity updates, and route model outputs to downstream consumers.

## Change Stream for Embedding Pipelines

When a new document is inserted, a change stream can trigger your embedding pipeline to compute vectors and store them back.

```sql
-- Create the document collection
CREATE COLLECTION articles TYPE document;

-- Create a change stream that captures inserts and updates
CREATE CHANGE STREAM article_changes ON articles
    OPERATIONS INSERT, UPDATE;

-- Create a consumer group for the embedding pipeline
CREATE CONSUMER GROUP embedding_pipeline ON article_changes;

-- Your embedding service subscribes and processes changes:
SELECT * FROM STREAM article_changes CONSUMER GROUP embedding_pipeline LIMIT 50;
-- Returns: operation, document_id, new_value, old_value, timestamp, lsn

-- For each change event, your service:
-- 1. Extracts the text content from new_value
-- 2. Calls the embedding API (OpenAI, Cohere, local model)
-- 3. Stores the vector back:
--    INSERT INTO articles { id: $doc_id, embedding: $vector };
```

**Why change streams instead of triggers?** Embedding generation is slow (50-500ms per call) and calls an external API. A synchronous trigger would block the write path. Change streams decouple the pipeline: writes commit immediately, embedding happens asynchronously with retry and dead-letter queue (DLQ) guarantees.

**Backpressure:** If the embedding service falls behind, the change stream buffer backpressures at 85% capacity (reduces throughput) and suspends at 95%. Events are WAL-backed — when the consumer catches up, it replays from the WAL watermark. No events are lost.

## Entity Update Stream for Knowledge Graph Re-Indexing

When entities or relationships change, automatically update the knowledge graph indexes.

```sql
-- Monitor entity collection for changes
CREATE CHANGE STREAM entity_changes ON entities
    OPERATIONS INSERT, UPDATE, DELETE;

CREATE CONSUMER GROUP graph_reindexer ON entity_changes;

-- Your graph re-indexing service subscribes:
SELECT * FROM STREAM entity_changes CONSUMER GROUP graph_reindexer LIMIT 100;

-- For each event, update the graph:
-- INSERT event → add edges to related entities
-- UPDATE event → update entity properties, re-compute embeddings
-- DELETE event → remove edges, clean up orphaned relationships
```

**Trigger-based approach** for lightweight graph updates that don't call external APIs:

```sql
-- Sync trigger: update graph edges in the same transaction
CREATE SYNC TRIGGER update_entity_edges AFTER INSERT ON entities FOR EACH ROW
BEGIN
    -- Auto-create a self-referencing "is_a" edge based on entity_type
    INSERT INTO is_a { from: NEW.id, to: NEW.entity_type };
END;

-- Async trigger: log entity changes for downstream consumers
CREATE TRIGGER log_entity_change AFTER UPDATE ON entities FOR EACH ROW
BEGIN
    INSERT INTO entity_changelog {
        entity_id: NEW.id,
        change_type: 'UPDATE',
        old_name: OLD.name,
        new_name: NEW.name,
        ts: now()
    };
END;
```

## Model Output Stream for Downstream Consumers

Route inference results (predictions, classifications, anomaly scores) to downstream systems via durable topics.

```sql
-- Create a topic for model predictions
CREATE TOPIC model_predictions;

-- Your inference service publishes results after each prediction
PUBLISH TO model_predictions {
    request_id: $req_id,
    model_name: 'fraud-detector-v3',
    input_hash: $input_hash,
    prediction: 0.87,
    label: 'high_risk',
    latency_ms: 42,
    ts: NOW()
};

-- Consumer 1: alerting service (high-risk predictions)
CREATE CONSUMER GROUP alerting ON model_predictions;
SELECT * FROM TOPIC model_predictions CONSUMER GROUP alerting LIMIT 50;
-- Filter for high_risk and send alerts

-- Consumer 2: dashboard metrics
CREATE CONSUMER GROUP dashboard ON model_predictions;
SELECT * FROM TOPIC model_predictions CONSUMER GROUP dashboard LIMIT 100;
-- Aggregate into timeseries for latency/throughput dashboards

-- Consumer 3: audit log (store all predictions for compliance)
CREATE CONSUMER GROUP audit ON model_predictions;
SELECT * FROM TOPIC model_predictions CONSUMER GROUP audit LIMIT 500;
-- Insert into an append-only audit collection
```

**Consumer group guarantees:**

- Each consumer group tracks its own offset independently
- Multiple consumers in the same group get partitioned delivery (no duplicates)
- Different consumer groups each receive every message (fan-out)
- Offsets are committed explicitly — at-least-once delivery

```sql
-- Commit offsets after processing
COMMIT OFFSETS FOR CONSUMER GROUP alerting ON model_predictions;
```

## Scheduled Inference Pipelines

Use the cron scheduler to run batch inference or re-embedding on a schedule.

```sql
-- Re-embed stale documents every night at 2 AM
CREATE SCHEDULE nightly_reembed
    CRON '0 2 * * *'
    AS BEGIN
        SELECT id, content FROM articles
        WHERE updated_at > embedding_updated_at
        ORDER BY updated_at DESC
        LIMIT 1000;
    END;

-- Your app polls schedule results and processes them.
-- Show schedule execution history:
SHOW SCHEDULES;
```

## Tips

- **Idempotency:** Embedding pipelines should be idempotent — re-processing the same document produces the same vector. This makes retry safe and simplifies error handling.
- **Batching:** Accumulate change events and call the embedding API in batches (10-50 documents per call) to amortize API round-trip overhead.
- **Versioning:** When switching embedding models, use [embedding model metadata](../vectors.md) to track which model produced which vectors. Process the change stream to re-embed with the new model.
- **Monitoring:** Change stream consumer lag is observable via `SHOW CONSUMER GROUPS`. Alert if a consumer group falls behind by more than your SLA threshold.
