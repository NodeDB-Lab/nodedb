# Real-Time Features

NodeDB is a real-time database. Every committed mutation publishes to an internal event bus. No external message broker needed — real-time is part of the database, not a sidecar.

The event infrastructure is built on the **Event Plane** — a third architectural layer alongside the Control Plane (query planning) and Data Plane (storage I/O). The Data Plane emits `WriteEvent` records (covering inserts, updates, and deletes via `WriteOp`) to the Event Plane via per-core bounded ring buffers after each WAL commit. The Event Plane routes these to change stream consumers, trigger dispatch, and webhook delivery. WAL-backed crash recovery ensures no events are lost across restarts.

## LIVE SELECT

Register a query and receive matching changes as they happen. No polling.

```sql
-- Subscribe to all new orders over $100
LIVE SELECT * FROM orders WHERE total > 100.00;

-- Subscribe to status changes
LIVE SELECT id, status FROM orders WHERE status != 'pending';
```

The server pushes matching inserts, updates, and deletes to the client in real time.

**Protocol support:**

- **pgwire** — Subscription is stored in the session. Notifications are delivered as `NotificationResponse` messages between query responses, using the standard pgwire async notification channel. Standard `psql` and JDBC clients receive them without any additional configuration.
- **WebSocket** — Delivered as JSON frames over the `/ws` endpoint.
- **NDB (native)** — Delivered as MessagePack frames on the native protocol connection.

On pgwire, `LIVE SELECT` stores the subscription in session state. Each subsequent command response may be preceded by one or more `NotificationResponse` frames carrying the matching change payloads. The subscription remains active until the session ends or `CANCEL LIVE SELECT <id>` is executed.

## SHOW CHANGES

Pull-based CDC with cursor pagination. Useful for batch consumers or point-in-time replay.

```sql
-- Get changes since a cursor
SHOW CHANGES FOR orders SINCE '2024-01-15T00:00:00Z' LIMIT 1000;
```

## Triggers

SQL blocks that fire on data mutations. Three execution modes let you choose the trade-off between write latency and atomicity:

```sql
-- ASYNC (default): fires after commit via Event Plane — zero write-latency impact
-- Side effects are eventually consistent; failures retry with backoff, then go to DLQ
CREATE TRIGGER audit_orders AFTER INSERT ON orders FOR EACH ROW
BEGIN
    INSERT INTO audit_log { collection: 'orders', action: 'INSERT', row_id: NEW.id, ts: now() };
END;

-- SYNC: fires in the same transaction on the Data Plane (ACID, write latency += trigger time)
CREATE SYNC TRIGGER enforce_balance AFTER UPDATE ON accounts FOR EACH ROW
BEGIN
    IF NEW.balance < 0 THEN
        RAISE EXCEPTION 'Balance cannot go negative';
    END IF;
END;

-- DEFERRED: fires at COMMIT time, batched (ACID, COMMIT is slower)
CREATE DEFERRED TRIGGER validate_totals AFTER INSERT ON line_items FOR EACH ROW
BEGIN
    -- cross-row validation at statement boundary
END;

DROP TRIGGER audit_orders ON orders;
SHOW TRIGGERS;
```

| Mode              | Atomicity                 | Write latency      | Rollback on failure |
| ----------------- | ------------------------- | ------------------ | ------------------- |
| `ASYNC` (default) | Eventually consistent     | None               | No                  |
| `SYNC`            | Same transaction (ACID)   | Trigger time added | Yes                 |
| `DEFERRED`        | Same transaction, batched | At COMMIT time     | Yes                 |

**Graph mutations emit events.** Graph edge and node-label writes publish `WriteEvent`s like any other mutation, so triggers, change streams, and streaming MVs can react to graph changes:

- **Edges** — published on the edge's collection with a stable `row_id` composed from the `(src, label, dst)` triple. Insert/put carries the edge properties in `new_value`; delete carries no payload.
- **Node labels** — published tenant-wide on the dedicated stream `__graph_node_labels__`. `SET` labels → `Insert` with the added labels in `new_value` (`{ "labels": [...] }`); `REMOVE` → `Delete` with the removed labels in `old_value`. `row_id` is the node id.

Implicit edges (a document insert carrying `_from`/`_to`) are not double-published — the underlying document write already emits its event. WAL replay reconstructs graph events byte-identically and dedups on LSN.

**UPSERT and `ON CONFLICT` firing semantics.** The `WriteOp` tag emitted to the Event Plane is derived from storage prior-bytes, not from the surface SQL verb. An `UPSERT` or `INSERT ... ON CONFLICT (pk) DO UPDATE` that finds an existing row fires `AFTER UPDATE`; the same statement against a non-existent key fires `AFTER INSERT`. `ON CONFLICT DO NOTHING` on a conflict emits no event at all.

## CDC Change Streams

Change streams provide durable, cursor-tracked access to the mutation log for a collection. Unlike `LIVE SELECT` (push to a session), change streams survive reconnects, support consumer groups, and can deliver to external systems via webhook.

```sql
-- Basic change stream
CREATE CHANGE STREAM order_changes ON orders;

-- With webhook delivery (HMAC-signed POST to an external endpoint)
CREATE CHANGE STREAM order_events ON orders
WITH (
    WEBHOOK_URL = 'https://hooks.example.com/orders',
    WEBHOOK_SECRET = 'whsec_abc123'
);

-- With log compaction: only the latest mutation per key is retained
CREATE CHANGE STREAM user_state ON users
WITH (COMPACTION = 'key', KEY = 'id');

DROP CHANGE STREAM order_changes;
-- Dropping a change stream atomically tears down its consumer groups and
-- persisted offset rows. Recreating a stream with the same name starts fresh
-- from the head; it does not resume at stale offsets.
SHOW CHANGE STREAMS;
```

### Consumer Groups

Consumer groups track read positions independently, enabling multiple consumers to process the same stream at their own pace.

Every event carries an `offset` token `<epoch>:<index>:<sequence>`. In a cluster, `index` is the Raft log index of the write, so every replica gives an event the same offset. `epoch` rises when a partition moves to another data group, so offsets never go backwards. A committed offset is replicated to every node. A consumer can move to another node, or continue after a leader change, and resume exactly after its last commit.

A node that joined a partition from a snapshot does not hold the events before the snapshot. A consumer whose offset lies below them gets a `reset_required` error naming the offset the node holds events from. It never skips events silently. When another replica still holds those events, the consume goes there instead.

```sql
-- Create a consumer group
CREATE CONSUMER GROUP analytics ON order_changes;
CREATE CONSUMER GROUP billing ON order_changes;

-- Commit offset for a specific partition
COMMIT OFFSET PARTITION 0 AT 0:42:2 ON order_changes CONSUMER GROUP analytics;

-- <epoch>:<index> acknowledges every event of that write
COMMIT OFFSET PARTITION 0 AT 0:42 ON order_changes CONSUMER GROUP analytics;

-- Or batch commit all partitions at their latest consumed position
COMMIT OFFSETS ON order_changes CONSUMER GROUP analytics;

DROP CONSUMER GROUP analytics ON order_changes;
```

## Durable Topics

Durable topics are standalone named channels with configurable retention. They provide persistent pub/sub with consumer group offset tracking — messages survive disconnect, and consumers resume from their last committed offset.

```sql
-- Create a durable topic with retention
CREATE TOPIC order_events WITH (RETENTION = '1 hour');

-- Publish a message
PUBLISH TO order_events 'order 123 shipped';

-- Create a consumer group for durable consumption
CREATE CONSUMER GROUP processors ON order_events;

-- Consume from the topic (reads from last committed offset)
SELECT * FROM TOPIC order_events CONSUMER GROUP processors LIMIT 100;

-- Commit offsets after processing
COMMIT OFFSETS ON order_events CONSUMER GROUP processors;

DROP TOPIC order_events;
SHOW TOPICS;
```

## Cron Scheduler

The Event Plane includes a distributed cron scheduler. Jobs are stored in the catalog, evaluated per-second, and dispatched through the normal Control Plane → Data Plane execution path.

```sql
-- Run a cleanup job at 2 AM UTC daily
CREATE SCHEDULE nightly_cleanup
CRON '0 2 * * *'
AS BEGIN
    DELETE FROM sessions WHERE expires_at < now();
    INSERT INTO maintenance_log { task: 'nightly_cleanup', ran_at: now() };
END;

-- Run a 5-minute aggregate refresh
CREATE SCHEDULE refresh_stats
CRON '*/5 * * * *'
AS BEGIN
    REFRESH CONTINUOUS AGGREGATE order_stats;
END;

DROP SCHEDULE nightly_cleanup;
SHOW SCHEDULES;
```

## Streaming Materialized Views

Continuously-updating aggregation fed by change streams. Each incoming event updates only the affected group key's partial aggregate state — O(1) per event, no full rescan.

```sql
CREATE MATERIALIZED VIEW order_stats STREAMING AS
SELECT time_bucket('5 minutes', event_time) AS bucket,
       count(*) AS order_count,
       sum(doc_get(new_value, '$.total')::FLOAT) AS revenue
FROM order_changes
WHERE event_type = 'INSERT'
GROUP BY bucket;

-- Query the continuously-updating view
SELECT * FROM order_stats;
```

Streaming MVs support COUNT, SUM, MIN, MAX, and AVG aggregates. State is persisted to redb every 30 seconds and restored on startup. Watermark-driven finalization marks time buckets as complete.

## LISTEN/NOTIFY

PostgreSQL-compatible ephemeral notifications, extended to cluster-wide delivery. A LISTEN on Node B receives NOTIFY from Node A.

```sql
-- Session-scoped, ephemeral (backward compatible with pgwire)
LISTEN order_events;
NOTIFY order_events, 'order 123 shipped';
```

For durable, resumable delivery, use [Durable Topics](#durable-topics) instead.

## External Delivery

### Webhook

Change streams can deliver events to external HTTP endpoints with automatic retry:

```sql
CREATE CHANGE STREAM order_events ON orders
WITH (
    DELIVERY = 'webhook',
    URL = 'https://hooks.example.com/orders',
    RETRY = 3,
    TIMEOUT = '5s'
);
```

Each POST includes `X-Idempotency-Key`, `X-Fencing-Token`, `X-CDC-Offset`, `X-Event-Sequence`, `X-Partition`, and `X-LSN` headers. 4xx client errors (except 429) are not retried.

The URL goes through the `[egress]` policy: `https://` to a DNS name that resolves to global unicast addresses, or a host on the allow-list. No redirect is followed. A URL the policy refuses stops the stream's delivery. Alert `NOTIFY WEBHOOK` URLs pass the same policy when the alert is created (`42501` on refusal) and before every send.

In a cluster, one node delivers a stream at a time: the node that holds the leader lease of the stream's owning Raft group. It reads every partition, including partitions whose rows live on other nodes. It checks its lease right before each POST and right before each offset commit. When that node fails, the next lease holder resumes from the committed offsets. Delivery is at-least-once: a POST can succeed after the old owner lost its lease but before it committed the offset, and the new owner then sends that event again. Two headers let an endpoint apply each event once:

| Header | Value | What the endpoint does with it |
|---|---|---|
| `X-Idempotency-Key` | `<partition>:<epoch>:<index>:<sequence>`: the event's partition and position | Store each applied key. Drop a request whose key is already stored. Every owner and every retry sends the same key for the same event. |
| `X-Fencing-Token` | The Raft term of the delivering node's leader lease on the owning group | Store the highest token accepted. Reject a request with a lower token: it comes from an owner a later owner replaced. Each new owner delivers with a higher token. |

A single node without Raft always sends token `0`.

### Kafka Bridge

Publish change stream events to an external Kafka topic (feature-gated with `--features kafka`):

```sql
CREATE CHANGE STREAM order_events ON orders
WITH (
    DELIVERY = 'kafka',
    BROKERS = 'localhost:9092',
    TOPIC = 'orders',
    FORMAT = 'json'
);
```

Supports transactional exactly-once semantics via `enable.idempotence` and `transactional.id`.

In a cluster, one node publishes a stream at a time, under the same lease and checks as webhook delivery. Each record's key is the event's `<partition>:<epoch>:<index>:<sequence>`, the same for every owner and retry. Each record carries a `fencing-token` header with the publishing node's lease term, which rises with each new owner.

### SSE Streaming

HTTP Server-Sent Events for CDC consumers that can't use WebSocket:

```
GET /v1/streams/{stream}/events?group={group}
Accept: text/event-stream
```

### HTTP Long-Poll

Pull-based consumption for simple integrations:

```
GET /v1/streams/{stream}/poll?group={group}&limit=100
```

The response includes gap-detection fields alongside the events:

```json
{
  "events": [...],
  "evicted_since_last_poll": 0,
  "oldest_available_offset": "0:19240:2"
}
```

`evicted_since_last_poll` is non-zero when the consumer fell behind and the stream buffer wrapped — events in that gap are lost for this consumer. `oldest_available_offset` lets consumers detect gaps without waiting for the next event to arrive by comparing it against their last-seen offset.

The `nodedb_cdc_events_dropped_total{tenant,stream}` Prometheus counter tracks drops per named stream. Alert on this counter increasing for a stream whose consumer is active.

## WebSocket RPC

JSON-RPC over WebSocket for SQL execution, live query delivery, and session management:

- Execute SQL queries
- Receive LIVE SELECT updates
- Session reconnect with event replay
- Auth token refresh during long-lived connections

## Sync to Embedded Clients

Columnar, Vector, FTS, and Spatial collections participate in **inbound sync** to NodeDB-Lite and NodeDB-WASM over the Sync protocol (WebSocket). DDL schema changes broadcast to connected embedded sessions after the Origin catalog commit, enabling automatic schema discovery. This allows offline-first applications to stay synchronized with production schema evolution.

## Related

- [Key-Value](kv.md) — KV changes appear in CDC and LIVE SELECT
- [Documents](documents.md) — Document mutations trigger events
- [NodeDB-Lite](lite.md) — CRDT sync uses the same CDC infrastructure
- [Offline Sync Patterns](offline-sync-patterns.md) — Offline-first workflows with sync

[Back to docs](README.md)
