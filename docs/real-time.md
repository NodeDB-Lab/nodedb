# Real-Time Features

NodeDB is a real-time database. Every committed mutation publishes to an internal Change Data Capture (CDC) bus. No external message broker needed — real-time is part of the database, not a sidecar.

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

## Event Triggers

Define SQL actions that fire when specific conditions are met on a collection:

```sql
-- Fire when an order ships
DEFINE EVENT on_shipment ON orders
WHEN status = 'shipped'
THEN INSERT INTO notifications { user_id: $new.customer_id, message: 'Your order shipped!' };

-- Audit trail
DEFINE EVENT audit_changes ON sensitive_data
THEN INSERT INTO audit_log { table: 'sensitive_data', action: $event, user: $auth.user_id, ts: now() };
```

## Pub/Sub

Named topics with consumer groups, bounded retention, and backlog replay on reconnect.

```sql
-- Create a topic
CREATE TOPIC order_events WITH (retention = '7 days');

-- Publish
PUBLISH TO order_events { event: 'created', order_id: '...', total: 99.99 };

-- Subscribe (with consumer group for load balancing)
SUBSCRIBE TO order_events GROUP 'processors';
```

## SSE Streaming

HTTP Server-Sent Events for CDC consumers that can't use WebSocket:

```
GET /api/v1/collections/orders/changes?since=2024-01-15T00:00:00Z
Accept: text/event-stream
```

## WebSocket RPC

JSON-RPC over WebSocket for SQL execution, live query delivery, and session management:

- Execute SQL queries
- Receive LIVE SELECT updates
- Session reconnect with event replay
- Auth token refresh during long-lived connections

## Related

- [Key-Value](kv.md) — KV changes appear in CDC and LIVE SELECT
- [Documents](documents.md) — Document mutations trigger events
- [NodeDB-Lite](lite.md) — CRDT sync uses the same CDC infrastructure

[Back to docs](README.md)
