# Key-Value Engine

NodeDB's KV engine is a purpose-built hash-indexed store with O(1) point lookups, native TTL, and optional secondary indexes. Unlike a standalone KV store, data here is SQL-queryable, joinable with other collections, and syncable to edge devices via CRDTs.

## When to Use

- Session state and tokens
- Feature flags and configuration
- Rate limiters and counters
- Caching (without needing an external cache)
- Any workload dominated by primary-key access

## Why Not Just Use Redis?

Running an external KV store alongside your database means a second deployment, a second failure domain, application-level cache invalidation, data duplication, and no ability to query or join KV data with the rest of your system.

NodeDB's KV engine eliminates this:

- Hot reads serve from mmap'd memory at sub-millisecond latency — there's no slow database that needs a cache in front
- Real-time is native (LIVE SELECT, CDC, pub/sub) — no Redis Streams sidecar
- KV data is SQL-queryable: `SELECT region, COUNT(*) FROM sessions GROUP BY region`
- KV data joins with other collections, appears in CDC, and syncs to edge devices

## Key Features

- **O(1) point lookups** — Hash-indexed by user-defined key
- **Typed value fields** — Not serialized blobs. Fields have types and are individually accessible.
- **Native TTL** — Index-backed expiry wheel. Set TTL per key, auto-expired.
- **Secondary indexes** — Optional indexes on value fields for filtered scans
- **Atomic operations** — INCR, DECR, CAS, GETSET without read-modify-write cycles
- **Sorted indexes** — O(log N) rank lookups, top-K queries, time-windowed leaderboards
- **Rate gates** — Per-key action throttling with SQL-callable cooldowns
- **SQL-queryable** — Full SQL works on KV collections (aggregations, joins, WHERE clauses)
- **Wire protocol** — Redis-compatible RESP including INCR/ZADD/ZRANK plus dedicated GET/SET/DEL/EXPIRE/SCAN

## Examples

```sql
-- Create a KV collection
CREATE COLLECTION sessions TYPE kv;

-- Set with TTL (seconds)
INSERT INTO sessions { key: 'sess_abc123', user_id: 'alice', role: 'admin', ttl: 3600 };

-- Get by key
SELECT * FROM sessions WHERE key = 'sess_abc123';

-- Update
UPDATE sessions SET role = 'superadmin' WHERE key = 'sess_abc123';

-- Delete
DELETE FROM sessions WHERE key = 'sess_abc123';

-- Secondary index for filtered queries
CREATE INDEX ON sessions FIELDS role;
SELECT key, user_id FROM sessions WHERE role = 'admin';

-- Analytical query on KV data
SELECT role, COUNT(*) AS active_sessions
FROM sessions
GROUP BY role;

-- Join KV data with other collections
SELECT u.name, s.role, s.key
FROM users u
JOIN sessions s ON u.id = s.user_id
WHERE s.role = 'admin';
```

## Redis-Compatible Access (RESP)

NodeDB speaks the Redis wire protocol (RESP2), so existing Redis clients work out of the box for KV operations. RESP is **disabled by default** — enable it by setting a port:

```toml
# nodedb.toml
[server.ports]
resp = 6381
```

Or via environment variable: `NODEDB_PORT_RESP=6381`

Then connect with any Redis client:

```bash
redis-cli -p 6381

# Switch to a KV collection (default: "default")
SELECT sessions

# Standard Redis commands
SET sess_abc123 '{"user_id":"alice","role":"admin"}' EX 3600
GET sess_abc123
DEL sess_abc123
EXPIRE sess_abc123 7200
TTL sess_abc123

# Batch operations
MSET key1 val1 key2 val2
MGET key1 key2

# Scan with glob pattern
SCAN 0 MATCH sess_* COUNT 100

# Field-level access (hash commands)
HSET sess_abc123 role superadmin
HGET sess_abc123 role

# Pub/sub (backed by NodeDB change streams)
SUBSCRIBE sessions
```

**Supported commands:** `GET`, `SET` (with `EX`/`PX`/`NX`/`XX`), `DEL`, `EXISTS`, `MGET`, `MSET`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `SCAN`, `KEYS`, `HGET`, `HMGET`, `HSET`, `FLUSHDB`, `DBSIZE`, `SUBSCRIBE`, `PUBLISH`, `PING`, `ECHO`, `SELECT`, `INFO`, `QUIT`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `GETSET`, `ZADD`, `ZREM`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`.

## Atomic Operations

Atomic increment, decrement, compare-and-swap, and get-and-set without full-value read-modify-write cycles. Each operation is atomic within a single TPC core — no cross-core coordination needed.

```sql
-- Atomic increment (returns new value)
SELECT KV_INCR('player_scores', 'player-123', 10);

-- Atomic decrement
SELECT KV_DECR('player_currency', 'player-123', 50);

-- Increment with TTL (create-if-not-exists with expiry)
SELECT KV_INCR('daily_logins', 'player-123', 1, TTL => 86400);

-- Float increment
SELECT KV_INCR_FLOAT('damage_dealt', 'player-123', 95.5);

-- Compare-and-swap (set only if current value matches expected)
SELECT KV_CAS('player_state', 'player-123', 'idle', 'in_match');
-- Returns: {"success": true, "current_value": "aWRsZQ=="}

-- Atomic get-and-set (swap value, return old)
SELECT KV_GETSET('session_token', 'player-123', 'new-token-xyz');
```

**RESP (Redis) equivalents:** `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `GETSET` — all work over the RESP protocol.

**Error handling:**

- `TYPE_MISMATCH` (SQLSTATE 42846) — INCR on a non-numeric value
- `OVERFLOW` (SQLSTATE 22003) — i64 overflow on INCR

## Sorted Indexes (Leaderboards)

Sorted indexes maintain an order-statistic tree alongside a KV collection, providing O(log N) rank lookups, O(log N + K) top-K queries, and automatic maintenance on every PUT/DELETE.

```sql
-- Create a sorted index on a score column (descending, with tiebreak)
CREATE SORTED INDEX lb_global ON player_scores (score DESC, updated_at ASC)
    KEY player_id;

-- "What's my rank?"
SELECT RANK(lb_global, 'player-123');
-- Returns: {"rank": 4523}

-- "Show me the top 10"
SELECT * FROM TOPK(lb_global, 10);
-- Returns rows: (rank, key)

-- "How many players on the leaderboard?"
SELECT SORTED_COUNT(lb_global);

-- Score range query
SELECT * FROM RANGE(lb_global, 1000, 2000);

-- Drop a sorted index
DROP SORTED INDEX lb_global;
```

### Time-Windowed Leaderboards

Sorted indexes support time windows — the index only considers entries from the current window. Old data stays in the collection but is filtered out of queries.

```sql
-- Daily leaderboard (resets at UTC midnight)
CREATE SORTED INDEX lb_daily ON player_scores (score DESC)
    KEY player_id
    WINDOW DAILY ON updated_at;

-- Weekly leaderboard
CREATE SORTED INDEX lb_weekly ON player_scores (score DESC)
    KEY player_id
    WINDOW WEEKLY ON updated_at;

-- Season leaderboard (custom date range)
CREATE SORTED INDEX lb_season ON player_scores (score DESC)
    KEY player_id
    WINDOW CUSTOM START '1714600000000' END '1717200000000';
```

**RESP (Redis) equivalents:** `ZADD`, `ZREM`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE` — all work over the RESP protocol for sorted set operations.

## Rate Gates (Cooldowns)

SQL-callable rate limiting for per-key action throttling. Built on atomic KV counters with TTL.

```sql
-- Check and consume a rate gate (3 attacks per 10 seconds)
SELECT RATE_CHECK('attack_cooldown', 'player-123', 3, 10);
-- Returns: {"allowed": true, "current": 1, "max_count": 3, "remaining": 2}
-- If over limit: SQLSTATE 54001 error with retry_after_ms

-- Check remaining budget without consuming
SELECT RATE_REMAINING('attack_cooldown', 'player-123', 3, 10);
-- Returns: {"remaining": 2, "current": 1, "max_count": 3, "resets_in_ms": 7500}

-- Admin reset (clear a player's cooldown)
SELECT RATE_RESET('attack_cooldown', 'player-123');
```

## Converting to KV

```sql
-- Convert an existing collection when access pattern is key-dominant
CONVERT COLLECTION cache TO kv;
```

## Related

- [Documents](documents.md) — For richer document structures
- [Real-Time](real-time.md) — KV changes appear in CDC, LIVE SELECT, pub/sub

[Back to docs](README.md)
