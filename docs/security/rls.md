# Row-Level Security (RLS)

RLS policies filter rows based on the authenticated user's context. Policies apply transparently to all queries — no application code changes needed. Works across all eight engines.

## Creating Policies

```sql
-- Users see only their own orders
CREATE RLS POLICY user_orders ON orders FOR READ
    USING (customer_id = $auth.id);

-- Users can only modify their own data
CREATE RLS POLICY user_write ON orders FOR WRITE
    USING (customer_id = $auth.id);

-- Combined read + write
CREATE RLS POLICY own_data ON profiles FOR ALL
    USING (user_id = $auth.id);

-- Org-scoped access
CREATE RLS POLICY org_access ON projects FOR ALL
    USING (org_id = $auth.org_id);

-- Role-based bypass: admins see everything
CREATE RLS POLICY admin_bypass ON orders FOR READ
    USING ($auth.role = 'admin' OR customer_id = $auth.id);
```

## Policy Types

| Type    | Applies to             |
| ------- | ---------------------- |
| `READ`  | SELECT queries         |
| `WRITE` | INSERT, UPDATE, DELETE |
| `ALL`   | Both read and write    |

### What a `WRITE` policy decides

The predicate is evaluated against the **row image the statement persists** —
the new row for an insert or update, the removed row for a delete, the merged
row for an upsert's conflict branch. A row that fails the predicate fails the
whole statement; it is never silently skipped, so an affected-row count never
reports a write that did not happen.

Document `INSERT`, `UPDATE`, `DELETE`, `UPSERT`, `MERGE`, and `UPDATE ... FROM` build their policy image inside the storage engine. The compiled predicate travels with the query plan and evaluates the actual materialized row before persistence. Defaults, coercions, typeguards, and generated columns therefore participate in the decision. A denied row rolls back its statement, including every conforming row in the same batch.

Columnar, timeseries, and spatial collections evaluate their compiled write predicates against the rows about to be written or removed. Timeseries ingest checks each parsed row across SQL, JSON, MessagePack, and line-protocol ingest.

### When the decision happens inside a transaction

A write issued inside a `BEGIN..COMMIT` block runs at statement time: the row
image is produced, staged, and made readable to that transaction's own queries
before COMMIT replays the write durably. The policy decides the row at both
points. The statement-time decision is what a client sees — a refused write
fails at the statement, with no affected-row count and nothing visible to the
transaction — and the COMMIT-time decision guards the durable apply. This holds
for the document, key-value, columnar, timeseries, and spatial engines alike.

Some write shapes carry no such row body at all: a key-value atomic computes
from the stored value, `TRUNCATE` removes every row without reading one, and
vector, FTS, and graph writes carry an embedding, extracted text, or edge
endpoints instead of the row the predicate names. A write policy on the
collection **refuses** those statements rather than letting them persist rows
the predicate was never evaluated against. The refusal names the collection and
says which image was unavailable.

Externally submitted CRDT deltas are the exception: they are admitted against
the storage engine's authoritative post-merge image, so the predicate decides
the merged row exactly as it decides an inserted one.

## Permissive vs Restrictive

By default, multiple policies on the same collection are **OR-combined** (permissive). If ANY policy passes, the row is visible.

Use `RESTRICTIVE` to AND-combine. ALL restrictive policies must pass.

```sql
-- Both must pass: same org AND not deleted
CREATE RLS POLICY org_filter ON docs FOR READ
    USING (org_id = $auth.org_id) RESTRICTIVE;

CREATE RLS POLICY not_deleted ON docs FOR READ
    USING (status != 'deleted') RESTRICTIVE;
```

## Session Variables

RLS predicates use `$auth.*` variables populated from the authenticated session (JWT claims, DB user, API key context):

| Variable            | Source                    | Example                               |
| ------------------- | ------------------------- | ------------------------------------- |
| `$auth.id`          | JWT `sub` or DB user ID   | `customer_id = $auth.id`              |
| `$auth.role`        | JWT role claim or DB role | `$auth.role = 'admin'`                |
| `$auth.org_id`      | JWT org claim             | `org_id = $auth.org_id`               |
| `$auth.tenant_id`   | Tenant context            | `tenant_id = $auth.tenant_id`         |
| `$auth.database_id` | Current database          | `shard_id = $auth.database_id`        |
| `$auth.scopes`      | JWT scopes                | `$auth.scopes CONTAINS 'read:orders'` |

**`$auth.database_id`** enables sharding by database:

```sql
-- Rows visible only if they belong to the session's bound database
CREATE RLS POLICY database_shard ON documents FOR READ
    USING (shard = $auth.database_id);

-- Cross-database shards (multi-tenant SaaS)
CREATE RLS POLICY shard_access ON documents FOR READ
    USING (tenant_id = $auth.tenant_id AND shard = $auth.database_id);
```

Substitution is fail-closed: if the session lacks a database ID, the policy evaluation fails and the row is blocked.

## Managing Policies

```sql
-- View all policies
SHOW RLS POLICIES;

-- Drop a policy
DROP RLS POLICY user_orders ON orders;
```

## How It Works

1. User authenticates (SCRAM, JWT, API key, mTLS)
2. `$auth.*` variables populated from credentials
3. On every query, RLS predicates injected into WHERE clause at plan time
4. Data Plane never sees unfiltered data
5. Applies to all engines: document, vector, graph, columnar, KV, FTS, spatial

## Cross-Engine Behavior

RLS filters are injected at the SQL plan level, before engine-specific dispatch. This means:

- **Vector search**: `SEARCH articles USING VECTOR(...)` respects RLS — unauthorized vectors excluded from results
- **Graph traversal**: `GRAPH TRAVERSE FROM 'a'` skips edges to nodes the user can't see
- **FTS**: `text_match(body, 'query')` returns only documents passing the RLS predicate
- **KV**: `SELECT * FROM cache WHERE key = 'k1'` returns empty if RLS blocks the row

[Back to security](README.md)
