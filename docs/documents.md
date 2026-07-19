# Document Engine

NodeDB supports two document storage modes — **schemaless** and **strict** — each optimized for different workloads. You choose per collection and can convert between them at any time.

## Schemaless Documents

Flexible JSON-like documents stored as MessagePack. No schema required — fields can vary between documents. This is what you'd use MongoDB for.

### When to Use

- Prototyping and rapid iteration
- AI agent state and episodic memory
- User profiles, config, nested data
- Any data where structure is unknown or frequently changing
- Offline-first apps with CRDT sync

### Key Features

- **MessagePack storage** — Compact binary encoding with fast serialization
- **Secondary indexes** — Index any field for filtered queries
- **CRDT sync** — Offline-first variant with delta-based sync to Origin via Loro CRDTs
- **Vector, graph, spatial, and full-text indexes** — Add any cross-engine index to document collections

### Examples

```sql
CREATE COLLECTION users;

-- Fields are flexible — no schema needed
INSERT INTO users { name: 'Alice', email: 'alice@example.com', age: 30 };
INSERT INTO users { name: 'Bob', role: 'admin', tags: ['ops', 'dev'] };

-- Create a secondary index
CREATE INDEX ON users FIELDS email;

-- Query with SQL
SELECT * FROM users WHERE age > 25;
SELECT name, tags FROM users WHERE role = 'admin';
```

## Typeguards (Schemaless Validation)

Typeguards add write-time validation to schemaless collections without changing the storage format. Fields are type-checked, required fields are enforced, and CHECK constraints run — but unknown fields still pass freely. Think of it as "gradually typed" documents.

```sql
CREATE TYPEGUARD ON users (
    email STRING REQUIRED CHECK (email LIKE '%@%.%'),
    age INT CHECK (age >= 0 AND age <= 150),
    role STRING DEFAULT 'user',
    updated_at TIMESTAMP VALUE now()
);

-- Valid: all guarded fields pass
INSERT INTO users { id: 'u1', name: 'Alice', email: 'alice@example.com', age: 30 };

-- Fails: email is REQUIRED
INSERT INTO users { id: 'u2', name: 'Bob' };
-- ERROR: field 'email' is required but absent or null

-- Fails: age must be INT
INSERT INTO users { id: 'u3', email: 'x@y.com', age: 'old' };
-- ERROR: field 'age' must be INT, got STRING

-- 'name' is NOT in the typeguard — passes freely (schemaless flexibility)
INSERT INTO users { id: 'u4', email: 'z@w.com', extra_field: 'anything' };
```

### Typeguard Features

- **DEFAULT** — inject a value when the field is absent (does not overwrite user input)
- **VALUE** — always inject/overwrite (for computed fields like `updated_at`)
- **CHECK** — SQL boolean expression validated at write time
- **REQUIRED** — field must be present and non-null
- **VALIDATE** — scan existing data for violations without blocking writes
- **CONVERT TO document_strict** — typeguard fields become schema columns, CHECK constraints carry over

```sql
-- Modify guards
ALTER TYPEGUARD ON users ADD score FLOAT CHECK (score >= 0);
ALTER TYPEGUARD ON users DROP age;

-- Introspect
SHOW TYPEGUARD ON users;
SHOW CONSTRAINTS ON users;

-- Audit existing data
VALIDATE TYPEGUARD ON users;

-- Graduate to strict schema
CONVERT COLLECTION users TO document_strict;
```

## Strict Documents

Schema-enforced documents stored as Binary Tuples with O(1) field extraction. The engine jumps directly to the byte offset of any column without parsing the rest of the row — 3-4x better cache density than MessagePack or BSON. This is what you'd use PostgreSQL for.

### When to Use

- OLTP workloads with known schemas (CRM, accounting, ERP)
- High-throughput transactional writes
- Data that benefits from schema enforcement and constraints
- When you need ALTER COLUMN support with zero-downtime migration

### Key Features

- **O(1) field extraction** — Direct byte-offset access, no row parsing
- **3-4x cache density** — No repeated field names in storage
- **Schema enforcement** — Types, NOT NULL, DEFAULT, CHECK constraints
- **ALTER ADD COLUMN** — Multi-version reads for zero-downtime schema evolution
- **CRDT adapter** — Sync-capable with Loro integration
- **HTAP bridge** — Automatic CDC to columnar materialized views for analytics

### Examples

```sql
CREATE COLLECTION orders (
    id UUID DEFAULT gen_uuid_v7(),
    customer_id UUID NOT NULL,
    total DECIMAL NOT NULL,
    status STRING DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT now()
) WITH (engine='document_strict');

INSERT INTO orders (customer_id, total, status)
VALUES ('550e8400-e29b-41d4-a716-446655440000', 149.99, 'shipped');

-- Fast point lookups
SELECT * FROM orders WHERE id = '...';

-- Schema evolution
ALTER COLLECTION orders ADD COLUMN region STRING DEFAULT 'us-east';

-- Create a materialized view for analytics (HTAP)
CREATE MATERIALIZED VIEW order_stats AS
SELECT status, COUNT(*), SUM(total)
FROM orders
GROUP BY status;
```

## CRDT Document Collections

Declare a document collection CRDT-backed at creation time and plain SQL DML converges via last-writer-wins instead of overwriting:

```sql
CREATE COLLECTION crdt_notes (
    id TEXT PRIMARY KEY,
    title TEXT,
    body TEXT
) WITH (crdt=true);

-- Full-replace write (untouched keys pruned)
INSERT INTO crdt_notes (id, title, body) VALUES ('a', 'v1', 'text');
UPSERT INTO crdt_notes (id, title) VALUES ('a', 't2');

-- PK-targeted UPDATE is a per-field LWW merge: only the provided
-- fields are written; untouched fields survive concurrent writers
UPDATE crdt_notes SET title = 't3' WHERE id = 'a';

-- PK-targeted DELETE writes a tombstone
DELETE FROM crdt_notes WHERE id = 'a';

-- RETURNING works on CRDT UPDATE/DELETE
UPDATE crdt_notes SET title = 't4' WHERE id = 'a' RETURNING id, title;
DELETE FROM crdt_notes WHERE id = 'a' RETURNING id;
```

`crdt=true` is only valid on document collections. DML on a CRDT collection routes through the CRDT engine — there is no silent fallthrough to the plain document path (that would bypass convergence). Statement shapes that cannot be expressed as a CRDT operation are rejected with a typed error rather than downgraded:

- Predicate (non-primary-key) `UPDATE` / `DELETE`
- `UPDATE` with a non-literal right-hand side (`SET count = count + 1`)
- `INSERT ... ON CONFLICT DO UPDATE`

The `crdt` flag is part of the collection descriptor: it replicates across the cluster and travels in sync `CollectionSchema` announcements, so Lite/WASM peers see the same convergence semantics.

**Movable lists (SDK).** The client SDK exposes CRDT movable-list operations on documents: `list_insert(collection, doc_id, list_path, index, fields)`, `list_delete(...)`, and `list_move(collection, doc_id, list_path, from_index, to_index)` — dispatched as native opcodes and merged conflict-free across devices.

## Choosing Between Modes

|               | Schemaless                            | Strict                               |
| ------------- | ------------------------------------- | ------------------------------------ |
| Schema        | Flexible, evolves freely              | Fixed, enforced on write             |
| Field access  | Parse MessagePack                     | O(1) byte offset                     |
| Cache density | Good                                  | 3-4x better                          |
| Best for      | Prototyping, agent state, varied data | OLTP, transactions, known schemas    |
| CRDT sync     | Native                                | Via adapter                          |
| HTAP          | No                                    | Yes (materialized views to columnar) |

## Converting Between Modes

```sql
-- Start schemaless, convert when schema stabilizes
CONVERT COLLECTION users TO document_strict;

-- Or move into KV
CONVERT COLLECTION cache TO kv;
```

No data loss on conversion. NodeDB infers the schema from existing documents when converting to strict mode.

## Bitemporal Support

Both schemaless and strict documents support bitemporal queries — tracking system time (when data was inserted) and valid time (when the data represents).

```sql
-- Query documents as they existed yesterday (system time)
SELECT * FROM users
AS OF SYSTEM TIME '2026-06-06T00:00:00Z';

-- Query documents that were valid at a past date (valid time)
SELECT * FROM users
AS OF VALID TIME 1700000000000;

-- Full temporal lineage: what did we know then?
SELECT * FROM users
AS OF SYSTEM TIME 1700000000000
AS OF VALID TIME 1700000001000;
```

This enables audit trails, compliance (GDPR history), and correction workflows. See [Bitemporal](bitemporal.md) for detailed examples.

## Related

- [Bitemporal](bitemporal.md) — Cross-engine temporal queries and audit trails
- [Columnar](columnar.md) — HTAP bridge from strict documents to columnar analytics
- [Key-Value](kv.md) — For key-dominant access patterns
- [NodeDB-Lite](lite.md) — Schemaless documents with CRDT sync on edge devices

[Back to docs](README.md)
