# NodeDB

**A local-first, real-time, edge-to-cloud hybrid database for multi-modal workloads.**

NodeDB provides Vector, Graph, Document (schemaless + strict), Columnar (with Timeseries and Spatial profiles), Key-Value, and Full-Text Search engines in a single Rust binary. All engines share the same storage, memory, and query planner — cross-engine queries execute in one process with zero network hops.

## Engines

| Engine                                       | What it does                                                                       |
| -------------------------------------------- | ---------------------------------------------------------------------------------- |
| [Vector](docs/vectors.md)                    | HNSW index with SQ8/PQ/IVF-PQ quantization and adaptive bitmap filtering           |
| [Graph](docs/graph.md)                       | CSR adjacency index, 13 algorithms, Cypher-subset MATCH, GraphRAG fusion           |
| [Document](docs/documents.md)                | Schemaless (MessagePack + CRDT sync) or Strict (Binary Tuples, O(1) field access)  |
| [Columnar](docs/columnar.md)                 | Per-column compression (ALP, FastLanes, FSST), predicate pushdown, HTAP bridge     |
| [Timeseries](docs/timeseries.md)             | Columnar profile with ILP ingest, continuous aggregation, PromQL, 12 SQL functions |
| [Spatial](docs/spatial.md)                   | R\*-tree, geohash, H3, OGC predicates, hybrid spatial-vector search                |
| [Key-Value](docs/kv.md)                      | Hash-indexed O(1) lookups, TTL, secondary indexes, SQL-queryable                   |
| [Full-Text Search](docs/full-text-search.md) | BM25 + 15-language stemming + fuzzy + hybrid vector fusion                         |

## Deployment Modes

- **Origin (server)** — Full distributed database. Multi-Raft consensus, Thread-per-Core data plane with io_uring, PostgreSQL-compatible SQL over pgwire. Horizontal scaling with automatic shard balancing.
- **Origin (local)** — Same binary, single-node. No cluster overhead. Like running Postgres locally.
- **[NodeDB-Lite](docs/lite.md) (embedded)** — In-process library for phones, browsers (WASM), and desktops. All engines run locally with sub-millisecond reads. Offline-first with CRDT sync to Origin.

Application code is the same across all three modes — the `NodeDb` trait exposes identical methods whether backed by an in-process Lite engine or a remote Origin server.

## Quick Start

```bash
# Build
cargo build --release

# Run single-node server
./target/release/nodedb

# Connect with the CLI
./target/release/ndb

# Or connect with psql
psql -h localhost -p 6432
```

```sql
-- Create a document collection and insert data
CREATE COLLECTION users TYPE document;
INSERT INTO users { name: 'Alice', email: 'alice@example.com' };
SELECT * FROM users WHERE name = 'Alice';

-- Create a vector index and search
CREATE COLLECTION articles TYPE document;
CREATE VECTOR INDEX ON articles FIELDS embedding DIMENSION 384;
SELECT * FROM articles WHERE embedding <-> $query_vec LIMIT 10;
```

See [Getting Started](docs/getting-started.md) for a fuller walkthrough.

## Documentation

- [Getting Started](docs/getting-started.md) — Build, run, connect, first queries
- [Architecture](docs/architecture.md) — How the hybrid execution model works
- [Engine Guides](docs/README.md) — Deep dives into each engine
- [Security](docs/security.md) — Auth, RBAC, RLS, encryption
- [Real-Time](docs/real-time.md) — LIVE SELECT, CDC, pub/sub
- [NodeDB-Lite](docs/lite.md) — Embedded edge database
- [CLI (`ndb`)](docs/cli.md) — Terminal client

API reference will be published at [nodedb-docs](https://github.com/NodeDB-Lab/nodedb-docs) (coming soon). In the meantime, `cargo doc --open` generates full Rust API documentation.

## Building

```bash
cargo build --release           # Build all crates
cargo test --all-features       # Run all tests
cargo fmt --all --check         # Check formatting
cargo clippy --all-targets -- -D warnings  # Lint
```

## Status

NodeDB is pre-release. All engines are implemented and tested, but the system has not yet been deployed in production.

## License

NodeDB is licensed under the [Business Source License 1.1](LICENSE.md). You can use NodeDB for any commercial purpose — SaaS products, AI platforms, internal tools, self-hosted deployments, anything. The only restriction is offering NodeDB itself as a hosted database service (DBaaS) or commercial database tooling; that requires a [commercial license](LICENSE.md). Converts to Apache 2.0 on 2030-01-01.
