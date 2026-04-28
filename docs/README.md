# NodeDB Documentation

Welcome to the NodeDB docs. These guides explain what each engine does, when to use it, and how to get started. For full API reference, see the [cargo docs](https://docs.rs/nodedb) or the [API reference](https://github.com/NodeDB-Lab/nodedb-docs) (coming soon).

## Getting Started

- [Getting Started](getting-started.md) — Prerequisites, build, run, first queries
- [Architecture](architecture.md) — How the three-plane execution model works (now includes cross-engine identity)
- [Query Language](query-language.md) — Full SQL reference (DDL, DML, engine-specific syntax, functions)
- [Protocols](protocols.md) — Six wire protocols (pgwire, NDB, HTTP, RESP, ILP, Sync)

## Engine Guides

- [Vector Search](vectors.md) — HNSW index, quantization, adaptive filtering, hybrid search
- [Graph](graph.md) — CSR adjacency, 13 algorithms, MATCH patterns, GraphRAG
- [Documents](documents.md) — Schemaless (MessagePack + CRDT) and Strict (Binary Tuples, OLTP)
- [Columnar](columnar.md) — Per-column compression, predicate pushdown, HTAP bridge
- [Timeseries](timeseries.md) — ILP ingest, continuous aggregation, PromQL, approximate aggregation
- [Spatial](spatial.md) — R\*-tree, geohash, H3, OGC predicates, hybrid spatial-vector
- [Key-Value](kv.md) — O(1) lookups, TTL, atomic INCR/CAS, sorted indexes (leaderboards), rate gates, SQL-queryable
- [Full-Text Search](full-text-search.md) — BM25, stemming, fuzzy, hybrid vector fusion
- [Array (NDArray)](array.md) — Multi-dimensional tiles with Z-order indexing, compression, bitemporal support

## AI/ML Patterns

- [AI Pattern Guides](ai/README.md) — Index of all AI/ML guides
- [RAG Pipelines](ai/rag-pipelines.md) — Basic, hybrid, filtered, parent-document, conversational RAG
- [GraphRAG](ai/graphrag.md) — Entity extraction, graph expansion, community summarization
- [Agent Memory](ai/agent-memory.md) — Episodic, semantic, working memory with scheduled consolidation
- [On-Device AI](ai/on-device.md) — NodeDB-Lite for offline RAG, CRDT sync, WASM, privacy
- [Multi-Modal Search](ai/multi-modal-search.md) — Multiple vector columns, cross-modal CLIP, RRF fusion
- [Feature Store](ai/feature-store.md) — Training features, point-in-time lookups, batch export, online serving
- [CDC for Inference Triggers](ai/cdc-inference-triggers.md) — Embedding pipelines, graph re-indexing, model output routing
- [Evaluation Tracking](ai/evaluation-tracking.md) — Experiment metrics, retriever comparison, drift detection
- [Multi-Tenancy for AI SaaS](ai/multi-tenancy.md) — WAL-level isolation, RLS during search, per-tenant budgets

## Cross-Engine Features

- [Bitemporal Queries](bitemporal.md) — System time and valid time, audit trails, corrections, compliance
- [Cross-Engine Identity](architecture.md#cross-engine-identity) — Surrogate bitmaps for fused multi-engine queries

## Deployment & Clients

- [WASM (Experimental)](wasm.md) — Browser and Node.js build, offline-first CRDT sync
- [NodeDB-Lite](https://github.com/NodeDB-Lab/nodedb-lite) — Embedded database for phones, browsers, desktops (separate repo)

## Platform & Operations

- [Security](security/README.md) — Overview, encryption, quick reference
  - [Authentication](security/auth.md) — Users, passwords, API keys, JWKS, mTLS
  - [Roles & Permissions](security/rbac.md) — RBAC, GRANT, REVOKE
  - [Row-Level Security](security/rls.md) — Per-row filtering with `$auth.*` context
  - [Audit & Change Tracking](security/audit.md) — Hash-chained audit log, SIEM export, `updated_at` patterns
  - [Multi-Tenancy](security/tenants.md) — Isolation, quotas, backup, GDPR purge
- [Real-Time](real-time.md) — LIVE SELECT, CDC change streams, consumer groups, webhook delivery, durable topics, cron scheduler
- [CLI (`ndb`)](https://github.com/NodeDB-Lab/nodedb-cli) — Terminal client usage and configuration (separate repo)
- [Studio](https://github.com/NodeDB-Lab/nodedb-studio) — GUI client (Dioxus desktop + web) (separate repo)
