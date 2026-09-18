# Contributing to NodeDB

Thank you for your interest in contributing. NodeDB is a complex, multi-engine distributed database — this guide helps you get oriented before diving in.

---

## Where to Start

- **[Discord](https://discord.gg/s54gDMVc7B)** — best place for questions, ideas, and getting feedback before opening a PR
- **[GitHub Discussions](https://github.com/NodeDB-Lab/nodedb/discussions)** — design proposals, architecture questions, RFCs
- **[GitHub Issues](https://github.com/NodeDB-Lab/nodedb/issues)** — bug reports and well-scoped feature requests

For anything substantial (new engine feature, protocol change, storage format change), open a Discussion first. Cold PRs for large changes will be asked to go through discussion before review begins.

---

## Architecture Primer

NodeDB uses a **Three-Plane Execution Model**. Understanding this is mandatory before writing any code that touches the Data Plane.

```
Control Plane  (Tokio)      — SQL parsing, query planning, connection handling
Data Plane     (TPC + io_uring) — physical execution, storage I/O, SIMD math
Event Plane    (Tokio)      — triggers, CDC, cron, CRDT sync coordination
```

These planes communicate only through bounded lock-free bridges:

- **SPSC bridge** — Control ↔ Data (request/response)
- **Event Bus** — Data → Event (write events, WAL-backed)

**Mixing planes is a correctness bug, not a style issue.** Spawning a Tokio task from Data Plane code, or calling io_uring from Control Plane code, will be rejected in review regardless of how small the change is.

Before writing any Data Plane or engine code, read `nodedb/CLAUDE.md` in full. It covers the plane rules, engine routing via `EngineRules`, serialization requirements, error handling conventions, and file size limits — all of which are enforced in review.

---

## What We Welcome

- Bug fixes with a reproducing test
- Performance improvements with benchmark evidence
- New quantization codecs (`nodedb-codec`, `nodedb-vector`)
- FTS analyzer additions (`nodedb-fts`) — new language support, tokenizers
- Spatial predicate additions (`nodedb-spatial`)
- Graph algorithm additions (`nodedb-graph`)
- Test coverage improvements — unit tests, integration tests, failpoint tests
- Documentation fixes and additions
- Benchmark improvements (`nodedb-bench`)

## What to Discuss First

- New engine types or major engine features
- SQL syntax additions or changes
- Wire protocol changes (pgwire, HTTP, native, RESP, ILP)
- Storage format or WAL format changes
- Raft or replication changes
- Security-relevant changes

A wire or storage **version bump** in particular needs discussion before you open the PR — it forces a coordinated upgrade, so we decide together when it lands. Don't bump ahead of the code that reads the new field; sequence the bump with its consumer so one upgrade delivers a real capability.

## What We Don't Accept

- Cosmetic changes (whitespace, comment reformatting) without functional change
- AI-generated code submitted without careful review and understanding by the author
- Changes that break the plane separation model
- `_ =>` catch-alls on `EngineType`, `CollectionType`, or `PhysicalPlan`
- New `Arc<Mutex<T>>` used as cross-plane communication
- Roadmap markers in source or test names — phase/unit/wave labels, or comments that reference an internal planning doc a reader can't open. Comments explain _behavior_; test names state _what they assert_.

---

## Development Setup

**Requirements:**

- Linux kernel ≥ 5.1 (io_uring required for the Data Plane)
- Rust stable (see `rust-toolchain.toml` for the pinned version)
- `cargo-nextest` — required for the test suite

```bash
# Install nextest (one-time)
cargo install cargo-nextest --locked

# Clone and build
git clone https://github.com/NodeDB-Lab/nodedb.git
cd nodedb
cargo build --release

# Run the full test suite
# Debug-profile test binaries overflow the 2 MiB thread default; 32 MiB is the value CI sets.
RUST_MIN_STACK=33554432 cargo nextest run --all-features
```

**Why nextest, not `cargo test`?** The `.config/nextest.toml` defines a `cluster` test group that serializes 3-node integration tests and retries known-flaky ones. `cargo test` ignores all of that and will hang or fail on the cluster suite.

---

## Code Standards

All of the following are enforced in CI and will block merge if failing:

```bash
cargo fmt --all                                             # formatting
cargo clippy --workspace --all-targets --all-features -- -D warnings  # lints
cargo nextest run --all-features                            # tests
cargo deny check                                            # dependency audit
```

Key rules (full list in `nodedb/CLAUDE.md`):

- No `.unwrap()` in library code — use `thiserror`, propagate errors
- No `Result<T, String>` — always typed error enums
- `mod.rs` files contain only `pub mod` and `pub use` — no logic, no types
- Files stay under 500 lines of non-test code — split by concern before you hit the limit
- `sonic_rs` for runtime JSON, `zerompk` (MessagePack) for internal transport — never `serde_json::to_vec` in engine code
- `nodedb_types::Value` for all internal values

---

## Engine-Specific Guidance

NodeDB routes all DML through `EngineRules` in `nodedb-sql`. This is the single source of truth for what each engine supports.

**Adding a feature to an existing engine:**

- Find the engine's `EngineRules` impl in `nodedb-sql/src/engine_rules/<engine>.rs`
- Add the new `SqlPlan` variant if needed — the compiler will tell you every handler that requires updating
- Wire the physical handler in `nodedb/src/data/executor/handlers/`
- Add integration tests in `nodedb/tests/`

**Adding a new engine:**

- Implement the `EngineRules` trait — the compiler enforces completeness across all operations
- Add a new `EngineType` variant — exhaustive matches will surface every place that needs updating
- New engines must fit within the Three-Plane model; no cross-plane shortcuts

**Engine-specific entry points:**

| Engine                          | EngineRules                                                | Physical handlers                       |
| ------------------------------- | ---------------------------------------------------------- | --------------------------------------- |
| Vector                          | `engine_rules/vector.rs`                                   | `handlers/vector_*.rs`                  |
| Graph                           | `engine_rules/graph.rs`                                    | `handlers/graph_*.rs`                   |
| Array                           | `engine_rules/array.rs` + `parser/array_stmt/`             | `handlers/array_*.rs`                   |
| FTS                             | `engine_rules/fts.rs`                                      | `handlers/fts_*.rs`                     |
| Columnar / Timeseries / Spatial | `engine_rules/{columnar,timeseries,spatial}.rs`            | `handlers/columnar_*.rs`                |
| Document / KV                   | `engine_rules/{document_schemaless,document_strict,kv}.rs` | `handlers/kv_*.rs`, `handlers/doc_*.rs` |

---

## Testing

**Unit tests** — place in the same file as the code under test using `#[cfg(test)] mod tests { ... }`. Can test private functions.

**Integration tests** — place in `nodedb/tests/` or the relevant crate's `tests/` directory. Test public API only. Use `tests/common/mod.rs` for shared helpers (not `tests/common.rs`).

**Failpoint tests** — for fault injection and panic recovery, use the `failpoints` feature flag and `FailGuard` from `nodedb::fail_point`. Compile with `--features failpoints`.

**Cluster tests** — 3-node integration tests live in `nodedb/tests/` and are annotated with the `cluster` nextest group. They serialize automatically via `.config/nextest.toml` — don't run them with `cargo test`.

When adding a new engine feature, include:

1. A unit test covering the core logic
2. An integration test covering the SQL surface
3. A negative test (invalid input, wrong engine, constraint violation)

---

## Commits and Pull Requests

**Commit format** — we use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(vector): add BBQ quantization codec
fix(wal): correct group commit flush under high concurrency
docs(fts): add analyzer configuration guide
test(array): add bitemporal tile-version roundtrip test
```

Types: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `chore`

**Keep commits small and focused.** One logical change per commit. If your PR touches multiple concerns, split into separate commits so reviewers can follow the intent.

**Draft PRs are welcome.** Open a draft early to get directional feedback before investing in a full implementation.

**PR description should include:**

- What the change does and why
- How to test it
- Any tradeoffs or alternatives considered
- For performance changes: benchmark results (before/after)

**Review timeline:** NodeDB is a small team. PRs may not be reviewed immediately. If a PR sits for more than two weeks without a response, ping us on Discord. PRs with no activity for 60 days may be closed to keep the queue manageable — they can always be reopened.

---

## Security

Do not report security vulnerabilities in public issues. Use [GitHub Security Advisories](https://github.com/NodeDB-Lab/nodedb/security/advisories/new) to report privately. See [SECURITY.md](SECURITY.md) for the full disclosure policy.

---

## Related Projects

This repository covers **NodeDB Origin** (the server) only. The following projects are maintained separately, each with their own contributing guide and license:

| Project                | Repository                                                   | License    |
| ---------------------- | ------------------------------------------------------------ | ---------- |
| NodeDB Lite (embedded) | [nodedb-lite](https://github.com/NodeDB-Lab/nodedb-lite)     | Apache 2.0 |
| `ndb` CLI              | [nodedb-cli](https://github.com/NodeDB-Lab/nodedb-cli)       | Apache 2.0 |
| NodeDB Studio (GUI)    | [nodedb-studio](https://github.com/NodeDB-Lab/nodedb-studio) | Apache 2.0 |
| Official Docs          | [nodedb-docs](https://github.com/NodeDB-Lab/nodedb-docs)     | Apache 2.0 |
| Benchmarks             | [nodedb-bench](https://github.com/NodeDB-Lab/nodedb-bench)   | Apache 2.0 |

If you want to contribute to those projects, start from their own `CONTRIBUTING.md`.

## Building Tools and SDKs

If you want to build a client library, SDK, framework integration, or any other tool that works with NodeDB — go for it. You don't need permission.

NodeDB exposes stable wire protocols (pgwire, HTTP, native MessagePack, RESP, ILP, WebSocket) that you can build against. Any PostgreSQL-compatible driver works out of the box.

A few things to keep in mind:

- **Naming** — don't use "NodeDB" as the primary name of your project in a way that implies official endorsement. `nodedb-rs`, `nodedb-go`, `nodedb-django` as unofficial community clients are fine; "NodeDB Cloud" or "NodeDB Enterprise" as a product name is not.
- **Let us know** — if you build something useful, share it on [Discord](https://discord.gg/s54gDMVc7B). We're happy to link to community tools from the docs.
- **License** — the NodeDB wire protocols and SQL dialect are not encumbered by BUSL. Building a client that speaks pgwire or MessagePack to a NodeDB server does not make your client subject to the NodeDB license.

## License

NodeDB uses a dual-license model:

| Crates                                                                                                                                                                                             | License                          |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| `nodedb-types`, `nodedb-client`, `nodedb-query`, `nodedb-codec`, `nodedb-spatial`, `nodedb-graph`, `nodedb-vector`, `nodedb-fts`, `nodedb-strict`, `nodedb-columnar`, `nodedb-array`, `nodedb-sql` | [Apache 2.0](LICENSE.APACHE-2.0) |
| `nodedb` (server), `nodedb-bridge`, `nodedb-wal`, `nodedb-mem`, `nodedb-crdt`, `nodedb-raft`, `nodedb-cluster`                                                                                     | [BUSL-1.1](LICENSE)              |

**What this means for contributors:** contributions to Apache-2.0 crates are under Apache 2.0; contributions to server crates are under BUSL-1.1. No CLA is required for either.

**What this means for SDK/tool builders:** you can depend on the Apache-2.0 crates freely in your own open-source or commercial projects. The BUSL restriction only applies to the server crates — you are not affected unless you are redistributing the NodeDB server itself as a hosted service.

Related projects (Lite, CLI, Studio, Docs) are Apache 2.0 — check their respective repositories for details.
