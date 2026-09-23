# NodeDB-Lite

NodeDB-Lite is the embedded build of NodeDB. It runs the same query surface and storage engines
inside your process instead of as a server, and it lives in its own repository —
[NodeDB-Lab/nodedb-lite](https://github.com/NodeDB-Lab/nodedb-lite) — because it ships on a
different cadence and targets platforms the server does not: mobile, desktop, and WASM.

## What is shared

Lite consumes the same crates the server compiles: `nodedb-array`, `nodedb-columnar`,
`nodedb-crdt`, `nodedb-fts`, `nodedb-graph`, `nodedb-spatial`, `nodedb-strict`, `nodedb-vector`,
plus the cross-cutting `nodedb-types`, `nodedb-physical`, `nodedb-mem`, `nodedb-query` and
`nodedb-codec`. An engine fix lands in both builds.

The engines without a shared crate — document, KV, timeseries, sparse vectors, HTAP — are
implemented once per repository. Their behaviour is not compared by a shared test suite today;
treat cross-build parity there as unverified.

## Sync

Lite syncs to an Origin cluster over the Sync protocol (WebSocket), so an embedded client writes
locally and replicates in the background. The protocol itself is described in
[Protocols](protocols.md); the offline patterns are in
[Offline sync patterns](offline-sync-patterns.md).

Sync coverage differs by engine: array has a dedicated subtree, the columnar family (columnar,
timeseries, spatial), vector and FTS have dedicated outbound paths, document/KV/CRDT/strict use
the generic delta path, and graph plus sparse vectors have no sync path yet.

## WASM

Lite compiles to WebAssembly for browsers and Node.js under the `nodedb-lite-wasm` crate — see
[WASM Build and Deployment](wasm.md). Lite-WASM is a client only: it never acts as a Raft member
or a vShard host.

## Where to go next

- [Getting Started](getting-started.md) — the server path
- [WASM Build and Deployment](wasm.md) — building and running Lite in a browser
- [Protocols](protocols.md) — the Sync protocol Lite speaks
