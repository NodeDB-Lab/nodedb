# WASM Build and Deployment

**WASM is a NodeDB-Lite-only target.** The Origin server (Tokio Control Plane + io_uring Data Plane + cluster transport) does not target WebAssembly and never will — it depends on io_uring, multi-threading, and OS-level networking that browsers and the WASM runtime do not provide. When you see "WASM" in NodeDB documentation, it always refers to `nodedb-lite-wasm`: the embedded library compiled for browsers and Node.js.

NodeDB-Lite compiles to WebAssembly and exposes the same `NodeDb` trait you use in native Lite. To talk to an Origin cluster from the browser, use Lite-WASM locally and replicate via CRDT sync over WebSocket — never run Origin in the browser.

**Status: Experimental.** Lite-WASM support is feature-complete for all eight engines. Testing and CI integration are ongoing; treat the build as preview-quality. Report issues via GitHub.

## Building for WASM

### Prerequisites

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-pack
```

### Build Steps

From the `nodedb-lite-wasm` directory:

```bash
cd nodedb-lite-wasm

# Development (unoptimized, faster build, good for testing)
wasm-pack build --target web --dev

# Production (optimized for size and speed)
wasm-pack build --target web --release

# Node.js target (if targeting a Node.js runtime)
wasm-pack build --target nodejs --release
```

Output is in `pkg/`:

- `nodedb_lite_wasm.js` — ES6 module export
- `nodedb_lite_wasm_bg.wasm` — The WASM binary
- `package.json` — NPM metadata

### Size

A size budget will be set once the WASM build pipeline is stabilized. Until then, treat bundle size as a moving target — measure the output of your own build rather than relying on a documented number. Disabling unused engines via Cargo features reduces size; running `wasm-opt` on the release artifact reduces it further.

## JavaScript / TypeScript Usage

### Installation

```bash
npm install nodedb-lite-wasm
```

Or link locally during development:

```bash
cd nodedb-lite-wasm
wasm-pack build --target web --release
cd ../my-app
npm link ../nodedb-lite-wasm/pkg
```

### Basic Example

```javascript
import init, { NodeDbLite, Collection } from "nodedb-lite-wasm";

// Initialize WASM module (one-time)
const wasm = await init();

// Create an in-memory database
const db = new NodeDbLite();

// Create a collection
await db.sql("CREATE COLLECTION users");

// Insert documents
await db.sql(
  "INSERT INTO users { name: 'Alice', email: 'alice@example.com', age: 30 }",
);
await db.sql(
  "INSERT INTO users { name: 'Bob', email: 'bob@example.com', age: 25 }",
);

// Query
const results = await db.sql("SELECT * FROM users WHERE age > 25");
console.log(results);
// Output: [{ name: 'Alice', email: 'alice@example.com', age: 30 }, ...]
```

### Vector Search

```javascript
// Create vector index
await db.sql(
  "CREATE VECTOR INDEX idx_users_embedding ON users METRIC cosine DIM 384",
);

// Insert with embedding
await db.sql(
  "INSERT INTO users { name: 'Charlie', embedding: [0.1, 0.2, ...] }",
);

// Search
const results = await db.sql(
  `SEARCH users USING VECTOR(embedding, [0.15, 0.25, ...], 10)`,
);
```

### Graph Traversal

```javascript
// Create edge collection
await db.sql("CREATE COLLECTION follows");

// Insert edges
await db.sql(
  "GRAPH INSERT EDGE IN 'follows' FROM 'user:alice' TO 'user:bob' TYPE 'FOLLOWS'",
);

// Traverse
const neighbors = await db.sql(
  "GRAPH NEIGHBORS OF 'user:alice' DIRECTION both",
);
```

### Full-Text Search

```javascript
// Create FTS index
await db.sql("CREATE COLLECTION articles");
await db.sql("CREATE FTS INDEX idx_articles_body ON articles FIELD body");

// Insert and search
await db.sql(
  "INSERT INTO articles { title: 'ML Basics', body: 'Machine learning fundamentals...' }",
);

const results = await db.sql(
  "SELECT title FROM articles WHERE text_match(body, 'machine learning')",
);
```

### Array Operations

```javascript
// Create array
await db.sql(`
  CREATE ARRAY spatial_grid
  DIMS (x INT64 DOMAIN [0, 1000), y INT64 DOMAIN [0, 1000))
  ATTRS (temperature FLOAT32)
  TILE_EXTENTS (64, 64)
`);

// Insert cells
await db.sql(
  "INSERT INTO spatial_grid (x, y, temperature) VALUES (10, 20, 23.5)",
);

// Query slice
const results = await db.sql(
  `SELECT * FROM NDARRAY_SLICE('spatial_grid', {x: [0, 100), y: [0, 100)}, ['temperature'])`,
);
```

## Offline-First with CRDT Sync

WASM supports the same CRDT sync as native NodeDB-Lite. Writes happen locally, deltas sync to Origin when online:

```javascript
import init, { NodeDbLite } from "nodedb-lite-wasm";

const db = new NodeDbLite();

// Configure sync to Origin
await db.sync_config({
  server_url: "ws://localhost:6433",
  auth_token: "your-token",
  auto_sync: true,
  sync_interval_ms: 5000,
});

// All writes are local-first
await db.sql("INSERT INTO users { name: 'Alice' }");

// Sync happens automatically on the interval
// On conflict, Origin's validation sends compensation hints back
```

See [NodeDB-Lite](https://github.com/NodeDB-Lab/nodedb-lite) for full CRDT sync documentation.

## Limitations and Known Issues

- **Lite only — no Origin in WASM.** The distributed Origin server is not a WASM target. Browser/Node clients run Lite-WASM locally and sync to a separately-deployed Origin cluster over WebSocket
- **No file persistence** — WASM runs in-memory only. For persistence, use `localStorage` or IndexedDB via a wrapper
- **Single-threaded** — no thread-per-core, no parallel execution; everything runs on the JS/WASM main thread
- **No io_uring, no native sockets** — storage and network I/O go through JS host APIs (`fetch`, IndexedDB, WebSocket); there is no NVMe path
- **No cluster role** — Lite-WASM is a client/edge node only. It cannot act as a Raft member or vShard host
- **Module size** — measure your own build; gzip before serving
- **Browser compatibility** — Requires WebAssembly support (all modern browsers + Node.js 14+)

## Deployment

### Browser (Web App)

Bundle with your web framework:

```javascript
// React example
import { useEffect, useState } from "react";
import init, { NodeDbLite } from "nodedb-lite-wasm";

export function useNodeDb() {
  const [db, setDb] = useState(null);

  useEffect(() => {
    (async () => {
      await init();
      setDb(new NodeDbLite());
    })();
  }, []);

  return db;
}

export function MyComponent() {
  const db = useNodeDb();

  const [users, setUsers] = useState([]);

  const handleQuery = async () => {
    if (!db) return;
    const results = await db.sql("SELECT * FROM users");
    setUsers(results);
  };

  return (
    <div>
      <button onClick={handleQuery}>Load Users</button>
      {users.map((u) => (
        <div key={u.id}>{u.name}</div>
      ))}
    </div>
  );
}
```

### Node.js Server

Use as an embedded database in Node.js:

```javascript
import init, { NodeDbLite } from "nodedb-lite-wasm";

const wasm = await init();

const db = new NodeDbLite();
await db.sql("CREATE COLLECTION products");
await db.sql("INSERT INTO products { name: 'Widget', price: 99.99 }");

// Query
const all = await db.sql("SELECT * FROM products");
console.log(all);

// Server would expose via HTTP/WebSocket
app.post("/products", async (req, res) => {
  const result = await db.sql(req.body.sql);
  res.json(result);
});
```

### Size Optimization

Use `wasm-opt` for additional size reduction:

```bash
# Install binaryen (includes wasm-opt)
cargo install wasm-opt

# Optimize WASM binary
wasm-opt -O4 pkg/nodedb_lite_wasm_bg.wasm -o pkg/nodedb_lite_wasm_bg.optimized.wasm

# Move optimized version back
mv pkg/nodedb_lite_wasm_bg.optimized.wasm pkg/nodedb_lite_wasm_bg.wasm
```

`wasm-opt -O4` typically yields meaningful size reduction on the release artifact. Measure before and after on your own build.

## Testing

Run WASM tests via wasm-pack:

```bash
wasm-pack test --headless --firefox
wasm-pack test --headless --chrome
```

Tests run in a real browser environment, exercising the full WASM API.

## Troubleshooting

### Module fails to initialize

```
Error: failed to initialize WASM module
```

Ensure the WASM binary is being loaded correctly. In browsers, check the Network tab of DevTools to confirm the `.wasm` file is being fetched. In Node.js, ensure the package.json `main` field points to the correct entry point.

### Out of memory

WASM runs in a single linear memory space (limited by browser/runtime). Large datasets (>100 MB) may exhaust available memory. Use Lite + Origin sync instead for unbounded data.

### Performance is slow

WASM code runs single-threaded and interpreted. Complex queries (large full-text searches, pagerank on million-node graphs) will be slow. Offload heavy workloads to Origin server queries.

### Sync conflicts

When syncing to Origin, row-level constraint violations (UNIQUE, FK) trigger conflict resolution. Origin returns compensation hints; the local database applies them. See [NodeDB-Lite CRDT Sync](https://github.com/NodeDB-Lab/nodedb-lite) for conflict resolution semantics.

## Related

- [NodeDB-Lite](https://github.com/NodeDB-Lab/nodedb-lite) — Full embedded database documentation (the native crate behind Lite-WASM)
- [Offline Sync Patterns](offline-sync-patterns.md) — How Lite syncs to Origin via CRDT
- [Query Language](query-language.md) — SQL reference (same surface in Lite-WASM and Origin)

[Back to docs](README.md)
