# `nodedb-mem`

> Per-engine memory governor backed by jemalloc arenas

Pins each Data Plane core to a dedicated jemalloc arena, enforces per-engine memory budgets via a `MemoryGovernor::reserve()` API, and exposes pressure metrics. Eliminates allocator lock contention across TPC cores.

## Features

- `jemalloc` (off by default): arena pinning and allocator stats. Enable it only in a binary that installs jemalloc as its global allocator. Without it, budgets work unchanged, arena pinning is a no-op, and no jemalloc C library is linked.

## Status

Pre-1.0. APIs may change between minor versions until 1.0. See the
workspace [`README.md`](../README.md) for the full project overview and
the GitHub [release notes](https://github.com/NodeDB-Lab/nodedb/releases)
for per-version changes.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE-APACHE`](../LICENSE.APACHE-2.0)).
