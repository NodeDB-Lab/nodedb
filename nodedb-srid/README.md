# `nodedb-srid`

> Spatial reference system identifiers for NodeDB: SRID codes, CRS names and the resolver trait.

Pure Rust with no I/O, so it builds for `wasm32-unknown-unknown`. It carries no CRS table. A crate that only stores or compares SRIDs depends on this crate and never links the EPSG table in `nodedb-crs`.

## What it does

- `Srid` names a CRS by a non-zero code. `Srid::new` checks the code range only.
- `SridAuthority` tells which authority owns a code: EPSG, a user definition, or NodeDB.
- `code_from_name` reads a CRS name such as `EPSG:4326` into a code.
- `SridResolver` answers whether a code or a name is defined, and whether its CRS is geographic or projected (`CrsKind`). `nodedb-crs` implements it.

## Status

Pre-1.0. APIs can change between minor versions until 1.0. See the workspace [`README.md`](../README.md) for the full project overview and the GitHub [release notes](https://github.com/NodeDB-Lab/nodedb/releases) for per-version changes.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE-APACHE`](../LICENSE.APACHE-2.0)).
