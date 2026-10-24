# `nodedb-raster-load`

> Raster-to-array load planning for NodeDB: band mapping, grid alignment, nodata and pixel rows.

The crate does no I/O, so NodeDB Origin and Lite share it. It builds for `wasm32-unknown-unknown`.

## What it does

- `SourceHeader` describes one raster source opened with `nodedb-raster`. `check_same_layout` checks that every source shares one grid and block layout.
- `RasterPlan::new` runs every open-time check against the target array: CRS, band-to-attribute types, grid alignment, dimension domains, `TIME`, nodata and the pixel window.
- `BlockRows` turns one block from every source into array rows.
- `adopted_metadata` names the attribute metadata `ADOPT_METADATA` takes from the files.

## Status

Pre-1.0. APIs can change between minor versions until 1.0. See the workspace [`README.md`](../README.md) for the full project overview and the GitHub [release notes](https://github.com/NodeDB-Lab/nodedb/releases) for per-version changes.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE-APACHE`](../LICENSE.APACHE-2.0)).
