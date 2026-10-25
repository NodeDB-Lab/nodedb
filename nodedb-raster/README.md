# `nodedb-raster`

> Sans-IO raster decoding for NodeDB array bulk loads: classic TIFF, BigTIFF and cloud-optimised GeoTIFF.

Pure Rust with no I/O and no native dependency, so it builds for `wasm32-unknown-unknown`.

## What it does

- `RasterOpener` reads a raster header from a `RangeCache` and names the byte ranges it needs next. Any transport can feed it: a local file, HTTP range requests or an object store.
- `RasterReader` names each block's byte ranges and decodes a block from the fetched bytes.
- `RasterLimits` caps the header bytes and each block's decode size. Every decode allocation is also capped at `MAX_DECODE_CAPACITY`.
- `reader::driver` runs the opener and the reader over a blocking source.

## TIFF support

| Feature | Support |
|---|---|
| Containers | classic TIFF, BigTIFF |
| Layout | strips or tiles, chunky or planar bands |
| Images | full resolution, overviews, internal masks |
| Metadata | GeoKeys, `GDAL_NODATA`, `GDAL_METADATA` |
| Codecs | none, LZW, deflate, PackBits, ZSTD, JPEG, LERC (with deflate and ZSTD wrappers) |
| Predictors | horizontal (2), floating point (3) |

GeoKeys resolve to an SRID through `nodedb-crs`. A user-defined GeoKey CRS matches a registry CRS by its parameters.

## Status

Pre-1.0. APIs can change between minor versions until 1.0. See the workspace [`README.md`](../README.md) for the full project overview and the GitHub [release notes](https://github.com/NodeDB-Lab/nodedb/releases) for per-version changes.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE-APACHE`](../LICENSE.APACHE-2.0)).
