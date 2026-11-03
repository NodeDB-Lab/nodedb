# `nodedb-codec`

> Compression codecs: ALP, FastLanes, FSST, Gorilla, LZ4, vector quantization

Per-column codec implementations used by the columnar / timeseries / spatial engines, plus the vector quantization codecs OPQ, RaBitQ, BBQ, and Ternary (BitNet 1.58), with the shared quantized-vector layout and Hamming kernel. SQ8, PQ, and Binary live in `nodedb-vector`. Each codec is a free-standing module — pick what you need.

## Status

Pre-1.0. APIs may change between minor versions until 1.0. See the
workspace [`README.md`](../README.md) for the full project overview and
the GitHub [release notes](https://github.com/NodeDB-Lab/nodedb/releases)
for per-version changes.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE-APACHE`](../LICENSE.APACHE-2.0)).
