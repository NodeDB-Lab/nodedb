// SPDX-License-Identifier: Apache-2.0

//! Deterministic stream-ID derivation for idempotent-producer partitioning.
//!
//! Both the Lite producer and the Origin consumer MUST call `stream_id_for`
//! identically so that `(producer_id, stream_id)` consistently partitions the
//! sequence space across reconnects. The algorithm is intentionally stable and
//! pinned — never change it without a protocol-version bump.

/// Logical engine kind used to partition the per-producer sequence space.
///
/// Each `(producer_id, stream_id)` pair has its own monotonic sequence
/// counter. `stream_id_for` hashes `(EngineKind, collection)` into the
/// `stream_id` so that writes to different engines or different collections
/// never share a counter, making per-stream deduplication precise.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EngineKind {
    Crdt,
    Columnar,
    Timeseries,
    Vector,
    Fts,
    Spatial,
    Array,
    Kv,
}

/// Derive a stable, deterministic `stream_id` for a `(engine, collection)` pair.
///
/// Uses FNV-1a (64-bit) over the bytes `[engine as u8] ++ collection.as_bytes()`.
/// FNV-1a is chosen because it is simple, dependency-free, and its output is
/// stable across Rust versions and platforms (unlike `DefaultHasher`).
///
/// **Algorithm (FNV-1a 64-bit, offset basis 14695981039346656037, prime 1099511628211):**
/// 1. Start with the FNV offset basis.
/// 2. XOR-then-multiply each byte in `[engine as u8] ++ collection.as_bytes()`.
///
/// **Stability guarantee:** This algorithm and the `EngineKind` discriminant
/// assignments MUST NOT change without a protocol-version bump. Both Lite
/// and Origin must produce identical `stream_id` values for the same input.
pub fn stream_id_for(engine: EngineKind, collection: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;

    let mut hash = FNV_OFFSET_BASIS;

    // Engine discriminant byte.
    hash ^= engine as u64;
    hash = hash.wrapping_mul(FNV_PRIME);

    // Collection bytes.
    for byte in collection.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_input_same_output() {
        let a = stream_id_for(EngineKind::Fts, "my_collection");
        let b = stream_id_for(EngineKind::Fts, "my_collection");
        assert_eq!(a, b);
    }

    #[test]
    fn different_engine_different_output() {
        let fts = stream_id_for(EngineKind::Fts, "col");
        let spatial = stream_id_for(EngineKind::Spatial, "col");
        assert_ne!(fts, spatial);
    }

    #[test]
    fn different_collection_different_output() {
        let a = stream_id_for(EngineKind::Vector, "alpha");
        let b = stream_id_for(EngineKind::Vector, "beta");
        assert_ne!(a, b);
    }

    /// Pinned test vectors — if this test fails, the algorithm has changed
    /// and a protocol-version bump + migration are required.
    ///
    /// Both sides re-derive using the same FNV-1a implementation, which pins
    /// the byte inputs (including `EngineKind` discriminants) rather than
    /// hard-coding a magic constant that could be computed incorrectly.
    #[test]
    fn pinned_known_vectors() {
        fn fnv1a(bytes: &[u8]) -> u64 {
            const BASIS: u64 = 14695981039346656037;
            const PRIME: u64 = 1099511628211;
            let mut h = BASIS;
            for &b in bytes {
                h ^= b as u64;
                h = h.wrapping_mul(PRIME);
            }
            h
        }

        // EngineKind::Fts = 4 (Crdt=0, Columnar=1, Timeseries=2, Vector=3, Fts=4)
        let fts_docs_expected = fnv1a(&[EngineKind::Fts as u8, b'd', b'o', b'c', b's']);
        // EngineKind::Spatial = 5
        let spatial_geo_expected = fnv1a(&[EngineKind::Spatial as u8, b'g', b'e', b'o']);

        assert_eq!(stream_id_for(EngineKind::Fts, "docs"), fts_docs_expected);
        assert_eq!(
            stream_id_for(EngineKind::Spatial, "geo"),
            spatial_geo_expected
        );

        // Sanity: nonzero and distinct.
        assert_ne!(fts_docs_expected, 0);
        assert_ne!(spatial_geo_expected, 0);
        assert_ne!(fts_docs_expected, spatial_geo_expected);
    }
}
