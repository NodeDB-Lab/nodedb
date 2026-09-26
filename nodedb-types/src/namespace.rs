// SPDX-License-Identifier: Apache-2.0

//! Storage namespace identifiers for the blob KV store.
//!
//! Both SQLite (native) and OPFS (WASM) backends use the same namespace
//! scheme to partition data by engine.

use serde::{Deserialize, Serialize};

/// Storage namespace. Each engine writes to its own namespace in the
/// blob KV store, preventing key collisions.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[repr(u8)]
#[non_exhaustive]
pub enum Namespace {
    /// Database metadata: schema version, config, Shape subscriptions.
    Meta = 0,
    /// Vector engine: HNSW graph layers, vector data.
    Vector = 1,
    /// Graph engine: CSR arrays, node/label interning tables.
    Graph = 2,
    /// CRDT deltas: unsent mutations awaiting sync.
    Crdt = 3,
    /// Loro state snapshots: compacted CRDT state for fast cold-start.
    LoroState = 4,
    /// Spatial engine: R-tree checkpoints, geohash indexes.
    Spatial = 5,
    /// Strict document engine: Binary Tuple rows keyed by PK.
    Strict = 6,
    /// Columnar engine: compressed segments, delete bitmaps, segment metadata.
    Columnar = 7,
    /// KV engine: direct key-value storage (bypasses Loro CRDT).
    /// Used when sync is disabled or for the local-only KV fast path.
    Kv = 8,
    /// Array engine: ND sparse arrays, catalog, manifests, segment bytes.
    Array = 9,
    /// Array CRDT op-log: append-only ops awaiting sync + GC.
    ArrayOpLog = 10,
    /// Array sync pending queue: ops waiting for transport delivery.
    ArrayDelta = 11,
    /// Full-text search engine: posting lists, doc-length maps, BM25 stats,
    /// fieldnorm blobs, segment bytes, and surrogate maps.
    Fts = 12,
    /// Bitemporal history table for strict document collections.
    ///
    /// Keys: `{collection}:{system_from_ms_8be}:{pk_bytes}` — value is the
    /// full Binary Tuple followed by an 8-byte big-endian `system_to_ms`
    /// (u64::MAX = open / still-current at time of supersession).
    StrictHistory = 13,
    /// Bitemporal history table for graph edge collections.
    ///
    /// Keys: `{collection}:{edge_id_8be}:{system_from_ms_8be}` — value is
    /// the MessagePack-encoded edge props followed by an 8-byte big-endian
    /// `system_to_ms` (u64::MAX = current / not yet deleted).
    GraphHistory = 14,
    /// Bitemporal history table for schemaless document collections.
    ///
    /// Keys: `{collection}:{doc_id}\x00{system_from_ms:020}` — value is
    /// `[tag:u8][valid_from_ms:i64 LE][valid_until_ms:i64 LE][body_msgpack...]`.
    /// `tag = 0x00` (live), `0xFF` (tombstone), `0xFE` (GDPR erased).
    DocumentHistory = 15,
    /// O(1) pointer to the currently-live DocumentHistory version.
    ///
    /// Keys: `{collection}:{doc_id}` — value is the `system_from_ms` of the
    /// live row encoded as a 20-digit zero-padded ASCII decimal (matching the
    /// suffix used in `DocumentHistory` keys).  Absent when no live version
    /// exists (document never written, tombstoned, or GDPR-erased).
    ///
    /// Written atomically alongside every `DocumentHistory` mutation so that
    /// `versioned_get_current` can resolve the current version with one index
    /// lookup + one history fetch instead of a full prefix scan.
    LatestVersion = 16,
    /// Durable FIFO queue of outbound columnar row batches waiting for
    /// transport to Origin. Keys are big-endian monotonic u64 IDs; values are
    /// zerompk-encoded `PendingColumnarBatch` payloads.
    ColumnarPending = 17,
    /// Durable FIFO queue of outbound timeseries row batches waiting for
    /// transport to Origin. Keys are big-endian monotonic u64 IDs; values are
    /// zerompk-encoded `PendingTimeseriesBatch` payloads.
    TimeseriesPending = 18,
    /// Durable FIFO queue of outbound vector insert operations waiting for
    /// transport to Origin. Keys are big-endian monotonic u64 IDs; values are
    /// zerompk-encoded `PendingVectorInsert` payloads.
    VectorInsertPending = 19,
    /// Durable FIFO queue of outbound vector delete operations waiting for
    /// transport to Origin. Keys are big-endian monotonic u64 IDs; values are
    /// zerompk-encoded `PendingVectorDelete` payloads.
    VectorDeletePending = 20,
    /// Durable FIFO queue of outbound FTS index operations waiting for
    /// transport to Origin. Keys are big-endian monotonic u64 IDs; values are
    /// zerompk-encoded `PendingFtsIndex` payloads.
    FtsIndexPending = 21,
    /// Durable FIFO queue of outbound FTS delete operations waiting for
    /// transport to Origin. Keys are big-endian monotonic u64 IDs; values are
    /// zerompk-encoded `PendingFtsDelete` payloads.
    FtsDeletePending = 22,
    /// Durable FIFO queue of outbound spatial insert operations waiting for
    /// transport to Origin. Keys are big-endian monotonic u64 IDs; values are
    /// zerompk-encoded `PendingSpatialInsert` payloads.
    SpatialInsertPending = 23,
    /// Durable FIFO queue of outbound spatial delete operations waiting for
    /// transport to Origin. Keys are big-endian monotonic u64 IDs; values are
    /// zerompk-encoded `PendingSpatialDelete` payloads.
    SpatialDeletePending = 24,
    /// Durable FIFO queue of outbound KV writes waiting for transport to
    /// Origin. Keys are big-endian monotonic u64 IDs; values are
    /// zerompk-encoded `PendingKvWrite` payloads.
    KvPushPending = 25,
}

impl Namespace {
    /// Convert from raw u8 (for storage layer deserialization).
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Meta),
            1 => Some(Self::Vector),
            2 => Some(Self::Graph),
            3 => Some(Self::Crdt),
            4 => Some(Self::LoroState),
            5 => Some(Self::Spatial),
            6 => Some(Self::Strict),
            7 => Some(Self::Columnar),
            8 => Some(Self::Kv),
            9 => Some(Self::Array),
            10 => Some(Self::ArrayOpLog),
            11 => Some(Self::ArrayDelta),
            12 => Some(Self::Fts),
            13 => Some(Self::StrictHistory),
            14 => Some(Self::GraphHistory),
            15 => Some(Self::DocumentHistory),
            16 => Some(Self::LatestVersion),
            17 => Some(Self::ColumnarPending),
            18 => Some(Self::TimeseriesPending),
            19 => Some(Self::VectorInsertPending),
            20 => Some(Self::VectorDeletePending),
            21 => Some(Self::FtsIndexPending),
            22 => Some(Self::FtsDeletePending),
            23 => Some(Self::SpatialInsertPending),
            24 => Some(Self::SpatialDeletePending),
            25 => Some(Self::KvPushPending),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_roundtrip() {
        for v in 0u8..=25 {
            let ns = Namespace::from_u8(v).unwrap();
            assert_eq!(ns as u8, v);
        }
        assert!(Namespace::from_u8(26).is_none());
    }
}
