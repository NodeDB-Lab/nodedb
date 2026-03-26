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
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_roundtrip() {
        for v in 0u8..=5 {
            let ns = Namespace::from_u8(v).unwrap();
            assert_eq!(ns as u8, v);
        }
        assert!(Namespace::from_u8(6).is_none());
    }
}
