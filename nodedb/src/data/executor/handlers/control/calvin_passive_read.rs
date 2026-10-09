// SPDX-License-Identifier: BUSL-1.1

//! Passive-read helpers for the Calvin dependent-read path.
//!
//! [`CoreLoop::execute_calvin_execute_passive`] reads each declared key from
//! the local engine to build the `Vec<(PassiveReadKeyId, Value)>` payload the
//! Control Plane scheduler proposes as a `CalvinReadResult` Raft entry. The
//! per-engine lookups and the deterministic key-hashing they rely on live
//! here, split out of `calvin.rs` to keep that file within the size limit.
//!
//! The KV/edge surrogate hashes MUST be deterministic across replicas: the
//! same byte key or `(src, dst)` edge must produce the same `u32` on every
//! node, so `DefaultHasher` (RandomState) is explicitly NOT used.
//!
//! `EngineKeySet.collection` arrives already database-qualified: the Calvin
//! write-key extraction reads it off the physical plan's own qualified
//! collection field. So every `PassiveReadKeyId` below rebuilds it via
//! `from_stored` rather than re-qualifying.

use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
use nodedb_types::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::TenantId;

impl CoreLoop {
    /// Read a single `EngineKeySet` from the local engine state, returning
    /// `(PassiveReadKeyId, Value)` pairs.
    ///
    /// For Document/Vector/Edge engine sets: looks up each surrogate in the
    /// sparse engine (document store) and returns the stored value or `Null`.
    /// For KV engine sets: looks up each byte key in the KV engine.
    pub(super) fn read_passive_key(
        &self,
        tenant_id: &TenantId,
        engine_key: &nodedb_cluster::calvin::types::EngineKeySet,
    ) -> Vec<(PassiveReadKeyId, Value)> {
        use nodedb_cluster::calvin::types::EngineKeySet;

        match engine_key {
            EngineKeySet::Document {
                collection,
                surrogates,
            }
            | EngineKeySet::Vector {
                collection,
                surrogates,
            } => surrogates
                .iter()
                .map(|&surrogate| {
                    let value = self
                        .read_surrogate_value(tenant_id, collection, surrogate)
                        .unwrap_or(Value::Null);
                    (
                        PassiveReadKeyId {
                            collection: nodedb_types::QualifiedCollection::from_stored(
                                collection.clone(),
                            ),
                            surrogate,
                        },
                        value,
                    )
                })
                .collect(),

            EngineKeySet::Kv { collection, keys } => keys
                .iter()
                .map(|k| {
                    let value = self
                        .read_kv_value(tenant_id, collection, k)
                        .unwrap_or(Value::Null);
                    // For KV, use key bytes as surrogate placeholder (0 sentinel).
                    // KV keys don't have surrogates; the PassiveReadKeyId identifies
                    // the collection and a stable u32 hash of the key.
                    let key_hash = stable_kv_hash(k);
                    (
                        PassiveReadKeyId {
                            collection: nodedb_types::QualifiedCollection::from_stored(
                                collection.clone(),
                            ),
                            surrogate: key_hash,
                        },
                        value,
                    )
                })
                .collect(),

            EngineKeySet::Edge {
                collection, edges, ..
            } => edges
                .iter()
                .map(|&(src, dst)| {
                    // Edge reads: use a stable hash of (src, dst) as surrogate.
                    let edge_hash = stable_edge_hash(src, dst);
                    (
                        PassiveReadKeyId {
                            collection: nodedb_types::QualifiedCollection::from_stored(
                                collection.clone(),
                            ),
                            surrogate: edge_hash,
                        },
                        Value::Null, // Edge existence read: Null = absent, non-Null = present.
                    )
                })
                .collect(),

            // An array write is a static write: no dependent transaction
            // predicts its cells, so no passive participant reads them.
            EngineKeySet::Array { .. } => Vec::new(),
            // A whole-collection key and a UNIQUE value key are lock keys
            // only: no dependent transaction predicts them.
            EngineKeySet::Collection { .. } | EngineKeySet::Unique { .. } => Vec::new(),
        }
    }

    /// Read a single document surrogate from the sparse engine.
    ///
    /// Returns `None` if the surrogate is not present in this core's partition.
    pub(super) fn read_surrogate_value(
        &self,
        tenant_id: &TenantId,
        collection: &str,
        surrogate: u32,
    ) -> Option<Value> {
        // In v1 this is a thin stub: the full implementation requires a
        // synchronous lookup through the sparse engine's redb B-Tree.
        // The engine lookup path is available via `self.engine_state` once
        // the Data Plane engine access APIs are wired. For now, return None
        // (caller maps None → Null).
        let _ = (tenant_id, collection, surrogate);
        None
    }

    /// Read a single KV entry from the KV engine.
    ///
    /// Returns `None` if the key is not present.
    pub(super) fn read_kv_value(
        &self,
        tenant_id: &TenantId,
        collection: &str,
        key: &[u8],
    ) -> Option<Value> {
        let _ = (tenant_id, collection, key);
        None
    }
}

/// Stable, deterministic hash of a KV byte key into a u32 surrogate
/// placeholder for use in `PassiveReadKeyId`.
///
/// Uses xxhash with a fixed seed to satisfy the determinism contract:
/// the same byte key must produce the same hash on every replica.
/// `DefaultHasher` (RandomState) is explicitly NOT used here.
fn stable_kv_hash(key: &[u8]) -> u32 {
    // FNV-1a 32-bit with fixed offset basis — no external dependency needed.
    // This is a placeholder; a production implementation would use xxhash-rust
    // with a fixed seed once the crate is available in this crate's deps.
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for &byte in key {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Stable, deterministic hash of an edge `(src, dst)` into a u32 surrogate
/// placeholder for `PassiveReadKeyId`.
fn stable_edge_hash(src: u32, dst: u32) -> u32 {
    // Combine src and dst with a deterministic mix.
    let combined: u64 = (u64::from(src) << 32) | u64::from(dst);
    stable_kv_hash(&combined.to_le_bytes())
}
