//! CRDT engine for NodeDB-Lite.
//!
//! Wraps `nodedb-crdt::CrdtState` (Loro-backed) with:
//! - Delta accumulation: tracks unsent mutations for sync
//! - State persistence: save/load Loro snapshots to `StorageEngine`
//! - Delta persistence: save/load unsent deltas to `StorageEngine`
//! - Vector clock: local version tracking for sync handshake
//! - History compaction: periodic Loro GC to prevent unbounded growth
//!
//! Every mutation on Lite (vector insert, graph edge, document put) flows
//! through this engine. It wraps each as a Loro operation, generating a
//! delta that will eventually sync to Origin.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use loro::LoroValue;
use nodedb_crdt::CrdtState;

use crate::error::LiteError;

/// A single field in a CRDT operation: `(field_name, value)`.
pub type CrdtField<'a> = (&'a str, LoroValue);

/// A batch CRDT operation: `(collection, doc_id, fields)`.
pub type CrdtBatchOp<'a> = (&'a str, &'a str, &'a [CrdtField<'a>]);

/// Key prefix for delta blobs in the `Crdt` namespace.
const DELTA_KEY_PREFIX: &[u8] = b"delta:";
/// Key for the Loro state snapshot in the `LoroState` namespace.
const SNAPSHOT_KEY: &[u8] = b"loro_snapshot";
/// Key for the vector clock in the `Meta` namespace.
const VCLOCK_KEY: &[u8] = b"vector_clock";

/// CRDT engine for edge devices.
///
/// Not `Send` — owned by a single task. The `NodeDbLite` wrapper handles
/// the async bridging via `spawn_blocking` or `Mutex` as needed.
pub struct CrdtEngine {
    state: CrdtState,
    /// Monotonically increasing mutation ID. Used as delta ordering key.
    next_mutation_id: AtomicU64,
    /// Unsent deltas accumulated since last sync ACK.
    /// Each entry: `(mutation_id, collection, doc_id, delta_bytes)`.
    pending_deltas: Vec<PendingDelta>,
    /// Per-collection version: highest mutation_id that's been ACK'd by Origin.
    acked_versions: HashMap<String, u64>,
    /// Conflict resolution policies per collection.
    /// Evaluated on sync when Origin rejects a delta.
    policies: nodedb_crdt::PolicyRegistry,
}

/// A pending (unsent) delta waiting to be synced to Origin.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingDelta {
    /// Monotonic mutation ID (for ordering and dedup).
    pub mutation_id: u64,
    /// Collection this delta applies to.
    pub collection: String,
    /// Document/row ID affected.
    pub document_id: String,
    /// Loro delta bytes (compact binary).
    pub delta_bytes: Vec<u8>,
}

impl CrdtEngine {
    /// Create a new empty CRDT engine with the given peer ID.
    pub fn new(peer_id: u64) -> Result<Self, LiteError> {
        let state = CrdtState::new(peer_id).map_err(|e| LiteError::Storage {
            detail: format!("failed to create CrdtState: {e}"),
        })?;

        Ok(Self {
            state,
            next_mutation_id: AtomicU64::new(1),
            pending_deltas: Vec::new(),
            acked_versions: HashMap::new(),
            policies: nodedb_crdt::PolicyRegistry::new(),
        })
    }

    /// Restore from a Loro snapshot (cold start).
    pub fn from_snapshot(peer_id: u64, snapshot: &[u8]) -> Result<Self, LiteError> {
        let state = CrdtState::new(peer_id).map_err(|e| LiteError::Storage {
            detail: format!("failed to create CrdtState: {e}"),
        })?;
        state.import(snapshot).map_err(|e| LiteError::Storage {
            detail: format!("failed to import snapshot: {e}"),
        })?;

        Ok(Self {
            state,
            next_mutation_id: AtomicU64::new(1),
            pending_deltas: Vec::new(),
            acked_versions: HashMap::new(),
            policies: nodedb_crdt::PolicyRegistry::new(),
        })
    }

    /// The peer ID of this engine.
    pub fn peer_id(&self) -> u64 {
        self.state.peer_id()
    }

    // ─── Mutations ───────────────────────────────────────────────────

    /// Insert or update a document (used by document_put, vector_insert metadata, etc.).
    ///
    /// Generates a Loro delta and accumulates it as a pending sync item.
    pub fn upsert(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<u64, LiteError> {
        // Snapshot before mutation for delta extraction.
        let version_before = self.state.doc().oplog_vv();

        self.state
            .upsert(collection, doc_id, fields)
            .map_err(|e| LiteError::Storage {
                detail: format!("CRDT upsert failed: {e}"),
            })?;

        // Extract the delta (operations since version_before).
        let delta_bytes = self
            .state
            .doc()
            .export(loro::ExportMode::updates(&version_before))
            .map_err(|e| LiteError::Storage {
                detail: format!("delta export failed: {e}"),
            })?;

        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: collection.to_string(),
            document_id: doc_id.to_string(),
            delta_bytes,
        });

        Ok(mutation_id)
    }

    /// Delete a document/row.
    pub fn delete(&mut self, collection: &str, doc_id: &str) -> Result<u64, LiteError> {
        let version_before = self.state.doc().oplog_vv();

        self.state
            .delete(collection, doc_id)
            .map_err(|e| LiteError::Storage {
                detail: format!("CRDT delete failed: {e}"),
            })?;

        let delta_bytes = self
            .state
            .doc()
            .export(loro::ExportMode::updates(&version_before))
            .map_err(|e| LiteError::Storage {
                detail: format!("delta export failed: {e}"),
            })?;

        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: collection.to_string(),
            document_id: doc_id.to_string(),
            delta_bytes,
        });

        Ok(mutation_id)
    }

    /// Batch upsert: apply N mutations with a single delta export.
    ///
    /// This is O(1) Loro exports instead of O(N). Use for bulk inserts
    /// (cold-start hydration, batch vector insert, graph edge loading).
    pub fn batch_upsert(&mut self, ops: &[CrdtBatchOp<'_>]) -> Result<u64, LiteError> {
        if ops.is_empty() {
            return Ok(0);
        }

        let version_before = self.state.doc().oplog_vv();

        for &(collection, doc_id, fields) in ops {
            self.state
                .upsert(collection, doc_id, fields)
                .map_err(|e| LiteError::Storage {
                    detail: format!("CRDT batch upsert failed: {e}"),
                })?;
        }

        let delta_bytes = self
            .state
            .doc()
            .export(loro::ExportMode::updates(&version_before))
            .map_err(|e| LiteError::Storage {
                detail: format!("batch delta export failed: {e}"),
            })?;

        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: "batch".to_string(),
            document_id: format!("{}_ops", ops.len()),
            delta_bytes,
        });

        Ok(mutation_id)
    }

    // ─── Reads ───────────────────────────────────────────────────────

    /// Read a document's fields.
    pub fn read(&self, collection: &str, doc_id: &str) -> Option<LoroValue> {
        self.state.read_row(collection, doc_id)
    }

    /// Check if a document exists.
    pub fn exists(&self, collection: &str, doc_id: &str) -> bool {
        self.state.row_exists(collection, doc_id)
    }

    /// List all document IDs in a collection.
    pub fn list_ids(&self, collection: &str) -> Vec<String> {
        self.state.row_ids(collection)
    }

    /// List all collection names (top-level Loro map keys).
    pub fn collection_names(&self) -> Vec<String> {
        self.state.collection_names()
    }

    /// Set conflict resolution policy for a collection.
    pub fn set_policy(&mut self, collection: &str, policy: nodedb_crdt::CollectionPolicy) {
        self.policies.set(collection, policy);
    }

    /// Get the policy registry (for sync conflict resolution).
    pub fn policies(&self) -> &nodedb_crdt::PolicyRegistry {
        &self.policies
    }

    // ─── Sync: Delta Management ──────────────────────────────────────

    /// Get all pending (unsent) deltas.
    pub fn pending_deltas(&self) -> &[PendingDelta] {
        &self.pending_deltas
    }

    /// Number of unsent deltas.
    pub fn pending_count(&self) -> usize {
        self.pending_deltas.len()
    }

    /// Mark deltas as acknowledged by Origin (after DeltaAck received).
    ///
    /// Removes all pending deltas with `mutation_id <= acked_id`.
    pub fn acknowledge(&mut self, acked_id: u64) {
        self.pending_deltas.retain(|d| d.mutation_id > acked_id);
    }

    /// Roll back a specific pending delta (after DeltaReject with CompensationHint).
    ///
    /// This is a best-effort operation — Loro CRDTs don't support true undo.
    /// For document mutations, we delete the affected row and let the
    /// application re-create it with corrected values.
    ///
    /// Returns the rejected delta if found.
    pub fn reject_delta(&mut self, mutation_id: u64) -> Option<PendingDelta> {
        if let Some(pos) = self
            .pending_deltas
            .iter()
            .position(|d| d.mutation_id == mutation_id)
        {
            let delta = self.pending_deltas.remove(pos);
            // Best-effort rollback: delete the affected document.
            // The application should handle the CompensationHint and
            // re-create with corrected values.
            let _ = self.state.delete(&delta.collection, &delta.document_id);
            Some(delta)
        } else {
            None
        }
    }

    /// Import remote deltas from Origin (received via sync).
    pub fn import_remote(&self, data: &[u8]) -> Result<(), LiteError> {
        self.state.import(data).map_err(|e| LiteError::Storage {
            detail: format!("remote delta import failed: {e}"),
        })
    }

    // ─── Snapshot & Persistence ──────────────────────────────────────

    /// Export a full Loro state snapshot (for persistence to StorageEngine).
    pub fn export_snapshot(&self) -> Result<Vec<u8>, LiteError> {
        self.state
            .export_snapshot()
            .map_err(|e| LiteError::Storage {
                detail: format!("snapshot export failed: {e}"),
            })
    }

    /// Compact Loro history to prevent unbounded growth.
    ///
    /// Replaces the internal LoroDoc with a shallow snapshot. Historical
    /// operations are discarded. Current state is fully preserved.
    pub fn compact_history(&mut self) -> Result<(), LiteError> {
        self.state
            .compact_history()
            .map_err(|e| LiteError::Storage {
                detail: format!("history compaction failed: {e}"),
            })
    }

    /// Estimated memory usage in bytes.
    pub fn estimated_memory_bytes(&self) -> usize {
        let state_bytes = self.state.estimated_memory_bytes();
        let delta_bytes: usize = self
            .pending_deltas
            .iter()
            .map(|d| d.delta_bytes.len())
            .sum();
        state_bytes + delta_bytes
    }

    // ─── Vector Clock ────────────────────────────────────────────────

    /// Export the current vector clock as a serializable map.
    ///
    /// Format: `{ peer_id_hex: counter }` — matches the Loro version vector.
    pub fn export_vector_clock(&self) -> HashMap<String, u64> {
        let vv = self.state.doc().oplog_vv();
        let mut clock = HashMap::new();
        // Loro's VersionVector maps PeerID → Counter.
        // We encode PeerID as hex string for wire compatibility.
        for (peer, counter) in vv.iter() {
            clock.insert(format!("{peer:016x}"), *counter as u64);
        }
        clock
    }

    /// Set the acked version for a collection (after sync handshake).
    pub fn set_acked_version(&mut self, collection: &str, version: u64) {
        self.acked_versions.insert(collection.to_string(), version);
    }

    /// Get the acked version for a collection.
    pub fn acked_version(&self, collection: &str) -> u64 {
        self.acked_versions.get(collection).copied().unwrap_or(0)
    }

    // ─── Persistence Helpers ─────────────────────────────────────────

    /// Serialize pending deltas to bytes for StorageEngine persistence.
    pub fn serialize_pending_deltas(&self) -> Result<Vec<u8>, crate::error::LiteError> {
        rmp_serde::to_vec_named(&self.pending_deltas).map_err(|e| {
            crate::error::LiteError::Serialization {
                detail: format!("pending deltas: {e}"),
            }
        })
    }

    /// Restore pending deltas from bytes (cold start).
    pub fn restore_pending_deltas(&mut self, bytes: &[u8]) {
        match rmp_serde::from_slice::<Vec<PendingDelta>>(bytes) {
            Ok(deltas) => {
                // Advance mutation ID counter past any restored deltas.
                let max_id = deltas.iter().map(|d| d.mutation_id).max().unwrap_or(0);
                self.next_mutation_id.store(max_id + 1, Ordering::Relaxed);
                self.pending_deltas = deltas;
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to restore pending deltas, continuing with empty state");
            }
        }
    }

    /// Key for storing the Loro snapshot in `StorageEngine`.
    pub fn snapshot_key() -> &'static [u8] {
        SNAPSHOT_KEY
    }

    /// Key for storing pending deltas in `StorageEngine`.
    pub fn delta_key() -> &'static [u8] {
        DELTA_KEY_PREFIX
    }

    /// Key for storing the vector clock in `StorageEngine`.
    pub fn vclock_key() -> &'static [u8] {
        VCLOCK_KEY
    }

    /// Access the underlying `CrdtState` for advanced operations.
    pub fn state(&self) -> &CrdtState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_engine() {
        let engine = CrdtEngine::new(1).unwrap();
        assert_eq!(engine.peer_id(), 1);
        assert_eq!(engine.pending_count(), 0);
    }

    #[test]
    fn upsert_generates_delta() {
        let mut engine = CrdtEngine::new(1).unwrap();
        let mid = engine
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("Alice".into()))],
            )
            .unwrap();

        assert_eq!(mid, 1);
        assert_eq!(engine.pending_count(), 1);
        assert!(!engine.pending_deltas()[0].delta_bytes.is_empty());
    }

    #[test]
    fn read_after_upsert() {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine
            .upsert("users", "u1", &[("age", LoroValue::I64(30))])
            .unwrap();

        assert!(engine.exists("users", "u1"));
        let val = engine.read("users", "u1").unwrap();
        // The value should be a map containing "age": 30.
        assert!(format!("{val:?}").contains("30"));
    }

    #[test]
    fn delete_generates_delta() {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine
            .upsert("users", "u1", &[("name", LoroValue::String("X".into()))])
            .unwrap();
        let mid = engine.delete("users", "u1").unwrap();

        assert_eq!(mid, 2); // Second mutation.
        assert_eq!(engine.pending_count(), 2);
        assert!(!engine.exists("users", "u1"));
    }

    #[test]
    fn acknowledge_removes_deltas() {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine
            .upsert("a", "1", &[("x", LoroValue::I64(1))])
            .unwrap(); // mid=1
        engine
            .upsert("a", "2", &[("x", LoroValue::I64(2))])
            .unwrap(); // mid=2
        engine
            .upsert("a", "3", &[("x", LoroValue::I64(3))])
            .unwrap(); // mid=3

        assert_eq!(engine.pending_count(), 3);
        engine.acknowledge(2); // ACK up to mid=2.
        assert_eq!(engine.pending_count(), 1);
        assert_eq!(engine.pending_deltas()[0].mutation_id, 3);
    }

    #[test]
    fn reject_delta_rolls_back() {
        let mut engine = CrdtEngine::new(1).unwrap();
        let mid = engine
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("Alice".into()))],
            )
            .unwrap();

        assert!(engine.exists("users", "u1"));
        let rejected = engine.reject_delta(mid).unwrap();
        assert_eq!(rejected.collection, "users");
        assert!(!engine.exists("users", "u1"));
        assert_eq!(engine.pending_count(), 0);
    }

    #[test]
    fn snapshot_and_restore() {
        let mut engine1 = CrdtEngine::new(1).unwrap();
        engine1
            .upsert(
                "docs",
                "d1",
                &[("title", LoroValue::String("Hello".into()))],
            )
            .unwrap();
        engine1
            .upsert(
                "docs",
                "d2",
                &[("title", LoroValue::String("World".into()))],
            )
            .unwrap();

        let snapshot = engine1.export_snapshot().unwrap();
        assert!(!snapshot.is_empty());

        let engine2 = CrdtEngine::from_snapshot(2, &snapshot).unwrap();
        assert!(engine2.exists("docs", "d1"));
        assert!(engine2.exists("docs", "d2"));
    }

    #[test]
    fn import_remote_deltas() {
        let mut engine1 = CrdtEngine::new(1).unwrap();
        engine1
            .upsert("items", "i1", &[("val", LoroValue::I64(42))])
            .unwrap();

        // Export engine1's state as a snapshot and import into engine2.
        let snapshot = engine1.export_snapshot().unwrap();
        let engine2 = CrdtEngine::new(2).unwrap();
        engine2.import_remote(&snapshot).unwrap();

        assert!(engine2.exists("items", "i1"));
    }

    #[test]
    fn pending_deltas_persistence() {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine
            .upsert("a", "1", &[("x", LoroValue::I64(1))])
            .unwrap();
        engine
            .upsert("b", "2", &[("y", LoroValue::I64(2))])
            .unwrap();

        let bytes = engine.serialize_pending_deltas().unwrap();
        assert!(!bytes.is_empty());

        let mut engine2 = CrdtEngine::new(1).unwrap();
        engine2.restore_pending_deltas(&bytes);
        assert_eq!(engine2.pending_count(), 2);
        // Mutation ID counter should be advanced past restored deltas.
        let mid = engine2
            .upsert("c", "3", &[("z", LoroValue::I64(3))])
            .unwrap();
        assert!(mid > 2);
    }

    #[test]
    fn vector_clock_export() {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine
            .upsert("x", "1", &[("v", LoroValue::I64(1))])
            .unwrap();

        let clock = engine.export_vector_clock();
        assert!(!clock.is_empty());
        // Should contain our peer_id's entry.
        let our_key = format!("{:016x}", engine.peer_id());
        assert!(
            clock.contains_key(&our_key),
            "clock should contain peer {our_key}: {clock:?}"
        );
    }

    #[test]
    fn compact_history_preserves_state() {
        let mut engine = CrdtEngine::new(1).unwrap();
        for i in 0..50 {
            engine
                .upsert(
                    "items",
                    &format!("i{i}"),
                    &[("val", LoroValue::I64(i as i64))],
                )
                .unwrap();
        }

        let mem_before = engine.estimated_memory_bytes();
        engine.compact_history().unwrap();

        // State should be preserved.
        assert!(engine.exists("items", "i0"));
        assert!(engine.exists("items", "i49"));

        // New operations should still work.
        engine
            .upsert("items", "i50", &[("val", LoroValue::I64(50))])
            .unwrap();
        assert!(engine.exists("items", "i50"));

        // Memory should be reduced (or at least not much larger).
        let mem_after = engine.estimated_memory_bytes();
        // History compaction should not increase memory significantly.
        assert!(
            mem_after <= mem_before * 2,
            "memory after compact ({mem_after}) should not be much larger than before ({mem_before})"
        );
    }

    #[test]
    fn list_ids() {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine
            .upsert("col", "a", &[("x", LoroValue::I64(1))])
            .unwrap();
        engine
            .upsert("col", "b", &[("x", LoroValue::I64(2))])
            .unwrap();

        let mut ids = engine.list_ids("col");
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn acked_version_tracking() {
        let mut engine = CrdtEngine::new(1).unwrap();
        assert_eq!(engine.acked_version("users"), 0);

        engine.set_acked_version("users", 42);
        assert_eq!(engine.acked_version("users"), 42);
    }

    #[test]
    fn memory_estimation() {
        let mut engine = CrdtEngine::new(1).unwrap();
        let before = engine.estimated_memory_bytes();

        for i in 0..100 {
            engine
                .upsert("big", &format!("k{i}"), &[("data", LoroValue::I64(i))])
                .unwrap();
        }

        let after = engine.estimated_memory_bytes();
        assert!(after > before);
    }
}
