//! Snapshot chunk buffering and assembly for inbound array sync.
//!
//! Split from [`super::inbound`] to keep that module within the project's
//! file-size budget. The buffer state itself lives on
//! [`super::inbound::OriginArrayInbound`]; this module only owns the
//! assembly logic and the per-snapshot scratch type.

use std::collections::BTreeMap;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op_codec;
use nodedb_array::sync::snapshot::{SnapshotChunk, SnapshotHeader, assemble_chunks};
use nodedb_types::sync::wire::array::{
    ArrayRejectMsg, ArrayRejectReason, ArraySnapshotChunkMsg, ArraySnapshotMsg,
};
use tracing::{error, warn};

use super::inbound::{InboundOutcome, OriginArrayInbound};
use super::reject::build_reject;

/// In-flight snapshot scratch buffer keyed by `(array, snapshot_hlc_bytes)`
/// inside [`OriginArrayInbound`].
pub(super) struct SnapshotAssembly {
    pub(super) header: Option<SnapshotHeader>,
    pub(super) chunks: BTreeMap<u32, SnapshotChunk>,
}

impl SnapshotAssembly {
    pub(super) fn new() -> Self {
        Self {
            header: None,
            chunks: BTreeMap::new(),
        }
    }
}

impl OriginArrayInbound {
    /// Buffer an incoming snapshot header.
    pub fn handle_snapshot_header(
        &self,
        msg: &ArraySnapshotMsg,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let header: SnapshotHeader = match zerompk::from_msgpack(&msg.header_payload) {
            Ok(h) => h,
            Err(e) => {
                warn!(array = %msg.array, error = %e, "array_inbound: snapshot header decode failed");
                return Err(Some(build_reject(
                    &msg.array,
                    Hlc::ZERO,
                    ArrayRejectReason::ShapeInvalid,
                    format!("snapshot header decode: {e}"),
                )));
            }
        };

        let total = header.total_chunks;
        let key = (msg.array.clone(), header.snapshot_hlc.to_bytes());

        let mut snapshots = match self.snapshots().lock() {
            Ok(g) => g,
            Err(_) => {
                error!(array = %msg.array, "array_inbound: snapshot mutex poisoned");
                return Err(None);
            }
        };
        let entry = snapshots.entry(key).or_insert_with(SnapshotAssembly::new);
        entry.header = Some(header);

        Ok(InboundOutcome::SnapshotPartial { received: 0, total })
    }

    /// Buffer a snapshot chunk and, when complete, assemble and apply all ops.
    pub async fn handle_snapshot_chunk(
        &self,
        msg: &ArraySnapshotChunkMsg,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let key = (msg.array.clone(), msg.snapshot_hlc_bytes);

        let assembled: Option<(SnapshotHeader, Vec<SnapshotChunk>)> = {
            let mut snapshots = match self.snapshots().lock() {
                Ok(g) => g,
                Err(_) => {
                    error!(array = %msg.array, "array_inbound: snapshot mutex poisoned (chunk)");
                    return Err(None);
                }
            };
            let entry = snapshots
                .entry(key.clone())
                .or_insert_with(SnapshotAssembly::new);

            let chunk = SnapshotChunk {
                array: msg.array.clone(),
                chunk_index: msg.chunk_index,
                total_chunks: msg.total_chunks,
                payload: msg.payload.clone(),
                snapshot_hlc: Hlc::from_bytes(&msg.snapshot_hlc_bytes),
            };
            entry.chunks.insert(msg.chunk_index, chunk);

            let total = msg.total_chunks as usize;
            if let Some(h) = &entry.header {
                if entry.chunks.len() == total {
                    let header = h.clone();
                    let chunks_vec: Vec<SnapshotChunk> = entry.chunks.values().cloned().collect();
                    Some((header, chunks_vec))
                } else {
                    None
                }
            } else {
                None
            }
        };

        let Some((header, mut chunks)) = assembled else {
            let snapshots = match self.snapshots().lock() {
                Ok(g) => g,
                Err(_) => return Err(None),
            };
            let received = snapshots
                .get(&key)
                .map(|e| e.chunks.len() as u32)
                .unwrap_or(0);
            return Ok(InboundOutcome::SnapshotPartial {
                received,
                total: msg.total_chunks,
            });
        };

        // Assemble snapshot.
        let snapshot = match assemble_chunks(&header, &mut chunks) {
            Ok(s) => s,
            Err(e) => {
                warn!(array = %msg.array, error = %e, "array_inbound: assemble_chunks failed");
                return Err(Some(build_reject(
                    &msg.array,
                    header.snapshot_hlc,
                    ArrayRejectReason::ShapeInvalid,
                    format!("assemble_chunks: {e}"),
                )));
            }
        };

        // Decode embedded ops.
        let ops = match op_codec::decode_op_batch(&snapshot.tile_blob) {
            Ok(ops) => ops,
            Err(e) => {
                warn!(array = %msg.array, error = %e, "array_inbound: snapshot op batch decode failed");
                return Err(Some(build_reject(
                    &msg.array,
                    header.snapshot_hlc,
                    ArrayRejectReason::ShapeInvalid,
                    format!("decode_op_batch: {e}"),
                )));
            }
        };

        let mut ops_applied: u64 = 0;
        for op in &ops {
            match self.apply_op(op.clone()).await {
                Ok(InboundOutcome::Applied) => ops_applied += 1,
                Ok(_) => {}
                Err(Some(reject)) => {
                    warn!(
                        array = %msg.array,
                        reason = ?reject.reason,
                        "array_inbound: op rejected during snapshot assembly"
                    );
                }
                Err(None) => {}
            }
        }

        // Clean up assembly state.
        if let Ok(mut snapshots) = self.snapshots().lock() {
            snapshots.remove(&key);
        }

        Ok(InboundOutcome::SnapshotApplied { ops_applied })
    }
}
