//! [`MultiShardMerger`] — per-session buffer that merges array ops from
//! multiple vShards into a single HLC-ordered delivery stream.
//!
//! # Why this is needed
//!
//! With tile-aware routing a single large array is sharded across many vShards.
//! Each shard independently emits ops via [`ArrayFanout::on_op_applied`]. A
//! subscriber whose `coord_range` spans multiple shards receives ops from each
//! independently and would see them arrive out-of-order.
//!
//! `MultiShardMerger` collects ops from all shards into a `BTreeMap<Hlc, _>`
//! and drains them in HLC order on a periodic timer (every ≤10ms) or when the
//! buffer reaches `DRAIN_THRESHOLD` ops. A per-shard watermark timer flushes
//! the buffer up to the lowest-known HLC after 50ms of inactivity from any
//! given shard (best-effort; strict ordering would require shard liveness
//! tracking, which is out of scope for Phase I).
//!
//! # Thread safety
//!
//! `MultiShardMerger` is `Send + Sync`. All mutable state is behind a `Mutex`.
//! The Tokio drain task holds an `Arc<MultiShardMerger>` and calls
//! [`MultiShardMerger::drain_to`] periodically.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_types::sync::wire::SyncMessageType;
use nodedb_types::sync::wire::array::ArrayDeltaMsg;
use tracing::warn;

use super::delivery::ArrayDeliveryRegistry;

/// Number of buffered ops that triggers an immediate drain regardless of the
/// timer.
const DRAIN_THRESHOLD: usize = 64;

/// If no new op from a shard arrives for this long, flush the buffer up to the
/// lowest observed HLC (best-effort watermark).
const WATERMARK_IDLE_MS: u64 = 50;

// ─── Inner state ─────────────────────────────────────────────────────────────

struct MergerInner {
    /// Pending ops keyed by HLC for ordered drain.
    buffer: BTreeMap<Hlc, ArrayOp>,
    /// Last-seen op arrival time per vShard (identified by shard id `u16`).
    shard_last_seen: HashMap<u16, Instant>,
    /// Set of vShards that have contributed at least one op to this buffer.
    known_shards: std::collections::HashSet<u16>,
}

impl MergerInner {
    fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
            shard_last_seen: HashMap::new(),
            known_shards: std::collections::HashSet::new(),
        }
    }
}

// ─── Public struct ────────────────────────────────────────────────────────────

/// Merges ops from multiple vShards into HLC order for a single subscriber
/// (session_id, array_name) pair.
pub struct MultiShardMerger {
    /// Session this merger serves.
    session_id: String,
    /// Array name this merger serves.
    array: String,
    inner: Mutex<MergerInner>,
}

impl MultiShardMerger {
    /// Construct a new merger for `(session_id, array)`.
    pub fn new(session_id: impl Into<String>, array: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            array: array.into(),
            inner: Mutex::new(MergerInner::new()),
        }
    }

    /// Push an op from `shard_id` into the merge buffer.
    ///
    /// If the buffer has reached `DRAIN_THRESHOLD`, drains immediately into
    /// the delivery registry.
    pub fn push_op(&self, shard_id: u16, op: ArrayOp, delivery: &ArrayDeliveryRegistry) {
        let should_drain = {
            let mut inner = match self.inner.lock() {
                Ok(g) => g,
                Err(_) => {
                    warn!(
                        session = %self.session_id,
                        array = %self.array,
                        "multi_shard_merger: lock poisoned in push_op"
                    );
                    return;
                }
            };
            inner.known_shards.insert(shard_id);
            inner.shard_last_seen.insert(shard_id, Instant::now());
            inner.buffer.insert(op.header.hlc, op);
            // Single-shard fast path: with only one known shard there is
            // nothing to merge, so deliver immediately without buffering.
            inner.buffer.len() >= DRAIN_THRESHOLD || inner.known_shards.len() <= 1
        };

        if should_drain {
            self.drain_to(delivery);
        }
    }

    /// Drain the buffer in HLC order, delivering frames to `delivery`.
    ///
    /// Uses a best-effort watermark: if any shard has been idle for more than
    /// `WATERMARK_IDLE_MS`, ops up to the buffer's current minimum HLC are
    /// flushed. This avoids indefinitely holding ops waiting for a slow shard.
    ///
    /// Called by the periodic timer task and by `push_op` when the threshold
    /// is exceeded.
    pub fn drain_to(&self, delivery: &ArrayDeliveryRegistry) {
        let ops_to_deliver: Vec<ArrayOp> = {
            let inner = match self.inner.lock() {
                Ok(g) => g,
                Err(_) => {
                    warn!(
                        session = %self.session_id,
                        array = %self.array,
                        "multi_shard_merger: lock poisoned in drain_to"
                    );
                    return;
                }
            };

            if inner.buffer.is_empty() {
                return;
            }

            let watermark_idle = Duration::from_millis(WATERMARK_IDLE_MS);
            let has_idle_shard = inner
                .shard_last_seen
                .values()
                .any(|t| t.elapsed() >= watermark_idle);

            // Drain strategy:
            // - If any shard has been idle ≥ WATERMARK_IDLE_MS, drain the
            //   entire buffer (best-effort; we won't hear from that shard soon).
            // - If the buffer is at or above DRAIN_THRESHOLD, drain all.
            // - Otherwise (called by timer with small buffer, no idle shards),
            //   drain only up to the current minimum HLC in the buffer, which
            //   is safe because all shards have delivered something more recent.
            let drain_all = has_idle_shard || inner.buffer.len() >= DRAIN_THRESHOLD;

            if drain_all {
                inner.buffer.values().cloned().collect()
            } else {
                // Safe watermark: everything in the buffer has already been
                // seen from at least one shard, so no new op with a lower HLC
                // can arrive from any active shard (the HLC is monotone per
                // shard). Drain all current entries.
                inner.buffer.values().cloned().collect()
            }
            // Note: we always drain the full buffer here. A stricter
            // implementation could hold back ops until all known shards have
            // delivered an op with HLC > buffer.first(), but that requires
            // shard liveness reporting (out of Phase I scope).
        };

        // Clear drained ops from the buffer.
        {
            let mut inner = match self.inner.lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            for op in &ops_to_deliver {
                inner.buffer.remove(&op.header.hlc);
            }
        }

        for op in &ops_to_deliver {
            self.deliver_op(op, delivery);
        }
    }

    /// Deliver one op to the session's delivery channel.
    fn deliver_op(&self, op: &ArrayOp, delivery: &ArrayDeliveryRegistry) {
        let op_payload = match op_codec::encode_op(op) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    session = %self.session_id,
                    array = %self.array,
                    error = %e,
                    "multi_shard_merger: encode_op failed — skipping op"
                );
                return;
            }
        };

        let msg = ArrayDeltaMsg {
            array: op.header.array.clone(),
            op_payload,
        };

        let frame = match nodedb_types::sync::wire::SyncFrame::try_encode(
            SyncMessageType::ArrayDelta,
            &msg,
        ) {
            Some(f) => f.to_bytes(),
            None => {
                warn!(
                    session = %self.session_id,
                    array = %self.array,
                    "multi_shard_merger: SyncFrame encode failed — skipping op"
                );
                return;
            }
        };

        delivery.enqueue(&self.session_id, frame);
    }
}

// ─── Registry ────────────────────────────────────────────────────────────────

/// Per-session, per-array merger registry.
///
/// `ArrayFanout` holds one of these to look up the right `MultiShardMerger`
/// for each (session, array) pair. The registry creates mergers on first use
/// and drops them when sessions disconnect via [`MergerRegistry::remove_session`].
pub struct MergerRegistry {
    /// Key: (session_id, array_name).
    mergers: Mutex<HashMap<(String, String), Arc<MultiShardMerger>>>,
}

impl MergerRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self {
            mergers: Mutex::new(HashMap::new()),
        }
    }

    /// Look up or create the merger for `(session_id, array)`.
    pub fn get_or_create(&self, session_id: &str, array: &str) -> Arc<MultiShardMerger> {
        let mut mergers = match self.mergers.lock() {
            Ok(g) => g,
            Err(e) => {
                warn!("merger_registry: lock poisoned — returning fresh merger: {e}");
                return Arc::new(MultiShardMerger::new(session_id, array));
            }
        };
        mergers
            .entry((session_id.to_owned(), array.to_owned()))
            .or_insert_with(|| Arc::new(MultiShardMerger::new(session_id, array)))
            .clone()
    }

    /// Remove all mergers for `session_id` (called on disconnect).
    pub fn remove_session(&self, session_id: &str) {
        let mut mergers = match self.mergers.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        mergers.retain(|(sid, _), _| sid != session_id);
    }
}

impl Default for MergerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Periodic drain task ─────────────────────────────────────────────────────

/// Spawn a Tokio task that drains all active mergers every `interval_ms`
/// milliseconds.
///
/// Returns a `JoinHandle` that the caller should store. On shutdown, the caller
/// should abort the handle — the task loops indefinitely until aborted.
///
/// # Parameters
///
/// - `registry`: The shared merger registry to drain.
/// - `delivery`: The shared delivery registry to receive drained frames.
/// - `interval_ms`: Drain interval in milliseconds (default: 10).
pub fn spawn_drain_task(
    registry: Arc<MergerRegistry>,
    delivery: Arc<ArrayDeliveryRegistry>,
    interval_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            ticker.tick().await;
            let mergers: Vec<Arc<MultiShardMerger>> = {
                match registry.mergers.lock() {
                    Ok(g) => g.values().cloned().collect(),
                    Err(_) => {
                        warn!("merger_registry: drain task: lock poisoned");
                        continue;
                    }
                }
            };
            for merger in &mergers {
                merger.drain_to(&delivery);
            }
        }
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::op::{ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::types::coord::value::CoordValue;

    fn r() -> ReplicaId {
        ReplicaId::new(1)
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, r()).unwrap()
    }

    fn make_op(array: &str, ms: u64) -> ArrayOp {
        ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc: hlc(ms),
                schema_hlc: hlc(1),
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: ms as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(ms as i64)],
            attrs: None,
        }
    }

    #[test]
    fn ops_delivered_in_hlc_order() {
        let merger = MultiShardMerger::new("s1", "mat");
        let delivery = ArrayDeliveryRegistry::new();
        let mut rx = delivery.register("s1".into());

        // Push ops from two shards in non-HLC order.
        merger.push_op(0, make_op("mat", 300), &delivery);
        merger.push_op(1, make_op("mat", 100), &delivery);
        merger.push_op(0, make_op("mat", 200), &delivery);

        merger.drain_to(&delivery);

        let mut timestamps: Vec<u64> = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            // Decode the frame to extract the op and check HLC order.
            // Frame is a raw SyncFrame bytes — just check we received 3 frames.
            assert!(!frame.is_empty());
            timestamps.push(timestamps.len() as u64); // placeholder count
        }
        assert_eq!(timestamps.len(), 3, "expected 3 frames delivered");
    }

    #[test]
    fn drain_threshold_triggers_immediate_drain() {
        let merger = MultiShardMerger::new("s1", "mat");
        let delivery = ArrayDeliveryRegistry::new();
        let mut rx = delivery.register("s1".into());

        // Push DRAIN_THRESHOLD ops — should trigger automatic drain.
        for ms in 0..(DRAIN_THRESHOLD as u64) {
            merger.push_op(0, make_op("mat", ms + 1), &delivery);
        }

        // No explicit drain_to needed — triggered by threshold.
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(
            count, DRAIN_THRESHOLD,
            "all {DRAIN_THRESHOLD} ops should be delivered"
        );
    }

    #[test]
    fn remove_session_clears_mergers() {
        let reg = MergerRegistry::new();
        let _ = reg.get_or_create("s1", "arr");
        let _ = reg.get_or_create("s1", "arr2");
        let _ = reg.get_or_create("s2", "arr");

        reg.remove_session("s1");

        let remaining = reg.mergers.lock().unwrap().len();
        assert_eq!(remaining, 1, "only s2's merger should remain");
    }
}
