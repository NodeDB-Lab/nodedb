// SPDX-License-Identifier: BUSL-1.1

//! `ShuffleFanoutSink` — the produce-side sink that hash-partitions each scanned
//! row and fans it out to the per-part owners (E4a).
//!
//! # Plane discipline
//!
//! This runs on the producer node's CONTROL plane (the Tokio transport reactor).
//! It is fed by the existing streaming executor
//! ([`crate::control::LocalPlanExecutor::execute_plan_streaming`]), which
//! dispatches the actual scan to the Data Plane through the SPSC bridge — this
//! sink never touches storage or io_uring. The QUIC fan-out streams it drives are
//! Control-Plane I/O, which is allowed here.
//!
//! # Bounded memory (design D1)
//!
//! A whole side is NEVER materialized. Each chunk is exploded one at a time and
//! its rows are appended to per-part buffers; a part buffer is flushed as one
//! `ShufflePushChunk` as soon as it reaches [`FLUSH_ROWS`]. Peak RAM is therefore
//! `num_parts × FLUSH_ROWS` rows (num_parts ≈ node count, small), independent of
//! the scanned side's size.
//!
//! # Loopback
//!
//! For a part whose owner is THIS node, there is no QUIC round-trip to self:
//! rows are deposited directly into the local
//! [`super::inbox::ShuffleReceiverRegistry`] — the same `append_chunk` /
//! `record_end` path the inbound `ShufflePush` read-loop uses.
//!
//! # Termination
//!
//! On a clean scan end, [`ShuffleFanoutSink::finish`] flushes every part's
//! residual buffer and sends a `ShufflePushEnd` to EVERY part for this side
//! (even zero-row parts) so each receiver's per-part barrier reaches
//! `producer_count`. On a scan error it `End`s every part with the error so
//! consumers fail fast — rows are never silently dropped.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_cluster::{NexarTransport, ShufflePushRequest, ShufflePushStream, TypedClusterError};

use super::frame_explode::explode_row_array;
use super::inbox::ShuffleReceiverRegistry;
use crate::data::executor::response_codec::{encode_binary_rows, flatten_to_relational_rows};

/// Per-part row-buffer flush threshold (rows). Bounds peak RAM to
/// `num_parts × FLUSH_ROWS` rows across the whole fan-out.
const FLUSH_ROWS: usize = 1024;

/// Routing target for one partition: either a remote QUIC push stream (opened
/// lazily on the first row routed there) or a loopback into the local registry.
enum PartTarget {
    /// Remote owner. `stream` is `None` until the first chunk is flushed to it.
    Remote {
        node_id: u64,
        stream: Option<ShufflePushStream>,
    },
    /// This node owns the part — deposit straight into the local registry.
    Loopback,
}

/// One partition's outbound state: its owner target plus the residual row buffer
/// not yet flushed.
struct PartState {
    target: PartTarget,
    buffer: Vec<Vec<u8>>,
}

/// The produce-side fan-out sink (see module docs).
pub struct ShuffleFanoutSink {
    transport: Arc<NexarTransport>,
    registry: Arc<ShuffleReceiverRegistry>,
    shuffle_id: u64,
    side: u8,
    num_parts: u32,
    producer_count: u32,
    keys: Vec<String>,
    /// `part -> PartState`, one entry per part in `0..num_parts`.
    parts: HashMap<u32, PartState>,
    /// The read versions observed across every scanned frame fed to this sink,
    /// one per vShard. A producer scans exactly one collection, so the fold
    /// holds that collection's observed versions: the comparand the
    /// coordinator uses for cross-shard OCC read validation of an
    /// in-transaction distributed aggregate.
    read_versions: crate::types::ReadVersions,
}

/// Parameters for [`ShuffleFanoutSink::new`].
pub struct ShuffleFanoutSinkParams<'a> {
    pub self_node_id: u64,
    pub shuffle_id: u64,
    pub side: u8,
    pub num_parts: u32,
    pub producer_count: u32,
    pub keys: Vec<String>,
    pub part_node_map: &'a [(u32, u64)],
}

impl ShuffleFanoutSink {
    /// Build a sink for one `(shuffle_id, side)` produce.
    ///
    /// `part_node_map` maps every part to its owning node id. Parts owned by
    /// `self_node_id` are wired for loopback; all others open a QUIC push stream
    /// lazily on first use. Every part in `0..num_parts` is materialized up front
    /// so `finish` can `End` it even if it received zero rows.
    pub fn new(
        transport: Arc<NexarTransport>,
        registry: Arc<ShuffleReceiverRegistry>,
        params: ShuffleFanoutSinkParams<'_>,
    ) -> Self {
        let ShuffleFanoutSinkParams {
            self_node_id,
            shuffle_id,
            side,
            num_parts,
            producer_count,
            keys,
            part_node_map,
        } = params;
        let owner: HashMap<u32, u64> = part_node_map.iter().copied().collect();
        let mut parts = HashMap::with_capacity(num_parts as usize);
        for part in 0..num_parts {
            // A part with no explicit owner defaults to loopback (this node),
            // which keeps rows on a reachable path rather than dropping them.
            let target = match owner.get(&part) {
                Some(&node_id) if node_id != self_node_id => PartTarget::Remote {
                    node_id,
                    stream: None,
                },
                _ => PartTarget::Loopback,
            };
            parts.insert(
                part,
                PartState {
                    target,
                    buffer: Vec::new(),
                },
            );
        }
        Self {
            transport,
            registry,
            shuffle_id,
            side,
            num_parts,
            producer_count,
            keys,
            parts,
            read_versions: crate::types::ReadVersions::new(),
        }
    }

    /// The read versions observed across every scanned frame fed to this
    /// sink (empty if no frame carried one). The producer hook reads them
    /// after the streaming scan completes and reports them on the
    /// `ShuffleProduceResponse` so the coordinator can validate the read.
    pub fn observed_read_versions(&self) -> &crate::types::ReadVersions {
        &self.read_versions
    }

    /// The `ShufflePushRequest` opener for `part` (shared shape for remote +
    /// loopback paths).
    fn push_request(&self, part: u32) -> ShufflePushRequest {
        ShufflePushRequest {
            shuffle_id: self.shuffle_id,
            part,
            side: self.side,
            num_parts: self.num_parts,
            producer_count: self.producer_count,
        }
    }

    /// Partition one chunk's rows into per-part buffers, flushing any part that
    /// reaches the row threshold. Bounded memory: the chunk is exploded once and
    /// only the residual per-part buffers are held.
    async fn route_chunk(&mut self, payload: Vec<u8>) -> crate::Result<()> {
        // Normalize storage `{id, data:<value>}` scan wrappers → flat relational
        // rows BEFORE hashing/staging — the same canonical boundary the broadcast
        // path applies (`flatten_to_relational_rows` is the ONE place storage rows
        // become relational). Without it `partition_hash` and the consumer's grace
        // join would look for the join-key fields at the wrapper's top level (which
        // holds only `id`/`data`), find nothing, mishash to a single part, and
        // produce an empty join. Already-flat rows (computed producers) pass
        // through unchanged.
        let flat = flatten_to_relational_rows(&payload);
        let rows = explode_row_array(&flat)?;
        for row in rows {
            let part =
                (nodedb_query::partition_hash(row, &self.keys) % self.num_parts as u64) as u32;
            // Push into the part buffer, then drop the borrow before any flush so
            // the `&mut self` flush call below does not alias the `state` borrow.
            let needs_flush = {
                let state = self
                    .parts
                    .get_mut(&part)
                    .ok_or_else(|| crate::Error::Internal {
                        detail: format!(
                            "shuffle fanout: row hashed to part {part} outside 0..{}",
                            self.num_parts
                        ),
                    })?;
                state.buffer.push(row.to_vec());
                state.buffer.len() >= FLUSH_ROWS
            };
            if needs_flush {
                self.flush_part(part).await?;
            }
        }
        Ok(())
    }

    /// Flush one part's residual buffer as a single `ShufflePushChunk` (no-op if
    /// empty). Remote parts open their stream lazily on first flush; loopback
    /// parts deposit straight into the local registry's inbox.
    async fn flush_part(&mut self, part: u32) -> crate::Result<()> {
        // Take the part's residual buffer up front and drop the `self.parts`
        // borrow immediately, so the loopback / remote work below can freely
        // borrow the OTHER `self` fields (`registry` / `transport`) without
        // aliasing the part-map borrow.
        let rows = {
            let state = self
                .parts
                .get_mut(&part)
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!("shuffle fanout: flush of unknown part {part}"),
                })?;
            if state.buffer.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut state.buffer)
        };
        let chunk = encode_binary_rows(&rows);

        // Re-borrow the target to route the flushed chunk. The first borrow above
        // ended at the block close, so this single short-lived borrow is the only
        // live `self.parts` borrow while the field accesses below run.
        let is_loopback = matches!(
            self.parts.get(&part).map(|s| &s.target),
            Some(PartTarget::Loopback)
        );

        if is_loopback {
            let inbox = self.registry.get_or_create(
                self.shuffle_id,
                part,
                self.side,
                self.producer_count as usize,
            );
            inbox.append_chunk(&chunk).await?;
            return Ok(());
        }

        // Remote: ensure the per-part stream is open (lazily), then push.
        let (node_id, opener) = {
            let state = self
                .parts
                .get_mut(&part)
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!("shuffle fanout: flush of unknown part {part}"),
                })?;
            match &mut state.target {
                PartTarget::Remote { node_id, stream } => (*node_id, stream.is_none()),
                // `is_loopback` returned early above, so this part is Remote.
                PartTarget::Loopback => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "shuffle fanout: part {part} reclassified to loopback mid-flush"
                        ),
                    });
                }
            }
        };
        if opener {
            let req = self.push_request(part);
            let opened = self
                .transport
                .open_shuffle_push_stream(node_id, req)
                .await
                .map_err(|e| crate::Error::Internal {
                    detail: format!(
                        "shuffle fanout: open push stream to node {node_id} for part {part}: {e}"
                    ),
                })?;
            if let Some(PartState {
                target: PartTarget::Remote { stream, .. },
                ..
            }) = self.parts.get_mut(&part)
            {
                *stream = Some(opened);
            }
        }
        let state = self
            .parts
            .get_mut(&part)
            .ok_or_else(|| crate::Error::Internal {
                detail: format!("shuffle fanout: flush of unknown part {part}"),
            })?;
        let PartTarget::Remote {
            stream: Some(s), ..
        } = &mut state.target
        else {
            return Err(crate::Error::Internal {
                detail: format!("shuffle fanout: push stream absent for part {part}"),
            });
        };
        s.push_chunk(chunk)
            .await
            .map_err(|e| crate::Error::Internal {
                detail: format!(
                    "shuffle fanout: push chunk to node {node_id} for part {part}: {e}"
                ),
            })?;
        Ok(())
    }

    /// Finalize the fan-out: flush every part's residual buffer, then send a
    /// terminal `ShufflePushEnd` to EVERY part for this side.
    ///
    /// `error` is `None` for a clean produce or `Some(e)` when the local scan
    /// failed — in the error case every part is still `End`ed (with the error)
    /// so each receiver's barrier reaches `producer_count` and surfaces the
    /// failure rather than hanging. A flush/End failure to any one part is itself
    /// terminal and is returned so the caller surfaces it to the coordinator.
    pub async fn finish(mut self, error: Option<TypedClusterError>) -> crate::Result<()> {
        // On a clean produce, flush residual rows first. On an error produce,
        // skip flushing partial residue — the End{error} fails the side anyway,
        // and any opened stream still needs its terminal frame below.
        if error.is_none() {
            for part in 0..self.num_parts {
                self.flush_part(part).await?;
            }
        }

        // End every part. Drain the map by key order so each part — including
        // zero-row parts that never opened a stream — gets exactly one End.
        for part in 0..self.num_parts {
            let Some(state) = self.parts.remove(&part) else {
                continue;
            };
            match state.target {
                PartTarget::Loopback => {
                    let inbox = self.registry.get_or_create(
                        self.shuffle_id,
                        part,
                        self.side,
                        self.producer_count as usize,
                    );
                    if let Some(e) = error.clone() {
                        inbox.set_error(e);
                    }
                    if inbox.record_end() {
                        inbox.finalize().await?;
                    }
                }
                PartTarget::Remote { node_id, stream } => {
                    match stream {
                        // A stream was opened for this part — send its terminal
                        // End on the same stream.
                        Some(s) => {
                            s.finish(error.clone())
                                .await
                                .map_err(|e| crate::Error::Internal {
                                    detail: format!(
                                        "shuffle fanout: finish push stream to node {node_id} \
                                         for part {part}: {e}"
                                    ),
                                })?;
                        }
                        // Zero rows ever routed here, so no stream was opened.
                        // Open a fresh one and immediately End it so the
                        // receiver's barrier still counts this producer.
                        None => {
                            let req = self.push_request(part);
                            let s = self
                                .transport
                                .open_shuffle_push_stream(node_id, req)
                                .await
                                .map_err(|e| crate::Error::Internal {
                                    detail: format!(
                                        "shuffle fanout: open empty-part stream to node \
                                         {node_id} for part {part}: {e}"
                                    ),
                                })?;
                            s.finish(error.clone())
                                .await
                                .map_err(|e| crate::Error::Internal {
                                    detail: format!(
                                        "shuffle fanout: finish empty-part stream to node \
                                         {node_id} for part {part}: {e}"
                                    ),
                                })?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// `ChunkSink` is implemented on `&mut ShuffleFanoutSink` (not the owned value)
/// so the producer hook RETAINS ownership of the sink across the streaming call:
/// `execute_plan_streaming` consumes the `&mut` borrow, and once it returns the
/// hook still owns the sink and drives [`ShuffleFanoutSink::finish`] (flush
/// residuals + `End` every part). The whole produce runs on one Control-Plane
/// task, so the borrow never crosses a plane boundary.
impl nodedb_cluster::ChunkSink for &mut ShuffleFanoutSink {
    async fn send_chunk(
        &mut self,
        payload: Vec<u8>,
        _watermark_lsn: u64,
        read_versions: Vec<nodedb_types::ShardVersion>,
    ) -> nodedb_cluster::Result<()> {
        // The streaming executor calls this per scan batch. Fold this frame's
        // read versions so the producer can report what it observed for
        // cross-shard OCC validation (NOT the core-global watermark). Then
        // hash-partition the batch and buffer/flush per part. A
        // routing/flush failure is surfaced as a cluster error so the streaming
        // executor terminates the scan; the producer hook then `finish`es the
        // fan-out with the error so consumers fail fast (never a silent drop).
        self.read_versions
            .merge(&crate::types::ReadVersions::from_wire(&read_versions));
        self.route_chunk(payload)
            .await
            .map_err(|e| nodedb_cluster::ClusterError::Storage {
                detail: format!("shuffle fanout route: {e}"),
            })
    }
}
