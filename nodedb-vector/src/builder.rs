// SPDX-License-Identifier: Apache-2.0

//! Background HNSW builder thread.
//!
//! A build makes the segment's HNSW graph and then its codec, so codec
//! training runs here and never on the core. A codec error never discards
//! the graph: the build carries the error beside the graph.
//!
//! Each Data Plane core owns one builder thread. The core sends build
//! requests with `try_send` and drains finished builds with `try_recv` once
//! per tick, so a build never blocks the core's reactor. Both channels are
//! bounded:
//!
//! - The request queue holds `capacity` requests. When it is full the core
//!   keeps the job in its own backlog and sends it on a later tick. The
//!   segment stays searchable by brute force meanwhile.
//! - The completion queue holds `capacity` results. When it is full the
//!   builder thread waits for the core to drain it.
//!
//! The thread builds requests in FIFO order and stops when the core drops
//! its request sender.

use std::sync::mpsc;
use std::thread::JoinHandle;

use tracing::{debug, info, warn};

use crate::collection::{BuildComplete, BuildRequest, BuiltSegment, CodecCharge, SegmentCodec};
use crate::error::VectorError;
use crate::hnsw::HnswIndex;

/// Sender half: TPC core sends build requests to the builder thread.
pub type BuildSender = mpsc::SyncSender<BuildRequest>;

/// Receiver half: TPC core receives completed builds.
pub type CompleteReceiver = mpsc::Receiver<BuildComplete>;

/// Build requests a core's builder queue holds. Each request carries a whole
/// segment's vectors, so the bound caps the memory in flight.
pub const BUILD_QUEUE_CAPACITY: usize = 4;

/// Spawn the HNSW builder thread for Data Plane core `core_id`, with request
/// and completion queues of `capacity` entries each. Fails when the OS
/// refuses the thread.
pub fn spawn_builder(
    core_id: usize,
    capacity: usize,
) -> std::io::Result<(BuildSender, CompleteReceiver, JoinHandle<()>)> {
    let (request_tx, request_rx) = mpsc::sync_channel::<BuildRequest>(capacity);
    let (complete_tx, complete_rx) = mpsc::sync_channel::<BuildComplete>(capacity);

    let handle = std::thread::Builder::new()
        .name(format!("hnsw-builder-{core_id}"))
        .spawn(move || {
            info!(core_id, "HNSW builder thread started");
            builder_loop(core_id, request_rx, complete_tx);
            info!(core_id, "HNSW builder thread stopped");
        })?;

    Ok((request_tx, complete_rx, handle))
}

fn builder_loop(
    core_id: usize,
    rx: mpsc::Receiver<BuildRequest>,
    tx: mpsc::SyncSender<BuildComplete>,
) {
    while let Ok(req) = rx.recv() {
        debug!(
            core_id,
            key = %req.key,
            segment_id = req.segment_id,
            vectors = req.vectors.len(),
            dim = req.dim,
            "building HNSW index"
        );
        let start = std::time::Instant::now();
        let (key, segment_id, kind) = (req.key.clone(), req.segment_id, req.kind);
        let result = build(core_id, req);
        match &result {
            Ok(built) => match &built.codec {
                Ok(_) => info!(
                    core_id,
                    key = %key,
                    segment_id,
                    vectors = built.index.len(),
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "HNSW index built"
                ),
                Err(e) => warn!(
                    core_id,
                    key = %key,
                    segment_id,
                    vectors = built.index.len(),
                    error = %e,
                    "HNSW index built; its codec build failed"
                ),
            },
            Err(e) => warn!(core_id, key = %key, segment_id, error = %e, "HNSW build failed"),
        }
        let complete = BuildComplete {
            key,
            segment_id,
            kind,
            result,
        };
        if tx.send(complete).is_err() {
            warn!(core_id, "builder: core channel closed, stopping");
            break;
        }
    }
}

/// Build one segment: the HNSW graph, inserting every vector in local-id
/// order so node `i` of the graph is vector `i` of the request, then the
/// codec the request names. A graph error fails the build: a skipped vector
/// shifts every later id. A codec error keeps the graph and is carried in
/// [`BuiltSegment::codec`], so the segment still answers exactly.
pub fn build_segment(seed: u64, req: BuildRequest) -> Result<BuiltSegment, VectorError> {
    let mut index = HnswIndex::with_seed(req.dim, req.params, seed);
    for vector in req.vectors {
        index.insert(vector)?;
    }
    let (codec, charge) = match SegmentCodec::build(req.codec, &index, req.memory.clone(), seed) {
        Ok((codec, charge)) => (Ok(codec), charge),
        Err(e) => (Err(e), CodecCharge::default()),
    };
    Ok(BuiltSegment {
        index,
        codec,
        charge,
        memory: req.memory,
    })
}

fn build(core_id: usize, req: BuildRequest) -> Result<BuiltSegment, VectorError> {
    let seed = (core_id as u64 + 1) * 1000 + u64::from(req.segment_id);
    build_segment(seed, req)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::{BuildKind, CodecPlan};
    use crate::hnsw::HnswParams;
    use crate::test_support::test_memory;

    fn request(segment_id: u32, vectors: Vec<Vec<f32>>) -> BuildRequest {
        BuildRequest {
            key: "k".into(),
            segment_id,
            kind: BuildKind::Seal,
            vectors,
            dim: 2,
            params: HnswParams::default(),
            codec: CodecPlan::Sq8,
            memory: test_memory(),
        }
    }

    #[test]
    fn builds_keep_one_node_per_vector_and_report_errors() {
        let (tx, rx, handle) = spawn_builder(0, 1).unwrap();
        tx.send(request(1, vec![vec![1.0, 0.0], vec![0.0, 1.0]]))
            .unwrap();
        tx.send(request(2, vec![vec![1.0, 0.0], vec![1.0]]))
            .unwrap();

        let first = rx.recv().unwrap();
        assert_eq!(first.segment_id, 1);
        let built = first.result.unwrap();
        assert_eq!(built.index.len(), 2);
        assert!(matches!(built.codec, Ok(SegmentCodec::Sq8 { .. })));
        let second = rx.recv().unwrap();
        assert!(matches!(
            second.result,
            Err(VectorError::DimensionMismatch { .. })
        ));

        drop(tx);
        handle.join().unwrap();
    }

    #[test]
    fn a_full_request_queue_refuses_without_blocking() {
        let (tx, _rx, _handle) = spawn_builder(0, 1).unwrap();
        // The thread holds at most one request in hand and one queued; with
        // the completion side undrained, later sends find the queue full.
        let mut refused = false;
        for id in 0..8 {
            if let Err(mpsc::TrySendError::Full(_)) = tx.try_send(request(id, vec![vec![1.0, 0.0]]))
            {
                refused = true;
                break;
            }
        }
        assert!(refused, "a bounded queue must refuse once full");
    }
}
