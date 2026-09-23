// SPDX-License-Identifier: Apache-2.0

//! Background HNSW index builder thread.
//!
//! Each Data Plane core has one builder thread that processes HNSW
//! construction requests sequentially (FIFO).

use std::sync::mpsc;
use std::thread::JoinHandle;

use tracing::{debug, info, warn};

use crate::collection::{BuildComplete, BuildRequest};
use crate::hnsw::HnswIndex;

/// Sender half: TPC core sends build requests to the builder thread.
pub type BuildSender = mpsc::Sender<BuildRequest>;

/// Receiver half: TPC core receives completed builds.
pub type CompleteReceiver = mpsc::Receiver<BuildComplete>;

/// Spawn a background HNSW builder thread for a Data Plane core.
pub fn spawn_builder(core_id: usize) -> (BuildSender, CompleteReceiver, JoinHandle<()>) {
    let (request_tx, request_rx) = mpsc::channel::<BuildRequest>();
    let (complete_tx, complete_rx) = mpsc::channel::<BuildComplete>();

    let handle = std::thread::Builder::new()
        .name(format!("hnsw-builder-{core_id}"))
        .spawn(move || {
            info!(core_id, "HNSW builder thread started");
            builder_loop(core_id, request_rx, complete_tx);
            info!(core_id, "HNSW builder thread stopped");
        })
        .expect("failed to spawn HNSW builder thread");

    (request_tx, complete_rx, handle)
}

fn builder_loop(core_id: usize, rx: mpsc::Receiver<BuildRequest>, tx: mpsc::Sender<BuildComplete>) {
    while let Ok(req) = rx.recv() {
        debug!(
            core_id,
            key = %req.key,
            segment_id = req.segment_id,
            vectors = req.vectors.len(),
            dim = req.dim,
            "building HNSW index"
        );

        // The build log carries its duration; wasm32-unknown-unknown has no
        // clock, so the timer is skipped there and the log reports 0.
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let start = std::time::Instant::now();
        let mut index = HnswIndex::with_seed(
            req.dim,
            req.params,
            (core_id as u64 + 1) * 1000 + req.segment_id as u64,
        );

        for vector in req.vectors {
            index
                .insert(vector)
                .unwrap_or_else(|e| tracing::error!(error = %e, "HNSW insert failed"));
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let elapsed_ms = start.elapsed().as_millis() as u64;
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let elapsed_ms = 0u64;
        info!(
            core_id,
            key = %req.key,
            segment_id = req.segment_id,
            vectors = index.len(),
            elapsed_ms,
            "HNSW index built"
        );

        if tx
            .send(BuildComplete {
                key: req.key,
                segment_id: req.segment_id,
                index,
            })
            .is_err()
        {
            warn!(core_id, "builder: core channel closed, stopping");
            break;
        }
    }
}
