// SPDX-License-Identifier: BUSL-1.1

//! The Raft event loop: the tick loop and the metadata apply lane, run side
//! by side in one task until shutdown.

use std::time::Instant;

use tracing::debug;

use crate::forward::PlanExecutor;

use super::loop_core::{CommitApplier, RaftLoop};
use super::tick::metadata_lane::METADATA_LANE_DEPTH;

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Run the event loop until shutdown.
    ///
    /// This drives Raft elections, heartbeats, and message dispatch.
    /// Call [`crate::transport::NexarTransport::serve`] separately with
    /// `Arc<Self>` as the handler.
    ///
    /// The metadata apply lane runs beside the tick loop in this task (see
    /// `tick::metadata_lane`). A metadata apply that awaits the host never
    /// holds up a tick. When shutdown begins, the tick loop stops, closes
    /// the lane and propagates the signal to the internal cooperative
    /// shutdown channel, so every detached task spawned inside `do_tick`
    /// exits promptly and drops its `Arc<Mutex<MultiRaft>>` clone. `run`
    /// returns once the lane ended too.
    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) {
        let (tx, rx) = tokio::sync::mpsc::channel(METADATA_LANE_DEPTH);
        self.tick_state.open_metadata_lane(tx);
        let lane = self.run_metadata_lane(rx, shutdown.clone());
        let persister = async {
            if let Some(persister) = self.routing_persister.as_ref() {
                persister.run(shutdown.clone()).await;
            }
        };
        let ticks = async {
            self.run_ticks(shutdown.clone()).await;
            self.tick_state.close_metadata_lane();
        };
        tokio::join!(ticks, lane, persister);
    }

    /// Tick until shutdown begins.
    async fn run_ticks(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        self.loop_metrics.set_up(true);

        // Startup GC sweep: remove orphaned partial-snapshot files from
        // previous runs that did not complete. It walks the disk, so it runs
        // on a blocking thread.
        if let Some(dir) = self.data_dir.clone() {
            let max_age = self.orphan_partial_max_age_secs;
            let swept = tokio::task::spawn_blocking(move || {
                crate::install_snapshot::gc::sweep_orphans(&dir, max_age)
            })
            .await;
            match swept {
                Err(e) => {
                    tracing::warn!(error = %e, "startup: partial snapshot sweep task failed");
                }
                Ok(Ok((removed, errs))) => {
                    if removed > 0 {
                        tracing::info!(removed, "startup: removed orphaned partial snapshot files");
                    }
                    for e in errs {
                        tracing::warn!(error = %e, "startup: partial snapshot GC error");
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "startup: failed to sweep partial snapshot directory");
                }
            }
        }

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let started = Instant::now();
                    self.do_tick().await;
                    self.loop_metrics.observe(started.elapsed());
                }
                _ = self.drive_signal.notified() => {
                    self.do_drive().await;
                }
                _ = self.reconcile_notify.notified() => {
                    if *shutdown.borrow() {
                        break;
                    }
                    self.reconcile_placement();
                }
                changed = shutdown.changed() => {
                    // A dropped sender is a shutdown too: no signal can come.
                    if changed.is_err() || *shutdown.borrow() {
                        debug!("raft loop shutting down");
                        self.begin_shutdown();
                        break;
                    }
                }
            }
        }
        self.loop_metrics.set_up(false);
    }
}
