// SPDX-License-Identifier: BUSL-1.1

//! Per-core HNSW build queue: the core's side of its builder thread.
//!
//! Each Data Plane core owns one builder thread (`nodedb_vector::builder`).
//! A build is CPU-heavy, so it never runs on the core's reactor, and the
//! core never blocks on the builder:
//!
//! - A job enters the `backlog` as a descriptor (index key plus segment),
//!   not as vectors. The request is read from the collection when the job
//!   is sent, so a job whose segment is gone is dropped then.
//! - Sending uses `try_send` on the bounded request queue. A full queue
//!   leaves the job at the head of the backlog for the next tick, and the
//!   segment stays searchable by brute force.
//! - Finished builds come back on a bounded completion queue the core
//!   drains every tick; the builder thread waits while it is full.
//!
//! The backlog holds one small descriptor per unbuilt segment, so its size
//! is bounded by the segments the core's collections hold.
//!
//! A codec build the memory budget refused is retried as a rebuild after a
//! backoff that doubles from `CODEC_RETRY_BASE` up to `CODEC_RETRY_MAX`, at
//! most `MAX_CODEC_RETRIES` times per segment. The delayed jobs hold one
//! descriptor per segment, so they are bounded the same way.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::engine::vector::builder::{BUILD_QUEUE_CAPACITY, BuildSender, CompleteReceiver};

/// What a queued build produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum BuildJobKind {
    /// Build the graph of building segment `segment_id`.
    Seal { segment_id: u32 },
    /// Rebuild the sealed segment at `base_id` in place.
    Rebuild { base_id: u32 },
}

/// One queued build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) struct BuildJob {
    pub key: VectorIndexKey,
    pub kind: BuildJobKind,
}

/// First backoff of a codec rebuild the memory budget refused.
const CODEC_RETRY_BASE: Duration = Duration::from_secs(1);
/// Longest backoff of a codec rebuild.
const CODEC_RETRY_MAX: Duration = Duration::from_secs(60);
/// Codec rebuilds one segment gets after budget refusals.
pub(in crate::data::executor) const MAX_CODEC_RETRIES: u32 = 8;

/// A codec rebuild waiting out its backoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) struct DelayedJob {
    pub job: BuildJob,
    /// When the job enters the backlog.
    pub due: Instant,
}

/// The core's builder channels, backlog and in-flight count.
pub(in crate::data::executor) struct VectorBuildQueue {
    /// `None` when the builder thread could not be spawned. Jobs then wait
    /// in the backlog and the next dispatch spawns it again.
    pub(in crate::data::executor) tx: Option<BuildSender>,
    pub(in crate::data::executor) rx: Option<CompleteReceiver>,
    /// Jobs waiting for room in the builder queue, oldest first.
    pub(in crate::data::executor) backlog: VecDeque<BuildJob>,
    /// Jobs sent and not yet finished, per index.
    pub(in crate::data::executor) in_flight: HashMap<VectorIndexKey, usize>,
    /// Codec rebuilds waiting out a backoff, at most one per segment.
    pub(in crate::data::executor) delayed: Vec<DelayedJob>,
    /// Budget refusals of each segment's codec build, by index and base id.
    /// A codec that installs clears its entry.
    pub(in crate::data::executor) codec_attempts: HashMap<(VectorIndexKey, u32), u32>,
    /// Pending jobs (backlog plus in flight) last added to the shared
    /// backlog gauge.
    pub(in crate::data::executor) reported_pending: u64,
}

impl VectorBuildQueue {
    /// Spawn the builder thread for core `core_id`.
    pub(in crate::data::executor) fn spawn(core_id: usize) -> Self {
        let mut queue = Self {
            tx: None,
            rx: None,
            backlog: VecDeque::new(),
            in_flight: HashMap::new(),
            delayed: Vec::new(),
            codec_attempts: HashMap::new(),
            reported_pending: 0,
        };
        queue.respawn(core_id);
        queue
    }

    /// Spawn a fresh builder thread, replacing a dead one. On failure the
    /// channels stay `None` and the next dispatch tries again.
    pub(in crate::data::executor) fn respawn(&mut self, core_id: usize) {
        match crate::engine::vector::builder::spawn_builder(core_id, BUILD_QUEUE_CAPACITY) {
            Ok((tx, rx, _handle)) => {
                // The thread stops on its own once `tx` drops.
                self.tx = Some(tx);
                self.rx = Some(rx);
                // Builds the dead thread held never come back.
                self.in_flight.clear();
            }
            Err(e) => {
                tracing::error!(core = core_id, error = %e, "HNSW builder thread spawn failed");
                crate::diag::vector_builder_spawn_failed(&e, core_id);
                self.tx = None;
                self.rx = None;
            }
        }
    }

    /// Queue `job` unless the same job already waits.
    pub(in crate::data::executor) fn push(&mut self, job: BuildJob) {
        if !self.backlog.contains(&job) {
            self.backlog.push_back(job);
        }
    }

    /// Builds of `key` waiting, delayed or running.
    pub(in crate::data::executor) fn pending_for(&self, key: &VectorIndexKey) -> usize {
        self.backlog.iter().filter(|j| &j.key == key).count()
            + self.delayed.iter().filter(|d| &d.job.key == key).count()
            + self.in_flight.get(key).copied().unwrap_or(0)
    }

    /// Builds waiting, delayed or running on this core.
    pub(in crate::data::executor) fn pending_total(&self) -> u64 {
        (self.backlog.len() + self.delayed.len() + self.in_flight.values().sum::<usize>()) as u64
    }

    /// Delay a codec rebuild of the sealed segment at `base_id` of `key`
    /// after the memory budget refused its codec. The backoff doubles per
    /// refusal from `CODEC_RETRY_BASE` up to `CODEC_RETRY_MAX`. Returns
    /// `false`, queueing nothing, once the segment spent `MAX_CODEC_RETRIES`.
    pub(in crate::data::executor) fn delay_codec_retry(
        &mut self,
        key: &VectorIndexKey,
        base_id: u32,
        now: Instant,
    ) -> bool {
        let attempts = self
            .codec_attempts
            .entry((key.clone(), base_id))
            .or_insert(0);
        if *attempts >= MAX_CODEC_RETRIES {
            return false;
        }
        let delay = CODEC_RETRY_BASE
            .saturating_mul(1u32 << (*attempts).min(16))
            .min(CODEC_RETRY_MAX);
        *attempts += 1;
        let job = BuildJob {
            key: key.clone(),
            kind: BuildJobKind::Rebuild { base_id },
        };
        if !self.delayed.iter().any(|d| d.job == job) {
            self.delayed.push(DelayedJob {
                job,
                due: now + delay,
            });
        }
        true
    }

    /// Forget the budget refusals of the segment at `base_id` of `key`: its
    /// codec installed.
    pub(in crate::data::executor) fn codec_built(&mut self, key: &VectorIndexKey, base_id: u32) {
        self.codec_attempts.remove(&(key.clone(), base_id));
    }

    /// Move every delayed job due by `now` into the backlog.
    pub(in crate::data::executor) fn release_due(&mut self, now: Instant) {
        let (due, waiting): (Vec<DelayedJob>, Vec<DelayedJob>) =
            self.delayed.drain(..).partition(|d| d.due <= now);
        self.delayed = waiting;
        for d in due {
            self.push(d.job);
        }
    }

    /// Record a job sent for `key`.
    pub(in crate::data::executor) fn note_sent(&mut self, key: VectorIndexKey) {
        *self.in_flight.entry(key).or_insert(0) += 1;
    }

    /// Record a finished job for `key`.
    pub(in crate::data::executor) fn note_finished(&mut self, key: &VectorIndexKey) {
        if let Some(n) = self.in_flight.get_mut(key) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.in_flight.remove(key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId};

    fn queue() -> VectorBuildQueue {
        VectorBuildQueue {
            tx: None,
            rx: None,
            backlog: VecDeque::new(),
            in_flight: HashMap::new(),
            delayed: Vec::new(),
            codec_attempts: HashMap::new(),
            reported_pending: 0,
        }
    }

    #[test]
    fn a_codec_retry_backs_off_and_stops_after_the_last_attempt() {
        let mut q = queue();
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(1), "docs:emb".into());
        let start = Instant::now();
        assert!(q.delay_codec_retry(&key, 0, start));
        assert_eq!(q.delayed[0].due, start + CODEC_RETRY_BASE);
        assert_eq!(q.pending_for(&key), 1, "a delayed retry is pending");

        q.release_due(start);
        assert!(q.backlog.is_empty(), "the retry waits out its backoff");
        q.release_due(start + CODEC_RETRY_BASE);
        assert_eq!(
            q.backlog.front().map(|j| j.kind),
            Some(BuildJobKind::Rebuild { base_id: 0 })
        );
        assert!(q.delayed.is_empty());

        q.backlog.clear();
        for _ in 1..MAX_CODEC_RETRIES {
            assert!(q.delay_codec_retry(&key, 0, start));
        }
        assert!(
            q.delayed[0].due <= start + CODEC_RETRY_MAX,
            "the backoff is bounded"
        );
        assert!(
            !q.delay_codec_retry(&key, 0, start),
            "the last attempt is spent"
        );
        q.codec_built(&key, 0);
        assert!(q.delay_codec_retry(&key, 0, start), "a built codec resets");
    }
}
