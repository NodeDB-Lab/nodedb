// SPDX-License-Identifier: BUSL-1.1

//! Sending HNSW builds to this core's builder thread.
//!
//! Every graph build — a sealed segment's first build, a boot re-queue of a
//! segment sealed before a restart, and a REINDEX or `ALTER VECTOR INDEX`
//! rebuild — goes through [`CoreLoop::dispatch_vector_builds`]. See
//! `core_loop::vector_build_queue` for the queue and its backpressure.
//! Finished builds are installed by `poll_build_completions`.

use std::sync::mpsc::TrySendError;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::vector_build_queue::{BuildJob, BuildJobKind};
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::engine::vector::collection::BuildRequest;

impl CoreLoop {
    /// Send the build request `seal` produced for `key`. It goes straight to
    /// the builder when nothing waits ahead of it and the queue has room;
    /// otherwise it waits in the backlog as a descriptor.
    pub(in crate::data::executor) fn queue_sealed_build(
        &mut self,
        key: &VectorIndexKey,
        req: BuildRequest,
    ) {
        let segment_id = req.segment_id;
        if self.vector_builds.backlog.is_empty()
            && let Some(tx) = &self.vector_builds.tx
        {
            match tx.try_send(req) {
                Ok(()) => {
                    self.vector_builds.note_sent(key.clone());
                    if let Some(m) = &self.metrics {
                        m.record_vector_build_started();
                    }
                    self.sync_build_pending_metric();
                    return;
                }
                Err(TrySendError::Full(_)) => {
                    if let Some(m) = &self.metrics {
                        m.record_vector_build_deferred();
                    }
                }
                Err(TrySendError::Disconnected(_)) => {}
            }
        }
        self.vector_builds.push(BuildJob {
            key: key.clone(),
            kind: BuildJobKind::Seal { segment_id },
        });
        self.dispatch_vector_builds();
    }

    /// Queue a rebuild of every non-empty sealed segment of `key`, under the
    /// collection's current params. Returns the number of segments queued.
    pub(in crate::data::executor) fn queue_vector_rebuild(
        &mut self,
        key: &VectorIndexKey,
    ) -> usize {
        let Some(coll) = self.vector_collections.get(key) else {
            return 0;
        };
        let base_ids = coll.sealed_base_ids();
        for &base_id in &base_ids {
            self.vector_builds.push(BuildJob {
                key: key.clone(),
                kind: BuildJobKind::Rebuild { base_id },
            });
        }
        self.dispatch_vector_builds();
        base_ids.len()
    }

    /// Queue a codec rebuild for every sealed segment of `key` whose codec
    /// differs from its collection's plan: a compaction dropped a graph
    /// codec whose stale nodes outnumbered the rest. Returns the number of
    /// segments queued.
    pub(in crate::data::executor) fn queue_missing_codecs(
        &mut self,
        key: &VectorIndexKey,
    ) -> usize {
        let Some(coll) = self.vector_collections.get(key) else {
            return 0;
        };
        // A segment waiting out a budget backoff keeps its delayed job.
        let jobs: Vec<BuildJob> = coll
            .sealed_ids_missing_codec()
            .into_iter()
            .map(|base_id| BuildJob {
                key: key.clone(),
                kind: BuildJobKind::Rebuild { base_id },
            })
            .filter(|job| !self.vector_builds.delayed.iter().any(|d| &d.job == job))
            .collect();
        let queued = jobs.len();
        for job in jobs {
            self.vector_builds.push(job);
        }
        if queued > 0 {
            self.dispatch_vector_builds();
        }
        queued
    }

    /// Queue a build for every building segment of every collection. Boot
    /// runs it after replay, so a segment sealed before a restart gets its
    /// graph; a builder respawn runs it for the builds the dead thread held.
    pub fn queue_unbuilt_segments(&mut self) {
        for job in self.unbuilt_segment_jobs() {
            self.vector_builds.push(job);
        }
        self.dispatch_vector_builds();
    }

    /// A seal job for every building segment of every collection, and a
    /// rebuild job for every sealed segment whose codec differs from its
    /// collection's plan. A checkpoint stores no graph codec, so a restored
    /// segment rebuilds its graph codec here, off the core.
    fn unbuilt_segment_jobs(&self) -> Vec<BuildJob> {
        self.vector_collections
            .iter()
            .flat_map(|(key, coll)| {
                let seals = coll
                    .building_segment_ids()
                    .into_iter()
                    .map(|segment_id| BuildJob {
                        key: key.clone(),
                        kind: BuildJobKind::Seal { segment_id },
                    });
                let rebuilds =
                    coll.sealed_ids_missing_codec()
                        .into_iter()
                        .map(|base_id| BuildJob {
                            key: key.clone(),
                            kind: BuildJobKind::Rebuild { base_id },
                        });
                seals.chain(rebuilds).collect::<Vec<_>>()
            })
            .collect()
    }

    /// The memory scope a vector build of `key` charges its codec training to.
    pub(in crate::data::executor) fn vector_build_memory(
        &self,
        key: &VectorIndexKey,
    ) -> nodedb_mem::ScopedMemory {
        nodedb_mem::ScopedMemory::new(
            self.governor.clone(),
            key.0,
            key.1,
            nodedb_mem::EngineId::Vector,
        )
    }

    /// Send backlog jobs, oldest first, until the builder queue is full.
    ///
    /// Each job's request is read from its collection now. A job whose
    /// segment is gone (truncated, dropped, already rebuilt) is dropped. A
    /// rebuild whose vectors cannot be read counts as a failed build. A
    /// dead builder thread is replaced and every unbuilt segment re-queued.
    pub(in crate::data::executor) fn dispatch_vector_builds(&mut self) {
        let mut respawned = false;
        while let Some(job) = self.vector_builds.backlog.front().cloned() {
            if self.vector_builds.tx.is_none() {
                if respawned {
                    break;
                }
                self.vector_builds.respawn(self.core_id);
                respawned = true;
                continue;
            }
            let Some(req) = self.read_build_request(&job) else {
                self.vector_builds.backlog.pop_front();
                continue;
            };
            let Some(tx) = &self.vector_builds.tx else {
                break;
            };
            match tx.try_send(req) {
                Ok(()) => {
                    self.vector_builds.backlog.pop_front();
                    self.vector_builds.note_sent(job.key);
                    if let Some(m) = &self.metrics {
                        m.record_vector_build_started();
                    }
                }
                Err(TrySendError::Full(_)) => {
                    // The segment stays on brute force; the next tick retries.
                    if let Some(m) = &self.metrics {
                        m.record_vector_build_deferred();
                    }
                    break;
                }
                Err(TrySendError::Disconnected(_)) => {
                    tracing::error!(
                        core = self.core_id,
                        "HNSW builder thread gone; spawning a new one"
                    );
                    crate::diag::vector_builder_disconnected(self.core_id);
                    if respawned {
                        break;
                    }
                    self.vector_builds.respawn(self.core_id);
                    respawned = true;
                    for job in self.unbuilt_segment_jobs() {
                        self.vector_builds.push(job);
                    }
                }
            }
        }
        self.sync_build_pending_metric();
    }

    /// Read the build request `job` names, or `None` when its segment is
    /// gone or a rebuild's vectors cannot be read.
    fn read_build_request(&mut self, job: &BuildJob) -> Option<BuildRequest> {
        let build_key = CoreLoop::vector_build_key(&job.key);
        let memory = self.vector_build_memory(&job.key);
        let coll = self.vector_collections.get_mut(&job.key)?;
        match job.kind {
            BuildJobKind::Seal { segment_id } => {
                coll.build_request_for(&build_key, segment_id, memory)
            }
            BuildJobKind::Rebuild { base_id } => {
                match coll.rebuild_request_for(&build_key, base_id, memory) {
                    Ok(req) => req,
                    Err(e) => {
                        coll.note_build_failed();
                        crate::diag::vector_rebuild_unreadable(
                            &e,
                            &crate::diag::VectorBuildTarget {
                                kind: "rebuild",
                                database_id: job.key.0.as_u64(),
                                tenant_id: job.key.1.as_u64(),
                                index: &job.key.2,
                                segment: base_id,
                            },
                        );
                        tracing::error!(
                            core = self.core_id,
                            key = %job.key.2,
                            base_id,
                            error = %e,
                            "HNSW rebuild cannot read its segment; the segment stays as it is"
                        );
                        if let Some(m) = &self.metrics {
                            m.record_vector_build_failed();
                        }
                        None
                    }
                }
            }
        }
    }

    /// Bring the cross-core pending-build gauge up to this core's count.
    pub(in crate::data::executor) fn sync_build_pending_metric(&mut self) {
        let now = self.vector_builds.pending_total();
        if let Some(m) = &self.metrics {
            m.move_vector_build_pending(self.vector_builds.reported_pending, now);
            self.vector_builds.reported_pending = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::distance::DistanceMetric;
    use crate::engine::vector::hnsw::HnswParams;
    use crate::types::{DatabaseId, TenantId};

    const SEGMENTS: usize = 6;
    const PER_SEGMENT: usize = 8;

    /// A collection holding `SEGMENTS` sealed-but-unbuilt segments, as a
    /// restart leaves one whose builds had not finished.
    fn unbuilt_collection(memory: &nodedb_mem::ScopedMemory) -> VectorCollection {
        let params = HnswParams {
            metric: DistanceMetric::L2,
            ..HnswParams::default()
        };
        let mut coll = VectorCollection::with_seal_threshold(2, params, PER_SEGMENT);
        for s in 0..SEGMENTS {
            for i in 0..PER_SEGMENT {
                let n = (s * PER_SEGMENT + i) as f32;
                coll.insert(vec![n, (i % 3) as f32]).unwrap();
            }
            // The request is dropped: the build never reached a builder.
            coll.seal("lost", memory.clone()).unwrap();
        }
        coll
    }

    #[test]
    fn unbuilt_segments_queue_past_the_bounded_queue_and_all_install() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(1), "docs:emb".into());
        let memory = core.vector_build_memory(&key);
        core.vector_collections
            .insert(key.clone(), unbuilt_collection(&memory));

        core.queue_unbuilt_segments();
        assert_eq!(core.vector_builds.pending_for(&key), SEGMENTS);

        let deadline = Instant::now() + Duration::from_secs(20);
        while core.vector_builds.pending_total() > 0 {
            assert!(Instant::now() < deadline, "builds did not finish");
            std::thread::sleep(Duration::from_millis(10));
            core.poll_build_completions();
        }

        let coll = core.vector_collections.get(&key).expect("collection");
        let stats = coll.stats();
        assert_eq!(stats.sealed_count, SEGMENTS);
        assert_eq!(stats.building_count, 0);
        assert_eq!(stats.builds_completed, SEGMENTS as u64);
        // Every node kept its id through the build.
        for id in [0u32, 7, 8, 47] {
            let hit = &coll
                .search(&[id as f32, (id as usize % PER_SEGMENT % 3) as f32], 1, 64)
                .unwrap()[0];
            assert_eq!(hit.id, id);
        }
    }

    #[test]
    fn a_rebuild_goes_through_the_builder_and_keeps_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(1), "docs:emb".into());
        let memory = core.vector_build_memory(&key);
        core.vector_collections
            .insert(key.clone(), unbuilt_collection(&memory));
        core.queue_unbuilt_segments();
        let deadline = Instant::now() + Duration::from_secs(20);
        while core.vector_builds.pending_total() > 0 {
            assert!(Instant::now() < deadline, "builds did not finish");
            std::thread::sleep(Duration::from_millis(10));
            core.poll_build_completions();
        }
        if let Some(coll) = core.vector_collections.get_mut(&key) {
            coll.delete(9);
        }

        assert_eq!(core.queue_vector_rebuild(&key), SEGMENTS);
        while core.vector_builds.pending_total() > 0 {
            assert!(Instant::now() < deadline, "rebuilds did not finish");
            std::thread::sleep(Duration::from_millis(10));
            core.poll_build_completions();
        }

        let coll = core.vector_collections.get(&key).expect("collection");
        assert_eq!(coll.stats().builds_completed, 2 * SEGMENTS as u64);
        assert!(!coll.is_live(9), "a tombstone carries over");
        assert_eq!(coll.len(), SEGMENTS * PER_SEGMENT, "no node added or lost");
        let hit = &coll.search(&[40.0, 0.0], 1, 64).unwrap()[0];
        assert_eq!(hit.id, 40);
    }

    /// Install every queued build, failing the test past `deadline`.
    fn drain_builds(core: &mut CoreLoop, deadline: Instant) {
        while core.vector_builds.pending_total() > 0 {
            assert!(Instant::now() < deadline, "builds did not finish");
            std::thread::sleep(Duration::from_millis(10));
            core.poll_build_completions();
        }
    }

    #[test]
    fn a_restored_graph_codec_rebuilds_through_the_builder() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(1), "docs:emb".into());
        let memory = core.vector_build_memory(&key);
        let mut coll = unbuilt_collection(&memory);
        coll.set_quantization(nodedb_types::VectorQuantization::Binary);
        core.vector_collections.insert(key.clone(), coll);
        let deadline = Instant::now() + Duration::from_secs(20);
        core.queue_unbuilt_segments();
        drain_builds(&mut core, deadline);

        // A checkpoint stores no graph codec: every restored segment lacks it.
        let bytes = core
            .vector_collections
            .get(&key)
            .expect("collection")
            .checkpoint_to_bytes(None)
            .expect("checkpoint");
        let restored = VectorCollection::from_checkpoint(&bytes, None, memory).expect("restore");
        assert_eq!(restored.sealed_ids_missing_codec().len(), SEGMENTS);
        core.vector_collections.insert(key.clone(), restored);

        core.queue_unbuilt_segments();
        drain_builds(&mut core, deadline);

        let coll = core.vector_collections.get(&key).expect("collection");
        assert!(coll.sealed_ids_missing_codec().is_empty());
        assert_eq!(
            coll.stats().quantization,
            nodedb_types::VectorIndexQuantization::Binary
        );
        assert_eq!(coll.len(), SEGMENTS * PER_SEGMENT, "no node added or lost");
    }
}
