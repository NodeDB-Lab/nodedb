// SPDX-License-Identifier: BUSL-1.1

//! Installing finished HNSW builds into the live collections.
//!
//! Lives beside the checkpoint because it shares the checkpoint's key
//! encoding: a `BuildComplete.key` is the same `"{db}:{tid}:{coll}"` string a
//! checkpoint filename carries, so both parse it through
//! [`parse_build_key`](super::paths::parse_build_key).

use super::paths::parse_build_key;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::index_charge::IndexChargeKey;
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::engine::vector::collection::{BuildComplete, BuildKind, InstallOutcome};
use nodedb_vector::error::VectorError;

impl CoreLoop {
    /// Drain finished builds from this core's builder thread, install each
    /// one on its collection, then send the next backlog jobs.
    ///
    /// Called at the top of `tick()`. An install happens on this core, in one
    /// call, so a search sees either the brute-force segment or the built
    /// graph, never a mix.
    pub fn poll_build_completions(&mut self) {
        // Codec retries whose backoff ran out join the backlog.
        self.vector_builds.release_due(std::time::Instant::now());
        // A dropped index takes its codec refusal counts with it.
        let collections = &self.vector_collections;
        self.vector_builds
            .codec_attempts
            .retain(|(key, _), _| collections.contains_key(key));
        let mut drained = Vec::new();
        if let Some(rx) = &self.vector_builds.rx {
            while let Ok(complete) = rx.try_recv() {
                drained.push(complete);
            }
        }
        for complete in drained {
            self.install_build(complete);
        }
        if !self.vector_builds.backlog.is_empty() {
            self.dispatch_vector_builds();
        } else {
            self.sync_build_pending_metric();
        }
    }

    /// Install one finished build. A failed graph build, or one whose segment
    /// was truncated, dropped or renumbered meanwhile, leaves the collection
    /// as it is; the segment keeps answering by brute force or by its old
    /// graph. A failed codec build installs the graph without the codec.
    fn install_build(&mut self, complete: BuildComplete) {
        let BuildComplete {
            key,
            segment_id,
            kind,
            result,
        } = complete;
        let Some(tuple_key) = parse_build_key(&key) else {
            tracing::error!(
                core = self.core_id,
                key = %key,
                "HNSW build completion has unparseable key; dropping"
            );
            return;
        };
        self.vector_builds.note_finished(&tuple_key);
        let Some(coll) = self.vector_collections.get_mut(&tuple_key) else {
            return;
        };
        let built = match result {
            Ok(built) => built,
            Err(e) => {
                coll.note_build_failed();
                let (kind_label, segment) = match kind {
                    BuildKind::Seal => ("seal", segment_id),
                    BuildKind::Rebuild { base_id, .. } => ("rebuild", base_id),
                };
                crate::diag::vector_build_failed(
                    &e,
                    &crate::diag::VectorBuildTarget {
                        kind: kind_label,
                        database_id: tuple_key.0.as_u64(),
                        tenant_id: tuple_key.1.as_u64(),
                        index: &tuple_key.2,
                        segment,
                    },
                );
                tracing::error!(
                    core = self.core_id,
                    key = %key,
                    segment_id,
                    error = %e,
                    "HNSW build failed; the segment stays as it is"
                );
                if let Some(m) = &self.metrics {
                    m.record_vector_build_failed();
                }
                return;
            }
        };
        let outcome = match kind {
            BuildKind::Seal => coll.complete_build(segment_id, built),
            BuildKind::Rebuild { base_id, len } => {
                coll.complete_rebuild(segment_id, base_id, len, built)
            }
        };
        let InstallOutcome::Installed {
            base_id,
            codec_error,
        } = outcome
        else {
            tracing::debug!(
                core = self.core_id,
                key = %key,
                segment_id,
                ?kind,
                "HNSW build no longer matches its segment; discarded"
            );
            return;
        };
        tracing::info!(
            core = self.core_id,
            key = %key,
            segment_id,
            ?kind,
            "HNSW build installed"
        );
        // A rebuilt segment replaces the old one in this single call on the
        // owning core, so search reads the old graph or the new one, never
        // a mix. REINDEX observers count this event, one per segment.
        if let BuildKind::Rebuild { base_id, len } = kind {
            tracing::info!(
                target: "nodedb::reindex",
                core = self.core_id,
                index = "hnsw",
                key = %key,
                base_id,
                len,
                "atomic_cutover"
            );
        }
        self.checkpoint_coordinator.mark_dirty("vector", 1);
        if let Some(m) = &self.metrics {
            m.record_vector_build_completed();
        }
        // The build's codec reservation dropped at install: the segment's
        // resident bytes carry its charge from here.
        self.recharge_index(&IndexChargeKey::Vector(tuple_key.clone()));
        match codec_error {
            None => self.vector_builds.codec_built(&tuple_key, base_id),
            Some(e) => self.codec_build_failed(&tuple_key, kind, base_id, &e),
        }
    }

    /// Report a segment whose graph installed without its codec. A budget
    /// refusal queues a codec rebuild after a backoff, while retries remain.
    /// Any other error repeats on every attempt, so it is reported once and
    /// the segment is not queued again.
    fn codec_build_failed(
        &mut self,
        key: &VectorIndexKey,
        kind: BuildKind,
        base_id: u32,
        err: &VectorError,
    ) {
        let retried = err.is_transient()
            && self
                .vector_builds
                .delay_codec_retry(key, base_id, std::time::Instant::now());
        if let Some(coll) = self.vector_collections.get_mut(key) {
            coll.note_build_failed();
            if !retried {
                coll.note_codec_refused(base_id);
            }
        }
        let kind_label = match kind {
            BuildKind::Seal => "seal",
            BuildKind::Rebuild { .. } => "rebuild",
        };
        crate::diag::vector_codec_not_built(
            err,
            &crate::diag::VectorBuildTarget {
                kind: kind_label,
                database_id: key.0.as_u64(),
                tenant_id: key.1.as_u64(),
                index: &key.2,
                segment: base_id,
            },
            retried,
        );
        tracing::error!(
            core = self.core_id,
            key = %key.2,
            base_id,
            retried,
            error = %err,
            "vector codec build failed; the segment answers exactly without it"
        );
        if let Some(m) = &self.metrics {
            m.record_vector_build_failed();
        }
        self.sync_build_pending_metric();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::distance::DistanceMetric;
    use crate::engine::vector::hnsw::HnswParams;
    use crate::types::{DatabaseId, TenantId};

    /// A memory scope whose vector budget holds `bytes`.
    fn tight_memory(bytes: usize) -> nodedb_mem::ScopedMemory {
        use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor};
        let governor = std::sync::Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: bytes * EngineId::ALL.len(),
                engine_limits: EngineLimits::uniform(bytes),
            })
            .expect("tight governor"),
        );
        nodedb_mem::ScopedMemory::new(
            governor,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            EngineId::Vector,
        )
    }

    /// A core holding one collection with one unbuilt segment of eight
    /// vectors, and the finished build of that segment under `memory`.
    fn core_with_built_segment(
        dir: &std::path::Path,
        coll: VectorCollection,
        memory: nodedb_mem::ScopedMemory,
    ) -> (CoreLoop, VectorIndexKey, BuildComplete) {
        let (mut core, _tx, _rx) = make_core_with_dir(dir);
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(1), "docs:emb".into());
        let mut coll = coll;
        for i in 0..8u32 {
            coll.insert(vec![i as f32, 1.0, 2.0, 3.0]).unwrap();
        }
        let build_key = CoreLoop::vector_build_key(&key);
        let req = coll.seal(&build_key, memory).expect("seal");
        let segment_id = req.segment_id;
        core.vector_collections.insert(key.clone(), coll);
        let built =
            crate::engine::vector::builder::build_segment(7, req).expect("the graph builds");
        let complete = BuildComplete {
            key: build_key,
            segment_id,
            kind: BuildKind::Seal,
            result: Ok(built),
        };
        (core, key, complete)
    }

    fn l2() -> HnswParams {
        HnswParams {
            metric: DistanceMetric::L2,
            ..HnswParams::default()
        }
    }

    #[test]
    fn a_codec_over_budget_installs_the_graph_and_queues_a_retry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = VectorCollection::with_seal_threshold(4, l2(), 8);
        let (mut core, key, complete) = core_with_built_segment(dir.path(), coll, tight_memory(64));

        core.install_build(complete);

        let coll = core.vector_collections.get(&key).expect("collection");
        let stats = coll.stats();
        assert_eq!(stats.building_count, 0, "the graph is installed");
        assert_eq!(stats.builds_failed, 1, "the codec error is reported");
        assert_eq!(coll.search(&[5.0, 1.0, 2.0, 3.0], 1, 64).unwrap()[0].id, 5);
        assert_eq!(
            core.vector_builds
                .delayed
                .iter()
                .map(|d| d.job.kind)
                .collect::<Vec<_>>(),
            vec![
                crate::data::executor::core_loop::vector_build_queue::BuildJobKind::Rebuild {
                    base_id: 0
                }
            ],
            "a budget refusal queues a codec rebuild"
        );
    }

    #[test]
    fn a_codec_shape_error_installs_the_graph_and_is_not_retried() {
        let dir = tempfile::tempdir().expect("tempdir");
        // pq_m 3 does not divide dim 4.
        let coll = VectorCollection::with_seal_threshold_and_pq_config(4, l2(), 3, 8);
        let memory = tight_memory(1 << 40);
        let (mut core, key, complete) = core_with_built_segment(dir.path(), coll, memory);

        core.install_build(complete);

        let coll = core.vector_collections.get(&key).expect("collection");
        assert_eq!(coll.stats().building_count, 0, "the graph is installed");
        assert_eq!(coll.stats().builds_failed, 1, "the codec error is reported");
        assert!(core.vector_builds.delayed.is_empty(), "no retry");
        assert!(
            coll.sealed_ids_missing_codec().is_empty(),
            "a refused codec is not queued again"
        );
    }
}
