// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for HNSW build capture sites.
//!
//! A build that never installs leaves its segment on the path it had before:
//! brute force for a newly sealed segment, the old graph for a rebuild.
//! Search stays correct, but it runs slower than the index promises, and
//! nothing but a counter shows it.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A build of one segment of one index that did not install.
pub(in crate::diag) struct VectorBuildNotInstalled<'a> {
    /// Stage that failed: `build` (the builder refused the vectors) or
    /// `rebuild_read` (the core could not read a sealed segment to rebuild).
    pub stage: &'static str,
    /// `seal` for a first build, `rebuild` for a REINDEX or ALTER rebuild.
    pub kind: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    /// Index key: `collection` or `collection:field`.
    pub index: &'a str,
    /// Segment id for a first build, base id for a rebuild.
    pub segment: u32,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for VectorBuildNotInstalled<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.vector_build_not_installed"
    }

    fn grouping_key(&self) -> String {
        // Stage, kind and error class name the bug. The index and segment
        // are the occurrence, so a boot re-queue of the same bad segment
        // files one report.
        format!(
            "stage={};kind={};cause={}",
            self.stage, self.kind, self.error_class
        )
    }

    fn to_json(&self) -> Value {
        json!({
            "stage": self.stage,
            "kind": self.kind,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "index": self.index,
            "segment": self.segment,
            "error_class": self.error_class,
            "why_reported": "the segment keeps answering search without the graph this \
                             build was meant to install: by brute force for a first build, \
                             by its old graph for a rebuild. Results stay correct. Latency \
                             grows with the segment, and a rebuild's new params or \
                             quantization never take effect",
            "operator_action": "run SHOW VECTOR INDEX on the index: 'building_segments' \
                                 stays above zero and 'builds_failed' rises. A restart \
                                 re-queues every unbuilt segment. If the same stage fails \
                                 again, the segment's vectors or the index params are bad: \
                                 check the dimension and metric against the data",
        })
    }
}

/// A segment whose graph installed but whose codec build failed.
pub(in crate::diag) struct VectorCodecNotBuilt<'a> {
    /// `seal` for a first build, `rebuild` for a rebuild.
    pub kind: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    /// Index key: `collection` or `collection:field`.
    pub index: &'a str,
    /// Base id of the sealed segment.
    pub segment: u32,
    /// Whether the core queues a retry: the memory budget refused the build.
    pub retried: bool,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for VectorCodecNotBuilt<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.vector_codec_not_built"
    }

    fn grouping_key(&self) -> String {
        // Kind, retry and error class name the bug. The index and segment
        // are the occurrence, so a retry loop files one report.
        format!(
            "kind={};retried={};cause={}",
            self.kind, self.retried, self.error_class
        )
    }

    fn to_json(&self) -> Value {
        json!({
            "kind": self.kind,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "index": self.index,
            "segment": self.segment,
            "retried": self.retried,
            "error_class": self.error_class,
            "why_reported": "the segment's HNSW graph installed, but the quantization codec \
                             the index names did not build. The segment answers from its \
                             stored vectors: results stay exact, and the codec's memory \
                             saving and speed are missing",
            "operator_action": "a budget refusal retries with backoff: raise the vector \
                                 memory budget if it keeps failing. Any other error repeats \
                                 on every attempt: check the index's quantization, pq_m and \
                                 dimension against the codec's limits",
        })
    }
}

/// The core's builder thread could not be started, or died.
pub(in crate::diag) struct VectorBuilderUnavailable<'a> {
    /// `spawn_failed` or `disconnected`.
    pub cause: &'static str,
    pub core_id: usize,
    /// What failed, without the per-occurrence detail. Empty for a
    /// disconnect, which carries no error.
    pub error_class: &'a str,
}

impl DomainContext for VectorBuilderUnavailable<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.vector_builder_unavailable"
    }

    fn grouping_key(&self) -> String {
        // The core id is the occurrence: every core fails the same way.
        format!("cause={};class={}", self.cause, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "cause": self.cause,
            "core_id": self.core_id,
            "error_class": self.error_class,
            "why_reported": "no HNSW graph gets built on this core until a builder thread \
                             runs. Sealed segments answer search by brute force meanwhile, \
                             and the core retries the spawn on every tick that has builds \
                             waiting",
            "operator_action": "'spawn_failed' means the OS refused a thread: check the \
                                 process thread and memory limits. 'disconnected' means \
                                 the builder thread panicked: the panic report filed next \
                                 to this one names the cause",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> VectorBuildNotInstalled<'static> {
        VectorBuildNotInstalled {
            stage: "build",
            kind: "seal",
            database_id: 1,
            tenant_id: 2,
            index: "docs:emb",
            segment: 3,
            error_class: "dimension mismatch",
        }
    }

    #[test]
    fn grouping_ignores_the_index_and_segment() {
        let first = sample();
        let second = VectorBuildNotInstalled {
            database_id: 9,
            tenant_id: 8,
            index: "other:emb",
            segment: 70,
            ..sample()
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
    }

    #[test]
    fn grouping_separates_first_builds_from_rebuilds() {
        let rebuild = VectorBuildNotInstalled {
            kind: "rebuild",
            ..sample()
        };
        assert_ne!(sample().grouping_key(), rebuild.grouping_key());
    }

    #[test]
    fn grouping_ignores_the_core() {
        let a = VectorBuilderUnavailable {
            cause: "disconnected",
            core_id: 0,
            error_class: "",
        };
        let b = VectorBuilderUnavailable { core_id: 7, ..a };
        assert_eq!(a.grouping_key(), b.grouping_key());
    }
}
