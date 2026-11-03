// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for HNSW builds that never installed, and for a core that
//! lost its builder thread.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Where a failed build belongs: the index and the segment it covered.
pub struct VectorBuildTarget<'a> {
    /// `seal` or `rebuild`.
    pub kind: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    pub index: &'a str,
    pub segment: u32,
}

/// Report a build the builder thread refused. Called from the completion
/// arm that receives the builder's error.
pub fn vector_build_failed(err: &nodedb_vector::VectorError, target: &VectorBuildTarget<'_>) {
    record_not_installed("build", err, target);
}

/// Report a rebuild whose sealed segment the core could not read. Called
/// from the dispatch arm that reads the segment's vectors.
pub fn vector_rebuild_unreadable(err: &nodedb_vector::VectorError, target: &VectorBuildTarget<'_>) {
    record_not_installed("rebuild_read", err, target);
}

/// Report a segment whose graph installed without its codec. `retried` is
/// whether the core queues a retry. Called from the completion arm that
/// installs the graph.
pub fn vector_codec_not_built(
    err: &nodedb_vector::VectorError,
    target: &VectorBuildTarget<'_>,
    retried: bool,
) {
    let class = error_class(err);
    let ctx = context::VectorCodecNotBuilt {
        kind: target.kind,
        database_id: target.database_id,
        tenant_id: target.tenant_id,
        index: target.index,
        segment: target.segment,
        retried,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "HNSW graph installed without its codec; the segment answers exactly",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Shared emit for the two not-installed causes. Private so the only entry
/// points are the one-per-cause functions above.
fn record_not_installed(
    stage: &'static str,
    err: &nodedb_vector::VectorError,
    target: &VectorBuildTarget<'_>,
) {
    let class = error_class(err);
    let ctx = context::VectorBuildNotInstalled {
        stage,
        kind: target.kind,
        database_id: target.database_id,
        tenant_id: target.tenant_id,
        index: target.index,
        segment: target.segment,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "HNSW build did not install; the segment keeps its previous search path",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a core whose builder thread could not be spawned. Called from the
/// spawn arm of the core's build queue.
pub fn vector_builder_spawn_failed(err: &std::io::Error, core_id: usize) {
    let class = error_class(err);
    let ctx = context::VectorBuilderUnavailable {
        cause: "spawn_failed",
        core_id,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "HNSW builder thread could not be spawned; no graph builds on this core",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a core whose builder thread died. Called from the dispatch arm
/// that finds the request queue disconnected.
pub fn vector_builder_disconnected(core_id: usize) {
    let ctx = context::VectorBuilderUnavailable {
        cause: "disconnected",
        core_id,
        error_class: "",
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "HNSW builder thread died with builds in flight",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
