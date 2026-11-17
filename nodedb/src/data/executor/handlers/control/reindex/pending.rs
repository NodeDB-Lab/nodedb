// SPDX-License-Identifier: BUSL-1.1

//! In-flight rebuilds a core tracks, and the per-tick poll that cuts each
//! finished one over.
//!
//! A full-text rebuild the core cancelled ends silently: its result is
//! discarded with no refusal note, error log, or capture, and its scratch
//! file is removed. A full-text pass whose journal passed its bound runs a
//! catch-up round on a new thread under the same token.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, TryRecvError};

use nodedb_graph::csr::rebuild::CsrRebuilt;
use nodedb_types::DatabaseId;
use tracing::{error, info};

use super::fts::{FtsCutover, FtsPassRx, FtsPassShared};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::text_rebuild::build::{
    BuildPass, TextBuildEnd, remove_scratch,
};
use crate::data::executor::handlers::text_rebuild::ensure::TextRebuildOutcome;
use crate::engine::sparse::inverted::FtsCatchUp;
use crate::types::TenantId;

/// The collection one REINDEX covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildTarget {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub collection: String,
}

/// A build running on its own OS thread. `token` ties it to the write
/// journal on the live index.
pub(in crate::data::executor) enum PendingBuild {
    Fts {
        token: u64,
        rx: FtsPassRx,
        /// The declared text fields the rebuild indexes.
        text_fields: Vec<String>,
        /// Items the rebuild's walk visited so far.
        progress: Arc<AtomicU64>,
        /// Set when the core abandons the rebuild.
        cancel: Arc<AtomicBool>,
    },
    Csr {
        token: u64,
        rx: mpsc::Receiver<Result<CsrRebuilt, nodedb_graph::GraphError>>,
    },
}

/// One in-flight rebuild of one index of one collection.
pub struct PendingReindex {
    pub target: RebuildTarget,
    pub(in crate::data::executor) build: PendingBuild,
}

/// A full-text pass that ended.
struct FtsPassDone {
    token: u64,
    text_fields: Vec<String>,
    progress: Arc<AtomicU64>,
    cancel: Arc<AtomicBool>,
    result: crate::Result<TextBuildEnd>,
}

/// What one poll of a build found.
enum BuildPoll {
    Running(PendingBuild),
    Fts(FtsPassDone),
    Csr {
        token: u64,
        result: crate::Result<CsrRebuilt>,
    },
}

impl PendingBuild {
    fn poll(self) -> BuildPoll {
        match self {
            Self::Fts {
                token,
                rx,
                text_fields,
                progress,
                cancel,
            } => {
                let result = match rx.try_recv() {
                    Ok(result) => result,
                    Err(TryRecvError::Empty) => {
                        return BuildPoll::Running(Self::Fts {
                            token,
                            rx,
                            text_fields,
                            progress,
                            cancel,
                        });
                    }
                    Err(TryRecvError::Disconnected) => Err(thread_lost("fts")),
                };
                BuildPoll::Fts(FtsPassDone {
                    token,
                    text_fields,
                    progress,
                    cancel,
                    result,
                })
            }
            Self::Csr { token, rx } => match rx.try_recv() {
                Ok(result) => BuildPoll::Csr {
                    token,
                    result: result.map_err(crate::Error::from),
                },
                Err(TryRecvError::Empty) => BuildPoll::Running(Self::Csr { token, rx }),
                Err(TryRecvError::Disconnected) => BuildPoll::Csr {
                    token,
                    result: Err(thread_lost("csr")),
                },
            },
        }
    }
}

fn thread_lost(index: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!("{index} rebuild thread exited without a result"),
    }
}

impl CoreLoop {
    /// Cut over every finished rebuild. Called from `tick()`.
    pub fn poll_pending_reindex(&mut self) {
        if self.maintenance.pending_reindex.is_empty() {
            return;
        }
        let entries = std::mem::take(&mut self.maintenance.pending_reindex);
        let mut running = Vec::with_capacity(entries.len());
        for PendingReindex { target, build } in entries {
            match build.poll() {
                BuildPoll::Running(build) => running.push(PendingReindex { target, build }),
                BuildPoll::Fts(done) => {
                    if let Some(next) = self.end_fts_pass(&target, done) {
                        running.push(PendingReindex {
                            target,
                            build: next,
                        });
                    }
                }
                BuildPoll::Csr { token, result } => {
                    let installed = result.and_then(|rebuilt| self.install_csr(&target, rebuilt));
                    if let Err(e) = installed {
                        self.abort_csr_rebuild(&target, token);
                        self.report_rebuild_refused("csr", &target, &e);
                        self.note_reindex_refused(&target, &e);
                    }
                }
            }
        }
        self.maintenance.pending_reindex.extend(running);
    }

    /// Abandon the full-text rebuild `token`: close its journal and tell
    /// its thread to stop. Its result is discarded with no refusal report.
    pub(in crate::data::executor) fn cancel_fts_rebuild(&mut self, token: u64) {
        self.inverted.abort_rebuild(token);
        for pending in &self.maintenance.pending_reindex {
            if let PendingBuild::Fts {
                token: t, cancel, ..
            } = &pending.build
                && *t == token
            {
                cancel.store(true, Ordering::Release);
            }
        }
    }

    /// Handle one ended full-text pass. Returns the next pass when a
    /// catch-up round runs.
    fn end_fts_pass(&mut self, target: &RebuildTarget, done: FtsPassDone) -> Option<PendingBuild> {
        let FtsPassDone {
            token,
            text_fields,
            progress,
            cancel,
            result,
        } = done;
        let rebuilt = match result {
            Ok(TextBuildEnd::Built(rebuilt)) if cancel.load(Ordering::Acquire) => {
                // The core cancelled the rebuild after its thread finished.
                let path = rebuilt.scratch_path().to_path_buf();
                drop(rebuilt);
                remove_scratch(&path);
                return None;
            }
            Ok(TextBuildEnd::Cancelled) => return None,
            Err(_) if cancel.load(Ordering::Acquire) => return None,
            Ok(TextBuildEnd::Built(rebuilt)) => Ok(rebuilt),
            Err(e) => Err(e),
        };
        let step = match rebuilt.and_then(|rebuilt| self.install_fts(target, &text_fields, rebuilt))
        {
            Ok(FtsCutover::Installed) => Ok(None),
            Ok(FtsCutover::CatchUp(catch_up)) => {
                let shared = FtsPassShared {
                    text_fields: &text_fields,
                    progress: &progress,
                    cancel: &cancel,
                };
                self.start_catch_up_round(target, catch_up, &shared)
                    .map(Some)
            }
            Err(e) => Err(e),
        };
        let outcome = match step {
            Ok(Some(rx)) => {
                return Some(PendingBuild::Fts {
                    token,
                    rx,
                    text_fields,
                    progress,
                    cancel,
                });
            }
            Ok(None) => TextRebuildOutcome::Installed {
                replayed: progress.load(Ordering::Relaxed),
            },
            Err(e) => {
                self.inverted.abort_rebuild(token);
                self.report_rebuild_refused("fts", target, &e);
                self.note_reindex_refused(target, &e);
                TextRebuildOutcome::Failed {
                    detail: e.to_string(),
                }
            }
        };
        self.note_text_rebuild_outcome(target, token, outcome);
        None
    }

    /// Run the catch-up round `catch_up` of a rebuild of `target` on a new
    /// thread.
    fn start_catch_up_round(
        &self,
        target: &RebuildTarget,
        catch_up: Box<FtsCatchUp>,
        shared: &FtsPassShared<'_>,
    ) -> crate::Result<FtsPassRx> {
        let round = catch_up.ticket.round();
        let touched = catch_up.touched.len();
        let key = (
            target.database_id,
            target.tenant_id,
            target.collection.clone(),
        );
        let rx = self.spawn_fts_pass(&key, BuildPass::CatchUp(*catch_up), shared)?;
        info!(
            target: "nodedb::reindex",
            core = self.core_id,
            index = "fts",
            collection = %target.collection,
            round,
            touched,
            "catch_up_round"
        );
        Ok(rx)
    }

    /// Log and record a rebuild the core discarded. The live index keeps
    /// every write; only the rebuild is lost.
    pub(super) fn report_rebuild_refused(
        &self,
        index: &'static str,
        target: &RebuildTarget,
        err: &crate::Error,
    ) {
        error!(
            target: "nodedb::reindex",
            core = self.core_id,
            index,
            collection = %target.collection,
            error = %err,
            "rebuild_refused"
        );
        crate::diag::index_rebuild_not_installed(
            err,
            &crate::diag::IndexRebuildTarget {
                index,
                database_id: target.database_id.as_u64(),
                tenant_id: target.tenant_id.as_u64(),
                collection: &target.collection,
            },
        );
    }
}
