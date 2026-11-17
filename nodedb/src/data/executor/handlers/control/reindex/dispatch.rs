// SPDX-License-Identifier: BUSL-1.1

//! The `MetaOp::RebuildIndex` handler.
//!
//! `index_name` picks the index kinds to rebuild: `hnsw`, `fts` or `csr`,
//! matched without case. `None` rebuilds every kind the collection has.
//!
//! Both REINDEX forms rebuild the same way, and never on the core: HNSW
//! segments on the core's HNSW builder thread, FTS and CSR on their own
//! threads as `fts` and `csr` describe. The core keeps serving reads and
//! writes, journals the writes, and swaps each rebuilt index in on a later
//! tick. The forms differ only in when the core answers:
//!
//! - Concurrent: once every rebuild has started. This handler answers.
//! - Plain: once every rebuild has cut over. `waiter` holds the request
//!   and answers it from the tick.

use tracing::info;

use super::pending::RebuildTarget;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// The index kinds one REINDEX covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IndexSelection {
    pub(super) hnsw: bool,
    pub(super) fts: bool,
    pub(super) csr: bool,
}

impl IndexSelection {
    pub(super) fn from_name(index_name: Option<&str>) -> Self {
        match index_name {
            None => Self {
                hnsw: true,
                fts: true,
                csr: true,
            },
            Some(name) => Self {
                hnsw: name.eq_ignore_ascii_case("hnsw"),
                fts: name.eq_ignore_ascii_case("fts"),
                csr: name.eq_ignore_ascii_case("csr"),
            },
        }
    }
}

impl CoreLoop {
    /// Handle a `MetaOp::RebuildIndex` dispatch: start every selected
    /// rebuild and answer. A plain REINDEX reaches the core loop's waiter
    /// first, which holds its answer until the cutovers; this handler
    /// answers a concurrent one.
    pub(in crate::data::executor) fn execute_rebuild_index(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        index_name: Option<&str>,
    ) -> Response {
        let target = RebuildTarget {
            database_id: task.request.database_id,
            tenant_id: TenantId::new(tid),
            collection: collection.to_string(),
        };
        match self.start_rebuilds(&target, IndexSelection::from_name(index_name)) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(task, e),
        }
    }

    /// Start every selected rebuild. Each kind starts even when another
    /// fails to; the first error is returned.
    pub(super) fn start_rebuilds(
        &mut self,
        target: &RebuildTarget,
        selection: IndexSelection,
    ) -> crate::Result<()> {
        if self
            .maintenance
            .pending_reindex
            .iter()
            .any(|p| p.target == *target)
        {
            return Err(crate::Error::ObjectNotInPrerequisiteState {
                object: format!("collection \"{}\"", target.collection),
                detail: "a rebuild of its indexes is already running".to_string(),
            });
        }
        if selection.hnsw {
            self.start_hnsw_rebuild(target);
        }
        let fts = if selection.fts {
            let text_fields = self.current_text_fields(target);
            self.start_fts_rebuild(target, text_fields).map(|_| ())
        } else {
            Ok(())
        };
        let csr = if selection.csr {
            self.start_csr_rebuild(target)
        } else {
            Ok(())
        };
        fts.and(csr)
    }

    /// Queue a rebuild of every sealed HNSW segment of the collection's
    /// vector indexes on this core's builder thread. Each segment is rebuilt
    /// from its own vectors with its node ids kept, quantized again under the
    /// collection's config, and swapped in on this core; search reads the old
    /// graph until then. The growing and building segments are left alone.
    fn start_hnsw_rebuild(&mut self, target: &RebuildTarget) {
        for key in self.vector_keys_of(target) {
            let queued = self.queue_vector_rebuild(&key);
            info!(
                core = self.core_id,
                collection = %key.2,
                queued,
                "HNSW rebuild queued"
            );
        }
    }

    /// Keys of the collection's HNSW indexes on this core. A collection
    /// keys its indexes as `coll` (batch and native inserts) or as
    /// `coll:field` (SQL inserts). An IVF-PQ index keeps no HNSW segments,
    /// so it is left out.
    pub(super) fn vector_keys_of(
        &self,
        target: &RebuildTarget,
    ) -> Vec<(nodedb_types::DatabaseId, TenantId, String)> {
        let field_prefix = format!("{}:", target.collection);
        self.vector_collections
            .iter()
            .filter(|((d, t, k), coll)| {
                *d == target.database_id
                    && *t == target.tenant_id
                    && (k.as_str() == target.collection || k.starts_with(&field_prefix))
                    && !coll.is_ivf()
            })
            .map(|(key, _)| key.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_name_selects_every_kind() {
        assert_eq!(
            IndexSelection::from_name(None),
            IndexSelection {
                hnsw: true,
                fts: true,
                csr: true,
            }
        );
    }

    #[test]
    fn a_name_selects_its_kind_only() {
        assert_eq!(
            IndexSelection::from_name(Some("FTS")),
            IndexSelection {
                hnsw: false,
                fts: true,
                csr: false,
            }
        );
        assert_eq!(
            IndexSelection::from_name(Some("tag_idx")),
            IndexSelection {
                hnsw: false,
                fts: false,
                csr: false,
            }
        );
    }
}
