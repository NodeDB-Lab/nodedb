// SPDX-License-Identifier: BUSL-1.1

//! The committed-redo step every vector replay arm runs before its write.
//!
//! In the validate pass the arm refuses a write whose vectors do not fit the
//! existing index, claims the sub-record, and writes nothing. In the install
//! pass it records the write's undo (`capture_vector_write_undo`) and the
//! sync high-water mark its provenance advances, then writes. Restart replay
//! writes with neither.

use nodedb_physical::physical_plan::VectorWriteTargets;
use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;

use super::core_loop::CoreLoop;
use super::handlers::transaction::undo::vector_write::VectorWriteTarget;
use super::handlers::vector_direct_row::VectorIndexKey;

/// One vector write a replayed record carries.
pub(in crate::data::executor) struct RedoVectorWrite<'a> {
    pub index_key: &'a VectorIndexKey,
    pub tid: u64,
    pub collection: &'a str,
    /// The dimension of the write's vectors, `0` when it carries none.
    pub dim: usize,
    pub surrogates: &'a [Surrogate],
    pub ids: &'a [u32],
    /// Whether the write stores sidecar rows (a vector-primary write).
    pub sidecars: bool,
}

impl CoreLoop {
    /// Run the committed-redo step before a vector write. Returns whether the
    /// arm writes: `false` in the validate pass, and when the install could
    /// not read the write's pre-image (the error is kept on the apply).
    pub(in crate::data::executor) fn redo_vector_prelude(
        &mut self,
        write: RedoVectorWrite<'_>,
        provenance: Option<&SyncProvenance>,
        record_lsn: u64,
    ) -> bool {
        if !self.applying_committed_redo() {
            return true;
        }
        if write.dim != 0
            && let Some(existing) = self.vector_collections.get(write.index_key)
            && existing.dim() != write.dim
        {
            let index_dim = existing.dim();
            self.replay_record_unapplied(
                "vector",
                "dim",
                record_lsn,
                &format!(
                    "record for '{}' has dim {}, the index has dim {index_dim}",
                    write.collection, write.dim
                ),
            );
            return false;
        }
        if self.claim_for_validation() {
            return false;
        }
        let captured = self
            .capture_vector_write_undo(VectorWriteTarget {
                index_key: write.index_key,
                tid: write.tid,
                collection: write.collection,
                surrogates: write.surrogates,
                ids: write.ids,
                sidecars: write.sidecars,
            })
            .map(|undo| std::iter::once(undo).chain(self.capture_sync_hwm_undo(provenance)));
        self.record_redo_capture(captured)
    }

    /// [`Self::redo_vector_prelude`] for a vector-primary write that names
    /// its rows by `targets`. The rows are resolved the way the handler
    /// resolves them. A resolution error is kept on the apply.
    pub(in crate::data::executor) fn redo_vector_targets_prelude(
        &mut self,
        write: RedoVectorTargets<'_>,
        record_lsn: u64,
    ) -> bool {
        if !self.applying_committed_redo() {
            return true;
        }
        let surrogates = match self.resolve_vector_direct_targets(
            write.index_key.0.as_u64(),
            write.tid,
            write.collection,
            write.targets,
        ) {
            Ok(surrogates) => surrogates,
            Err(error) => {
                self.replay_record_unapplied(
                    "vector",
                    "resolve_targets",
                    record_lsn,
                    &format!("rows of '{}' do not resolve: {error:?}", write.collection),
                );
                return false;
            }
        };
        self.redo_vector_prelude(
            RedoVectorWrite {
                index_key: write.index_key,
                tid: write.tid,
                collection: write.collection,
                dim: write.dim,
                surrogates: &surrogates,
                ids: &[],
                sidecars: true,
            },
            None,
            record_lsn,
        )
    }
}

/// A vector-primary write that names its rows by [`VectorWriteTargets`].
pub(in crate::data::executor) struct RedoVectorTargets<'a> {
    pub index_key: &'a VectorIndexKey,
    pub tid: u64,
    pub collection: &'a str,
    /// The dimension of the write's vector, `0` when it carries none.
    pub dim: usize,
    pub targets: &'a VectorWriteTargets,
}
