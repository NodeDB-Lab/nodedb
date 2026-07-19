// SPDX-License-Identifier: BUSL-1.1

//! Streamed probe-against-index completion for the under-budget grace-hash join.
//!
//! When the build (right) side fits the per-query byte budget, the grace driver
//! keeps the buffered build rows, builds an in-memory [`HashIndex`] over them,
//! and streams the probe (left) side against that index in bounded ≤budget
//! batches. This is the "build fit budget" arm of [`super::grace_drive`]; it is
//! split out here so the driver file stays within the per-file size limit.
//!
//! The probe origin is a [`RowSource`], so this arm is identical whether the
//! probe rows come from a local-collection scan (local join) or a staged shuffle
//! file (cross-node shuffle-join consumer) — only the source variant differs.

use super::grace_partitioner::GraceSpec;
use super::hash::{HashIndex, ProbeParams, emit_unmatched_right_into, probe_rows_into};
use super::row_source::RowSource;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::scan_budget::budget_exceeded;

impl CoreLoop {
    /// Stream the probe (left) side against an in-memory `HashIndex` built over
    /// the buffered, under-budget build rows, in bounded ≤budget batches.
    ///
    /// Only ONE probe batch is resident at a time: rows accumulate into `batch`
    /// (tracking the same `id + value` byte total as the build side, `budget != 0`
    /// gated, strict `>`) until the running total crosses `budget`, at which
    /// point the batch is fed through [`probe_rows_into`] and cleared. The final
    /// partial batch is flushed after the stream ends. `results` and
    /// `index_matched` are shared across all batches so the global output limit
    /// (`spec.limit`) and RIGHT/FULL match tracking accumulate correctly; a final
    /// [`emit_unmatched_right_into`] sweep (gated on `emit_unmatched_right`)
    /// emits unmatched build rows.
    ///
    /// A `budget` of 0 (unlimited) never crosses, so the whole probe side flushes
    /// as a single final batch — still bounded only by the in-memory build index,
    /// matching the unlimited in-memory path.
    pub(super) fn stream_probe_against_index(
        &self,
        probe_source: &RowSource,
        build_docs: &[(String, Vec<u8>)],
        spec: &GraceSpec<'_>,
        budget: usize,
    ) -> crate::Result<Vec<Vec<u8>>> {
        let index = HashIndex::build(build_docs, spec.build_keys);

        let is_right = spec.join_type == "right" || spec.join_type == "full";
        let mut index_matched: Vec<bool> = if is_right {
            vec![false; build_docs.len()]
        } else {
            Vec::new()
        };
        let mut results: Vec<Vec<u8>> = Vec::new();

        // One ≤budget probe batch resident at a time.
        let mut batch: Vec<(String, Vec<u8>)> = Vec::new();
        let mut batch_bytes: usize = 0;

        // Process the accumulated batch through the shared emission loop, then
        // clear it for reuse. Honors `spec.limit` against the SHARED results.
        let flush = |batch: &mut Vec<(String, Vec<u8>)>,
                     batch_bytes: &mut usize,
                     results: &mut Vec<Vec<u8>>,
                     index_matched: &mut [bool]| {
            if batch.is_empty() {
                return;
            }
            probe_rows_into(
                &ProbeParams {
                    probe_docs: batch,
                    index: &index,
                    index_docs: build_docs,
                    probe_keys: spec.probe_keys,
                    join_type: spec.join_type,
                    limit: spec.limit,
                    probe_collection: spec.probe_collection,
                    index_collection: spec.index_collection,
                    join_filters: &[],
                    emit_unmatched_right: spec.emit_unmatched_right,
                },
                results,
                index_matched,
            );
            batch.clear();
            *batch_bytes = 0;
        };

        probe_source.for_each(self, |id, bytes| {
            batch_bytes = batch_bytes
                .saturating_add(bytes.len())
                .saturating_add(id.len());
            // Only the value bytes are fed to probe_rows_into; the id is never
            // used for matching, so avoid the allocation.
            batch.push((String::new(), bytes.to_vec()));
            // budget == 0 → unlimited → never flush mid-stream (matches the
            // build-side accounting and the unlimited in-memory path).
            if budget_exceeded(batch_bytes, budget) {
                flush(
                    &mut batch,
                    &mut batch_bytes,
                    &mut results,
                    &mut index_matched,
                );
            }
            Ok(())
        })?;

        // Flush the final partial batch.
        flush(
            &mut batch,
            &mut batch_bytes,
            &mut results,
            &mut index_matched,
        );

        // RIGHT/FULL: emit unmatched index-side rows ONCE, after all probe
        // batches. The in-memory path runs this same sweep via probe_hash_index.
        if is_right && spec.emit_unmatched_right {
            emit_unmatched_right_into(
                &ProbeParams {
                    probe_docs: &[],
                    index: &index,
                    index_docs: build_docs,
                    probe_keys: spec.probe_keys,
                    join_type: spec.join_type,
                    limit: spec.limit,
                    probe_collection: spec.probe_collection,
                    index_collection: spec.index_collection,
                    join_filters: &[],
                    emit_unmatched_right: spec.emit_unmatched_right,
                },
                &mut results,
                &index_matched,
            );
        }

        Ok(results)
    }
}
