// SPDX-License-Identifier: BUSL-1.1

//! Memory-bounded build-side driver for the grace-hash join.
//!
//! This module owns the core grace-hash machinery ([`CoreLoop::drive_grace_build`],
//! [`CoreLoop::finish_grace_join`], [`GraceSources`], [`LocalJoinSides`]) shared
//! by the local-join and shuffle-join entry points, plus the local-join entry
//! point [`CoreLoop::try_grace_hash_join`] that `execute_hash_join` calls ONLY
//! when both join sides are plain local scans (no Exchange sub-plan, no bitmap
//! prefilter) and the join is NOT a cross / keyless join.
//!
//! `try_grace_hash_join` streams the build (right) side row-at-a-time, tracking byte
//! total against the SAME budget the materializing path uses
//! (`scan_bytes_exceeded`: id + value bytes, strict `>`). Two outcomes:
//!
//! - **Under-budget build** — the build side finishes at or below budget. The
//!   driver KEEPS the fully-buffered build rows, builds an in-memory
//!   `HashIndex` over them, and STREAMS the probe (left) side against that index
//!   in bounded ≤budget batches (one batch resident at a time — the probe is
//!   never fully materialized). The shared `results` / `index_matched` are
//!   accumulated across batches via the reusable `probe_rows_into` /
//!   `emit_unmatched_right_into` pieces, so the output is byte-identical to the
//!   old in-memory path that materialized the whole probe side and called
//!   `probe_hash_index` once. Byte-identity holds because
//!   `scan_collection_for_each` yields rows in the SAME ORDER as
//!   `scan_collection` (proven by the order-contract tests in
//!   `scan_normalize.rs`), the build buffer is the same row set/order the
//!   in-memory path would have built its index from, and batching only changes
//!   WHEN a probe row is processed, never the order or the emission rule.
//! - **Over-budget build** — the build side crosses budget mid-stream. The
//!   driver switches to a [`PartitionedSpiller`], pushes the already-buffered
//!   build rows plus the rest of the build stream, then streams the probe (left)
//!   side straight into the spiller (never materialized). `finish_and_probe()`
//!   completes the join; the result is the already-encoded join rows. This path
//!   COMPLETES — it never returns `ResourcesExhausted` for being over input
//!   budget.
//!
//! Both arms return the completed, encoded-ready join rows (pre-`filter_and_project`)
//! as a `Vec<Vec<u8>>`; `try_grace_hash_join` then applies the SAME
//! output-budget guard + `filter_and_project` + `encode_binary_rows` to both.
//!
//! Cross / keyless joins are NOT handled here — `try_grace_hash_join` returns
//! `None` for them so the caller falls through to the unchanged in-memory path
//! (which handles the cartesian product). Streaming the cross-join probe is a
//! declared, separate deferral.
//!
//! Why a fixed `P = 64`: 64 partitions keeps each partition's working set small
//! enough that a per-partition `HashIndex` + probe stays well inside one core's
//! arena even for a build side that is many multiples of the budget, while the
//! per-partition file/handle overhead (64 build + 64 probe spill files per join)
//! remains modest. `per_partition_budget = max_scan_result_bytes / P` so the
//! aggregate in-memory residency across all partitions stays bounded by the same
//! `max_scan_result_bytes` budget the materializing path enforces.

use super::grace_partitioner::GraceSpec;
use super::grace_spill::PartitionedSpiller;
use super::params::JoinParams;
use super::row_source::RowSource;
use super::support::row_qualifier;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::scan_budget::budget_exceeded;

/// Fixed partition count for the grace-hash spill path. See module docs.
const GRACE_PARTITIONS: usize = 64;

/// The two row origins a grace-hash join consumes: the BUILD (right) side and
/// the PROBE (left) side, each as a [`RowSource`].
///
/// Bundling them keeps the grace entry points under the argument-count limit
/// while making the build/probe origin explicit. The local-join caller fills
/// both with [`RowSource::LocalScan`]; the cross-node shuffle-join consumer
/// fills both with [`RowSource::ShuffleStream`]. The grace ALGORITHM is identical
/// either way — only the input origin changes.
///
/// The probe source is consumed at one of two mutually-exclusive driver sites
/// (the spill-probe path OR the streamed-probe path, depending on whether the
/// build side spilled), each of which reads it through `for_each(&self)` — so it
/// is borrowed, never moved, and a single owned `probe` field suffices. Both
/// `RowSource` variants are cheap to clone should a future caller need a second
/// independent pass.
pub(super) struct GraceSources {
    /// BUILD (right) side rows.
    pub(super) build: RowSource,
    /// PROBE (left) side rows.
    pub(super) probe: RowSource,
}

/// The two local collections (and their optional aliases) a both-sides-local
/// hash join scans. Bundled so [`CoreLoop::try_grace_hash_join`] stays within
/// the argument-count limit; the alias falls back to the collection name when
/// absent.
pub(super) struct LocalJoinSides<'a> {
    /// PROBE (left) collection.
    pub(super) left_collection: &'a str,
    /// BUILD (right) collection.
    pub(super) right_collection: &'a str,
    /// Optional probe-side column qualifier (defaults to `left_collection`).
    pub(super) left_alias: Option<&'a str>,
    /// Optional build-side column qualifier (defaults to `right_collection`).
    pub(super) right_alias: Option<&'a str>,
    /// Row-level-security filters for the probe (left) side.
    pub(super) left_rls_filters: &'a [u8],
    /// Row-level-security filters for the build (right) side.
    pub(super) right_rls_filters: &'a [u8],
    /// The probe (left) side's own `WHERE` predicates.
    pub(super) left_scan_filters: &'a [u8],
    /// The build (right) side's own `WHERE` predicates.
    pub(super) right_scan_filters: &'a [u8],
}

/// Per-side streaming accumulation state. Starts `Buffering`; transitions to
/// `Spilling` exactly once, when the running byte total crosses `budget`.
enum BuildState {
    Buffering {
        docs: Vec<(String, Vec<u8>)>,
        bytes: usize,
    },
    // Boxed: `PartitionedSpiller` is large (P partition buffers + writers);
    // box it so `BuildState` stays compact next to the small `Buffering` variant.
    Spilling(Box<PartitionedSpiller>),
}

impl CoreLoop {
    /// Memory-bounded completion entry point for a both-sides-local hash join.
    ///
    /// Returns:
    /// - `None` — the join is a cross / keyless join (declared deferral: cross
    ///   probe streaming is separate). The caller MUST fall through to the
    ///   unchanged in-memory hash-join path, which handles the cartesian product.
    /// - `Some(response)` — the memory-bounded path completed the join (either
    ///   the under-budget-build streamed-probe path or the over-budget-build
    ///   grace-spill path) and produced the final encoded response, OR an error
    ///   response (scan failure, or the no-LIMIT output exceeding the per-query
    ///   byte budget) is being surfaced.
    ///
    /// For every both-local, non-cross join this returns `Some`. The path
    /// COMPLETES the join — it never returns `ResourcesExhausted` for being over
    /// the *input* budget. The output-budget enforcement below is the SAME guard
    /// the in-memory path applies: a no-LIMIT join whose output exceeds the byte
    /// budget surfaces a deterministic `ResourcesExhausted` rather than silently
    /// truncating.
    pub(super) fn try_grace_hash_join(
        &self,
        join: &JoinParams<'_>,
        tid: u64,
        sides: LocalJoinSides<'_>,
        budget: usize,
    ) -> Option<Response> {
        let LocalJoinSides {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            left_rls_filters,
            right_rls_filters,
            left_scan_filters,
            right_scan_filters,
        } = sides;
        let database_id = join.task.request.database_id;
        let probe_collection =
            left_alias.unwrap_or_else(|| row_qualifier(left_collection, database_id));
        let index_collection =
            right_alias.unwrap_or_else(|| row_qualifier(right_collection, database_id));

        // Probe-side (left) and build-side (right) join-key field names.
        let probe_keys: Vec<&str> = join.on.iter().map(|(l, _)| l.as_str()).collect();
        let build_keys: Vec<&str> = join.on.iter().map(|(_, r)| r.as_str()).collect();

        // Cross / keyless join: NOT streamed here. A cartesian product cannot be
        // hash-partitioned by key, and the streamed-probe path needs join keys to
        // build an index. Declared deferral — fall through to the unchanged
        // in-memory path (which handles cross). (Streaming the cross-join probe is
        // a separate unit.)
        if join.join_type == "cross" || build_keys.is_empty() || probe_keys.is_empty() {
            return None;
        }

        // Local-join origin: both sides scan local collections. Byte-identical
        // to the previous inline `RowSource::LocalScan` construction at each
        // grace-driver site — only the construction point moved here so the
        // driver is parameterized over `RowSource` values.
        let did = join.task.request.database_id.as_u64();
        let sources = GraceSources {
            build: RowSource::LocalScan {
                database_id: did,
                tenant_id: tid,
                collection: right_collection.to_string(),
                rls_filters: right_rls_filters.to_vec(),
                scan_filters: right_scan_filters.to_vec(),
            },
            probe: RowSource::LocalScan {
                database_id: did,
                tenant_id: tid,
                collection: left_collection.to_string(),
                rls_filters: left_rls_filters.to_vec(),
                scan_filters: left_scan_filters.to_vec(),
            },
        };

        // Identical output-bound derivation to the in-memory path: an explicit
        // user LIMIT is honored exactly (no budget check); a no-LIMIT join is
        // bounded by the per-query byte budget (or truly unbounded when 0).
        let (probe_limit, enforce_output_budget) = if join.limit != usize::MAX {
            (join.limit, false)
        } else if budget == 0 {
            (usize::MAX, false)
        } else {
            (
                crate::data::executor::handlers::scan_budget::fetch_limit_for(
                    usize::MAX,
                    0,
                    budget,
                ),
                true,
            )
        };

        let spec = GraceSpec {
            build_keys: &build_keys,
            probe_keys: &probe_keys,
            join_type: join.join_type,
            limit: probe_limit,
            probe_collection,
            index_collection,
            // Matches the in-memory call: local-scan joins always emit unmatched
            // build-side rows for RIGHT/FULL (no broadcast de-duplication here).
            emit_unmatched_right: true,
        };

        let unique_join_id = join.task.request_id().as_u64();
        Some(self.finish_grace_join(
            join,
            sources,
            &spec,
            budget,
            unique_join_id,
            enforce_output_budget,
        ))
    }

    /// Shared completion tail for every grace-hash join, regardless of input
    /// origin (local-collection scans or staged shuffle files).
    ///
    /// Drives the build + probe via [`Self::drive_grace_build`] over `sources`,
    /// then applies the EXACT same output-budget guard, `filter_and_project`,
    /// and `encode_binary_rows` the local path always applied — so the response
    /// shape is byte-identical whether the rows came from local scans or a
    /// cross-node shuffle. Errors are mapped deterministically:
    /// [`crate::Error::MemoryExhausted`] (over-budget skew) →
    /// `ResourcesExhausted`; any other driver error → `Internal`.
    pub(super) fn finish_grace_join(
        &self,
        join: &JoinParams<'_>,
        sources: GraceSources,
        spec: &GraceSpec<'_>,
        budget: usize,
        unique_join_id: u64,
        enforce_output_budget: bool,
    ) -> Response {
        let mut results = match self.drive_grace_build(sources, spec, budget, unique_join_id) {
            Ok(rows) => rows,
            // A depth-cap skew error is "over budget" semantics — identical to
            // what the in-memory path surfaces — so it must map to
            // `ResourcesExhausted`, NOT `Internal`. (The envelope maps
            // `Error::MemoryExhausted` → `ErrorCode::ResourcesExhausted`; we match
            // the variant here so the wire code is correct regardless of where
            // the error was minted.) Any other error stays `Internal`.
            Err(crate::Error::MemoryExhausted { .. }) => {
                return self.response_error(join.task, ErrorCode::ResourcesExhausted);
            }
            Err(e) => {
                return self.response_error(join.task, ErrorCode::from(e));
            }
        };

        // Same output-budget enforcement as the in-memory path: a no-LIMIT join
        // whose output fills the budget ceiling surfaces a deterministic error
        // rather than dropping rows.
        if enforce_output_budget && results.len() >= spec.limit {
            return self.response_error(join.task, ErrorCode::ResourcesExhausted);
        }

        if let Err(e) = join.filter_and_project(&mut results, &self.crs_context()) {
            return self.response_error(join.task, ErrorCode::from(e));
        }

        let payload = crate::data::executor::response_codec::encode_binary_rows(&results);
        self.response_with_payload(join.task, payload)
    }

    /// Drive the memory-bounded build + probe for a non-cross hash join over
    /// the supplied [`GraceSources`]. Always runs the join to completion and
    /// returns the encoded-ready join rows.
    ///
    /// The build/probe origins are whatever the caller passes: local-collection
    /// scans (local join) or staged shuffle files (cross-node shuffle-join
    /// consumer). The algorithm is identical for both — only the input origin
    /// differs.
    ///
    /// Streams the build (right) source, buffering until the per-query byte
    /// budget is crossed.
    ///
    /// - **Not crossed** (build fits budget, or `budget == 0` = unlimited): keep
    ///   the buffered build rows, build an in-memory `HashIndex`, and stream the
    ///   probe (left) source against it in bounded ≤budget batches (see
    ///   [`Self::stream_probe_against_index`]), sharing one `results` Vec and one
    ///   `index_matched` Vec across batches; a final unmatched-right sweep handles
    ///   RIGHT/FULL unmatched build rows. The probe is never fully materialized.
    /// - **Crossed**: switch to a [`PartitionedSpiller`], stream the probe into
    ///   it, complete the join, and remove the per-join spill directory.
    pub(super) fn drive_grace_build(
        &self,
        sources: GraceSources,
        spec: &GraceSpec<'_>,
        budget: usize,
        unique_join_id: u64,
    ) -> crate::Result<Vec<Vec<u8>>> {
        let spill_dir = self
            .data_dir
            .join("join-spill")
            .join(format!("core-{}", self.core_id()))
            .join(format!("{unique_join_id}"));

        // Per-partition residency bound: the aggregate across P partitions stays
        // within the same `max_scan_result_bytes` budget the in-memory path uses.
        // Floored at 1 so a tiny-but-nonzero budget still spills (a 0
        // per-partition budget would make `PartitionedSpiller` never spill —
        // i.e. stay fully in memory — which would defeat the bound). We only
        // reach this path when `budget != 0`, so the floor never fabricates a
        // bound for the unlimited case.
        let per_partition_budget = (budget / GRACE_PARTITIONS).max(1);

        let mut state = BuildState::Buffering {
            docs: Vec::new(),
            bytes: 0,
        };

        // Split the sources: the build pass below consumes `build_source`, and
        // exactly one of the two mutually-exclusive completion arms
        // (streamed-probe-against-index vs. spill-probe) borrows `probe_source`
        // via `for_each(&self)`. Destructuring keeps both owned for the duration.
        let GraceSources {
            build: build_source,
            probe: probe_source,
        } = sources;

        // Stream the BUILD (right) side. The closure transitions the state from
        // Buffering to Spilling exactly once, the first time the running byte
        // total crosses `budget` (matching `scan_bytes_exceeded`: id + value
        // bytes, strict `>`).
        build_source.for_each(self, |id, bytes| {
            // Append this row to the active side. When a buffering side crosses
            // budget, `mem::take` the buffered rows out (leaving the buffer empty)
            // and return them so we transition Buffering → Spilling exactly once.
            // Returning the drained rows from the match arm itself means there is
            // no separate (and unreachable) re-match of the post-transition state.
            let drained: Option<Vec<(String, Vec<u8>)>> = match &mut state {
                BuildState::Buffering { docs, bytes: total } => {
                    *total = total.saturating_add(bytes.len()).saturating_add(id.len());
                    // Only the value bytes are later fed to push_build; the id
                    // is never used, so avoid the allocation.
                    docs.push((String::new(), bytes.to_vec()));
                    // budget == 0 → unlimited → never spill.
                    if budget_exceeded(*total, budget) {
                        Some(std::mem::take(docs))
                    } else {
                        None
                    }
                }
                BuildState::Spilling(spiller) => {
                    spiller.push_build(bytes)?;
                    None
                }
            };

            if let Some(drained) = drained {
                // Transition Buffering → Spilling: create the spill dir, drain the
                // buffered build rows into the spiller, then continue streaming to it.
                std::fs::create_dir_all(&spill_dir).map_err(|e| crate::Error::Storage {
                    engine: "join-spill".into(),
                    detail: format!(
                        "failed to create grace-join spill dir {}: {e}",
                        spill_dir.display()
                    ),
                })?;
                let mut spiller = PartitionedSpiller::new(
                    spec,
                    GRACE_PARTITIONS,
                    per_partition_budget,
                    // FINISH-TIME re-partition trigger: the FULL per-query budget.
                    // `finish_and_probe` materializes ONE partition at a time, so
                    // a partition is only too big to materialize when it exceeds
                    // the whole-query budget — NOT `per_partition_budget` (which is
                    // `budget / 64` and would force every partition to look
                    // oversized). When this path runs `budget != 0` always (the
                    // build side only crosses into spilling when `budget != 0`), so
                    // `materialize_cap` is a real positive bound here.
                    budget,
                    spill_dir.clone(),
                );
                for (_, row) in &drained {
                    spiller.push_build(row)?;
                }
                state = BuildState::Spilling(Box::new(spiller));
            }
            Ok(())
        })?;

        match state {
            BuildState::Buffering { docs, .. } => {
                // Build side stayed within budget — keep the buffered build rows,
                // build the in-memory index, and STREAM the probe (left) side
                // against it in bounded ≤budget batches. Byte-identical to the old
                // in-memory path: same build row set/order, same `probe_rows_into`
                // emission, same global limit / index_matched accumulation; only
                // WHEN each probe row is processed differs, never the order.
                self.stream_probe_against_index(&probe_source, &docs, spec, budget)
            }
            BuildState::Spilling(mut spiller) => {
                // Stream the PROBE (left) side directly into the spiller — never
                // materialized in RAM. Errors propagate (no silent drop); on any
                // error we still remove the spill dir below by routing through the
                // shared cleanup tail.
                let probe_result = (|| -> crate::Result<Vec<Vec<u8>>> {
                    probe_source.for_each(self, |_id, bytes| spiller.push_probe(bytes))?;
                    spiller.finish_and_probe()
                })();

                // Remove the per-join spill directory regardless of outcome.
                // Best-effort: the rows have already been read back into RAM by
                // `finish_and_probe`, so a failed cleanup cannot corrupt results;
                // it only leaves temp files. Surface it loudly via tracing.
                if let Err(e) = std::fs::remove_dir_all(&spill_dir)
                    && spill_dir.exists()
                {
                    tracing::warn!(
                        error = %e,
                        dir = %spill_dir.display(),
                        "failed to remove grace-join spill dir"
                    );
                }

                probe_result
            }
        }
    }
}
