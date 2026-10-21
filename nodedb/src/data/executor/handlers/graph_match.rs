// SPDX-License-Identifier: BUSL-1.1

//! Graph pattern matching handler — executes MATCH queries on the Data Plane.

use tracing::{debug, warn};

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::pattern::ast::MatchQuery;
use crate::engine::graph::pattern::executor::{ContinuationSeed, PropertyLookup, VarLenResume};
use nodedb_physical::physical_plan::GraphReadScope;

pub(crate) use super::graph_match_envelope::{
    MATCH_ENVELOPE_FRONTIER_KEY, MATCH_ENVELOPE_RESUME_KEY, MATCH_ENVELOPE_ROWS_KEY,
    encode_match_envelope, encode_match_envelope_raw,
};

/// Bundled arguments for [`CoreLoop::execute_graph_match_continuation`].
pub(in crate::data::executor) struct GraphMatchContinuationParams<'a> {
    pub tid: u64,
    pub query_bytes: &'a [u8],
    pub resume_triple_idx: usize,
    pub partial_row_bytes: &'a [u8],
    pub source_node: &'a str,
    pub source_binding: &'a str,
    pub scope: &'a GraphReadScope,
}

impl CoreLoop {
    /// Encode a `MatchOutcome` into the DP→CP MATCH envelope and build the
    /// appropriate response (`partial` if the outcome was truncated, normal
    /// otherwise).
    ///
    /// The envelope is a 3-field msgpack map carrying the binding rows
    /// (exactly as `rows_to_msgpack` produces them — a bare msgpack array),
    /// the cross-shard `unresolved_frontier` (a zerompk-encoded array of
    /// `UnresolvedExpansion`), AND the variable-length truncation `resume`
    /// cursors (a zerompk-encoded array of [`VarLenResume`], empty when nothing
    /// truncated):
    ///
    /// ```text
    /// { "rows": <rows array>, "frontier": <frontier array>, "resume": <resume array> }
    /// ```
    ///
    /// The Control Plane's `broadcast_match_to_all_cores` unwraps this map:
    /// it merges the `rows` subfields across cores back into the SAME bare
    /// array shape `match_payload_to_response` already expects, and unions the
    /// `frontier` entries for cross-shard continuation dispatch. On a
    /// fully-local CSR the frontier array is empty, so single-node client
    /// behaviour after the unwrap is byte-identical to the prior bare-array
    /// response.
    ///
    /// Shared by [`execute_graph_match`] and [`execute_graph_match_continuation`]
    /// to avoid duplicating the encode → response tail.
    fn match_outcome_response(
        &self,
        task: &ExecutionTask,
        outcome: crate::engine::graph::pattern::executor::MatchOutcome,
    ) -> Response {
        // Truncation is signalled by the `resume` cursor array INSIDE the
        // envelope (decoded by `broadcast_match_to_all_cores` into
        // `MatchBroadcastOutcome.resume`), NOT by the frame status. The response
        // is ALWAYS a single terminal frame: the cross-core/cross-node gather
        // (`collect_bounded_response`) drains `Partial` frames until a terminal
        // one, so emitting a lone `Partial` frame here would hang the gather.
        let resume: Vec<VarLenResume> = outcome.truncation.into_iter().collect();
        match encode_match_envelope(&outcome.rows, &outcome.unresolved_frontier, &resume) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Return an empty MATCH result payload for a tenant that has no graph
    /// state on this shard.  An absent CSR partition is not an error.
    ///
    /// Shared by [`execute_graph_match`] and [`execute_graph_match_continuation`].
    fn match_empty_partition_response(&self, task: &ExecutionTask) -> Response {
        match encode_match_envelope(&[], &[], &[]) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    pub(in crate::data::executor) fn execute_graph_match(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        query_bytes: &[u8],
        frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
        cluster_mode: bool,
        scope: &GraphReadScope,
    ) -> Response {
        debug!(core = self.core_id, tid, "graph match execution");
        let database_id = task.request.database_id.as_u64();

        // Deserialize the MatchQuery from MessagePack.
        let query: MatchQuery = match zerompk::from_msgpack(query_bytes) {
            Ok(q) => q,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to deserialize MatchQuery");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid match query: {e}"),
                    },
                );
            }
        };

        // Execute the pattern match on the CSR at the plan's cut + EdgeStore.
        // An absent partition means "this tenant has no graph state" —
        // return the empty row set rather than error.
        if let Err(e) = self.prepare_match_view(task, tid, &query, scope) {
            return self.response_error(task, e);
        }
        let view = match self.match_view(task, tid, &query, scope) {
            Ok(Some(view)) => view,
            Ok(None) => return self.match_empty_partition_response(task),
            Err(e) => return self.response_error(task, e),
        };
        let partition = view.csr;
        // In cluster mode the Data Plane has no routing knowledge, so it
        // cannot pre-filter which bound zero-degree sources are genuinely
        // remote. It emits ALL of them as frontier candidates (predicate
        // returns `true` for every node) and the Control Plane filters them
        // precisely via routing in B2. In single-node mode (`false`) no
        // predicate is supplied, so the frontier stays empty and the
        // response is byte-identical to today.
        let all_remote = |_: &str| true;
        let is_remote_node: Option<&dyn Fn(&str) -> bool> = if cluster_mode {
            Some(&all_remote)
        } else {
            None
        };
        // Hard caps on variable-length expansion are an operational knob:
        // built from this node's GraphTuning (defaulting to 100k when no
        // override is set). On a cap hit the expansion truncates and surfaces
        // a resume cursor so the remainder pages across rounds.
        let varlen_caps = crate::engine::graph::pattern::executor::VarLenCaps::from_graph_tuning(
            &self.graph_tuning,
        );
        // Property predicates (`WHERE a.field = 'v'`) resolve each bound node's
        // document from the sparse engine, keyed by node-id within the query's
        // `IN '<collection>'`.
        let props = PropertyLookup {
            sparse: &self.sparse,
            csr: partition,
            database_id,
            tenant_id: tid,
            collection: view.collection.as_deref(),
            document_cut: view.document_cut,
        };
        let overlay = view.overlay;
        match crate::engine::graph::pattern::executor::execute(
            &query,
            crate::engine::graph::pattern::executor::MatchExecCtx {
                csr: partition,
                edge_store: &self.edge_store,
                frontier_bitmap,
                is_remote_node,
                varlen_caps,
                props: &props,
                overlay: overlay.as_ref(),
            },
        ) {
            Ok(outcome) => self.match_outcome_response(task, outcome),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Cross-shard MATCH continuation: resume a pattern on THIS shard.
    ///
    /// Deserializes the already-optimized `MatchQuery` and the accumulated
    /// `partial_row`, seeds the source binding (`source_binding -> source_node`)
    /// on top of the partial bindings, then resumes expansion from
    /// `resume_triple_idx` against this shard's CSR partition.
    ///
    /// A `MatchContinuation` ALWAYS runs cross-shard (it only exists because
    /// another shard emitted an `UnresolvedExpansion` routed here), so its
    /// remaining-pattern expansion MUST surface its OWN unresolved frontier:
    /// `is_remote_node = Some(&|_| true)`. This is what makes multi-round
    /// continuation work — deeper hops that again leave this shard's CSR are
    /// re-emitted as frontier entries for the Control-Plane coordinator to
    /// dispatch onward. The Control Plane filters them precisely via routing
    /// (dropping true local leaves), exactly as it does for the round-0
    /// `Match` frontier. No `cluster_mode` field is needed on
    /// `MatchContinuation` — the predicate is unconditionally `true` here.
    /// The response already envelopes `{rows, frontier}` via
    /// `match_outcome_response`.
    pub(in crate::data::executor) fn execute_graph_match_continuation(
        &mut self,
        task: &ExecutionTask,
        params: GraphMatchContinuationParams<'_>,
    ) -> Response {
        let GraphMatchContinuationParams {
            tid,
            query_bytes,
            resume_triple_idx,
            partial_row_bytes,
            source_node,
            source_binding,
            scope,
        } = params;
        debug!(
            core = self.core_id,
            tid, "graph match continuation execution"
        );
        let database_id = task.request.database_id.as_u64();

        // Deserialize the already-optimized MatchQuery.
        let query: MatchQuery = match zerompk::from_msgpack(query_bytes) {
            Ok(q) => q,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to deserialize MatchQuery");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid match query: {e}"),
                    },
                );
            }
        };

        // Deserialize the accumulated partial bindings.
        let mut seed_row: HashMap<String, String> = match zerompk::from_msgpack(partial_row_bytes) {
            Ok(r) => r,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to deserialize partial_row");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid continuation partial_row: {e}"),
                    },
                );
            }
        };
        // Seed the source binding so the resumed triple resolves its source
        // from the bound variable rather than free-ranging.
        seed_row.insert(source_binding.to_string(), source_node.to_string());

        // An absent partition means this tenant has no graph state on this
        // shard — return the empty row set rather than error.
        if let Err(e) = self.prepare_match_view(task, tid, &query, scope) {
            return self.response_error(task, e);
        }
        let view = match self.match_view(task, tid, &query, scope) {
            Ok(Some(view)) => view,
            Ok(None) => return self.match_empty_partition_response(task),
            Err(e) => return self.response_error(task, e),
        };
        let partition = view.csr;

        // A continuation only ever runs cross-shard, so it must surface its
        // own unresolved frontier (every bound zero-degree source becomes a
        // candidate; the Control Plane filters precisely via routing). This is
        // what enables multi-round continuation across >1 shard boundary.
        let all_remote = |_: &str| true;
        let is_remote_node: Option<&dyn Fn(&str) -> bool> = Some(&all_remote);
        let varlen_caps = crate::engine::graph::pattern::executor::VarLenCaps::from_graph_tuning(
            &self.graph_tuning,
        );
        let props = PropertyLookup {
            sparse: &self.sparse,
            csr: partition,
            database_id,
            tenant_id: tid,
            collection: view.collection.as_deref(),
            document_cut: view.document_cut,
        };
        // The transaction's staged edge overlay (a live read whose task
        // carries a `txn_id`) lets the resumed pattern observe
        // read-your-own-writes on this core, mirroring the round-0
        // `execute_graph_match` path. With the fixed-hop overlay merge
        // un-gated in cluster mode, a bound zero-degree source still emits
        // its cross-shard frontier for onward continuation.
        let overlay = view.overlay;
        match crate::engine::graph::pattern::executor::execute_continuation(
            &query,
            crate::engine::graph::pattern::executor::MatchExecCtx {
                csr: partition,
                edge_store: &self.edge_store,
                frontier_bitmap: None, // no anchor prefilter on the resume path
                is_remote_node,
                varlen_caps,
                props: &props,
                overlay: overlay.as_ref(),
            },
            ContinuationSeed {
                triple_idx: resume_triple_idx,
                seed_row,
            },
        ) {
            Ok(outcome) => self.match_outcome_response(task, outcome),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Cross-shard MATCH variable-length RESUME: continue a truncated
    /// `[*min..max]` expansion on THIS shard.
    ///
    /// Deserializes the already-optimized `MatchQuery` and the `VarLenResume`
    /// cursor (the capped triple index, the source bindings, and the
    /// un-expanded frontier / resume depth), then continues the BFS from that
    /// cursor and runs the remaining pattern triples over the resumed rows —
    /// producing the SAME `{rows, frontier}` envelope as a plain MATCH via
    /// [`Self::match_outcome_response`], including a FRESH `partial` flag /
    /// truncation cursor when the resume itself caps again.
    ///
    /// Like `MatchContinuation`, a varlen resume only ever runs cross-shard, so
    /// its remaining-pattern expansion surfaces its OWN unresolved frontier
    /// (`is_remote_node = Some(&|_| true)`) for the Control-Plane coordinator to
    /// route onward. The query MUST NOT be re-optimized — `triple_idx` indexes
    /// the originating shard's triple order.
    pub(in crate::data::executor) fn execute_graph_match_varlen_resume(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        query_bytes: &[u8],
        resume_bytes: &[u8],
        scope: &GraphReadScope,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid, "graph match variable-length resume execution"
        );
        let database_id = task.request.database_id.as_u64();

        let query: MatchQuery = match zerompk::from_msgpack(query_bytes) {
            Ok(q) => q,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to deserialize MatchQuery");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid match query: {e}"),
                    },
                );
            }
        };

        let resume: crate::engine::graph::pattern::executor::VarLenResume =
            match zerompk::from_msgpack(resume_bytes) {
                Ok(r) => r,
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "failed to deserialize VarLenResume");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("invalid varlen resume cursor: {e}"),
                        },
                    );
                }
            };

        if let Err(e) = self.prepare_match_view(task, tid, &query, scope) {
            return self.response_error(task, e);
        }
        let view = match self.match_view(task, tid, &query, scope) {
            Ok(Some(view)) => view,
            Ok(None) => return self.match_empty_partition_response(task),
            Err(e) => return self.response_error(task, e),
        };
        let partition = view.csr;

        let all_remote = |_: &str| true;
        let is_remote_node: Option<&dyn Fn(&str) -> bool> = Some(&all_remote);
        let varlen_caps = crate::engine::graph::pattern::executor::VarLenCaps::from_graph_tuning(
            &self.graph_tuning,
        );
        let props = PropertyLookup {
            sparse: &self.sparse,
            csr: partition,
            database_id,
            tenant_id: tid,
            collection: view.collection.as_deref(),
            document_cut: view.document_cut,
        };
        // The transaction's staged edge overlay (a live read whose task
        // carries a `txn_id`) lets the resumed variable-length expansion
        // observe read-your-own-writes on this core — the name-keyed BFS
        // walks staged edges and continues onto their owning core via
        // boundary resumes.
        let overlay = view.overlay;
        match crate::engine::graph::pattern::executor::execute_varlen_resume(
            &query,
            crate::engine::graph::pattern::executor::MatchExecCtx {
                csr: partition,
                edge_store: &self.edge_store,
                frontier_bitmap: None, // no anchor prefilter on the resume path
                is_remote_node,
                varlen_caps,
                props: &props,
                overlay: overlay.as_ref(),
            },
            resume,
        ) {
            Ok(outcome) => self.match_outcome_response(task, outcome),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
