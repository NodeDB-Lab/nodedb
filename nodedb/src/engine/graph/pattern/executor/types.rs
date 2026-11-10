// SPDX-License-Identifier: BUSL-1.1

//! Data types shared across the MATCH pattern executor.

use std::collections::HashMap;

/// A single result row: variable bindings, each a typed value.
///
/// A node variable binds `Value::String(node name)`. A fixed-length edge
/// variable binds `Value::String("src|label|dst")` and a variable-length one
/// its path text. A variable an `OPTIONAL MATCH` left unmatched binds
/// `Value::Null`. A projected column holds the exact stored value. See
/// [`super::row`] for the reads and the hidden edge-property keys.
pub type BindingRow = HashMap<String, nodedb_types::Value>;

/// An expansion source that has zero local adjacency in the queried
/// direction — its out-edges (or in-edges) are homed on another shard.
///
/// Emitted by the executor so the Control Plane can dispatch a
/// continuation query to the owning shard. The frontier is produced
/// on every MATCH execution; on a fully-local CSR it is always empty.
///
/// # Cross-shard contract
///
/// The executor emits an `UnresolvedExpansion` for a source node when
/// **all four** conditions hold:
/// 1. The source variable was **bound** — it was resolved from an existing
///    binding in `input_row` (the multi-hop intermediate case), not
///    free-ranged over all local nodes.  A free-ranging anchor must NOT
///    emit because every shard covers all its own local nodes during its
///    own pass; emitting would duplicate work and flood the frontier with
///    every zero-degree local sink.
/// 2. The node has **zero raw adjacency** in the triple's direction
///    (regardless of edge-label filter).
/// 3. The caller supplied a locality predicate (`is_remote_node`).
/// 4. The predicate returns `true` for the node's name.
///
/// A node that has edges in the direction but none that pass the label
/// filter produces an empty local result and is NOT included in the
/// frontier (that is a legitimate "no match locally," not a
/// missing-shard situation).
///
/// Passing `None` for the predicate (the default for fully-local,
/// single-node deployments) guarantees the frontier is always empty.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct UnresolvedExpansion {
    /// The source binding variable name from the triple (e.g. `"b"`).
    pub binding_var: String,
    /// The resolved source node name with no local edges (e.g. `"bob"`).
    pub node_name: String,
    /// 0-based index of the triple in its chain that could not expand.
    pub triple_idx: usize,
    /// Bindings accumulated up to (but not including) this triple.
    pub partial_row: BindingRow,
}

/// A resume cursor for a variable-length expansion that hit a hard cap.
///
/// When `MATCH (a)-[*min..max]->(b)` truncates inside the executor, this
/// captures the LIVE state needed to continue the BFS on a later round
/// instead of silently dropping the un-expanded frontier:
///
/// - `triple_idx` — the within-chain triple whose expansion was capped.
/// - `source_row` — the bindings present at that triple's source (so the
///   resumed rows re-bind `a`/`b`/the edge var identically to a single pass).
/// - `frontier` — the surviving un-expanded frontier entries, each a
///   `(node_name, path_string)` pair (reached at `depth - 1`, awaiting
///   expansion AT `depth`). The path string is the accumulated path-so-far
///   from the original source when the edge variable is bound (`want_path`),
///   empty otherwise. Carrying it verbatim is what keeps resumed `RETURN p`
///   path strings continuous with the first pass. Keying the frontier by node
///   NAME (not a CSR-local dense id) is what makes a resume safe to broadcast
///   to ALL cores on a node: a local id is per-core and overlaps across cores,
///   so a foreign core would misinterpret it against its own CSR; a name is
///   global, so only the owning core resolves it to a local id and every other
///   core naturally skips names it does not own.
/// - `depth` — the hop depth at which `frontier` is resumed.
///
/// There is deliberately **no `visited` set**: termination relies on the
/// varlen `min..max` depth bound plus downstream coordinator row-dedup.
/// Carrying `visited` across a future wire boundary is explicitly rejected,
/// so the executor never depends on it for correctness — a node re-reached
/// on resume produces a duplicate row that the coordinator collapses, never
/// a skipped or mis-depthed row.
//
// The fields are produced by the executor on a capped expansion and consumed by
// the cross-plane resume dispatch (`GraphOp::MatchVarLenResume`). The struct is
// wire-serializable (serde + zerompk) so the cursor can ride the SPSC bridge
// inside the resume plan variant; each `frontier` entry is a `(node_name,
// path_string)` pair carrying a global node name alongside its accumulated
// path-so-far. Name-keying makes the cursor core-agnostic: the resume plan is
// fanned to all cores, and each core resolves the name against its own CSR —
// the owning core resumes, foreign cores skip names they do not own.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VarLenResume {
    /// 0-based index of the capped triple within its pattern chain.
    pub triple_idx: usize,
    /// Bindings present at the capped expansion's source.
    pub source_row: BindingRow,
    /// Surviving un-expanded frontier entries: `(node_name, path_so_far)`.
    pub frontier: Vec<(String, String)>,
    /// Hop depth reached at the cap (the depth `frontier` resumes at).
    pub depth: usize,
}

/// Result of running a MATCH query.
///
/// `truncation` is non-empty iff a hard cap inside variable-length expansion
/// fired OR a cross-boundary continuation was produced — the binding rows are
/// incomplete and each [`VarLenResume`] cursor records where to continue. Data
/// Plane handlers MUST set the `partial` flag on the response envelope when
/// this is non-empty so clients can observe the incomplete result. Use
/// [`MatchOutcome::truncated`] for the bare bool.
///
/// It is a `Vec` (not an `Option`) because a single free-ranging expansion can
/// produce MANY boundary frontiers — one per anchor whose edges are homed
/// remotely — each needing its own `source_row` anchor to resume from.
///
/// `unresolved_frontier` lists expansion sources whose edges are not
/// present in the local CSR partition. On a fully-local CSR this vec
/// is always empty and existing behaviour is byte-identical to before.
pub struct MatchOutcome {
    pub rows: Vec<BindingRow>,
    pub truncation: Vec<VarLenResume>,
    pub unresolved_frontier: Vec<UnresolvedExpansion>,
}

impl MatchOutcome {
    /// `true` iff a variable-length expansion hit a hard cap or produced a
    /// cross-boundary continuation, so the result set is incomplete.
    pub fn truncated(&self) -> bool {
        !self.truncation.is_empty()
    }
}

/// Seed for a cross-shard MATCH continuation: the within-chain triple index
/// at which to resume plus the binding row accumulated by the originating
/// shard up to (but not including) that triple.
///
/// Bundling these two together keeps [`super::continuation::execute_continuation`]'s
/// argument count within clippy's `too_many_arguments` limit while reflecting
/// that `triple_idx` and `seed_row` are always a unit — they describe the same
/// "point to resume from" in the originating shard's triple order.
pub struct ContinuationSeed {
    /// 0-based index of the triple within its pattern chain at which to resume.
    pub triple_idx: usize,
    /// Bindings accumulated by the originating shard for triples `[0, triple_idx)`.
    pub seed_row: BindingRow,
}

/// Shared mutable state collected during triple execution: the list of
/// binding rows being built + the across-query truncation flag +
/// the cross-shard unresolved frontier.
///
/// `is_remote_node` is an optional caller-supplied predicate: when
/// `Some(pred)`, `pred(node_name)` returns `true` for nodes that are
/// homed on a remote shard. When `None` every node is treated as local
/// and no frontier entries are ever emitted. The predicate is borrowed
/// for the lifetime `'a` of the execution call to avoid allocation.
pub(super) struct ExecutionState<'a> {
    /// Structured resume cursors accumulated during this execution: one per
    /// variable-length expansion that hit a cap, plus one per cross-boundary
    /// frontier node (zero local out-degree, homed remotely). A free-ranging
    /// expansion produces MANY boundary frontiers — each carries its own
    /// `source_row` anchor — so this is a `Vec`, not a single cursor. Empty
    /// until the first cursor is recorded.
    pub varlen_resume: Vec<VarLenResume>,
    pub frontier: Vec<UnresolvedExpansion>,
    pub is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
    /// Hard caps applied to every variable-length expansion in this execution.
    /// Threaded from node `GraphTuning` by the Data Plane handler (defaulting
    /// to `100_000` when no tuning override is set), so truncation is a real
    /// operational knob rather than a compile-time constant.
    pub varlen_caps: super::expansion::VarLenCaps,
    /// Collection scoping for edge traversal, resolved once from the query's
    /// `IN '<collection>'` clause against the CSR's collection interning.
    /// Defaults to [`CollectionFilter::Unscoped`] (tenant-wide) until the
    /// executor entry point resolves it.
    pub collection_filter: super::expansion::CollectionFilter,
    /// The edge variables whose properties the query reads, captured onto
    /// each row where the edge is bound. `None` captures nothing: a query
    /// that reads no relationship property, and a `NOT EXISTS` sub-pattern.
    pub edge_capture: Option<super::edge_props::EdgeCapture<'a>>,
}

impl<'a> ExecutionState<'a> {
    pub(super) fn new(
        is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
        varlen_caps: super::expansion::VarLenCaps,
    ) -> Self {
        Self {
            varlen_resume: Vec::new(),
            frontier: Vec::new(),
            is_remote_node,
            varlen_caps,
            collection_filter: super::expansion::CollectionFilter::Unscoped,
            edge_capture: None,
        }
    }

    /// Capture the crossed `edge`'s properties on `row` for edge variable
    /// `var`, when this execution captures that variable.
    pub(super) fn capture_edge(
        &self,
        row: &mut BindingRow,
        var: &str,
        edge: super::edge_props::CrossedEdge<'_>,
    ) -> crate::Result<()> {
        match &self.edge_capture {
            Some(capture) => capture.capture(row, var, edge),
            None => Ok(()),
        }
    }

    /// `true` iff any resume cursor was recorded during this execution (a cap
    /// hit or a cross-boundary continuation).
    pub(super) fn truncated(&self) -> bool {
        !self.varlen_resume.is_empty()
    }

    /// Append a resume cursor. Every capped expansion and every cross-boundary
    /// frontier node contributes its own cursor, so all are retained (the wire
    /// envelope carries the full list; the coordinator dispatches each round).
    pub(super) fn record_truncation(&mut self, resume: VarLenResume) {
        self.varlen_resume.push(resume);
    }
}
