// SPDX-License-Identifier: BUSL-1.1

//! Top-level scatter orchestration, result feeding, frontier resolution,
//! dedup/encode, and shared utilities used across the scatter sub-modules.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::bridge::envelope::Payload;
use crate::control::gateway::RouteDecision;
use crate::control::gateway::live_leaders::resolve_live_decision;
use crate::control::state::SharedState;
use crate::engine::graph::pattern::executor::{UnresolvedExpansion, VarLenResume, rows_to_msgpack};
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_cluster::distributed_graph::{
    DistributedMatchCoordinator, PatternContinuation, ResolvedContinuationArgs, ShardMatchResult,
};

use crate::control::server::graph_dispatch::match_broadcast::GraphRead;

use super::resume_queue::{PendingResume, resume_seed_key, resume_to_pending};
use super::round_loop::{dispatch_continuations, dispatch_resumes};
use super::round_zero::scatter_round_zero;

/// Anti-livelock ceiling on variable-length resume paging rounds.
///
/// The pattern's triple count (`max_rounds`) bounds cross-shard *hops*, but it
/// does NOT bound variable-length expansion *paging depth*: a single capped
/// triple can require many resume rounds to drain (one per cap re-fire), and the
/// coordinator has no live `|V|` to derive a tighter bound from. This generous
/// ceiling guarantees termination for pathological patterns (e.g. dense graphs
/// with a wide `*min..max` range) without truncating any realistic workload.
/// Exhausting it with cursors still pending sets the outcome's `partial` flag —
/// it is the single, surfaced backstop, never a silent drop.
const MAX_RESUME_ROUNDS: u32 = 10_000;

/// Ceiling on the per-pattern cross-shard hop-round budget (see
/// [`pattern_round_budget`]). Bounds an unbounded `[*]` traversal (whose
/// `max_hops` is near `usize::MAX`) to a large but finite number of rounds so
/// the budget cannot overflow; a pattern that genuinely needs more rounds than
/// this surfaces as `partial` rather than looping.
const MAX_TRAVERSAL_ROUNDS: usize = 10_000;

/// Result of a cross-shard MATCH scatter: the deduped binding rows as the bare
/// msgpack array shape `match_payload_to_response` expects, plus a `partial`
/// flag set ONLY on a real, unrecoverable partial: the coordinator exhausted
/// `max_rounds` with continuations still pending, or exhausted
/// `MAX_RESUME_ROUNDS` with variable-length resume cursors still pending. A
/// shard truncation that yields a resume cursor is RECOVERABLE — it is drained
/// across resume rounds and does NOT set `partial`.
pub struct MatchScatterOutcome {
    pub rows_payload: Payload,
    pub partial: bool,
}

/// One shard's round-0 / round-N result tagged with the node that produced it.
///
/// The emitting node id is required for the self-leaf drop: a frontier entry
/// whose owning node equals the node that emitted it is a true local leaf, not
/// a cross-shard ghost.
pub(super) struct TaggedShardResult {
    pub(super) emitting_node: u64,
    pub(super) rows: Vec<HashMap<String, String>>,
    pub(super) frontier: Vec<UnresolvedExpansion>,
    /// The variable-length truncation resume cursor(s) this shard produced,
    /// tagged (by `emitting_node`) like the frontier. A node fanned across
    /// cores can truncate on several at once, hence a `Vec`. Decoded from the
    /// shard's `{rows, frontier, resume}` envelope (local OR remote). The
    /// coordinator's resume re-dispatch path consumes these to re-issue
    /// `MatchVarLenResume` plans, draining a capped expansion across rounds
    /// until the full result set is returned.
    pub(super) resume: Vec<VarLenResume>,
}

/// Orchestrate a cross-shard MATCH. Caller guarantees cluster mode
/// (`cluster_routing.is_some()`); single-node never enters here.
///
/// `read.txn_id` is threaded onto every LOCAL scatter/resume leg so this node's
/// cores merge the transaction's staged edge overlay for read-your-own-writes;
/// remote legs read committed CSR (multi-node overlay forwarding is a separate
/// unit). `read.linearizable` makes every leg confirm its groups where it is
/// served.
pub async fn scatter_match(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    query_bytes: Vec<u8>,
    deadline_ms: u64,
    read: GraphRead,
) -> crate::Result<MatchScatterOutcome> {
    // Round budget = the maximum number of hops the pattern can take (summed
    // per-triple `max_hops`), since each round advances every frontier by one
    // hop and a variable-length triple crossing shard boundaries spends one
    // round per hop. A correctness-derived bound, not an arbitrary cap.
    let max_rounds = pattern_round_budget(&query_bytes).max(1) as u32;
    let mut coordinator = DistributedMatchCoordinator::new(max_rounds);

    let mut partial = false;

    // Variable-length resume cursors awaiting re-dispatch. A frontier-hop
    // continuation lives in the coordinator's pending queue; a varlen-paging
    // resume lives here (its cursor type is a nodedb-crate type, kept on the
    // Control-Plane side). Both feed back through `feed_result`, so a resume can
    // emit a fresh frontier (→ coordinator) and a fresh resume (→ this queue),
    // and they round-trip naturally.
    let mut pending_resumes: Vec<PendingResume> = Vec::new();
    // Seeds (anchor + frontier + depth) already dispatched as resumes. Boundary
    // continuation cursors are visited-less by contract, so without this the
    // coordinator re-dispatches the same boundary every round and the pending
    // queue fans out until it exhausts a core's dispatch admission queue. See
    // `resume_seed_key`.
    let mut dispatched_seeds: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut resume_rounds: u32 = 0;
    // Latches once the coordinator's hop budget (`max_rounds`) is exhausted with
    // continuations still pending, so the loop stops re-attempting `advance()`
    // (which will spin) but keeps draining any remaining resume cursors.
    let mut continuations_exhausted = false;

    // ---- Round 0: scatter the Match plan to local + every remote owner. ----
    let (round0, reads) = scatter_round_zero(
        state,
        tenant_id,
        database_id,
        &query_bytes,
        deadline_ms,
        read,
    )
    .await?;
    for tagged in round0 {
        feed_result(
            state,
            &mut coordinator,
            &mut pending_resumes,
            &mut dispatched_seeds,
            tagged,
        )?;
    }

    // ---- Round loop: drain both frontier continuations AND varlen resumes. ----
    while (coordinator.has_pending() && !continuations_exhausted) || !pending_resumes.is_empty() {
        // Frontier continuations: bounded by the pattern's hop count.
        if coordinator.has_pending() && !continuations_exhausted {
            if !coordinator.advance() {
                // Exhausted max_rounds with hops still pending: surface as
                // partial rather than silently dropping the continuations.
                // Latch so the loop stops re-attempting `advance()` (it will
                // spin) but still drains any pending resume cursors below.
                if coordinator.has_pending() {
                    partial = true;
                }
                continuations_exhausted = true;
            } else {
                let pending = coordinator.take_all_pending();
                let tagged = dispatch_continuations(
                    state,
                    tenant_id,
                    database_id,
                    &query_bytes,
                    deadline_ms,
                    read,
                    pending,
                )
                .await?;
                for t in tagged {
                    feed_result(
                        state,
                        &mut coordinator,
                        &mut pending_resumes,
                        &mut dispatched_seeds,
                        t,
                    )?;
                }
            }
        }

        // Variable-length resume cursors: bounded by MAX_RESUME_ROUNDS, a
        // separate paging budget (hops do not bound paging depth).
        if !pending_resumes.is_empty() {
            if resume_rounds >= MAX_RESUME_ROUNDS {
                // Budget exhausted with cursors pending: the surfaced backstop.
                // Never drop cursors silently.
                partial = true;
                break;
            }
            resume_rounds += 1;
            // Take the current batch so this round cannot spin on the same
            // cursors. Progress is guaranteed two ways: a cap-truncation resume
            // re-enqueues as a fresh, advanced cursor, and every resume seed is
            // deduped in `feed_result` (see `dispatched_seeds`) so a visited-less
            // boundary continuation is dispatched at most once per anchor/depth.
            let batch = std::mem::take(&mut pending_resumes);
            let tagged = dispatch_resumes(
                state,
                tenant_id,
                database_id,
                &query_bytes,
                deadline_ms,
                read,
                batch,
            )
            .await?;
            for t in tagged {
                feed_result(
                    state,
                    &mut coordinator,
                    &mut pending_resumes,
                    &mut dispatched_seeds,
                    t,
                )?;
            }
        }
    }

    // ---- Dedup + encode. ----
    let rows_payload = dedup_and_encode(&coordinator.completed)?;
    // Every vShard round 0 read joins the transaction read-set.
    reads.publish(
        tenant_id,
        database_id,
        pattern_collection(database_id, &query_bytes),
    );
    Ok(MatchScatterOutcome {
        rows_payload,
        partial,
    })
}

/// Convert a tagged shard result into a `ShardMatchResult` (filtering its
/// frontier to genuine cross-shard continuations) and feed it to the
/// coordinator. Any variable-length resume cursor(s) the shard produced are
/// routed back to their owning node and appended to `pending_resumes` for
/// re-dispatch.
///
/// A truncation that yields a resume cursor is RECOVERABLE (drained across
/// resume rounds) and is enqueued rather than surfaced — so this
/// reports no partial. The only surfaced partials come from the coordinator's
/// hop budget and the resume-round budget, both handled in `scatter_match`.
pub(super) fn feed_result(
    state: &SharedState,
    coordinator: &mut DistributedMatchCoordinator,
    pending_resumes: &mut Vec<PendingResume>,
    dispatched_seeds: &mut std::collections::HashSet<u64>,
    tagged: TaggedShardResult,
) -> crate::Result<()> {
    let TaggedShardResult {
        emitting_node,
        rows,
        frontier,
        resume,
    } = tagged;

    // Enqueue each resume cursor, routed back to the node owning its surviving
    // frontier. Degenerate (empty-frontier) cursors are skipped, and a seed
    // (anchor + frontier + depth) already dispatched this scatter is skipped so
    // visited-less boundary continuations converge instead of fanning out.
    for cursor in resume {
        if !dispatched_seeds.insert(resume_seed_key(&cursor)) {
            continue;
        }
        if let Some(pending) = resume_to_pending(state, cursor)? {
            pending_resumes.push(pending);
        }
    }

    let continuations = frontier_to_continuations(state, emitting_node, frontier)?;
    coordinator.add_shard_result(ShardMatchResult {
        // shard_id is informational on the coordinator; tag with the emitting
        // node id (the routing decision already lives in each continuation).
        shard_id: emitting_node as u32,
        completed_rows: rows,
        continuations,
    });
    Ok(())
}

/// Convert a shard's `UnresolvedExpansion` frontier into cross-shard
/// `PatternContinuation`s, applying the self-leaf drop.
///
/// A frontier entry's `node_name` is resolved to its owning node via
/// [`resolve_decision`]. If the owner is the SAME node that emitted the entry,
/// it is a true local leaf (the shard already held its edges and found none) —
/// DROP it. Otherwise emit a continuation targeting the owning vShard.
fn frontier_to_continuations(
    state: &SharedState,
    emitting_node: u64,
    frontier: Vec<UnresolvedExpansion>,
) -> crate::Result<Vec<PatternContinuation>> {
    let mut out = Vec::new();
    for entry in frontier {
        let target_vshard = VShardId::from_key(entry.node_name.as_bytes()).as_u32();
        let decision = resolve_live_decision(state, target_vshard);
        let owner_node = match decision {
            RouteDecision::Local => state.node_id,
            RouteDecision::Remote { node_id, .. } => node_id,
            RouteDecision::LeaderUnknown { vshard_id } => {
                return Err(crate::Error::NotLeader {
                    vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
                    leader_node: 0,
                    leader_addr: String::new(),
                    leader_term: 0,
                });
            }
            RouteDecision::Broadcast { .. } => {
                return Err(crate::Error::Internal {
                    detail: "match scatter: resolve_decision returned Broadcast for a \
                             single vShard"
                        .into(),
                });
            }
        };
        // Self-leaf drop: the frontier node is owned by the very shard that
        // emitted it — its own pass already had the edges and found none.
        if owner_node == emitting_node {
            continue;
        }
        out.push(PatternContinuation::from_resolved(
            ResolvedContinuationArgs {
                target_shard: target_vshard,
                source_shard: emitting_node as u32,
                bindings: entry.partial_row,
                next_triple_idx: entry.triple_idx,
                start_node: entry.node_name,
                start_binding: entry.binding_var,
            },
        ));
    }
    Ok(out)
}

/// Decode a bare msgpack rows array (the shape `rows_to_msgpack` produces and
/// `unwrap_match_envelope` returns) into binding rows. An empty payload is an
/// empty row set.
pub(super) fn decode_rows(payload: &Payload) -> crate::Result<Vec<HashMap<String, String>>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack::<Vec<HashMap<String, String>>>(payload.as_ref()).map_err(|e| {
        crate::Error::Codec {
            detail: format!("match scatter: invalid rows array: {e}"),
        }
    })
}

/// Dedup completed rows by a canonical sorted-(k,v) fingerprint and encode them
/// into the bare msgpack array shape `match_payload_to_response` expects.
///
/// Cross-shard union can legitimately overlap (undirected / edge cases), so we
/// ALWAYS dedup — not only when `RETURN DISTINCT` was requested.
fn dedup_and_encode(rows: &[HashMap<String, String>]) -> crate::Result<Payload> {
    let mut seen: HashSet<Vec<(String, String)>> = HashSet::new();
    let mut deduped: Vec<HashMap<String, String>> = Vec::with_capacity(rows.len());
    for row in rows {
        // BTreeMap gives a deterministic key order for the fingerprint.
        let fingerprint: Vec<(String, String)> = row
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect();
        if seen.insert(fingerprint) {
            deduped.push(row.clone());
        }
    }
    let bytes = rows_to_msgpack(&deduped)?;
    Ok(Payload::from_vec(bytes))
}

/// The database-qualified collection a serialized `MatchQuery` is scoped to
/// with `IN '<collection>'`, or `None` for a pattern over every collection.
fn pattern_collection(database_id: DatabaseId, query_bytes: &[u8]) -> Option<String> {
    use crate::engine::graph::pattern::ast::MatchQuery;
    let query: MatchQuery = zerompk::from_msgpack(query_bytes).ok()?;
    query.collection.map(|bare| {
        nodedb_types::QualifiedCollection::new(database_id, &bare)
            .as_str()
            .to_owned()
    })
}

/// Count the total pattern triples across every clause/chain in the serialized
/// `MatchQuery`. Used to bound the continuation rounds. A malformed query (or a
/// query with no triples) yields 0 — the caller floors `max_rounds` at 1.
/// Upper bound on cross-shard continuation rounds a pattern can require.
///
/// Each round advances every live frontier by at most ONE hop, so the budget
/// must cover the maximum number of hops the pattern can take. A fixed-length
/// triple is one hop; a variable-length triple `*min..max` is up to `max` hops,
/// and when its chain crosses a shard boundary on (nearly) every hop, each hop
/// becomes its own continuation round. Summing per-triple `max_hops` is the
/// correctness-derived bound — undercounting (e.g. counting a varlen triple as
/// one) starves the frontier and the query can never complete. Saturating-summed
/// and capped at [`MAX_TRAVERSAL_ROUNDS`] so an unbounded `[*]` (max_hops near
/// `usize::MAX`) yields a large finite budget rather than overflowing.
fn pattern_round_budget(query_bytes: &[u8]) -> usize {
    use crate::engine::graph::pattern::ast::MatchQuery;
    let query: MatchQuery = match zerompk::from_msgpack(query_bytes) {
        Ok(q) => q,
        Err(_) => return 0,
    };
    query
        .clauses
        .iter()
        .flat_map(|c| c.patterns.iter())
        .flat_map(|chain| chain.triples.iter())
        .map(|triple| triple.edge.max_hops.max(1))
        .fold(0usize, |acc, hops| acc.saturating_add(hops))
        .min(MAX_TRAVERSAL_ROUNDS)
}
