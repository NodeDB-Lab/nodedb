// SPDX-License-Identifier: BUSL-1.1

//! Public entry points and the `Resolved` result type.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::read_scope::ReadScope;
use crate::control::state::SharedState;
use crate::types::{Lsn, VShardId};

use crate::control::server::exchange::resolve::array_prefilter::resolve_array_prefilters;
use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::server::exchange::resolve::materialize::materialize_providers;
use crate::control::server::result_stream::ResultStream;

use super::dispatch::resolve_exchange;

/// Result of `resolve_and_materialize`.
pub enum Resolved {
    /// The plan was a root-level `Gather` — the coordinator has already
    /// executed it and the response is ready to return to the client. The
    /// second field carries the per-shard watermark LSNs the gather observed
    /// (one `(vshard, watermark_lsn)` per responding core), so an in-transaction
    /// read can record one read-set entry per participating shard rather than a
    /// single collapsed max. Empty for cross-node gathers (per-shard watermarks
    /// are not yet threaded through the gateway) and for shuffle joins.
    ///
    /// The third field carries per-collection read captures for a distributed
    /// read materialized on the coordinator — both the GATHER path (each base
    /// collection under a root `Exchange{Gather}`, including both sides of a
    /// gathered `HashJoin`) and the SHUFFLE JOIN path (probe/left and
    /// build/right). The record seam records one read-set entry per capture, so
    /// EVERY participating collection's vshard is validated at commit rather than
    /// only the plan's collapsed left collection. Empty when there is no
    /// in-transaction base-collection capture (autocommit reads, and shuffle
    /// AGGREGATE which carries its read vShards on the response).
    Gathered(Response, Vec<(VShardId, Lsn)>, Vec<DistributedReadCapture>),
    /// The plan (possibly mutated by catalog materialization or Broadcast
    /// embedding) is self-contained and is dispatched normally.
    Plan(Box<PhysicalPlan>),
    /// The plan was a single-node, unordered, non-aggregate scan eligible for
    /// streaming. The coordinator has eagerly dispatched it to all cores; the
    /// carried [`ResultStream`] yields row batches as they arrive. The pgwire
    /// path surfaces this lazily to the client; all other consumers
    /// `materialize` it back into a `Response`/bytes (behaviour-preserving).
    Stream(ResultStream),
}

/// Materialize catalog providers and resolve Exchange nodes in `plan`.
///
/// See module-level documentation for the two-pass behaviour.
///
/// `scope.txn_id` is the originating session transaction id (if the
/// dispatching task ran inside a transaction block); it is threaded down to
/// every per-core `Request` built by the gather primitives so in-transaction
/// scans can merge the transaction's staging overlay (read-your-own-writes).
/// Autocommit / non-transactional callers pass `None`. `scope.linearizable`
/// makes every leg confirm its group where it is served.
pub async fn resolve_and_materialize(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    plan: PhysicalPlan,
    scope: ReadScope,
) -> crate::Result<Resolved> {
    // Pass 1: fill empty ProviderScan rows (identity-scoped, per-request).
    let mut plan = materialize_providers(state, identity, scope.database_id, plan).await?;
    resolve_array_prefilters(state, &mut plan).await?;

    // Pass 2: resolve Exchange nodes. The captures accumulator is filled at every
    // base-collection gather point beneath the plan root and consumed (taken)
    // once at the root arm that returns `Resolved::Gathered`.
    let mut captures = Vec::new();
    resolve_exchange(state, scope, plan, &mut captures).await
}

/// Resolve only `Exchange` nodes (pass 2), without catalog provider
/// materialization. Used by the shared `dispatch_to_data_plane` funnel so that
/// internal query consumers (COPY, cursors, materialized-view refresh,
/// constraint subqueries) — which build `Exchange{Gather}`-wrapped read plans
/// over user tables but never carry catalog providers — still fan out and merge
/// correctly. Identity-free: catalog materialization happens earlier on the
/// pgwire/native paths that own the request identity. A no-op for plans with no
/// `Exchange` node.
///
/// See `resolve_and_materialize` for the `scope` semantics.
pub async fn resolve_exchange_in_plan(
    state: &SharedState,
    mut plan: PhysicalPlan,
    scope: ReadScope,
) -> crate::Result<Resolved> {
    resolve_array_prefilters(state, &mut plan).await?;
    let mut captures = Vec::new();
    resolve_exchange(state, scope, plan, &mut captures).await
}
