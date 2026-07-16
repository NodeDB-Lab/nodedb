// SPDX-License-Identifier: BUSL-1.1

//! Pass 2 of plan resolution: Exchange node resolution.
//!
//! - `Gather{as_aggregate}` at the plan root → fan child to all vShards,
//!   merge, and return `Resolved::Gathered`.
//! - `Broadcast` inside a `HashJoin.left_input` / `right_input` →
//!   gather child to coordinator, encode as a merged msgpack array, and
//!   embed as `ProviderScan{provider: None, rows}`.  The modified join is
//!   self-contained and returned as `Resolved::Plan`.
//! - Root `Shuffle{keys, num_parts}` wrapping a `HashJoin` → orchestrate a
//!   cross-node grace hash join (`super::shuffle`) and return the merged rows
//!   as `Resolved::Gathered`. `Shuffle` as a join INPUT is a typed error (it
//!   only ever wraps a complete join).
//! - No Exchange / no empty ProviderScan → `Resolved::Plan` unchanged.

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::flatten_to_relational_rows;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId, VShardId};

use crate::control::server::exchange::gather::{
    GatherOutcome, finalize_aggregate, gather_all_cores, gather_all_cores_stream,
    gather_all_vshards, outcome_to_response,
};
use crate::control::server::result_stream::ResultStream;

use super::materialize::materialize_providers;

/// Result of `resolve_and_materialize`.
pub enum Resolved {
    /// The plan was a root-level `Gather` — the coordinator has already
    /// executed it and the response is ready to return to the client. The
    /// second field carries the per-shard watermark LSNs the gather observed
    /// (one `(vshard, watermark_lsn)` per responding core), so an in-transaction
    /// read can record one read-set entry per participating shard rather than a
    /// single collapsed max. Empty for cross-node gathers (per-shard watermarks
    /// are not yet threaded through the gateway) and for shuffle joins.
    Gathered(Response, Vec<(VShardId, Lsn)>),
    /// The plan (possibly mutated by catalog materialization or Broadcast
    /// embedding) is self-contained and should be dispatched normally.
    Plan(PhysicalPlan),
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
/// `txn_id` is the originating session transaction id (if the dispatching
/// task ran inside a transaction block); it is threaded down to every
/// per-core `Request` built by the gather primitives so in-transaction scans
/// can merge the transaction's staging overlay (read-your-own-writes).
/// Autocommit / non-transactional callers pass `None`.
pub async fn resolve_and_materialize(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<Resolved> {
    // Pass 1: fill empty ProviderScan rows (identity-scoped, per-request).
    let plan = materialize_providers(state, identity, plan).await?;

    // Pass 2: resolve Exchange nodes.
    resolve_exchange(state, database_id, tenant_id, plan, trace_id, txn_id).await
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
/// See `resolve_and_materialize` for `txn_id` semantics.
pub async fn resolve_exchange_in_plan(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<Resolved> {
    resolve_exchange(state, database_id, tenant_id, plan, trace_id, txn_id).await
}

// ── pass 2 ───────────────────────────────────────────────────────────────────

/// Resolve any `Exchange` nodes in `plan`.
///
/// - Root-level `Gather` → gather all vShards, return `Resolved::Gathered`.
/// - `Broadcast` nested inside a `HashJoin` input → gather the child, embed
///   the `merged_array` as `ProviderScan{None, rows}`, return `Resolved::Plan`.
/// - Root-level `Shuffle` wrapping a `HashJoin` → orchestrate a cross-node
///   grace hash join, return `Resolved::Gathered`. `Shuffle` as a join input is
///   a typed error.
/// - Anything else → `Resolved::Plan` unchanged.
async fn resolve_exchange(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<Resolved> {
    match plan {
        // Root-level Gather: fan child to all vShards and merge. First resolve any
        // Exchange{Broadcast} nodes nested inside the child (e.g. a HashJoin's
        // build side) so the plan fanned to cores is self-contained — no
        // Exchange node may reach a Data-Plane core.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { as_aggregate },
        })) => {
            let child = match Box::pin(resolve_exchange(
                state,
                database_id,
                tenant_id,
                *child,
                trace_id,
                txn_id,
            ))
            .await?
            {
                Resolved::Plan(p) => p,
                Resolved::Gathered(resp, wms) => return Ok(Resolved::Gathered(resp, wms)),
                // A nested Exchange that itself resolved to a stream cannot be
                // re-wrapped by an outer Gather without materializing first;
                // surface it as the stream (the outer Gather is redundant —
                // nested root-level Gathers do not occur in practice, but if one
                // did, the inner stream is already the correct result).
                Resolved::Stream(s) => return Ok(Resolved::Stream(s)),
            };

            // Streaming fast path: a non-aggregate, unordered scan can stream
            // straight to the client without coordinator-side materialization.
            //
            // - Single-node (`gateway.is_none()`): fan to all local cores via
            //   `gather_all_cores_stream`.
            // - Cluster (`gateway.is_some()`): `gateway.execute_stream` routes
            //   the scan to its owning vShard — local cores when this node owns
            //   it, or the remote owner over QUIC (L4 streaming transport) —
            //   and merges the per-route streams with the same `select_all`.
            //
            // Aggregate gathers keep the materialize-then-merge behaviour.
            //
            // An in-transaction read (`txn_id.is_some()`) also keeps the
            // materialize path: streaming collapses per-core watermarks into one
            // value, but a transaction must record each participating shard's own
            // read version for optimistic-concurrency validation, so it takes the
            // `gather_all_vshards` branch below whose `GatherOutcome` preserves
            // `shard_watermarks`.
            if !as_aggregate && txn_id.is_none() && child.is_streamable_unordered_scan() {
                let stream = if let Some(gw) = state.gateway.as_ref() {
                    let ctx = crate::control::gateway::core::QueryContext {
                        tenant_id,
                        trace_id,
                        database_id,
                        txn_id: None,
                    };
                    // NOTE: cluster mode does not yet thread `txn_id` through
                    // `gateway.execute_stream` — cross-node in-transaction
                    // read-your-own-writes is a tracked gap; single-node
                    // (`gather_all_cores_stream` below) is fixed.
                    gw.execute_stream(&ctx, child).await?
                } else {
                    gather_all_cores_stream(state, tenant_id, database_id, child, trace_id, txn_id)?
                };
                return Ok(Resolved::Stream(stream));
            }

            let outcome: GatherOutcome =
                gather_all_vshards(state, tenant_id, database_id, child, trace_id, txn_id).await?;
            let payload = if as_aggregate {
                finalize_aggregate(&outcome.merged_array)
            } else {
                outcome.merged_array
            };
            Ok(Resolved::Gathered(
                outcome_to_response(payload, outcome.watermark_lsn),
                outcome.shard_watermarks,
            ))
        }

        // Root-level Broadcast: unusual but treat as Gather without merge.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast,
        })) => {
            let outcome =
                gather_all_vshards(state, tenant_id, database_id, *child, trace_id, txn_id).await?;
            Ok(Resolved::Gathered(
                outcome_to_response(outcome.merged_array, outcome.watermark_lsn),
                outcome.shard_watermarks,
            ))
        }

        // Root-level Shuffle: orchestrate a real cross-node grace hash join.
        // The child must be a `QueryOp::HashJoin` (shuffle wraps a complete hash
        // join); `super::shuffle` validates that, fans producers + consumers,
        // and returns the merged join rows as `Resolved::Gathered`.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Shuffle { keys, num_parts },
        })) => {
            super::shuffle::resolve_shuffle_join(
                state,
                database_id,
                tenant_id,
                *child,
                keys,
                num_parts,
                trace_id,
            )
            .await
        }

        // Root-level ShuffleAggregate: orchestrate a real cross-node distributed
        // GROUP BY shuffle. The child must be a `QueryOp::Aggregate` (shuffle
        // wraps a complete aggregate); `super::shuffle_aggregate` validates that,
        // fans the partial-state producers + per-part consumers, and returns the
        // merged finalized rows as `Resolved::Gathered`.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::ShuffleAggregate { keys, num_parts },
        })) => {
            super::shuffle_aggregate::resolve_shuffle_aggregate(
                state,
                database_id,
                tenant_id,
                *child,
                keys,
                num_parts,
                trace_id,
            )
            .await
        }

        // HashJoin: resolve Broadcast children embedded in left_input / right_input.
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
            post_group_by,
            post_aggregates,
            projection,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
        }) => {
            let left_input =
                resolve_join_input(state, database_id, tenant_id, left_input, trace_id, txn_id)
                    .await?;
            let mut right_input =
                resolve_join_input(state, database_id, tenant_id, right_input, trace_id, txn_id)
                    .await?;

            // Cross-node build-side gather (cluster only).
            //
            // The HashJoin task routes to the LEFT (probe) collection's owning
            // vShard, where the LEFT side is scanned locally. The RIGHT (build)
            // collection is otherwise scanned BY NAME from that same node — but
            // a single-vShard-homed build collection may live on a DIFFERENT
            // node, so the by-name scan returns nothing and the join drops rows.
            //
            // When running in cluster mode (`gateway.is_some()`), and the build
            // side has not already been materialized by `resolve_join_input`
            // (i.e. `right_input` is still `None`), and `right_collection` names
            // a real user collection (catalog sides carry an empty name and are
            // already embedded as a ProviderScan), gather the build collection
            // across all vShards on the coordinator and inline it as a
            // `ProviderScan`. The HashJoin shipped to the probe node is then
            // self-contained. Only the RIGHT/build side is gathered; the
            // LEFT/probe side stays local to the routed vShard.
            if state.gateway.is_some() && right_input.is_none() && !right_collection.is_empty() {
                right_input = gather_join_build_side(
                    state,
                    database_id,
                    tenant_id,
                    &right_collection,
                    trace_id,
                    txn_id,
                )
                .await?;
            }

            Ok(Resolved::Plan(PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on,
                join_type,
                limit,
                post_group_by,
                post_aggregates,
                projection,
                post_filters,
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
            })))
        }

        // All other plan variants: pass through unchanged.
        other => Ok(Resolved::Plan(other)),
    }
}

/// Resolve a `HashJoin` input slot.
///
/// When the slot contains an `Exchange{Broadcast}` child, gathers the child to
/// the coordinator and replaces the slot with a `ProviderScan{None, merged_array}`.
/// When the slot is already a `ProviderScan{None, ..}` or `None`, it is
/// returned unchanged.
async fn resolve_join_input(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    input: Option<Box<PhysicalPlan>>,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    let Some(boxed) = input else {
        return Ok(None);
    };

    match *boxed {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast,
        })) => {
            // Gather the broadcast child to the coordinator, then embed the
            // merged msgpack array as an inline ProviderScan.
            //
            // We use `merged_array` (not `raw`) because `merged_array` is a
            // single well-formed msgpack array.  The Data-Plane executor
            // materialises the ProviderScan via `response_with_payload(rows)`,
            // producing a Response whose payload is exactly `merged_array`.
            // `decode_response_to_docs` in `hash_handlers.rs` then reads that
            // Response as a msgpack array — so the two shapes match.
            let outcome =
                gather_all_cores(state, tenant_id, database_id, *child, trace_id, txn_id).await?;
            let provider_scan = PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: flatten_to_relational_rows(&outcome.merged_array),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            });
            Ok(Some(Box::new(provider_scan)))
        }

        // Exchange{Shuffle} inside a join input is never a shape the emit
        // produces: a shuffle wraps a WHOLE hash join (the root arm), so it
        // cannot appear as one join's input. Reject with a clear message rather
        // than speculatively implementing an unreachable nesting.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            mode: ExchangeMode::Shuffle { .. },
            ..
        })) => Err(crate::Error::Internal {
            detail: "ExchangeMode::Shuffle is only valid wrapping a complete hash join, \
                     not as a join input"
                .into(),
        }),

        // Exchange{ShuffleAggregate} inside a join input is never a shape the
        // emit produces: a shuffle-aggregate wraps a WHOLE root aggregate, so it
        // cannot appear as one join's input. Reject with a clear message rather
        // than speculatively implementing an unreachable nesting (mirrors the
        // Shuffle rejection above).
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            mode: ExchangeMode::ShuffleAggregate { .. },
            ..
        })) => Err(crate::Error::Internal {
            detail: "ExchangeMode::ShuffleAggregate is only valid wrapping a complete root \
                     aggregate, not as a join input"
                .into(),
        }),

        // Exchange{Gather} inside a join input: unusual but execute and embed.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { as_aggregate },
        })) => {
            let outcome =
                gather_all_cores(state, tenant_id, database_id, *child, trace_id, txn_id).await?;
            let merged = if as_aggregate {
                finalize_aggregate(&outcome.merged_array)
            } else {
                outcome.merged_array
            };
            let provider_scan = PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: flatten_to_relational_rows(&merged),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            });
            Ok(Some(Box::new(provider_scan)))
        }

        // Already resolved (ProviderScan{None, ..} or any other plan):
        // pass through.
        other => Ok(Some(Box::new(other))),
    }
}

/// Gather a HashJoin build collection across all vShards and inline it as a
/// `ProviderScan` (cluster mode only).
///
/// Looks up `collection`'s engine in the catalog, builds a minimal unfiltered
/// full-collection scan for that engine, gathers it across all vShards via the
/// gateway, and embeds the merged rows as a `ProviderScan{provider: None, rows}`
/// — mirroring the embedding shape used by `resolve_join_input`.
///
/// Returns `Ok(None)` (the name-scan fallback) when the catalog has no record
/// for `collection`. This is a graceful degradation, never an error: a missing
/// catalog entry on the coordinator falls back to the existing by-name scan on
/// the executing node.
async fn gather_join_build_side(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    // Build a minimal, unfiltered, unprojected full-collection scan for the
    // engine via the shared builder. `Ok(None)` (no catalog / unknown
    // collection) keeps the existing graceful name-scan fallback — never an
    // error. The build side of a hash join must be COMPLETE (every build row is
    // needed for correct match output); the shared builder uses an unbounded
    // scan, which is allocation-safe (see `full_scan`).
    //
    // (Memory for a very large build relation is the inherent cost of a hash
    // join; spill-to-disk is a future optimization, not a reason to truncate. The
    // probe side's local name-scan and the converter's 10k default for unbounded
    // SELECTs remain separately capped — that engine-wide unbounded-scan limit is
    // its own effort and is the remaining truncation source, TRACKED.)
    let Some(scan_plan) =
        crate::control::server::exchange::full_scan::full_scan_plan_for_collection(
            state,
            database_id,
            tenant_id,
            collection,
        )?
    else {
        // No catalog on this node, or unknown collection: fall back to name-scan.
        return Ok(None);
    };

    // `Box::pin` breaks the async-fn recursion cycle: `gather_all_vshards`
    // dispatches through the gateway, which re-enters `resolve_exchange_in_plan`
    // → `resolve_exchange` → here. The cycle terminates at runtime (the scan
    // plan is Exchange-free), but the future must be heap-indirected so its size
    // is finite.
    let outcome = Box::pin(gather_all_vshards(
        state,
        tenant_id,
        database_id,
        scan_plan,
        trace_id,
        txn_id,
    ))
    .await?;

    Ok(Some(Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: None,
        rows: flatten_to_relational_rows(&outcome.merged_array),
        filters: Vec::new(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
    }))))
}
