// SPDX-License-Identifier: BUSL-1.1

//! HashJoin input-slot resolution: embed Broadcast/Gather children as inline
//! `ProviderScan`s and gather cross-node build sides on the coordinator.

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};

use crate::control::state::SharedState;
use crate::data::executor::response_codec::flatten_to_relational_rows;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId};

use crate::control::server::exchange::full_scan::{ScanSide, full_scan_plan_for_collection};
use crate::control::server::exchange::gather::{
    finalize_aggregate, gather_all_cores, gather_all_vshards,
};

use super::capture::DistributedReadCapture;

/// Resolve a `HashJoin` input slot.
///
/// When the slot contains an `Exchange{Broadcast}` child, gathers the child to
/// the coordinator and replaces the slot with a `ProviderScan{None, merged_array}`.
/// When the slot is already a `ProviderScan{None, ..}` or `None`, it is
/// returned unchanged.
///
/// `captures` accumulates a [`DistributedReadCapture`] for an embedded
/// `Exchange{Gather}` input's base collection (in-transaction reads only) so the
/// materialized side is validated at commit like every other distributed read.
pub(super) async fn resolve_join_input(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    input: Option<Box<PhysicalPlan>>,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
    captures: &mut Vec<DistributedReadCapture>,
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
                computed_columns: Vec::new(),
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
            // Capture this embedded gather's base collection for the transaction
            // read-set before the child plan is consumed by the gather (a bare
            // single-collection scan so `extract_collection` re-homes exactly
            // this collection's vshard at commit). In-transaction reads only.
            let child_collection: Option<String> = if txn_id.is_some() {
                child.collection().map(str::to_owned)
            } else {
                None
            };
            let outcome =
                gather_all_cores(state, tenant_id, database_id, *child, trace_id, txn_id).await?;
            if let Some(coll) = child_collection
                && let Some(scan_plan) = full_scan_plan_for_collection(
                    state,
                    database_id,
                    tenant_id,
                    ScanSide::read_set_only(&coll),
                )?
            {
                captures.push(DistributedReadCapture {
                    scan_plan,
                    read_version_lsn: outcome.read_version_lsn,
                });
            }
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
                computed_columns: Vec::new(),
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
/// `ProviderScan`.
///
/// Looks up the side's engine in the catalog, builds a full-collection scan
/// under that side's read policy, gathers it across all vShards via the
/// gateway, and embeds the merged rows as a `ProviderScan{provider: None, rows}`
/// — mirroring the embedding shape used by `resolve_join_input`.
///
/// `side` carries the collection together with the RLS filters injected for
/// it, so the gathered rows are filtered per side *before* the join, exactly
/// as the local name-scan this gather replaces would have filtered them.
///
/// Returns `Ok(None)` (the name-scan fallback) when the catalog has no record
/// for the collection. This is graceful degradation, never an error: a missing
/// catalog entry on the coordinator falls back to the existing by-name scan on
/// the executing node.
pub(super) async fn gather_join_build_side(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    side: ScanSide<'_>,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
    captures: &mut Vec<DistributedReadCapture>,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    // Build an unprojected full-collection scan for the engine via the shared
    // builder, carrying this side's read policy. `Ok(None)` (no catalog / unknown
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
            side,
        )?
    else {
        // No catalog on this node, or unknown collection: fall back to name-scan.
        return Ok(None);
    };

    // Capture the build/right collection's read for the transaction read-set:
    // its own bare full-collection scan plan + the read-version its gather
    // observes. Previously DISCARDED — the hole that left the build side out of
    // the read-set entirely, so a concurrent write to it between the
    // in-transaction read and COMMIT went undetected by the OCC validator. Clone
    // before the scan plan is moved into the gather; in-transaction reads only.
    let capture_plan = if txn_id.is_some() {
        Some(scan_plan.clone())
    } else {
        None
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

    if let Some(scan_plan) = capture_plan {
        captures.push(DistributedReadCapture {
            scan_plan,
            read_version_lsn: outcome.read_version_lsn,
        });
    }

    Ok(Some(Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: None,
        rows: flatten_to_relational_rows(&outcome.merged_array),
        filters: Vec::new(),
        projection: Vec::new(),
                computed_columns: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
    }))))
}
