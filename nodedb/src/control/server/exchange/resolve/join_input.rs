// SPDX-License-Identifier: BUSL-1.1

//! HashJoin input-slot resolution: embed Broadcast/Gather children as inline
//! `ProviderScan`s and gather cross-node build sides on the coordinator.

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};

use crate::control::server::exchange::read_scope::ReadScope;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::flatten_to_relational_rows;

use crate::control::server::exchange::full_scan::{ScanSide, full_scan_plan_for_collection};
use crate::control::server::exchange::gather::gather_all_vshards;
use crate::control::server::exchange::gather_outcome::GatherOutcome;

use super::array_read_rows::is_cluster_array_read;
use super::capture::DistributedReadCapture;
use super::exchange::{
    ChildRows, emits_search_hits, materialize_child_rows, provider_scan_of_rows,
};
use crate::control::server::shared::authorization::gathered_sources;

/// Resolve a `HashJoin` input slot.
///
/// When the slot contains an `Exchange{Broadcast}` child, gathers the child to
/// the coordinator and replaces the slot with a `ProviderScan{None, rows}`.
/// When the slot is already a `ProviderScan{None, ..}` or `None`, it is
/// returned unchanged.
///
/// `captures` accumulates a [`DistributedReadCapture`] for an embedded
/// `Exchange{Gather}` input's base collection (in-transaction reads only) so the
/// materialized side is validated at commit like every other distributed read.
pub(super) async fn resolve_join_input(
    state: &SharedState,
    scope: ReadScope,
    input: Option<Box<PhysicalPlan>>,
    captures: &mut Vec<DistributedReadCapture>,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    let ReadScope {
        database_id,
        tenant_id,
        txn_id,
        ..
    } = scope;
    let Some(boxed) = input else {
        return Ok(None);
    };

    match *boxed {
        // Search hits carry a surrogate and a stored body: the join reads
        // them as rows only after the body's columns and the primary key are
        // resolved, as every other consumer of a search result does.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast | ExchangeMode::Gather { .. },
        })) if emits_search_hits(&child) => {
            Box::pin(search_hits_input(state, scope, captures, *child)).await
        }

        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast,
        })) => {
            // Gather the broadcast child to the coordinator, then embed its
            // rows as one msgpack array in an inline ProviderScan.
            //
            // The Data-Plane executor materialises the ProviderScan via
            // `response_with_payload(rows)`, producing a Response whose
            // payload is exactly those rows. `decode_response_to_docs` in
            // `hash_handlers.rs` then reads that Response as a msgpack
            // array, so the two shapes match.
            // The gather reads every vShard of the child from its owner, not
            // only the groups this node replicates, and confirms each leg as
            // `scope` requires.
            let sources = gathered_sources(&child, database_id)?;
            let array_rows = is_cluster_array_read(&child);
            let outcome = Box::pin(gather_all_vshards(state, *child, scope)).await?;
            let rows = relational_rows(array_rows, &outcome, false)?;
            Ok(Some(Box::new(provider_scan_of_rows(rows, sources))))
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
            let sources = gathered_sources(&child, database_id)?;
            let array_rows = is_cluster_array_read(&child);
            // Read from every vShard's owner, as the Broadcast arm does.
            let outcome = Box::pin(gather_all_vshards(state, *child, scope)).await?;
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
                    read_versions: outcome.read_versions.clone(),
                });
            }
            let rows = relational_rows(array_rows, &outcome, as_aggregate)?;
            Ok(Some(Box::new(provider_scan_of_rows(rows, sources))))
        }

        // Already resolved (ProviderScan{None, ..} or any other plan):
        // pass through.
        other => Ok(Some(Box::new(other))),
    }
}

/// The rows of a search-hit join input, inlined as a `ProviderScan`. Kept out
/// of [`resolve_join_input`] so that function's frame stays small: the
/// resolution recurses once per nested join input.
async fn search_hits_input(
    state: &SharedState,
    scope: ReadScope,
    captures: &mut Vec<DistributedReadCapture>,
    child: PhysicalPlan,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    match Box::pin(materialize_child_rows(state, scope, captures, child)).await? {
        ChildRows::Rows { rows, sources } => {
            Ok(Some(Box::new(provider_scan_of_rows(rows, sources))))
        }
        ChildRows::Passthrough(_) => Err(crate::Error::Internal {
            detail: "a search join input resolved to a root result".into(),
        }),
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
/// as the local name-scan this gather replaces filtered them.
///
/// Returns `Ok(None)` (the name-scan fallback) when the catalog has no record
/// for the collection. This is graceful degradation, never an error: a missing
/// catalog entry on the coordinator falls back to the existing by-name scan on
/// the executing node.
pub(super) async fn gather_join_build_side(
    state: &SharedState,
    scope: ReadScope,
    side: ScanSide<'_>,
    captures: &mut Vec<DistributedReadCapture>,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    let ReadScope {
        database_id,
        tenant_id,
        txn_id,
        ..
    } = scope;
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
    let sources = gathered_sources(&scan_plan, database_id)?;
    let outcome = Box::pin(gather_all_vshards(state, scan_plan, scope)).await?;

    if let Some(scan_plan) = capture_plan {
        captures.push(DistributedReadCapture {
            scan_plan,
            read_versions: outcome.read_versions.clone(),
        });
    }

    Ok(Some(Box::new(provider_scan_of_rows(
        flatten_to_relational_rows(&outcome.merged_rows(false)?),
        sources,
    ))))
}

/// The relational rows of a gathered join input. A cluster array read
/// (`array_rows`) keeps its rows as it emits them. Any other plan's merged
/// rows pass through the storage flatten. `as_aggregate` finalizes the
/// merged rows of a `Gather{as_aggregate}`.
fn relational_rows(
    array_rows: bool,
    outcome: &GatherOutcome,
    as_aggregate: bool,
) -> crate::Result<Vec<u8>> {
    if array_rows {
        return outcome.merged_array();
    }
    Ok(flatten_to_relational_rows(
        &outcome.merged_rows(as_aggregate)?,
    ))
}
