// SPDX-License-Identifier: BUSL-1.1

//! `PostProcess` exchange resolution: materialize the body's rows on the
//! coordinator once, then lower to a `ProviderScan` for the relational tail.

use nodedb_physical::physical_plan::{
    ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp, SortKeySpec, TextOp, VectorOp,
    plan_contains_cluster_partitioned_leaf,
};

use crate::control::server::exchange::full_scan::{ScanSide, full_scan_plan_for_collection};
use crate::control::server::exchange::gather::{
    GatherOutcome, finalize_aggregate, gather_all_vshards,
};
use crate::control::server::exchange::owning_core::gather_single_owning_core;
use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::server::response_translate::hit_key::parse_surrogate_hex;
use crate::control::server::response_translate::vector::resolve_surrogate_pk;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::{
    flatten_hybrid_hits_to_relational_rows, flatten_to_relational_rows,
    flatten_vector_hits_to_relational_rows,
};
use crate::types::VShardId;

use super::dispatch::{ResolveCtx, resolve_exchange};
use super::entry::Resolved;

/// Fields of a `QueryOp::PostProcess` plan node, carried through resolution
/// as one value instead of as individually threaded arguments.
pub(super) struct PostProcessFields {
    pub input: Box<PhysicalPlan>,
    pub filters: Vec<u8>,
    pub projection: Vec<String>,
    pub computed_columns: Vec<u8>,
    pub window_functions: Vec<u8>,
    pub sort_keys: Vec<SortKeySpec>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub distinct: bool,
}

/// The row shape a `PostProcess` body produces, driving how its gathered
/// payload is flattened into bare relational rows.
enum HitShape {
    /// Vector / sparse / multi-vector hits: `{id: <surrogate>, distance,
    /// doc_id?, body?}`. Merge the document `body` to top-level and resolve
    /// the surrogate to the user PK.
    Vector,
    /// Hybrid (RRF) fusion hits: `{doc_id: <surrogate hex>, <score alias>,
    /// ...}` with no body. Resolve `doc_id` to the user PK as `id`.
    Hybrid,
    /// Flat storage rows (`{id, data}` document / text, columnar, spatial) or
    /// computed rows — already fully columned after the storage flatten.
    None,
}

/// Classify a resolved `PostProcess` child by the row shape its engine emits.
fn classify_hit_shape(plan: &PhysicalPlan) -> HitShape {
    match plan {
        PhysicalPlan::Vector(
            VectorOp::Search { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::MultiVectorScoreSearch { .. },
        ) => HitShape::Vector,
        PhysicalPlan::Text(TextOp::HybridSearch { .. } | TextOp::HybridSearchTriple { .. }) => {
            HitShape::Hybrid
        }
        _ => HitShape::None,
    }
}

/// Collection a `PostProcess` child reads, for the surrogate→PK resolver.
///
/// The search ops that emit surrogate-keyed hits carry their collection in a
/// field `PhysicalPlan::collection` does not surface (sparse / multi-vector),
/// so match them explicitly; every other body defers to `collection()`.
fn hit_collection_name(plan: &PhysicalPlan) -> Option<String> {
    match plan {
        PhysicalPlan::Vector(
            VectorOp::Search { collection, .. }
            | VectorOp::MultiSearch { collection, .. }
            | VectorOp::SparseSearch { collection, .. }
            | VectorOp::MultiVectorScoreSearch { collection, .. },
        )
        | PhysicalPlan::Text(
            TextOp::HybridSearch { collection, .. } | TextOp::HybridSearchTriple { collection, .. },
        ) => Some(collection.to_string()),
        other => other.collection().map(str::to_owned),
    }
}

/// A `ProviderScan` carrying final `rows` and an empty relational tail:
/// no filter, projection, computed column, window, sort, limit, offset, or
/// distinct. The shape every coordinator-materialized child is embedded as.
pub(crate) fn provider_scan_of_rows(rows: Vec<u8>) -> PhysicalPlan {
    PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: None,
        rows,
        filters: Vec::new(),
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
    })
}

/// Rows of a materialized child, or a resolution the caller returns as-is.
pub(super) enum ChildRows {
    /// The child's rows, flattened to the bare relational row shape a
    /// `ProviderScan{provider: None}` consumes.
    Rows(Vec<u8>),
    /// The child resolved to a root `Gathered` / `Stream` result. The caller
    /// returns it unchanged.
    Passthrough(Resolved),
}

/// Materialize a coordinator-side child plan into relational rows.
///
/// Unwraps the converter's `Exchange{Gather}` wrapper, resolves any Exchange
/// nested inside the body (a `HashJoin` build-side `Broadcast`), gathers the
/// body across all vShards, finalizes a partial-aggregate payload, and
/// flattens hit-shaped payloads (vector / hybrid) to columned rows with the
/// surrogate resolved to the user PK. An in-transaction read records the
/// child's base collection in `captures` at its observed read-version.
pub(super) async fn materialize_child_rows(
    state: &SharedState,
    ctx: ResolveCtx,
    captures: &mut Vec<DistributedReadCapture>,
    input: PhysicalPlan,
) -> crate::Result<ChildRows> {
    let ResolveCtx {
        database_id,
        tenant_id,
        trace_id,
        txn_id,
    } = ctx;

    // The converter wraps a sharded body in `Exchange{Gather}`; unwrap
    // it so the child is the real body plan (a plain body has no
    // wrapper and routes to its owning vShard directly).
    let (child, as_aggregate) = match input {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { as_aggregate },
        })) => (*child, as_aggregate),
        other => (other, false),
    };

    // Resolve any Exchange nested inside the child first (e.g. a
    // `HashJoin` build-side `Broadcast`) so the plan gathered below is
    // self-contained — no Exchange may reach a Data-Plane core.
    let child = match Box::pin(resolve_exchange(
        state,
        database_id,
        tenant_id,
        child,
        trace_id,
        txn_id,
        captures,
    ))
    .await?
    {
        Resolved::Plan(p) => *p,
        // The unwrapped body is not itself a root Gather / stream;
        // surface these without dropping the caller's tail.
        Resolved::Gathered(resp, wms, caps) => {
            return Ok(ChildRows::Passthrough(Resolved::Gathered(resp, wms, caps)));
        }
        Resolved::Stream(s) => return Ok(ChildRows::Passthrough(Resolved::Stream(s))),
    };

    // Classify the body's row shape so the gathered payload is
    // flattened correctly:
    //  - `Vector`  → vector/sparse/multivec hits (`{id, distance,
    //    doc_id, body}`): merge the document `body` to top-level and
    //    resolve the surrogate to the user PK.
    //  - `Hybrid`  → RRF fusion hits (`{doc_id: hex, <score alias>}`,
    //    no body): resolve `doc_id` to the user PK as `id`.
    //  - `None`    → flat storage rows (document / text `{id, data}`,
    //    columnar, spatial) or computed rows: the ordinary storage
    //    flatten already exposes every column.
    // `collection` and `hit_kind` are captured before the gather
    // consumes `child`.
    let hit_kind = classify_hit_shape(&child);
    // Extract the collection from the hit op directly: `collection()`
    // has no arm for sparse / multi-vector search, so it would yield
    // `None` and the PK resolver would be handed an empty collection.
    let hit_collection = hit_collection_name(&child);

    // Record the child's single base collection in the in-transaction
    // read-set at its own observed read-version (mirrors the root
    // Gather arm). Autocommit reads skip the catalog lookup.
    let probe_collection: Option<String> = if txn_id.is_some() {
        hit_collection.clone()
    } else {
        None
    };

    // A coordinator-local body (a `ProviderScan` carrying embedded rows, a
    // nested `PostProcess`) reads no per-shard collection. It runs exactly
    // once on the coordinator vshard: fanning it to every core returns its
    // rows once per core.
    let coordinator_local = child.collection().is_none()
        && !child.is_sharded_source()
        && !plan_contains_cluster_partitioned_leaf(&child);
    let outcome: GatherOutcome = if coordinator_local {
        gather_single_owning_core(
            state,
            tenant_id,
            database_id,
            child,
            VShardId::from_collection_in_database(database_id, ""),
            trace_id,
            txn_id,
        )
        .await?
    } else {
        gather_all_vshards(state, tenant_id, database_id, child, trace_id, txn_id).await?
    };

    if let Some(coll) = probe_collection
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

    // Flatten to the bare relational row shape the `ProviderScan` tail
    // consumes, resolving surrogate→PK for hit-shaped bodies via the
    // catalog so `SELECT id` returns the user PK, not the surrogate.
    let coll = hit_collection.unwrap_or_default();
    let rows = match hit_kind {
        HitShape::Vector => flatten_vector_hits_to_relational_rows(&merged, |surrogate| {
            resolve_surrogate_pk(
                state,
                database_id,
                tenant_id,
                &coll,
                nodedb_types::Surrogate::new(surrogate),
            )
        }),
        HitShape::Hybrid => flatten_hybrid_hits_to_relational_rows(&merged, |key| {
            let surrogate = parse_surrogate_hex(key)?;
            resolve_surrogate_pk(state, database_id, tenant_id, &coll, surrogate)
        }),
        HitShape::None => flatten_to_relational_rows(&merged),
    };
    Ok(ChildRows::Rows(rows))
}

/// Resolve a `QueryOp::PostProcess` node: materialize the child's rows on the
/// coordinator, then lower to a `ProviderScan` that applies filter → offset →
/// sort → distinct → project → limit on a single core (its existing tail).
/// This keeps "run exactly once over the full union" correct: the child is
/// gathered here, so the relational tail never runs per-shard.
pub(super) async fn resolve_post_process(
    state: &SharedState,
    ctx: ResolveCtx,
    captures: &mut Vec<DistributedReadCapture>,
    fields: PostProcessFields,
) -> crate::Result<Resolved> {
    let PostProcessFields {
        input,
        filters,
        projection,
        computed_columns,
        window_functions,
        sort_keys,
        limit,
        offset,
        distinct,
    } = fields;

    let rows = match materialize_child_rows(state, ctx, captures, *input).await? {
        ChildRows::Rows(rows) => rows,
        ChildRows::Passthrough(resolved) => return Ok(resolved),
    };
    Ok(Resolved::Plan(Box::new(PhysicalPlan::Query(
        QueryOp::ProviderScan {
            provider: None,
            rows,
            filters,
            projection,
            computed_columns,
            window_functions,
            sort_keys,
            limit,
            offset,
            distinct,
        },
    ))))
}
