// SPDX-License-Identifier: BUSL-1.1

//! `PostProcess` exchange resolution: materialize the body's rows on the
//! coordinator once, then lower to a `ProviderScan` for the relational tail.

use nodedb_physical::physical_plan::{
    ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp, SortKeySpec, TextOp, VectorOp,
};

use crate::control::server::exchange::full_scan::{ScanSide, full_scan_plan_for_collection};
use crate::control::server::exchange::gather::{
    GatherOutcome, finalize_aggregate, gather_all_vshards,
};
use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::server::response_translate::hit_key::parse_surrogate_hex;
use crate::control::server::response_translate::vector::resolve_surrogate_pk;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::{
    flatten_hybrid_hits_to_relational_rows, flatten_to_relational_rows,
    flatten_vector_hits_to_relational_rows,
};

use super::dispatch::{ResolveCtx, resolve_exchange};
use super::entry::Resolved;

/// Fields of a `QueryOp::PostProcess` plan node, carried through resolution
/// as one value instead of as individually threaded arguments.
pub(super) struct PostProcessFields {
    pub input: Box<PhysicalPlan>,
    pub filters: Vec<u8>,
    pub projection: Vec<String>,
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
    let ResolveCtx {
        database_id,
        tenant_id,
        trace_id,
        txn_id,
    } = ctx;
    let PostProcessFields {
        input,
        filters,
        projection,
        sort_keys,
        limit,
        offset,
        distinct,
    } = fields;

    // The converter wraps a sharded body in `Exchange{Gather}`; unwrap
    // it so the child is the real body plan (a plain body has no
    // wrapper and routes to its owning vShard directly).
    let (child, as_aggregate) = match *input {
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
        // surface these defensively without dropping post-processing.
        Resolved::Gathered(resp, wms, caps) => {
            return Ok(Resolved::Gathered(resp, wms, caps));
        }
        Resolved::Stream(s) => return Ok(Resolved::Stream(s)),
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

    let outcome: GatherOutcome =
        gather_all_vshards(state, tenant_id, database_id, child, trace_id, txn_id).await?;

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
    Ok(Resolved::Plan(Box::new(PhysicalPlan::Query(
        QueryOp::ProviderScan {
            provider: None,
            rows,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
        },
    ))))
}
