// SPDX-License-Identifier: BUSL-1.1

//! `resolve_read`: walk the clone chain for ONE physical task and build its
//! source-side twins.

use nodedb_types::{CloneOrigin, CloneStatus, DatabaseId, Lsn, TenantId};

use crate::control::server::shared::plan_util::extract_collection;
use crate::control::state::SharedState;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::metadata::ClonePredicatesNote;
use super::refusal::SourceRewrite;
use super::rewrite::rewrite_plan_for_source;

/// Parameters for the clone read resolver.
pub struct CloneReadParams {
    /// The read's `AS OF SYSTEM TIME` milliseconds, `None` for a current read.
    /// Only a read of a live clone maps it to an LSN (T_lsn), through this
    /// node's WAL time anchors. A read of any other collection never consults
    /// the anchors, so a historical read of versions that predate them (a
    /// restored backup's) is answered from the versions themselves.
    pub as_of_ms: Option<i64>,
}

/// Outcome of attempting to resolve a clone read for one task.
pub enum ResolveOutcome {
    /// The query time predates the clone's creation — return empty + note.
    PreDatesClone(ClonePredicatesNote),
    /// `target_task` plus every source-side task the chain walk produced.
    Augmented {
        /// Boxed: `PhysicalTask` embeds `PhysicalPlan`, the crate's largest
        /// enum, which inline blows this variant's size far past
        /// `PreDatesClone`'s.
        target_task: Box<PhysicalTask>,
        source_tasks: Vec<PhysicalTask>,
        /// Collection key for tombstone lookups, e.g. `"1/users"`.
        target_collection_key: String,
        /// Clone predation note, `None` unless `T_lsn < clone_created_at`.
        note: Option<ClonePredicatesNote>,
    },
}

/// Attempt to resolve `task` against a cloned collection.
///
/// Returns `None` when the collection has no clone origin (fast path: zero
/// overhead). Returns `Some(ResolveOutcome)` when resolution is required.
pub async fn resolve_read(
    state: &SharedState,
    task: PhysicalTask,
    tenant_id: TenantId,
    params: &CloneReadParams,
) -> crate::Result<Option<ResolveOutcome>> {
    let db_id = task.database_id;

    // The shared extractor sees through the `Exchange` / `PostProcess`
    // wrappers the converter puts over every sharded read; a clone-local
    // copy drifts out of sync and misreads those as "not a clone".
    let Some(raw_coll) = extract_collection(&task.plan) else {
        return Ok(None);
    };
    // Strip the database prefix that db_qualified() prepends, e.g. "1/users" → "users".
    let coll_name = super::rewrite::strip_db_prefix(db_id, raw_coll);

    // Short-circuit: not a clone or fully materialized.
    let Some(live) = live_clone(state, db_id, tenant_id, coll_name, "get_collection")? else {
        return Ok(None);
    };

    // UNION DISTINCT/INTERSECT/EXCEPT dedup/subtract this task's response
    // against siblings' by exact row match — unsound without proven parity.
    if !matches!(task.post_set_op, PostSetOp::None) {
        return Err(crate::Error::PlanError {
            detail: format!(
                "a set operation over '{coll_name}' cannot be read through an unmaterialized \
                 clone; run ALTER DATABASE <clone> MATERIALIZE first"
            ),
        });
    }

    // T_lsn: the highest LSN committed by the `AS OF` time, and a time before
    // the oldest retained anchor is refused. Without one, the WAL frontier.
    let query_lsn = match params.as_of_ms {
        Some(as_of_ms) => state.ms_to_lsn(as_of_ms)?,
        None => state.wal.next_lsn(),
    };

    // Bitemporal correctness: check if T_lsn < clone_created_at.
    if query_lsn < live.origin.clone_created_at {
        return Ok(Some(ResolveOutcome::PreDatesClone(
            ClonePredicatesNote::new(query_lsn, live.origin.clone_created_at),
        )));
    }

    let first = ChainLevel {
        db_id,
        coll_name: coll_name.to_string(),
        live,
    };
    let source_tasks = walk_clone_chain(state, &task, tenant_id, query_lsn, first).await?;
    let target_collection_key =
        crate::control::planner::sql_plan_convert::convert::db_qualified(db_id, coll_name);

    Ok(Some(ResolveOutcome::Augmented {
        target_task: Box::new(task),
        source_tasks,
        target_collection_key,
        note: None,
    }))
}

/// A clone collection that is not yet materialized.
struct LiveClone {
    origin: CloneOrigin,
    bitemporal: bool,
}

/// One level of the clone chain: the clone a rewrite reads through.
struct ChainLevel {
    db_id: DatabaseId,
    coll_name: String,
    live: LiveClone,
}

/// The collection `name` as a clone that is not yet materialized. `None`
/// for a missing collection, a collection that is no clone, and a
/// materialized clone. `lookup` names the lookup in a catalog error.
fn live_clone(
    state: &SharedState,
    db_id: DatabaseId,
    tenant_id: TenantId,
    name: &str,
    lookup: &str,
) -> crate::Result<Option<LiveClone>> {
    let desc = state
        .credentials
        .catalog()
        .get_collection(db_id, tenant_id.as_u64(), name)
        .map_err(|e| crate::Error::Storage {
            engine: "catalog".into(),
            detail: format!("clone resolver: {lookup} failed: {e}"),
        })?;
    let Some(desc) = desc else {
        return Ok(None);
    };
    // A materialized clone holds all its data itself.
    match desc.clone_status {
        CloneStatus::Materialized => return Ok(None),
        CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
    }
    let bitemporal = desc.bitemporal;
    Ok(desc
        .cloned_from
        .map(|origin| LiveClone { origin, bitemporal }))
}

/// Walk source-side tasks up the clone chain until `cloned_from = None` or
/// `Materialized`. `MAX_CLONE_DEPTH` bounds the chain at create time; the
/// walk still caps at 8 levels as a guard against catalog corruption.
async fn walk_clone_chain(
    state: &SharedState,
    task: &PhysicalTask,
    tenant_id: TenantId,
    query_lsn: Lsn,
    first: ChainLevel,
) -> crate::Result<Vec<PhysicalTask>> {
    const MAX_WALK: u32 = 8;
    let mut source_tasks: Vec<PhysicalTask> = Vec::new();
    // Template for the next rewrite. After each level it holds the tasks
    // pushed, so the next level rewrites the correct per-level qualified name
    // rather than the original target task.
    let mut prev_level_tasks: Vec<PhysicalTask> = vec![task.clone()];
    let mut level = first;
    for _ in 0..MAX_WALK {
        let this_level_tasks =
            rewrite_level(state, &prev_level_tasks, &level, tenant_id, query_lsn).await?;
        source_tasks.extend(this_level_tasks.iter().cloned());
        prev_level_tasks = this_level_tasks;

        // The walk continues while the source is itself a live clone.
        let src_db_id = level.live.origin.source_database;
        let src_coll_name = level.live.origin.source_collection.clone();
        let Some(ancestor) = live_clone(
            state,
            src_db_id,
            tenant_id,
            &src_coll_name,
            "ancestor get_collection",
        )?
        else {
            break;
        };
        level = ChainLevel {
            db_id: src_db_id,
            coll_name: src_coll_name,
            live: ancestor,
        };
    }
    Ok(source_tasks)
}

/// Rewrite each task of the previous level into a task on this level's
/// source collection.
async fn rewrite_level(
    state: &SharedState,
    prev_level_tasks: &[PhysicalTask],
    level: &ChainLevel,
    tenant_id: TenantId,
    query_lsn: Lsn,
) -> crate::Result<Vec<PhysicalTask>> {
    let origin = &level.live.origin;
    let src_db_id = origin.source_database;
    let src_coll_name = origin.source_collection.as_str();
    // Effective source LSN: min(T_lsn, as_of_lsn), as wall-ms for the engine.
    let effective_source_lsn = if query_lsn > origin.as_of_lsn {
        origin.as_of_lsn
    } else {
        query_lsn
    };
    let effective_source_ms = crate::control::clone::lsn_resolve::source_as_of_ms(
        state,
        level.live.bitemporal,
        effective_source_lsn,
    );

    let mut tasks: Vec<PhysicalTask> = Vec::new();
    for level_task in prev_level_tasks {
        // An unsupported read shape over the cloned collection returns an
        // error here, not `NoSourceTask` — it propagates to the client
        // instead of quietly producing a target-only answer.
        let rewritten = rewrite_plan_for_source(super::rewrite::RewriteForSourceParams {
            plan: &level_task.plan,
            target_db_id: level.db_id,
            source_db_id: src_db_id,
            tenant_id,
            target_coll: &level.coll_name,
            source_coll: src_coll_name,
            effective_source_ms,
            kv_surrogate_ceiling: origin.kv_surrogate_ceiling,
            state,
        })
        .await?;
        let SourceRewrite::Task(source_plan) = rewritten else {
            continue;
        };
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: nodedb_types::CollectionKey::from_bare(src_db_id, src_coll_name).vshard(),
            database_id: src_db_id,
            plan: *source_plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }
    Ok(tasks)
}
