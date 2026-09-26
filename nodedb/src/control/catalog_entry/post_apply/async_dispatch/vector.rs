// SPDX-License-Identifier: BUSL-1.1

//! Async post-apply for vector-index catalog entries.
//!
//! `PutVectorIndexParams` appends this node's `VectorParams` redo record and
//! installs the committed parameters on every core. `DeleteVectorIndexParams`
//! appends and fsyncs the drop record, then dispatches `VectorOp::DropIndex`.
//! Both run on every node, so a follower builds and tears down the index it
//! serves instead of learning about it at its next boot.
//!
//! ## Two plans install one row
//!
//! `VectorOp::SetParams` writes the build config a core uses when it first
//! materializes the index, and `execute_set_vector_params` refuses a core
//! whose index already materialized. `VectorOp::Rebuild` is the reverse: it
//! reshapes a materialized index in place and reports `NotFound` on a core
//! that has none. Each core must accept one of the two, so `Rebuild` follows
//! `SetParams` wherever `SetParams` was refused. A core that accepted neither
//! is the one this module reports.
//!
//! `Rebuild` reshapes the HNSW graph only. A quantization change against a
//! materialized index therefore lands in the catalog row and reaches the cores
//! at the next boot seed, which runs against unmaterialized state.
//!
//! Nothing here can propagate a failure: the catalog row is already
//! committed. Every failed stage files a `Capture` instead, because a node
//! silently missing an index is the defect this module exists to stop.

use std::sync::Arc;

use tokio::sync::oneshot;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::{MintedRecords, RecordOwner};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::StoredVectorIndexParams;

use super::core_fanout::{CoreFanout, DISPATCH_TIMEOUT, FanoutAnswers, fan_out};

/// The longest a parameter install waits for its cores to answer: the
/// `SetParams` dispatch deadline, then the reshape's. Its record's window
/// closes within this of its open.
pub(crate) fn longest_core_wait() -> std::time::Duration {
    DISPATCH_TIMEOUT.saturating_mul(2)
}

/// One vector index, named the way every stage below reports it.
struct IndexTarget<'a> {
    database_id: u64,
    tenant_id: u64,
    collection: &'a str,
    field_name: &'a str,
}

impl<'a> IndexTarget<'a> {
    fn of(entry: &'a StoredVectorIndexParams) -> Self {
        Self {
            database_id: entry.database_id,
            tenant_id: entry.tenant_id,
            collection: &entry.collection,
            field_name: &entry.field_name,
        }
    }
}

/// Where a parameter install stands when a core outlives the dispatch
/// deadline.
enum PutStage {
    /// Some cores have not answered `SetParams` yet.
    SetParams(FanoutAnswers),
    /// Every core answered `SetParams`. `refused` took the reshape instead,
    /// and some cores have not answered it yet.
    Rebuild {
        refused: Vec<usize>,
        rebuild: FanoutAnswers,
    },
}

/// Build the `SetParams` plan the boot seed and the CREATE handler both
/// reproduce, so runtime and restart install identical parameters.
fn set_params_plan(entry: &StoredVectorIndexParams) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SetParams {
        collection: nodedb_types::QualifiedCollection::new(
            DatabaseId::new(entry.database_id),
            &entry.collection,
        ),
        field_name: entry.field_name.clone(),
        dim: entry.dim,
        m: entry.m,
        ef_construction: entry.ef_construction,
        metric: entry.metric.clone(),
        index_type: entry.index_type.clone(),
        pq_m: entry.pq_m,
        ivf_cells: entry.ivf_cells,
        ivf_nprobe: entry.ivf_nprobe,
    })
}

/// Build the in-place reshape plan for a core whose index already
/// materialized, so it converges on the same graph shape a fresh core builds
/// from `SetParams`.
///
/// `m0` is derived as `2 * m`, the ratio every other install path applies.
fn rebuild_plan(entry: &StoredVectorIndexParams) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::Rebuild {
        collection: nodedb_types::QualifiedCollection::new(
            DatabaseId::new(entry.database_id),
            &entry.collection,
        ),
        field_name: entry.field_name.clone(),
        m: entry.m,
        m0: entry.m * 2,
        ef_construction: entry.ef_construction,
    })
}

/// Build the `DropIndex` plan for one index.
fn drop_index_plan(database_id: u64, collection: &str, field_name: &str) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::DropIndex {
        collection: nodedb_types::QualifiedCollection::new(
            DatabaseId::new(database_id),
            collection,
        ),
        field_name: field_name.to_string(),
    })
}

/// Install one vector index's build parameters on this node: append the redo
/// record, then bring every core to the committed parameters.
///
/// The install owns its record's outcome-floor window, so it runs in a task
/// the caller does not own: a caller dropped mid-install leaves the task to
/// close the window. The call returns once every core answered or the
/// dispatch deadline passed. Cores still working then are waited for by the
/// task.
///
/// The single-node DDL handlers call this directly, where no applier runs and
/// the post-apply lane never fires.
pub async fn put_async(entry: StoredVectorIndexParams, shared: Arc<SharedState>) {
    let (ready_tx, ready_rx) = oneshot::channel();
    tokio::spawn(install_params(entry, shared, ready_tx));
    if ready_rx.await.is_err() {
        tracing::error!("the vector index install task ended before it reported");
    }
}

async fn install_params(
    entry: StoredVectorIndexParams,
    shared: Arc<SharedState>,
    ready: oneshot::Sender<()>,
) {
    let target = IndexTarget::of(&entry);
    let plan = set_params_plan(&entry);

    // The record makes this node's log self-sufficient: replay rebuilds the
    // index from it in LSN order alongside the vector writes around it. Its
    // outcome-floor window closes once every core gave its final answer.
    let minted = MintedRecords::open(&shared.outcome_floor);
    if let Err(error) = append_redo(&shared, &target, &plan, &minted) {
        report(&error, "set_params_wal_append", &target);
    }

    // The cores hold the record from here. It closes from their answers.
    minted.mark_sent();
    let set_params = fan_out(&shared, &fanout(&target), &plan).await;
    let stage = if set_params.pending.is_empty() {
        let refused = set_params.refused;
        if refused.is_empty() {
            minted.settle();
            // The caller can be gone. The install completes either way.
            let _ = ready.send(());
            return;
        }
        // Every refused core already holds a materialized index, which only
        // the in-place reshape reaches. A core that took `SetParams` answers
        // this with `NotFound` and stays on the parameters it accepted.
        let rebuild = fan_out(&shared, &fanout(&target), &rebuild_plan(&entry)).await;
        if rebuild.pending.is_empty() {
            close_put(&target, refused, rebuild.refused, minted);
            let _ = ready.send(());
            return;
        }
        PutStage::Rebuild { refused, rebuild }
    } else {
        PutStage::SetParams(set_params)
    };
    // Cores outlived the dispatch deadline. The caller moves on while this
    // task waits for their final answers.
    let _ = ready.send(());
    finish_put(&shared, &entry, stage, minted).await;
}

/// Resolve a handed-off parameter install once every core answered.
async fn finish_put(
    shared: &SharedState,
    entry: &StoredVectorIndexParams,
    stage: PutStage,
    minted: MintedRecords,
) {
    let target = IndexTarget::of(entry);
    let (refused, rebuild) = match stage {
        PutStage::SetParams(set_params) => {
            let refused = set_params.into_final_refusals().await;
            if refused.is_empty() {
                minted.settle();
                return;
            }
            let rebuild = fan_out(shared, &fanout(&target), &rebuild_plan(entry)).await;
            (refused, rebuild)
        }
        PutStage::Rebuild { refused, rebuild } => (refused, rebuild),
    };
    let reshaped = rebuild.into_final_refusals().await;
    close_put(&target, refused, reshaped, minted);
}

/// Close a parameter install's window from both answer sets. A core that
/// refused `SetParams` and the reshape missed the change.
fn close_put(
    target: &IndexTarget<'_>,
    refused: Vec<usize>,
    reshaped: Vec<usize>,
    minted: MintedRecords,
) {
    let missed: Vec<usize> = refused
        .into_iter()
        .filter(|core_id| reshaped.contains(core_id))
        .collect();
    if missed.is_empty() {
        minted.settle();
        return;
    }
    let error = crate::Error::Internal {
        detail: format!("cores did not apply the vector index change: {missed:?}"),
    };
    report(&error, "set_params_dispatch", target);
    // A core that missed the change still needs restart replay to reach the
    // record.
    minted.hold();
}

/// Remove one vector index from this node: append and fsync the drop record,
/// then dispatch `DropIndex` to every core.
///
/// Runs in a task the caller does not own, and returns once every core
/// answered or the dispatch deadline passed, as [`put_async`] does.
pub async fn delete_async(
    database_id: u64,
    tenant_id: u64,
    collection: String,
    field_name: String,
    shared: Arc<SharedState>,
) {
    let (ready_tx, ready_rx) = oneshot::channel();
    tokio::spawn(drop_index(
        IndexName {
            database_id,
            tenant_id,
            collection,
            field_name,
        },
        shared,
        ready_tx,
    ));
    if ready_rx.await.is_err() {
        tracing::error!("the vector index drop task ended before it reported");
    }
}

/// An owned index name, for the task that drops the index.
struct IndexName {
    database_id: u64,
    tenant_id: u64,
    collection: String,
    field_name: String,
}

async fn drop_index(name: IndexName, shared: Arc<SharedState>, ready: oneshot::Sender<()>) {
    let target = IndexTarget {
        database_id: name.database_id,
        tenant_id: name.tenant_id,
        collection: &name.collection,
        field_name: &name.field_name,
    };
    let plan = drop_index_plan(name.database_id, &name.collection, &name.field_name);

    // The vector writes this drop cancels are already fsynced in this node's
    // log, so replay rebuilds the dropped index unless the drop record is
    // durable too. Append and fsync before touching the cores. The record's
    // outcome-floor window closes once every core gave its final answer.
    let minted = MintedRecords::open(&shared.outcome_floor);
    match append_redo(&shared, &target, &plan, &minted) {
        Ok(Some(lsn)) => {
            if let Err(error) = shared.wal.wait_durable(lsn).await {
                report(&error, "drop_index_fsync", &target);
            }
        }
        Ok(None) => {
            let error = crate::Error::Internal {
                detail: "vector index drop minted no WAL record".to_string(),
            };
            report(&error, "drop_index_wal_append", &target);
        }
        Err(error) => report(&error, "drop_index_wal_append", &target),
    }

    // The cores hold the record from here. It closes from their answers.
    minted.mark_sent();
    let answers = fan_out(&shared, &fanout(&target), &plan).await;
    // Cores still working past the deadline answer later. The caller moves
    // on while this task waits for their final answers.
    let _ = ready.send(());
    let refused = answers.into_final_refusals().await;
    close_drop(&target, refused, minted);
}

/// Close a drop's window from the cores that did not drop the index.
fn close_drop(target: &IndexTarget<'_>, refused: Vec<usize>, minted: MintedRecords) {
    if refused.is_empty() {
        minted.settle();
        return;
    }
    let error = crate::Error::Internal {
        detail: format!("cores did not apply the vector index change: {refused:?}"),
    };
    report(&error, "drop_index_dispatch", target);
    // A core that kept the index still needs restart replay to reach the
    // drop record.
    minted.hold();
}

/// Append `plan`'s redo record to this node's WAL under `minted`'s window,
/// returning its LSN.
fn append_redo(
    shared: &SharedState,
    target: &IndexTarget<'_>,
    plan: &PhysicalPlan,
    minted: &MintedRecords,
) -> crate::Result<Option<Lsn>> {
    let database_id = DatabaseId::new(target.database_id);
    let owner = RecordOwner {
        tenant_id: TenantId::new(target.tenant_id),
        database_id,
        vshard_id: VShardId::from_collection_in_database(database_id, target.collection),
    };
    let outcome = minted.append_plan(
        &shared.wal,
        owner,
        plan,
        // A vector index change writes no row; its records carry no row
        // image, and the source names the committed DDL that ran it.
        crate::event::EventSource::User,
    )?;
    Ok(outcome.lsn)
}

/// Name this index for the core fan-out's ack line and error detail.
fn fanout<'a>(target: &'a IndexTarget<'a>) -> CoreFanout<'a> {
    CoreFanout {
        database_id: target.database_id,
        tenant_id: target.tenant_id,
        collection: target.collection,
        what: "vector index change",
        detail: target.field_name,
    }
}

/// File the one report for a stage this node lost, naming the index.
fn report(error: &crate::Error, stage: &'static str, target: &IndexTarget<'_>) {
    crate::diag::vector_index_not_applied(
        error,
        stage,
        target.database_id,
        target.tenant_id,
        target.collection,
        target.field_name,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored() -> StoredVectorIndexParams {
        StoredVectorIndexParams {
            database_id: 7,
            tenant_id: 3,
            collection: "documents".to_string(),
            field_name: "embedding".to_string(),
            dim: 384,
            metric: "l2".to_string(),
            m: 32,
            ef_construction: 400,
            index_type: "hnsw_pq".to_string(),
            pq_m: 8,
            ivf_cells: 64,
            ivf_nprobe: 16,
        }
    }

    /// Every build parameter the catalog row carries reaches the plan. A field
    /// dropped here is a follower index built with a different shape than the
    /// node that ran the statement.
    #[test]
    fn set_params_plan_carries_every_stored_parameter() {
        let entry = stored();
        let PhysicalPlan::Vector(VectorOp::SetParams {
            collection,
            field_name,
            dim,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        }) = set_params_plan(&entry)
        else {
            panic!("set_params_plan must build a VectorOp::SetParams");
        };
        assert_eq!(collection.as_str(), "7/documents");
        assert_eq!(field_name, "embedding");
        assert_eq!(dim, 384);
        assert_eq!(m, 32);
        assert_eq!(ef_construction, 400);
        assert_eq!(metric, "l2");
        assert_eq!(index_type, "hnsw_pq");
        assert_eq!(pq_m, 8);
        assert_eq!(ivf_cells, 64);
        assert_eq!(ivf_nprobe, 16);
    }

    /// The reshape plan carries the committed HNSW shape, with `m0` at the
    /// `2 * m` ratio every other install path applies. A core reshaped to a
    /// different ratio diverges from one that seeds from the same row at boot.
    #[test]
    fn rebuild_plan_carries_the_committed_hnsw_shape() {
        let entry = stored();
        let PhysicalPlan::Vector(VectorOp::Rebuild {
            collection,
            field_name,
            m,
            m0,
            ef_construction,
        }) = rebuild_plan(&entry)
        else {
            panic!("rebuild_plan must build a VectorOp::Rebuild");
        };
        assert_eq!(collection.as_str(), "7/documents");
        assert_eq!(field_name, "embedding");
        assert_eq!(m, 32);
        assert_eq!(m0, 64);
        assert_eq!(ef_construction, 400);
    }

    /// The drop plan targets the same `(database, collection, field)` the
    /// create plan installed, so it removes what the create built.
    #[test]
    fn drop_index_plan_targets_the_created_index() {
        let entry = stored();
        let PhysicalPlan::Vector(VectorOp::DropIndex {
            collection,
            field_name,
        }) = drop_index_plan(entry.database_id, &entry.collection, &entry.field_name)
        else {
            panic!("drop_index_plan must build a VectorOp::DropIndex");
        };
        assert_eq!(collection.as_str(), "7/documents");
        assert_eq!(field_name, "embedding");
    }

    /// An unnamed vector field keys on the empty string, matching
    /// `CoreLoop::vector_index_key`'s default-field slot.
    #[test]
    fn an_unnamed_field_keeps_the_empty_field_slot() {
        let PhysicalPlan::Vector(VectorOp::DropIndex { field_name, .. }) =
            drop_index_plan(DatabaseId::DEFAULT.as_u64(), "documents", "")
        else {
            panic!("drop_index_plan must build a VectorOp::DropIndex");
        };
        assert!(field_name.is_empty());
    }
}
