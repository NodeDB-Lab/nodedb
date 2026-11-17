// SPDX-License-Identifier: BUSL-1.1

//! Finish of full-text index steps a crash or an error left owed.
//!
//! A full-text index record names the step its index still needs:
//!
//! - `Building`: the CREATE rebuild did not finish on every node. The index
//!   misses rows stored before it.
//! - `Dropping`: the DROP did not finish. Its fields can still hold postings
//!   on some node, and the record still holds the name.
//!
//! The pass runs in the background once the node serves: the gateway never
//! waits on it. Serving meanwhile is correct: a search on a `Building`
//! field fails with 55000, and a `Dropping` record declares no field.
//!
//! - Every node that boots rebuilds its own rows of each owed collection
//!   once, through `rebuild_text_index` with `RebuildScope::ThisNode`.
//! - The metadata-group leader finishes each owed step cluster-wide:
//!   every node's rebuild, then the `Ready` flip or the record's removal.
//!   It runs at boot and every [`FINISH_INTERVAL`], so a step a non-leader
//!   left owed finishes whichever node boots.
//!
//! A core keeps the rebuild it ran over a field set, and answers a second
//! rebuild op over the same fields at once. The leader's finish therefore
//! rebuilds no node's rows that the node already rebuilt at its boot.
//! Independent records run concurrently, at most [`CONCURRENT_STEPS`] at a
//! time. A record of a soft-dropped collection waits: its collection takes
//! no write and serves no read, and the first pass after its UNDROP runs
//! it.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use tracing::warn;

use crate::control::security::catalog::{IndexKind, IndexRecordState, StoredIndexRecord};
use crate::control::server::shared::ddl::neutral::collection::index::text_build::{
    RebuildScope, build_text_index, rebuild_text_index,
};
use crate::control::server::shared::ddl::neutral::collection::index::text_drop::finish_text_index_drop;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// How often the metadata-group leader finishes owed steps after boot.
const FINISH_INTERVAL: Duration = Duration::from_secs(60);

/// Owed steps one pass runs at once.
const CONCURRENT_STEPS: usize = 4;

/// Spawn the background reconcile: the boot pass, then the leader's
/// finish every [`FINISH_INTERVAL`] until shutdown.
pub fn spawn_text_index_reconcile(shared: Arc<SharedState>) {
    let task_shared = Arc::clone(&shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "text_index_reconcile",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let shared = task_shared;
            log_pass(reconcile_text_indexes(&shared).await);
            let mut tick = tokio::time::interval(FINISH_INTERVAL);
            // The first tick fires at once: the boot pass covered it.
            tick.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                log_pass(finish_owed_text_steps(&shared).await);
            }
        },
    );
}

/// The boot pass: rebuild this node's rows of every owed collection, then
/// finish every owed step when this node leads the metadata group.
/// Returns how many steps are still owed.
///
/// A step that fails stays owed: its record keeps its state, the error is
/// logged, and a `Capture` names it. Only an unreadable index registry is
/// an error.
pub async fn reconcile_text_indexes(shared: &SharedState) -> crate::Result<usize> {
    let owed = owed_records(shared)?;
    let owed_ref = &owed;
    let local_failures: Vec<Option<DdlError>> = futures::stream::iter(0..owed.len())
        .map(|i| async move {
            let record = &owed_ref[i].0;
            rebuild_text_index(
                shared,
                TenantId::new(record.tenant_id),
                DatabaseId::new(record.database_id),
                &record.collection,
                RebuildScope::ThisNode,
            )
            .await
            .err()
        })
        .buffered(CONCURRENT_STEPS)
        .collect()
        .await;
    if shared.is_singleton_worker() {
        return Ok(finish_steps(shared, &owed).await);
    }
    // The leader finishes the steps. A local error is reported here.
    for ((record, step), failure) in owed.iter().zip(local_failures) {
        if let Some(error) = failure {
            report_owed(record, *step, &error);
        }
    }
    Ok(owed.len())
}

/// The leader's finish of every owed step. Returns how many are still
/// owed. A node that leads no metadata group finishes nothing.
pub async fn finish_owed_text_steps(shared: &SharedState) -> crate::Result<usize> {
    let owed = owed_records(shared)?;
    if !shared.is_singleton_worker() {
        return Ok(owed.len());
    }
    Ok(finish_steps(shared, &owed).await)
}

/// Run each owed step through the function its statement runs, at most
/// [`CONCURRENT_STEPS`] at a time. Returns how many failed.
async fn finish_steps(shared: &SharedState, owed: &[(StoredIndexRecord, OwedStep)]) -> usize {
    let outcomes: Vec<Result<(), DdlError>> = futures::stream::iter(0..owed.len())
        .map(|i| finish_step(shared, &owed[i].0, owed[i].1))
        .buffered(CONCURRENT_STEPS)
        .collect()
        .await;
    let mut still_owed = 0usize;
    for ((record, step), outcome) in owed.iter().zip(outcomes) {
        if let Err(error) = outcome {
            report_owed(record, *step, &error);
            still_owed += 1;
        }
    }
    still_owed
}

async fn finish_step(
    shared: &SharedState,
    record: &StoredIndexRecord,
    step: OwedStep,
) -> Result<(), DdlError> {
    let tenant_id = TenantId::new(record.tenant_id);
    let database_id = DatabaseId::new(record.database_id);
    match step {
        OwedStep::Build => {
            build_text_index(
                shared,
                tenant_id,
                database_id,
                &record.collection,
                &record.name,
            )
            .await
        }
        OwedStep::Drop => {
            finish_text_index_drop(
                shared,
                tenant_id,
                database_id,
                &record.collection,
                &record.name,
            )
            .await
        }
    }
}

/// Log a step that stays owed and file its report.
fn report_owed(record: &StoredIndexRecord, step: OwedStep, error: &DdlError) {
    warn!(
        index = %record.name,
        collection = %record.collection,
        tenant = record.tenant_id,
        step = step.name(),
        sqlstate = %error.sqlstate,
        error = %error.message,
        "full-text index step still owed; the metadata leader runs it again"
    );
    crate::diag::text_index_step_owed(
        &error.sqlstate,
        &error.message,
        &crate::diag::TextIndexStepTarget {
            step: step.name(),
            database_id: record.database_id,
            tenant_id: record.tenant_id,
            collection: &record.collection,
            index: &record.name,
        },
    );
}

/// Log the outcome of one background pass.
fn log_pass(outcome: crate::Result<usize>) {
    match outcome {
        Ok(_) => {}
        Err(error) => warn!(
            error = %error,
            "full-text index registry unreadable; the next pass reads it again"
        ),
    }
}

/// Every owed full-text step the registry holds.
fn owed_records(shared: &SharedState) -> crate::Result<Vec<(StoredIndexRecord, OwedStep)>> {
    let records = shared.credentials.catalog().list_all_index_records()?;
    Ok(records
        .into_iter()
        .filter_map(|r| owed_step(&r).map(|step| (r, step)))
        .collect())
}

/// A step a full-text record still needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwedStep {
    Build,
    Drop,
}

impl OwedStep {
    fn name(self) -> &'static str {
        match self {
            OwedStep::Build => "build",
            OwedStep::Drop => "drop",
        }
    }
}

/// The step `record` still needs, or `None` when it needs none or its
/// collection is soft-dropped.
fn owed_step(record: &StoredIndexRecord) -> Option<OwedStep> {
    if record.kind != IndexKind::FullText || !record.is_active {
        return None;
    }
    match record.state {
        IndexRecordState::Building => Some(OwedStep::Build),
        IndexRecordState::Dropping => Some(OwedStep::Drop),
        IndexRecordState::Ready => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::control::server::shared::ddl::neutral::collection::index::text_drop::tests::{
        answer_failing_rebuilds, record, seed_collection, seed_record,
    };

    /// A `Building` record gets its build and flips to `Ready`. A `Dropping`
    /// record gets its drop and leaves the catalog. A pass whose rebuilds
    /// fail leaves both owed and changes neither.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_boot_reconcile_finishes_every_owed_full_text_step() {
        let fail = Arc::new(AtomicBool::new(true));
        let core_fail = Arc::clone(&fail);
        let cluster = crate::control::cluster::test_one_node::boot_with_core(
            |_| {},
            move |state, side| tokio::spawn(answer_failing_rebuilds(state, side, core_fail)),
        )
        .await;
        let state = Arc::clone(&cluster.state);
        seed_collection(&state);
        seed_record(&state, "ft_building", IndexRecordState::Building);
        seed_record(&state, "ft_dropping", IndexRecordState::Dropping);
        seed_record(&state, "ft_ready", IndexRecordState::Ready);

        let still_owed = reconcile_text_indexes(&state)
            .await
            .expect("the registry reads");
        assert_eq!(still_owed, 2, "both owed steps fail with the rebuild");
        assert_eq!(
            record(&state, "ft_building").map(|r| r.state),
            Some(IndexRecordState::Building)
        );
        assert_eq!(
            record(&state, "ft_dropping").map(|r| r.state),
            Some(IndexRecordState::Dropping)
        );

        fail.store(false, Ordering::Release);
        let still_owed = reconcile_text_indexes(&state)
            .await
            .expect("the registry reads");
        assert_eq!(still_owed, 0);
        assert_eq!(
            record(&state, "ft_building").map(|r| r.state),
            Some(IndexRecordState::Ready),
            "the build ran and the record is Ready"
        );
        assert!(
            record(&state, "ft_dropping").is_none(),
            "the drop ran and the record is gone"
        );
        assert_eq!(
            record(&state, "ft_ready").map(|r| r.state),
            Some(IndexRecordState::Ready),
            "a Ready record is left alone"
        );

        drop(state);
        cluster.shutdown().await;
    }

    /// The leader's periodic finish runs a step a boot left owed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_leader_finish_runs_a_step_left_owed() {
        let fail = Arc::new(AtomicBool::new(false));
        let cluster = crate::control::cluster::test_one_node::boot_with_core(
            |_| {},
            move |state, side| tokio::spawn(answer_failing_rebuilds(state, side, fail)),
        )
        .await;
        let state = Arc::clone(&cluster.state);
        seed_collection(&state);
        seed_record(&state, "ft_building", IndexRecordState::Building);

        let still_owed = finish_owed_text_steps(&state)
            .await
            .expect("the registry reads");
        assert_eq!(still_owed, 0);
        assert_eq!(
            record(&state, "ft_building").map(|r| r.state),
            Some(IndexRecordState::Ready)
        );

        drop(state);
        cluster.shutdown().await;
    }
}
