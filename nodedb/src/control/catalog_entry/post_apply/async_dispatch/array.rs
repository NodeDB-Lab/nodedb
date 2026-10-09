// SPDX-License-Identifier: BUSL-1.1

//! Per-core effects of the array catalog entries on this node.
//!
//! - `PutArray` sends `OpenArray` to every core. The open purges a
//!   finalized-drop tombstone of the same identity, so a recreate starts
//!   empty on every core.
//! - `DeleteArray` stages the drop on every core, then purges the staged
//!   tombstones. The catalog row is already gone, so a failed stage returns
//!   `Err` and the re-delivered entry stages again. A failed purge counts as
//!   done: the tombstone fences a same-name open until the next `PutArray`
//!   purges it.
//! - A `DeleteArray` with `moved_to` rekeys the store on every core instead.
//!   Cells route by Hilbert prefix alone, so the vShard of every cell stays
//!   the same, and only the store directory carries the database.
//! - Every `DeleteArray` changes the mirror last, under the array
//!   incarnation's exclusive gate (see `control::write_gate`).

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::ArrayOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::core_fanout::{CoreFanout, dispatch_to_every_core};

/// Open `entry` on every core of this node.
pub(crate) async fn open_on_every_core(
    shared: &SharedState,
    entry: &ArrayCatalogEntry,
) -> crate::Result<()> {
    let plan = PhysicalPlan::Array(ArrayOp::OpenArray {
        array_id: entry.array_id.clone(),
        schema_msgpack: entry.schema_msgpack.clone(),
        schema_hash: entry.schema_hash,
        prefix_bits: entry.prefix_bits,
        audit_retain_ms: entry.audit_retain_ms,
        minimum_audit_retain_ms: entry.minimum_audit_retain_ms,
    });
    dispatch_to_every_core(shared, &fanout(&entry.array_id, "array open"), &plan).await
}

/// Drop `(database_id, tenant_id, name)` on every core of this node, or
/// rekey it there when the delete is the source side of a move.
pub(crate) async fn delete_on_every_core(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    moved_to: Option<u64>,
) -> crate::Result<()> {
    // Exclusive on this array's incarnation: no replica routes a cell write
    // to it while the cores and the mirror change. The mirror changes last,
    // once every core left the key. A key the mirror no longer holds takes
    // no write, so a replayed delete holds no gate.
    let incarnation = {
        let mirror = shared
            .array_catalog
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        mirror
            .lookup_by_name_in_database(
                TenantId::new(tenant_id),
                DatabaseId::new(database_id),
                name,
            )
            .map(|entry| entry.incarnation)
    };
    let _gate = match incarnation {
        Some(incarnation) => Some(
            crate::control::write_gate::exclusive(crate::control::write_gate::GateKey::Array(
                incarnation,
            ))
            .await,
        ),
        None => None,
    };
    match moved_to {
        Some(to) => rekey_on_every_core(shared, database_id, tenant_id, name, to).await?,
        None => drop_on_every_core(shared, database_id, tenant_id, name).await?,
    }
    super::super::array::remove_or_move(database_id, tenant_id, name, moved_to, shared);
    Ok(())
}

/// Stage and purge the drop of `(database_id, tenant_id, name)` on every
/// core of this node.
async fn drop_on_every_core(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
) -> crate::Result<()> {
    let array_id = identity(database_id, tenant_id, name);
    let stage = PhysicalPlan::Array(ArrayOp::DropArray {
        array_id: array_id.clone(),
    });
    dispatch_to_every_core(shared, &fanout(&array_id, "array drop stage"), &stage).await?;
    let purge = PhysicalPlan::Array(ArrayOp::PurgeArrayDrop {
        array_id: array_id.clone(),
    });
    if let Err(error) =
        dispatch_to_every_core(shared, &fanout(&array_id, "array drop purge"), &purge).await
    {
        tracing::warn!(
            database = database_id,
            tenant = tenant_id,
            array = %name,
            %error,
            "array drop post-apply: purge failed; the drop tombstone owns it"
        );
    }
    Ok(())
}

/// Move the store of `(database_id, tenant_id, name)` under `moved_to` on
/// every core of this node.
async fn rekey_on_every_core(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    moved_to: u64,
) -> crate::Result<()> {
    let array_id = identity(database_id, tenant_id, name);
    let plan = PhysicalPlan::Array(ArrayOp::RekeyArray {
        array_id: array_id.clone(),
        target: identity(moved_to, tenant_id, name),
    });
    dispatch_to_every_core(shared, &fanout(&array_id, "array rekey"), &plan).await
}

fn identity(database_id: u64, tenant_id: u64, name: &str) -> ArrayId {
    ArrayId::in_database(TenantId::new(tenant_id), DatabaseId::new(database_id), name)
}

fn fanout<'a>(array_id: &'a ArrayId, what: &'a str) -> CoreFanout<'a> {
    CoreFanout {
        database_id: array_id.database_id.as_u64(),
        tenant_id: array_id.tenant_id.as_u64(),
        collection: &array_id.name,
        what,
        detail: "",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
    use crate::bridge::envelope::{Payload, Response, Status};
    use crate::types::Lsn;
    use crate::wal::WalManager;

    const CORES: usize = 2;

    fn fixture() -> (
        Arc<SharedState>,
        Vec<CoreChannelDataSide>,
        tempfile::TempDir,
    ) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("array.wal")).expect("test WAL"),
        );
        let (dispatcher, sides) = Dispatcher::new(CORES, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, sides, directory)
    }

    fn step(plan: &PhysicalPlan) -> &'static str {
        match plan {
            PhysicalPlan::Array(ArrayOp::DropArray { .. }) => "drop",
            PhysicalPlan::Array(ArrayOp::PurgeArrayDrop { .. }) => "purge",
            PhysicalPlan::Array(ArrayOp::RekeyArray { .. }) => "rekey",
            PhysicalPlan::Array(ArrayOp::OpenArray { .. }) => "open",
            _ => "other",
        }
    }

    /// Run `work` while answering every core request with the status
    /// `status_of` gives its step. Returns the work's result and the steps
    /// each core saw, in order.
    async fn run_answering<T: Send + 'static>(
        state: &Arc<SharedState>,
        sides: &mut [CoreChannelDataSide],
        work: impl std::future::Future<Output = T> + Send + 'static,
        status_of: impl Fn(&'static str) -> Status,
    ) -> (T, Vec<Vec<&'static str>>) {
        let task = tokio::spawn(work);
        let mut seen = vec![Vec::new(); sides.len()];
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !task.is_finished() && std::time::Instant::now() < deadline {
            for (core_id, side) in sides.iter_mut().enumerate() {
                if let Ok(request) = side.request_rx.try_pop() {
                    let kind = step(&request.inner.plan);
                    seen[core_id].push(kind);
                    side.response_tx
                        .try_push(BridgeResponse {
                            inner: Response {
                                request_id: request.inner.request_id,
                                status: status_of(kind),
                                attempt: 1,
                                partial: false,
                                payload: Payload::empty(),
                                watermark_lsn: Lsn::ZERO,
                                error_code: None,
                                stage_vote: None,
                                read_version_lsn: Lsn::ZERO,
                                write_set: Vec::new(),
                            },
                        })
                        .expect("fake data-plane response queue has capacity");
                }
            }
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
        let result = task.await.expect("post-apply task");
        (result, seen)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_drop_stages_then_purges_on_every_core() {
        let (state, mut sides, _directory) = fixture();
        let shared = Arc::clone(&state);
        let (result, seen) = run_answering(
            &state,
            &mut sides,
            async move { drop_on_every_core(&shared, 1, 1, "grid").await },
            |_| Status::Ok,
        )
        .await;
        result.expect("the drop completes");
        assert!(seen.iter().all(|steps| steps == &["drop", "purge"]));
    }

    /// The catalog row is gone before the stage, so a refused stage returns
    /// `Err`: the re-delivered entry stages again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_stage_fails_the_post_apply() {
        let (state, mut sides, _directory) = fixture();
        let shared = Arc::clone(&state);
        let (result, seen) = run_answering(
            &state,
            &mut sides,
            async move { drop_on_every_core(&shared, 1, 1, "grid").await },
            |kind| {
                if kind == "drop" {
                    Status::Error
                } else {
                    Status::Ok
                }
            },
        )
        .await;
        assert!(result.is_err(), "a refused stage must fail the post-apply");
        assert!(seen.iter().all(|steps| steps == &["drop"]));
    }

    /// A failed purge leaves the tombstone to the next open of the identity.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_purge_counts_as_done() {
        let (state, mut sides, _directory) = fixture();
        let shared = Arc::clone(&state);
        let (result, _) = run_answering(
            &state,
            &mut sides,
            async move { drop_on_every_core(&shared, 1, 1, "grid").await },
            |kind| {
                if kind == "purge" {
                    Status::Error
                } else {
                    Status::Ok
                }
            },
        )
        .await;
        result.expect("the tombstone owns a failed purge");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_rekey_fails_the_post_apply() {
        let (state, mut sides, _directory) = fixture();
        let shared = Arc::clone(&state);
        let (result, seen) = run_answering(
            &state,
            &mut sides,
            async move { rekey_on_every_core(&shared, 1, 1, "grid", 2).await },
            |_| Status::Error,
        )
        .await;
        assert!(result.is_err(), "a refused rekey must fail the post-apply");
        assert!(seen.iter().all(|steps| steps == &["rekey"]));
    }
}
