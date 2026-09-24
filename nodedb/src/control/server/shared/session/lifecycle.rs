// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral BEGIN and ROLLBACK orchestration.
//!
//! Both drive the neutral session state plus the DDL buffer and, for ROLLBACK,
//! the GAP_FREE reservation rollback, sequence-log audit, cursor/notify
//! discard, and staging-overlay release via the injected [`TxnDataPlane`].
//! Transports only shape the returned tag / error.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::connection::SessionId;
use super::ddl_buffer;
use super::outcome::TxnDataPlane;
use super::overlay_drop::drop_txn_overlay;
use super::store::SessionStore;

/// Run the neutral BEGIN sequence: anchor the snapshot LSN, activate the DDL
/// buffer, and enter the transaction block. Returns the session error (mapped
/// to the transport's `25P02`) if the connection cannot begin a transaction.
pub fn run_begin(
    sessions: &SessionStore,
    session_id: SessionId,
    state: &SharedState,
) -> Result<(), crate::Error> {
    let snapshot_lsn = {
        let next = state.wal.next_lsn();
        crate::types::Lsn::new(next.as_u64().saturating_sub(1))
    };
    // Last globally-applied Calvin epoch as the cross-shard snapshot anchor.
    // 0 in single-node / no-Calvin deployments (the atomic is never advanced).
    let snapshot_epoch = state
        .last_applied_calvin_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    ddl_buffer::activate();
    sessions
        .begin(session_id, snapshot_lsn, snapshot_epoch)
        .map_err(|msg| crate::Error::BadRequest {
            detail: msg.to_owned(),
        })
}

/// Run the neutral ROLLBACK sequence.
///
/// Drains the DDL buffer and restores the Data Plane to the committed shape,
/// rolls back GAP_FREE reservations (with sequence-log audit), clears the write
/// buffer + read-set, closes non-hold cursors, discards buffered NOTIFY
/// messages, and releases the staging overlay on its home vShard. Infallible —
/// every cleanup step is best-effort, mirroring the original swallow-on-error
/// behavior.
pub async fn run_rollback(
    sessions: &SessionStore,
    session_id: SessionId,
    identity: &AuthenticatedIdentity,
    state: &SharedState,
    dp: &impl TxnDataPlane,
) {
    // Taken, not discarded: the entries name every collection whose Data-Plane
    // registration this transaction moved, and the restoration below needs them.
    let discarded_ddl = ddl_buffer::take();
    // Snapshot the overlay identity BEFORE `rollback()` clears session state,
    // so the staging overlay can be released on EVERY vShard the transaction
    // staged writes to (a transaction may span multiple cores).
    let (overlay_txn_id, overlay_vshards) = sessions.txn_identity(session_id);
    // Release this transaction's read reservations while the reservation owner is
    // still set — `rollback` below clears it. Best-effort; lease GC backstops.
    super::reservation_release::release_session_reservations(
        state,
        sessions,
        session_id,
        nodedb_cluster::calvin::types::ReleaseReason::Abort,
    )
    .await;
    // Keep the session's transaction identity intact until every overlay has
    // been released. Detached connection teardown can be cancelled at any
    // await point; clearing `tx_id` first would make an interrupted cleanup
    // permanently lose the only identifiers needed to reclaim the overlays.
    if let Some(txn_id) = overlay_txn_id {
        for vshard_id in overlay_vshards {
            // Teardown of an aborted transaction's overlay: surface a failure at
            // ERROR (the overlay, keyed by `txn_id`, is reclaimable by its holder)
            // and continue reaping the remaining vShards. ROLLBACK is infallible
            // for the client, so there is no outcome to change here.
            if let Err(e) = drop_txn_overlay(state, dp, identity.tenant_id, vshard_id, txn_id).await
            {
                tracing::error!(
                    vshard = vshard_id.as_u32(),
                    error = %e,
                    "failed to release per-transaction staging overlay on rollback"
                );
            }
        }
    }

    let reservations = sessions.rollback(session_id).unwrap_or_default();
    for handle in &reservations {
        let key = &handle.sequence_key;
        let registry = &state.sequence_registry;
        registry.gap_free_manager().rollback(handle, || {
            let map = registry.sequences_read();
            if let Some(h) = map.get(key.as_str()) {
                h.rollback_one();
            }
        });
        {
            let catalog = state.credentials.catalog();
            crate::control::sequence::log::log_reservation(
                catalog,
                &crate::control::sequence::log::rolled_back(
                    &handle.name,
                    handle.value,
                    &identity.username,
                    handle.database_id,
                    handle.tenant_id,
                ),
            );
        }
    }
    sessions.close_non_hold_cursors(session_id);
    // Discard NOTIFY messages buffered during this transaction.
    sessions.discard_pending_notifies(session_id);
    // Last, once the staging overlay is gone: put the Data Plane back to the
    // committed shape, so the rolled-back DDL survives nowhere.
    if let Some(discarded) = discarded_ddl {
        super::ddl_rollback::restore_data_plane(state, &discarded).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
    use crate::control::security::identity::DatabaseSet;
    use crate::types::{DatabaseId, Lsn, RequestId, TenantId, VShardId};
    use nodedb_physical::physical_plan::MetaOp;
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

    /// `run_begin` anchors the session's cross-shard snapshot to the last
    /// globally-applied Calvin epoch from `SharedState::last_applied_calvin_epoch`.
    #[tokio::test]
    async fn run_begin_anchors_snapshot_epoch() {
        use std::sync::atomic::Ordering;

        use crate::bridge::dispatch::Dispatcher;
        use crate::wal::WalManager;

        let dir = tempfile::tempdir().unwrap();
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap(),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).unwrap();

        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5100".parse().unwrap();
        store.ensure_session(addr);

        // Seed the applied epoch to 7 and BEGIN — the session anchors to 7.
        state.last_applied_calvin_epoch.store(7, Ordering::Release);
        run_begin(&store, SessionId::from(&addr), &state).unwrap();
        assert_eq!(store.snapshot_epoch(addr), Some(7));
        store.commit(addr).unwrap();
        assert_eq!(store.snapshot_epoch(addr), None);

        // Unset (single-node / no-Calvin): BEGIN anchors to 0.
        state.last_applied_calvin_epoch.store(0, Ordering::Release);
        run_begin(&store, SessionId::from(&addr), &state).unwrap();
        assert_eq!(store.snapshot_epoch(addr), Some(0));
    }

    /// A `TxnDataPlane` that records every dispatched overlay meta-op (per vShard)
    /// instead of touching a real core. `MarkSavepoint` replies with a `SAVEPOINT_MARKER_BYTES`-byte
    /// composite marker whose value component is `vshard + 1`, so a later
    /// ROLLBACK TO can be asserted to thread each vShard's own saved marker.
    #[derive(Default)]
    struct RecordingDp {
        ops: Mutex<Vec<(VShardId, MetaOp)>>,
    }

    impl TxnDataPlane for RecordingDp {
        fn dispatch_no_wal<'a>(
            &'a self,
            task: PhysicalTask,
        ) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>> {
            let vshard = task.vshard_id;
            let payload = if let PhysicalPlan::Meta(op) = &task.plan {
                self.ops.lock().unwrap().push((vshard, op.clone()));
                match op {
                    MetaOp::MarkSavepoint { .. } => {
                        let value = (vshard.as_u32() as u64) + 1;
                        let graph = 0u64;
                        let array = 0u64;
                        let mut bytes = Vec::with_capacity(
                            nodedb_physical::physical_plan::SAVEPOINT_MARKER_BYTES,
                        );
                        bytes.extend_from_slice(&value.to_le_bytes());
                        bytes.extend_from_slice(&graph.to_le_bytes());
                        bytes.extend_from_slice(&array.to_le_bytes());
                        Payload::from_vec(bytes)
                    }
                    _ => Payload::empty(),
                }
            } else {
                Payload::empty()
            };
            Box::pin(async move {
                Ok(Response {
                    request_id: RequestId::new(1),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload,
                    watermark_lsn: Lsn::ZERO,
                    error_code: None,
                    read_set_valid: None,
                    read_version_lsn: crate::types::Lsn::ZERO,
                    write_set: Vec::new(),
                })
            })
        }

        fn event_source(&self) -> crate::event::EventSource {
            crate::event::EventSource::User
        }
    }

    /// A benign staged write task homed on `vshard`. The plan content is irrelevant
    /// to overlay teardown — only the vShard it stages to is tracked.
    fn staged_task(vshard: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn test_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            1,
            "tester",
            TenantId::new(1),
            Vec::new(),
            true,
            None,
            DatabaseSet::All,
        )
    }

    /// ROLLBACK of a transaction that staged writes to TWO vShards must drop the
    /// staging overlay on BOTH — the pre-fix single-`tx_vshard` code leaked the
    /// second core's overlay.
    #[tokio::test]
    async fn multi_vshard_rollback_drops_every_overlay() {
        use crate::bridge::dispatch::Dispatcher;
        use crate::wal::WalManager;

        let dir = tempfile::tempdir().unwrap();
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap(),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).unwrap();

        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5200".parse().unwrap();
        store.ensure_session(addr);
        run_begin(&store, SessionId::from(&addr), &state).unwrap();

        // Stage to two distinct vShards/cores.
        assert!(store.buffer_write(addr, staged_task(3)));
        assert!(store.buffer_write(addr, staged_task(9)));

        let identity = test_identity();
        let dp = RecordingDp::default();
        run_rollback(&store, SessionId::from(&addr), &identity, &state, &dp).await;

        let ops = dp.ops.lock().unwrap();
        let drops: Vec<VShardId> = ops
            .iter()
            .filter_map(|(v, op)| matches!(op, MetaOp::DropTxnOverlay { .. }).then_some(*v))
            .collect();
        assert!(drops.contains(&VShardId::new(3)), "core A overlay dropped");
        assert!(
            drops.contains(&VShardId::new(9)),
            "core B overlay dropped (would leak pre-fix)"
        );
        assert_eq!(drops.len(), 2, "exactly the two staged overlays dropped");
    }
}
