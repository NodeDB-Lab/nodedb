// SPDX-License-Identifier: BUSL-1.1

//! The neutral COMMIT sequence: classify the transaction's dispatch, replay its
//! durable batch, then run every post-commit side effect (`post_commit`).

use crate::control::planner::calvin::read_vshards_of;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::commit_fence;
use super::super::connection::SessionId;
use super::super::outcome::{AbortReason, CommitOutcome, TxnDataPlane};
use super::super::read_set::read_snapshot_ordinal;
use super::super::reservation_release;
use super::super::store::SessionStore;
use super::buffered_dispatch::BufferedCommit;
use super::ddl_steps::{begin_ddl, compensate_finalized_ddl};
use super::metering::resolve_buffered_write_scopes;
use super::post_commit::{CommittedTxn, finish_committed};
use super::single_shard::commit_publishes;

/// Run the neutral COMMIT sequence for one collision-free session.
///
/// Returns [`CommitOutcome::Committed`] once every durable batch has flushed
/// and all post-commit side effects have fired, or [`CommitOutcome::Aborted`]
/// with the reason the transport maps to its wire error.
pub async fn run_commit(
    sessions: &SessionStore,
    session_id: SessionId,
    identity: &AuthenticatedIdentity,
    state: &SharedState,
    dp: &impl TxnDataPlane,
) -> CommitOutcome {
    // A cross-shard read made earlier in the same request as this COMMIT is
    // still pending in the connection scope. It joins the read-set first.
    super::super::pending_shard_reads::record_pending(sessions, session_id);
    let read_set = sessions.take_read_set(session_id);
    // The engine side effects this transaction's index DDL deferred. They run
    // once its catalog entries landed, below. An abort before then drops them
    // with the buffer.
    let deferred_effects = super::super::ddl_buffer::drain_effects();
    // Peek the buffered write tasks WITHOUT draining them or leaving the block.
    // The session stays `InBlock` through classification and dispatch; the
    // buffered batch is flushed to Calvin as the COMMIT finalization (see
    // `run_commit_calvin`), then `sessions.commit` below drains the buffer.
    let buffered = sessions.buffered_tasks(session_id);
    // The lines the statements reported rejected, to compare with COMMIT's.
    let ts_preview = super::ts_rejections::preview_by_collection(
        &buffered,
        &sessions.ts_preview_rejected(session_id),
    );
    // The messages the transaction's trigger bodies published. They commit in
    // its redo record, and the Event Plane delivers them from there.
    let publishes = sessions.take_pending_publishes(session_id);
    let tenant_id = identity.tenant_id;
    // The read-set widens dispatch classification: a transaction that writes
    // vShard X and read vShard Y participates in {X, Y} and routes through
    // Calvin with Y as a participant. A homed cross-shard read enlists its
    // home the same way, on a single node too. Each participant's apply
    // validates the reads homed on it.
    let read_vshards = match read_vshards_of(&read_set) {
        Ok(vshards) => vshards,
        Err(error) => {
            reservation_release::release_and_rollback(state, sessions, session_id).await;
            return CommitOutcome::Aborted {
                reason: AbortReason::Dispatch(error),
            };
        }
    };

    // The metering scopes for the non-stageable writes are resolved before
    // anything durable is written: a scope error aborts the COMMIT here,
    // where nothing needs undoing.
    let metering_scopes = match resolve_buffered_write_scopes(state, identity, &buffered) {
        Ok(scopes) => scopes,
        Err(error) => {
            reservation_release::release_and_rollback(state, sessions, session_id).await;
            return CommitOutcome::Aborted {
                reason: AbortReason::Dispatch(error),
            };
        }
    };

    // In-transaction `MERGE`, `UPDATE ... FROM <source>`, and `INSERT ... SELECT`
    // are resolved + staged into concrete, surrogate-carrying point writes
    // (`PointInsert` / `PointPut` / `PointDelete`) at STATEMENT time
    // (`session::expander_stage`), so by COMMIT the buffer already holds those
    // concrete point ops — no raw `Merge` / `UpdateFromJoin` / `InsertSelect`
    // plan remains to expand here, and COMMIT invokes no expander at all.

    // Buffered transactional DDL is finalized to the catalog before any
    // buffered DML dispatches — see `ddl_flush`'s module doc for why finalize
    // must precede dispatch. The reserved objects are handled inline in each
    // branch below, not hoisted here: only the buffered-DML branch dispatches
    // anything afterward that can fail in a way compensation will undo.
    if buffered.is_empty() {
        // Read-only interactive transaction: every read ran at the block's one
        // snapshot, so the reads are consistent with each other and the
        // transaction commits without validation.
        // No buffered DML for this DDL to race, so finalize it here,
        // symmetrically with the buffered branch below.
        let ddl_compensation = match begin_ddl(state, sessions, session_id).await {
            Ok(result) => result,
            Err(error) => {
                reservation_release::release_and_rollback(state, sessions, session_id).await;
                return CommitOutcome::Aborted {
                    reason: AbortReason::DdlPropose(error),
                };
            }
        };
        // A transaction whose bodies published with no buffered write (an
        // INSTEAD OF body that replaced every write) commits its messages in
        // a redo record of their own. Nothing else dispatches in this path:
        // the remaining steps (metering, session drain, offset/GAP_FREE
        // finalize, field-inference merge) report no dispatch failure for a
        // finalize to compensate.
        if let Some(reason) = commit_publishes(state, tenant_id, publishes).await {
            compensate_finalized_ddl(state, &ddl_compensation).await;
            reservation_release::release_and_rollback(state, sessions, session_id).await;
            return CommitOutcome::Aborted { reason };
        }
    } else {
        // Every buffered write was planned at STATEMENT time and is dispatched
        // only now, so the catalog has had the whole open block to move on.
        // Re-compare the descriptor versions the statements were planned
        // against before anything durable is written — an abort here leaves no
        // side effect and the client retries the transaction.
        if let Some(reason) = commit_fence::check_buffered_descriptors(
            state.credentials.catalog(),
            sessions,
            session_id,
        ) {
            reservation_release::release_and_rollback(state, sessions, session_id).await;
            return CommitOutcome::Aborted { reason };
        }
        // Reserve and finalize the buffered DDL BEFORE dispatching any
        // buffered DML: a crash after this point leaves a real, cataloged,
        // empty, droppable collection rather than rows durable in a
        // collection with no catalog row anywhere.
        // The snapshot the apply validates the reads and the written keys at.
        let snapshot_ordinal = match read_snapshot_ordinal(
            sessions.tx_snapshot(session_id).map(|cut| cut.ordinal),
            &read_set,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                reservation_release::release_and_rollback(state, sessions, session_id).await;
                return CommitOutcome::Aborted {
                    reason: AbortReason::Dispatch(error),
                };
            }
        };
        let ddl_compensation = match begin_ddl(state, sessions, session_id).await {
            Ok(result) => result,
            Err(error) => {
                reservation_release::release_and_rollback(state, sessions, session_id).await;
                return CommitOutcome::Aborted {
                    reason: AbortReason::DdlPropose(error),
                };
            }
        };
        let dispatch = BufferedCommit {
            sessions,
            session_id,
            identity,
            state,
            dp,
            buffered: &buffered,
            read_set: &read_set,
            snapshot_ordinal,
            ts_preview: &ts_preview,
        };
        if let Some(outcome) = dispatch
            .dispatch(&read_vshards, publishes, &ddl_compensation)
            .await
        {
            return outcome;
        }
    }

    // Every abort branch above has already returned, so every buffered write
    // durably committed.
    finish_committed(CommittedTxn {
        sessions,
        session_id,
        identity,
        state,
        dp,
        buffered: &buffered,
        metering_scopes: &metering_scopes,
        deferred_effects,
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;

    use super::*;
    use crate::bridge::envelope::Response;
    use crate::control::planner::calvin::{DispatchClass, classify_dispatch};
    use crate::control::security::identity::{DatabaseSet, Role};
    use crate::control::server::shared::session::read_set::{EngineTag, ReadKey, ReadSetEntry};
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_task::PhysicalTask;

    /// A Data Plane a read-only commit never reaches.
    struct UnreachedDp;

    impl TxnDataPlane for UnreachedDp {
        fn dispatch_no_wal<'a>(
            &'a self,
            _task: PhysicalTask,
        ) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>> {
            Box::pin(async {
                Err(crate::Error::Internal {
                    detail: "a read-only commit dispatches nothing".into(),
                })
            })
        }

        fn event_source(&self) -> crate::event::EventSource {
            crate::event::EventSource::User
        }
    }

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            0,
            "_commit_test",
            TenantId::new(1),
            vec![Role::Superuser],
            true,
            None,
            DatabaseSet::All,
        )
    }

    fn homed_read(home: u32) -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Graph,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: String::new(),
            key: ReadKey::Predicate,
            observed_at: None,
            home: Some(VShardId::new(home)),
        }
    }

    /// A read-only block read one snapshot, so its COMMIT validates nothing
    /// and dispatches nothing: no leader is asked about any read.
    #[tokio::test]
    async fn a_read_only_commit_skips_validation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = std::sync::Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"),
        );
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        let sessions = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5320".parse().expect("addr");
        sessions.ensure_session(addr);
        sessions.begin(addr).expect("begin");
        let id = SessionId::from(&addr);
        sessions.record_read_entries(id, vec![homed_read(3), homed_read(9)]);

        let outcome = run_commit(&sessions, id, &identity(), &state, &UnreachedDp).await;
        assert!(
            matches!(outcome, CommitOutcome::Committed),
            "a read-only block commits without validation"
        );
        assert!(!sessions.is_in_transaction_block(addr));
    }

    /// A homed read enlists its home vShard: a write on one vShard and a
    /// homed read on another commit as one multi-participant transaction,
    /// on a single node too.
    #[test]
    fn a_homed_read_enlists_its_vshard() {
        let write = PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(3),
            database_id: DatabaseId::DEFAULT,
            plan: crate::bridge::envelope::PhysicalPlan::Kv(
                nodedb_physical::physical_plan::KvOp::Put {
                    collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    ttl_ms: 0,
                    surrogate: nodedb_types::Surrogate::new(1),
                    returning: None,
                    rls_filters: Vec::new(),
                    provenance: None,
                },
            ),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        let reads = [homed_read(9)];
        let read_vshards = read_vshards_of(&reads).expect("read vShards");
        assert!(read_vshards.contains(&9));
        match classify_dispatch(std::slice::from_ref(&write), &read_vshards) {
            DispatchClass::MultiShard { vshards } => {
                assert!(vshards.contains(&3) && vshards.contains(&9));
            }
            DispatchClass::SingleShard { .. } => {
                panic!("a homed read on another vShard must enlist it")
            }
        }
    }
}
