// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral savepoint orchestration: SAVEPOINT, RELEASE SAVEPOINT,
//! ROLLBACK TO SAVEPOINT, plus the neutral COMMIT OFFSET parse helper.
//!
//! A Data-Plane core holds one set of staging overlays per transaction,
//! shared by every vShard it hosts. Each savepoint gets a fresh id. The
//! mark and the rewind go through every staged vShard (via the injected
//! [`TxnDataPlane`]), and each core records and rewinds its own overlay
//! positions under that id. The orchestrator also drives the neutral
//! `SessionStore` savepoint stack. Transports translate the returned
//! [`SavepointError`] into their own SQLSTATE (`25P01` / `25P02` / `3B001` /
//! `XX000`).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::bridge::envelope::PhysicalPlan;
use crate::event::cdc::CdcOffset;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::connection::SessionId;
use super::outcome::TxnDataPlane;
use super::state::TransactionState;
use super::store::SessionStore;

/// Mints savepoint ids. One counter for the process keeps every id unique
/// and larger than every id minted before it, so a transaction's later
/// savepoint always has the larger id.
static NEXT_OVERLAY_SAVEPOINT: AtomicU64 = AtomicU64::new(1);

/// Typed savepoint failure. Adapters map each variant to its SQLSTATE.
#[derive(Debug)]
pub enum SavepointError {
    /// A savepoint command was issued outside a transaction block. → `25P01`.
    NoActiveTransaction,
    /// SAVEPOINT or RELEASE ran in an aborted transaction block. Only
    /// ROLLBACK or ROLLBACK TO SAVEPOINT leave that state. → `25P02`.
    TransactionAborted,
    /// The named savepoint does not exist (or there is no active session).
    /// `message` is the exact human message. → `3B001`.
    NotFound { message: String },
    /// The overlay mark or rewind on a staged vShard failed. The block is
    /// aborted: its staged state no longer matches the savepoint stack.
    /// → `XX000`.
    OverlayDispatch { message: String },
}

/// Reject a savepoint command issued outside a transaction block.
fn require_active_txn(
    sessions: &SessionStore,
    session_id: SessionId,
) -> Result<(), SavepointError> {
    if sessions.transaction_state(session_id) == TransactionState::Idle {
        return Err(SavepointError::NoActiveTransaction);
    }
    Ok(())
}

/// Reject SAVEPOINT / RELEASE outside a usable transaction block.
///
/// An aborted block refuses both, as PostgreSQL does.
fn require_usable_txn(
    sessions: &SessionStore,
    session_id: SessionId,
) -> Result<(), SavepointError> {
    match sessions.transaction_state(session_id) {
        TransactionState::Idle => Err(SavepointError::NoActiveTransaction),
        TransactionState::Failed => Err(SavepointError::TransactionAborted),
        TransactionState::InBlock => Ok(()),
    }
}

/// Dispatch a savepoint overlay meta-op to a specific vShard's core.
async fn dispatch_overlay_savepoint(
    tenant_id: TenantId,
    vshard_id: VShardId,
    dp: &impl TxnDataPlane,
    op: MetaOp,
) -> Result<(), SavepointError> {
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Meta(op),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    // Savepoint overlay meta-ops are not writes — no WAL record, no version.
    dp.dispatch_no_wal(task)
        .await
        .map(|_| ())
        .map_err(|e| SavepointError::OverlayDispatch {
            message: format!("savepoint overlay meta-op on vShard {vshard_id:?} failed: {e}"),
        })
}

/// Handle SAVEPOINT `<name>`.
///
/// Mints the savepoint's id and sends the mark through EVERY vShard the
/// transaction has staged writes to. Each core records its value/TTL,
/// graph, and array overlay positions under the id, so a later ROLLBACK TO
/// rewinds every staged overlay to exactly here. With no vShard staged yet
/// no core records anything. A failed mark aborts the block: a savepoint
/// that a core did not record cannot rewind correctly.
pub async fn run_savepoint(
    sessions: &SessionStore,
    session_id: SessionId,
    tenant_id: TenantId,
    dp: &impl TxnDataPlane,
    name: &str,
) -> Result<(), SavepointError> {
    require_usable_txn(sessions, session_id)?;
    let savepoint = NEXT_OVERLAY_SAVEPOINT.fetch_add(1, Ordering::Relaxed);
    let (txn_id, vshards) = sessions.txn_identity(session_id);
    if let Some(txn_id) = txn_id {
        for vshard_id in vshards {
            dispatch_overlay_savepoint(
                tenant_id,
                vshard_id,
                dp,
                MetaOp::MarkSavepoint { txn_id, savepoint },
            )
            .await
            .inspect_err(|_| sessions.fail_transaction(session_id))?;
        }
    }
    sessions.create_savepoint(
        session_id,
        name.to_string(),
        savepoint,
        super::ddl_buffer::buffer_len(),
    );
    Ok(())
}

/// Handle RELEASE SAVEPOINT `<name>`.
///
/// RELEASE only pops the Control-Plane savepoint stack; the overlay journal
/// entries are retained (they merge into the enclosing scope), so no
/// Data-Plane meta-op is dispatched.
pub fn run_release_savepoint(
    sessions: &SessionStore,
    session_id: SessionId,
    name: &str,
) -> Result<(), SavepointError> {
    require_usable_txn(sessions, session_id)?;
    sessions
        .release_savepoint(session_id, name)
        .map_err(|e| SavepointError::NotFound {
            message: e.to_string(),
        })
}

/// Handle ROLLBACK TO SAVEPOINT `<name>`.
///
/// Truncates the write buffer to the saved position and sends the rewind
/// through every vShard the transaction has staged to. The current staged
/// set is a superset of the savepoint's: writes can stage to new vShards
/// after it. Each core rewinds its overlays to the positions it recorded
/// under the savepoint's id. A core with no record hosted no staged vShard
/// at the savepoint, so it drops all its staged writes. Several vShards on
/// one core rewind that core to the same record. A failed rewind aborts
/// the block: its staged state no longer matches the truncated write
/// buffer, so only ROLLBACK can end it.
pub async fn run_rollback_to_savepoint(
    sessions: &SessionStore,
    session_id: SessionId,
    tenant_id: TenantId,
    dp: &impl TxnDataPlane,
    name: &str,
) -> Result<(), SavepointError> {
    require_active_txn(sessions, session_id)?;
    let rewind = sessions
        .rollback_to_savepoint(session_id, name)
        .map_err(|e| SavepointError::NotFound {
            message: e.to_string(),
        })?;
    super::ddl_buffer::truncate(rewind.ddl_buffer_len);
    let savepoint = rewind.overlay_savepoint;
    let (txn_id, vshards) = sessions.txn_identity(session_id);
    if let Some(txn_id) = txn_id {
        for vshard_id in vshards {
            dispatch_overlay_savepoint(
                tenant_id,
                vshard_id,
                dp,
                MetaOp::RollbackToSavepoint { txn_id, savepoint },
            )
            .await
            .inspect_err(|_| sessions.fail_transaction(session_id))?;
        }
    }
    Ok(())
}

/// A parsed deferred COMMIT OFFSET / COMMIT OFFSETS command.
pub enum DeferredOffsetCmd {
    /// `COMMIT OFFSET PARTITION <p> AT <epoch>:<index>:<sequence> ON <stream> CONSUMER GROUP <name>`.
    Single {
        stream: String,
        group: String,
        partition_id: u32,
        offset: CdcOffset,
    },
    /// `COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>` — the caller resolves
    /// the latest LSN per partition from the CDC buffer.
    Batch { stream: String, group: String },
}

/// A `COMMIT OFFSET` statement whose partition or offset does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeferredOffsetError {
    #[error("invalid partition: '{0}'")]
    InvalidPartition(String),
    #[error(transparent)]
    InvalidOffset(#[from] crate::event::cdc::offset::ParseCdcOffsetError),
}

impl From<DeferredOffsetError> for crate::Error {
    fn from(error: DeferredOffsetError) -> Self {
        Self::BadRequest {
            detail: error.to_string(),
        }
    }
}

/// Parse a `COMMIT OFFSET` / `COMMIT OFFSETS` statement into a neutral command.
///
/// Returns `Ok(None)` when `sql` is not a deferred-offset commit. `upper` is
/// the caller's uppercased form of `sql`, passed in to avoid re-allocating it.
/// An `<epoch>:<index>` token acknowledges every event of that write.
pub fn parse_deferred_offset(
    sql: &str,
    upper: &str,
) -> Result<Option<DeferredOffsetCmd>, DeferredOffsetError> {
    if !(upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ")) {
        return Ok(None);
    }
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // Single-partition: COMMIT OFFSET PARTITION <p> AT <offset> ON <stream> CONSUMER GROUP <name>
    if parts.len() >= 11
        && parts[2].eq_ignore_ascii_case("PARTITION")
        && parts[4].eq_ignore_ascii_case("AT")
        && parts[6].eq_ignore_ascii_case("ON")
    {
        let partition_id: u32 = parts[3]
            .parse()
            .map_err(|_| DeferredOffsetError::InvalidPartition(parts[3].to_owned()))?;
        let offset: CdcOffset = parts[5].parse()?;
        return Ok(Some(DeferredOffsetCmd::Single {
            stream: parts[7].to_lowercase(),
            group: parts[10].to_lowercase(),
            partition_id,
            offset,
        }));
    }

    // Batch: COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>
    if parts.len() >= 7
        && parts[1].eq_ignore_ascii_case("OFFSETS")
        && parts[2].eq_ignore_ascii_case("ON")
    {
        return Ok(Some(DeferredOffsetCmd::Batch {
            stream: parts[3].to_lowercase(),
            group: parts[6].to_lowercase(),
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use crate::bridge::envelope::{Payload, Response, Status};
    use crate::control::server::shared::session::store::SessionStore;
    use crate::types::{DatabaseId, Lsn, RequestId};

    /// A `TxnDataPlane` that records every dispatched overlay meta-op (per vShard)
    /// instead of touching a real core.
    #[derive(Default)]
    struct RecordingDp {
        ops: Mutex<Vec<(VShardId, MetaOp)>>,
    }

    impl TxnDataPlane for RecordingDp {
        fn dispatch_no_wal<'a>(
            &'a self,
            task: PhysicalTask,
        ) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>> {
            if let PhysicalPlan::Meta(op) = &task.plan {
                self.ops.lock().unwrap().push((task.vshard_id, op.clone()));
            }
            Box::pin(async move {
                Ok(Response {
                    request_id: RequestId::new(1),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::empty(),
                    watermark_lsn: Lsn::ZERO,
                    error_code: None,
                    stage_vote: None,
                    read_versions: crate::types::ReadVersions::new(),
                    write_set: Vec::new(),
                })
            })
        }

        fn event_source(&self) -> crate::event::EventSource {
            crate::event::EventSource::User
        }
    }

    /// A `TxnDataPlane` whose every dispatch fails.
    struct FailingDp;

    impl TxnDataPlane for FailingDp {
        fn dispatch_no_wal<'a>(
            &'a self,
            _task: PhysicalTask,
        ) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>> {
            Box::pin(async {
                Err(crate::Error::Internal {
                    detail: "core unreachable".into(),
                })
            })
        }

        fn event_source(&self) -> crate::event::EventSource {
            crate::event::EventSource::User
        }
    }

    #[tokio::test]
    async fn a_failed_overlay_mark_aborts_the_block() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5203".parse().unwrap();
        store.ensure_session(addr);
        store.begin(addr, 0).unwrap();
        assert!(store.buffer_write(addr, staged_task(3)));

        let result = run_savepoint(
            &store,
            SessionId::from(&addr),
            TenantId::new(1),
            &FailingDp,
            "s1",
        )
        .await;
        assert!(matches!(
            result,
            Err(SavepointError::OverlayDispatch { .. })
        ));
        assert_eq!(store.transaction_state(addr), TransactionState::Failed);
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

    /// The mark goes through each vShard staged at the savepoint. The rewind
    /// goes through each vShard staged at the rollback, all under the one
    /// savepoint id, so each core rewinds to its own record. A core that
    /// hosts vShard 3 and vShard 9 rewinds to the same record through both.
    #[tokio::test]
    async fn rollback_to_savepoint_rewinds_every_staged_vshard_to_the_savepoint_id() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5201".parse().unwrap();
        store.ensure_session(addr);
        store.begin(addr, 0).unwrap();
        let tenant = TenantId::new(1);
        let dp = RecordingDp::default();

        // Stage on vShard 3, then SAVEPOINT: only vShard 3 is marked.
        assert!(store.buffer_write(addr, staged_task(3)));
        run_savepoint(&store, SessionId::from(&addr), tenant, &dp, "s1")
            .await
            .expect("savepoint");

        // Stage on vShard 9 AFTER the savepoint.
        assert!(store.buffer_write(addr, staged_task(9)));

        run_rollback_to_savepoint(&store, SessionId::from(&addr), tenant, &dp, "s1")
            .await
            .expect("rollback to savepoint");

        let ops = dp.ops.lock().unwrap();
        let marks: Vec<(u32, u64)> = ops
            .iter()
            .filter_map(|(v, op)| match op {
                MetaOp::MarkSavepoint { savepoint, .. } => Some((v.as_u32(), *savepoint)),
                _ => None,
            })
            .collect();
        let [(3, savepoint)] = marks.as_slice() else {
            panic!("only the vShard staged at the savepoint is marked: {marks:?}");
        };
        let rewinds: Vec<(u32, u64)> = ops
            .iter()
            .filter_map(|(v, op)| match op {
                MetaOp::RollbackToSavepoint { savepoint, .. } => Some((v.as_u32(), *savepoint)),
                _ => None,
            })
            .collect();
        assert_eq!(
            rewinds,
            vec![(3, *savepoint), (9, *savepoint)],
            "every staged vShard rewinds under the savepoint's id"
        );
    }

    /// A later savepoint gets a larger id, so a core can drop the records of
    /// the savepoints a rewind destroys.
    #[tokio::test]
    async fn a_later_savepoint_gets_a_larger_id() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5204".parse().unwrap();
        store.ensure_session(addr);
        store.begin(addr, 0).unwrap();
        let tenant = TenantId::new(1);
        let dp = RecordingDp::default();
        assert!(store.buffer_write(addr, staged_task(2)));

        for name in ["s1", "s2"] {
            run_savepoint(&store, SessionId::from(&addr), tenant, &dp, name)
                .await
                .expect("savepoint");
        }

        let ids: Vec<u64> = dp
            .ops
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, op)| match op {
                MetaOp::MarkSavepoint { savepoint, .. } => Some(*savepoint),
                _ => None,
            })
            .collect();
        assert!(
            matches!(ids.as_slice(), [first, second] if first < second),
            "{ids:?}"
        );
    }

    #[tokio::test]
    async fn an_aborted_block_refuses_savepoint_and_release_until_rollback_to() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5202".parse().unwrap();
        store.ensure_session(addr);
        store.begin(addr, 0).unwrap();
        let session = SessionId::from(&addr);
        let tenant = TenantId::new(1);
        let dp = RecordingDp::default();

        run_savepoint(&store, session, tenant, &dp, "s1")
            .await
            .expect("savepoint in a usable block");
        store.fail_transaction(addr);

        assert!(matches!(
            run_savepoint(&store, session, tenant, &dp, "s2").await,
            Err(SavepointError::TransactionAborted)
        ));
        assert!(matches!(
            run_release_savepoint(&store, session, "s1"),
            Err(SavepointError::TransactionAborted)
        ));

        run_rollback_to_savepoint(&store, session, tenant, &dp, "s1")
            .await
            .expect("rollback to savepoint in an aborted block");
        assert_eq!(store.transaction_state(addr), TransactionState::InBlock);
        run_release_savepoint(&store, session, "s1").expect("release after rollback to");
    }

    #[test]
    fn deferred_commit_offset_accepts_event_and_whole_write_tokens() {
        let canonical = parse_deferred_offset(
            "COMMIT OFFSET PARTITION 2 AT 0:42:7 ON orders CONSUMER GROUP analytics",
            "COMMIT OFFSET PARTITION 2 AT 0:42:7 ON ORDERS CONSUMER GROUP ANALYTICS",
        )
        .unwrap()
        .unwrap();
        let DeferredOffsetCmd::Single { offset, .. } = canonical else {
            panic!("expected single offset commit");
        };
        assert_eq!(offset, CdcOffset::new(42, 7));

        let whole_write = parse_deferred_offset(
            "COMMIT OFFSET PARTITION 2 AT 0:42 ON orders CONSUMER GROUP analytics",
            "COMMIT OFFSET PARTITION 2 AT 0:42 ON ORDERS CONSUMER GROUP ANALYTICS",
        )
        .unwrap()
        .unwrap();
        let DeferredOffsetCmd::Single { offset, .. } = whole_write else {
            panic!("expected single offset commit");
        };
        assert_eq!(offset, CdcOffset::whole_index(42));
    }

    #[test]
    fn deferred_commit_offset_refuses_a_bad_partition_or_offset_with_a_typed_error() {
        let bad_partition = parse_deferred_offset(
            "COMMIT OFFSET PARTITION x AT 0:1 ON s CONSUMER GROUP g",
            "COMMIT OFFSET PARTITION X AT 0:1 ON S CONSUMER GROUP G",
        );
        assert!(matches!(
            bad_partition,
            Err(DeferredOffsetError::InvalidPartition(ref p)) if p == "x"
        ));
        let bad_offset = parse_deferred_offset(
            "COMMIT OFFSET PARTITION 1 AT 42 ON s CONSUMER GROUP g",
            "COMMIT OFFSET PARTITION 1 AT 42 ON S CONSUMER GROUP G",
        );
        let Err(error) = bad_offset else {
            panic!("a one-part offset must be refused");
        };
        assert!(matches!(error, DeferredOffsetError::InvalidOffset(_)));
        assert!(matches!(
            crate::Error::from(error),
            crate::Error::BadRequest { .. }
        ));
    }
}
