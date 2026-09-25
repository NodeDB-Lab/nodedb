// SPDX-License-Identifier: BUSL-1.1

//! The write-admission gate.
//!
//! Every write-class `PhysicalPlan` — regardless of transport or path — passes
//! through [`admit`] before it is enqueued to a Data-Plane core. The gate
//! decides one of four outcomes per write:
//!
//! - [`WriteAdmission::FastPath`] — an uncontended POINT write whose exact
//!   deterministic lock keys were acquired here. It carries a RAII
//!   [`WriteAdmissionGuard`] the caller holds across the enqueue + response; the
//!   guard releases the keys on drop. This is the normal autocommit path.
//! - [`WriteAdmission::FastPathBlocking`] — a single-node POINT write for a
//!   vShard with no Calvin scheduler. There is no lock table to fence against,
//!   so the caller awaits a FIFO-fair per-key async order-lock before the WAL
//!   append + enqueue, serializing concurrent same-key writes in arrival order.
//! - [`WriteAdmission::RouteToCalvin`] — a point write whose keys are currently
//!   held by a pending commit (acquire returned `Blocked`), OR any predicate /
//!   bulk / multi-home write. The caller submits it through the deterministic
//!   scheduler, which queues it FIFO behind the holder and applies it in order.
//! - [`WriteAdmission::ExemptRead`] — a non-write (read / meta op), or a
//!   Calvin-scheduled apply that already holds its locks.
//!
//! The fence holds because the fast path and the scheduler share the SAME
//! `Arc<Mutex<LockManager>>` (via [`CalvinLocalState::lock_managers`]): a
//! commit's lock validation calls `acquire` on the same key, is `Blocked`, and
//! waits; whoever takes the OS mutex first wins, with no time-of-check /
//! time-of-use gap.
//!
//! [`CalvinLocalState::lock_managers`]: crate::control::state::CalvinLocalState::lock_managers

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::UnboundedSender;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::calvin::scheduler::driver::core::routing::{PlanRouting, plan_vshard};
use crate::control::cluster::calvin::scheduler::lock_manager::{LockKey, LockManager, TxnId};
use crate::control::planner::calvin::is_dependent_predicate;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::MetaOp;

use super::lock_keys::plan_lock_keys;
use super::predicate::plan_is_write;
use super::write_order_lock::KeyedWriteOrderLock;

/// Count of writes the gate routed to the deterministic scheduler instead of
/// the fast path (either a `Blocked` point write or a non-point write). Read by
/// the fence tests.
static ROUTED_TO_CALVIN: AtomicU64 = AtomicU64::new(0);

/// Number of writes routed to the deterministic Calvin scheduler by the gate.
pub fn cp_routed_to_calvin() -> u64 {
    ROUTED_TO_CALVIN.load(Ordering::Relaxed)
}

/// The shard slice a write targets, plus the plan whose write-class and point
/// identity the gate consults.
pub struct WriteTarget<'a> {
    /// Tenant scope of the write.
    pub tenant_id: TenantId,
    /// Database (catalog namespace) scope of the write.
    pub database_id: DatabaseId,
    /// Target virtual shard whose lock manager gates the write.
    pub vshard_id: VShardId,
    /// The plan being admitted.
    pub plan: &'a PhysicalPlan,
}

/// The gate's decision for one write. See the module docs.
pub enum WriteAdmission {
    /// Uncontended point write admitted to the fast path. `guard` is `Some` when
    /// real keys were acquired (a scheduler is active for the vShard) and `None`
    /// when no lock manager is registered and the write carries no single point
    /// key to serialize on (predicate / bulk / uncovered shapes — left unordered
    /// for now). Either way the caller holds it across enqueue + response.
    FastPath { guard: Option<WriteAdmissionGuard> },
    /// Single-node (no Calvin scheduler for this vShard) POINT write. There is no
    /// lock table to fence against, but concurrent same-key writes must still
    /// serialize so WAL-LSN order equals apply order per key. The caller awaits
    /// `keyed_lock.lock_owned(key)` — a FIFO-fair per-key async lock — BEFORE the
    /// WAL append + enqueue, and holds the returned guard across exactly that
    /// window (the same window as the Calvin-mode `FastPath` guard).
    FastPathBlocking {
        /// The single deterministic point key this write serializes on.
        key: LockKey,
        /// The global keyed order-lock (from `SharedState::write_order_locks`).
        keyed_lock: Arc<KeyedWriteOrderLock>,
    },
    /// Submit the write through the deterministic Calvin scheduler.
    RouteToCalvin,
    /// A non-write, or an already-locked Calvin apply — no fence needed.
    ExemptRead,
}

/// RAII holder of a fast-path write's deterministic locks.
///
/// Holds the shared lock table and the reserved autocommit holder id. `Drop`
/// releases every key held by that holder under a short guard (the lock table
/// tracks the key set by holder, so the guard needs only the id).
///
/// The fast path acquires a key only when it is uncontended AT ACQUIRE TIME, but
/// a multi-vShard Calvin scheduler transaction can still queue behind that key
/// AFTERWARDS (it calls `acquire` on the same shared table, is `Blocked`, and
/// waits). When this guard drops, [`LockManager::release`] promotes that waiter
/// to holder and returns its `TxnId`. The guard runs on the Control Plane, not
/// inside the owning vShard's scheduler task, so it forwards the promoted ids
/// over `promotion_sender` to the scheduler, which runs its normal
/// promotion -> dispatch path. Without this hand-off a promoted scheduler txn
/// would sit in the scheduler's `blocked` map forever, holding the key and
/// stalling every later txn behind a zombie holder.
///
/// [`LockManager::release`]: crate::control::cluster::calvin::scheduler::lock_manager::LockManager::release
pub struct WriteAdmissionGuard {
    lock_manager: Arc<Mutex<LockManager>>,
    txn: TxnId,
    /// Promotion channel to the owning vShard's scheduler. `Some` when a Calvin
    /// scheduler is registered for this vShard (the only case in which a waiter
    /// can queue behind a fast-path key); `None` in single-node / no-Calvin
    /// deployments, where `release` never promotes anything.
    promotion_sender: Option<UnboundedSender<Vec<TxnId>>>,
}

impl Drop for WriteAdmissionGuard {
    fn drop(&mut self) {
        // Ordering is load-bearing: take the lock-manager mutex, release the
        // holder (promoting any waiter queued behind it), DROP the mutex guard,
        // and only THEN send. `release`'s temporary `MutexGuard` is dropped at the
        // end of this `let` statement, so the send below never holds it.
        let promoted = self
            .lock_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .release(self.txn);

        // Hand promoted scheduler waiters to their scheduler for dispatch. The
        // send is synchronous and non-blocking (unbounded channel), safe from a
        // `Drop`. A send error means the scheduler task is gone (shutdown); log
        // and continue — never unwrap or panic in `Drop`.
        if !promoted.is_empty()
            && let Some(sender) = &self.promotion_sender
            && let Err(e) = sender.send(promoted)
        {
            tracing::warn!(
                error = %e,
                "write-admission gate: could not deliver promoted Calvin waiters to \
                 the scheduler (receiver gone); those transactions may stall"
            );
        }
    }
}

/// Admit a plan destined for a Data-Plane core.
///
/// Synchronous: never awaits and never parks — it only *chooses* the outcome;
/// any per-key await happens in the caller. `RouteToCalvin` is returned ONLY
/// when a deterministic scheduler is actually registered for the write's vShard.
/// With no scheduler (single-node / no-Calvin — the common case) a point write
/// returns `FastPathBlocking` carrying the global keyed order-lock so concurrent
/// same-key writes still serialize; every other shape fast-paths unfenced.
pub fn admit(shared: &SharedState, target: &WriteTarget<'_>) -> WriteAdmission {
    // A Calvin-scheduled apply already holds its locks (acquired by the
    // scheduler); it must never re-acquire at the gate. Defensive — these ops
    // do not normally reach the gate.
    if matches!(
        target.plan,
        PhysicalPlan::Meta(
            MetaOp::CalvinExecuteStatic { .. }
                | MetaOp::CalvinExecuteActive { .. }
                | MetaOp::CalvinFlush { .. }
                | MetaOp::CalvinDrop { .. }
                | MetaOp::CalvinResolve { .. }
        )
    ) {
        return WriteAdmission::ExemptRead;
    }

    if !plan_is_write(target.plan) {
        return WriteAdmission::ExemptRead;
    }

    // Only two write shapes participate in the fence: a single-home POINT write
    // (Document / KV / Vector / single-home graph edge — a statically-known
    // deterministic key) and a single-shard PREDICATE write (BulkUpdate /
    // BulkDelete — its write set discovered by scheduler reconnaissance). Every
    // other write — batch, INSERT..SELECT, upsert, CRDT, columnar / timeseries /
    // spatial / array, and cross-home edges — has no Calvin lock representation
    // and fast-paths unchanged.
    let point_keys = plan_lock_keys(target.plan);
    let is_predicate = is_dependent_predicate(target.plan);
    let vshard = match &point_keys {
        Some((v, _)) => *v,
        None if is_predicate => match plan_vshard(target.plan) {
            PlanRouting::Vshards(v) => match v.as_slice() {
                [v] => *v,
                _ => return WriteAdmission::FastPath { guard: None },
            },
            PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => {
                return WriteAdmission::FastPath { guard: None };
            }
        },
        None => return WriteAdmission::FastPath { guard: None },
    };

    // Availability gate: with no scheduler registered for this vShard there is no
    // Calvin lock table to fence against. A POINT write still needs per-key
    // arrival-order serialization so WAL-LSN order equals Data-Plane apply order
    // per key — hand it the global keyed order-lock, on which concurrent same-key
    // writers queue FIFO while distinct keys never contend. A predicate write has
    // no single static point key here, so it stays unordered on the fast path
    // (widening that coverage is a later unit).
    let Some(lock_manager) = shared
        .calvin
        .lock_managers
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&vshard.as_u32())
        .map(Arc::clone)
    else {
        return match point_keys.and_then(|(_v, keys)| single_point_key(keys)) {
            Some(key) => WriteAdmission::FastPathBlocking {
                key,
                keyed_lock: Arc::clone(&shared.write_order_locks),
            },
            None => WriteAdmission::FastPath { guard: None },
        };
    };

    // Calvin IS running for this vShard. A predicate write has no static point
    // key to acquire; the scheduler discovers its write set, so route it.
    let Some((_v, keys)) = point_keys else {
        ROUTED_TO_CALVIN.fetch_add(1, Ordering::Relaxed);
        return WriteAdmission::RouteToCalvin;
    };

    // Point write: mint a holder id in the reserved band so it never collides
    // with a real Calvin schedule position, then probe the exact keys WITHOUT
    // blocking. `try_acquire` never enqueues a waiter on the contended path, so a
    // routed write leaves no orphaned autocommit holder that a later `release`
    // could promote to an unowned (never-released) lock.
    let txn = TxnId::new(
        TxnId::AUTOCOMMIT_EPOCH,
        shared
            .calvin
            .autocommit_lock_seq
            .fetch_add(1, Ordering::Relaxed),
    );
    let acquired = {
        let mut lm = lock_manager.lock().unwrap_or_else(|p| p.into_inner());
        lm.try_acquire(txn, keys)
    };
    if acquired {
        // Look up this vShard's promotion channel so the guard can hand any
        // scheduler waiter it promotes on drop back to the scheduler for
        // dispatch. `None` only if no scheduler is registered — but a registered
        // lock manager without a promotion sender should not happen, since both
        // are inserted together per vShard.
        let promotion_sender = shared
            .calvin
            .promotion_senders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&vshard.as_u32())
            .cloned();
        WriteAdmission::FastPath {
            guard: Some(WriteAdmissionGuard {
                lock_manager,
                txn,
                promotion_sender,
            }),
        }
    } else {
        // A pending commit (or another fast-path write) holds a key: route behind
        // it via the scheduler. Nothing was acquired or enqueued here.
        ROUTED_TO_CALVIN.fetch_add(1, Ordering::Relaxed);
        WriteAdmission::RouteToCalvin
    }
}

/// The single point key of an eligible fast-path write, or `None` if the set is
/// not exactly one key. [`plan_lock_keys`] always yields a one-key set for a
/// point write; anything else is not a single-identity write and stays
/// unordered on the single-node fast path.
fn single_point_key(keys: BTreeSet<LockKey>) -> Option<LockKey> {
    let mut it = keys.into_iter();
    match (it.next(), it.next()) {
        (Some(key), None) => Some(key),
        _ => None,
    }
}
