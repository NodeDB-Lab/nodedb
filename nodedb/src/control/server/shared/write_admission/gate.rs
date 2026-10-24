// SPDX-License-Identifier: BUSL-1.1

//! The write-admission gate.
//!
//! Every write-class `PhysicalPlan` passes through [`admit`] before it is
//! ordered: an autocommit write that applies on this node alone before its
//! enqueue, and a replicated write on its data-group leader before its
//! propose (see `leader_gate`). The gate decides one of six outcomes:
//!
//! - [`WriteAdmission::FastPath`] — every lock key of the write was free and
//!   is held now by the RAII [`WriteAdmissionGuard`]. A local write holds it
//!   across its enqueue. A replicated write holds it until its leader starts
//!   the entry's apply. The guard releases the keys on drop.
//! - [`WriteAdmission::FastPathBlocking`] — a single-node POINT write for a
//!   vShard with no Calvin scheduler. There is no lock table to fence against,
//!   so the caller awaits a FIFO-fair per-key async order-lock before the WAL
//!   append + enqueue, serializing concurrent same-key writes in arrival order.
//! - [`WriteAdmission::Unscheduled`] — any other write to vShards with no
//!   Calvin scheduler. It takes no key.
//!
//!   Every admitted write carries an admission ticket: in its guard, its
//!   wait, or one of these two outcomes. The ticket fences each vShard's next
//!   scheduler until the write reached its core or its log (see `tickets`).
//! - [`WriteAdmission::RouteToCalvin`] — a key is held, and a Calvin
//!   transaction can sequence the write, or the write homes on several
//!   vShards. The caller submits it through the deterministic scheduler, which
//!   queues it FIFO behind the holder and applies it in order.
//! - [`WriteAdmission::Wait`] — a key is held, and no Calvin transaction can
//!   sequence the write. The caller awaits [`AdmissionWait::acquire`].
//! - [`WriteAdmission::ExemptRead`] — a non-write (read / meta op), or a
//!   Calvin-scheduled apply that already holds its locks.
//!
//! Every write shape has a lock request (see `admission_keys`): its row keys
//! `Exclusive` plus its collection `Intent`, or its collection `Exclusive`.
//!
//! The fence holds because the gate and the scheduler share the SAME
//! `Arc<Mutex<LockManager>>` (via [`CalvinLocalState::lock_managers`]): a
//! commit's lock validation calls `acquire` on the same key, is `Blocked`, and
//! waits; whoever takes the OS mutex first wins, with no time-of-check /
//! time-of-use gap.
//!
//! The gate cannot deadlock with a Calvin transaction. It takes a write's keys
//! all at once with `try_acquire`, or none of them. A contended write routes
//! to the scheduler, which orders it by sequencer position, or waits holding
//! no key. A write that holds keys waits only for its own enqueue, which
//! never waits for a lock.
//!
//! [`CalvinLocalState::lock_managers`]: crate::control::state::CalvinLocalState::lock_managers
//! [`AdmissionWait::acquire`]: super::wait::AdmissionWait::acquire

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::UnboundedSender;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::calvin::scheduler::driver::core::routing::{PlanRouting, plan_vshard};
use crate::control::cluster::calvin::scheduler::lock_manager::{LockKey, LockManager, TxnId};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};

use super::admission_keys::{AdmissionKeys, is_calvin_apply, plan_admission_keys};
use super::lock_keys::plan_row_key;
use super::predicate::plan_is_write;
use super::tickets::AdmissionTicket;
use super::wait::AdmissionWait;
use super::write_order_lock::KeyedWriteOrderLock;

/// Count of writes the gate routed to the deterministic scheduler instead of
/// the fast path (a contended write, or a write on several vShards). Read by
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
    /// Uncontended write admitted to the fast path. `guard` is `Some` when
    /// its keys were acquired. It is `None` when no Calvin scheduler runs for
    /// the vShard and the write has no point key, or when the write names no
    /// lock key. No Calvin transaction can then conflict with it, and the
    /// vShard's write-order fence orders it against other writes.
    FastPath { guard: Option<WriteAdmissionGuard> },
    /// POINT write on a vShard with no Calvin scheduler registered. There is no
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
        /// Fences the vShard's next scheduler until this write reached its
        /// core or its log.
        ticket: AdmissionTicket,
    },
    /// A write that is not a point write, on vShards with no Calvin
    /// scheduler. No Calvin transaction can conflict with it. `ticket`
    /// fences each vShard's next scheduler until it reached its core or its
    /// log.
    Unscheduled { ticket: AdmissionTicket },
    /// Submit the write through the deterministic Calvin scheduler.
    RouteToCalvin,
    /// A key is held, and no Calvin transaction can sequence the write. The
    /// caller awaits [`AdmissionWait::acquire`] before it orders the write.
    Wait(AdmissionWait),
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
/// will sit in the scheduler's `blocked` map forever, holding the key and
/// stalling every later txn behind a zombie holder.
///
/// [`LockManager::release`]: crate::control::cluster::calvin::scheduler::lock_manager::LockManager::release
pub struct WriteAdmissionGuard {
    lock_manager: Arc<Mutex<LockManager>>,
    txn: TxnId,
    /// Promotion channel to the owning vShard's scheduler. `Some` when a Calvin
    /// scheduler is registered for this vShard (the only case in which a waiter
    /// can queue behind a fast-path key); `None` when no scheduler is
    /// registered, where `release` never promotes anything.
    promotion_sender: Option<UnboundedSender<Vec<TxnId>>>,
    /// The admission ticket of the guarded write. It fences a scheduler that
    /// starts on a new table of the vShard while the write is in flight.
    ticket: Option<AdmissionTicket>,
}

impl WriteAdmissionGuard {
    /// The guard of the keys `txn` holds on `lock_manager`.
    pub(super) fn new(
        lock_manager: Arc<Mutex<LockManager>>,
        txn: TxnId,
        promotion_sender: Option<UnboundedSender<Vec<TxnId>>>,
    ) -> Self {
        Self {
            lock_manager,
            txn,
            promotion_sender,
            ticket: None,
        }
    }

    /// Carry `ticket` until the guard drops or its write lands.
    pub(super) fn carry_ticket(&mut self, ticket: AdmissionTicket) {
        self.ticket = Some(ticket);
    }

    /// The guarded write landed at `log_index` of `group_id`. Its ticket
    /// records the index and ends. The keys stay held.
    pub(super) fn landed(&mut self, group_id: u64, log_index: u64) {
        if let Some(ticket) = self.ticket.take() {
            ticket.landed(group_id, log_index);
        }
    }
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

/// Admit a write-class plan.
///
/// Synchronous: never awaits and never parks — it only *chooses* the outcome;
/// any await happens in the caller. `RouteToCalvin` and `Wait` are returned
/// only when a deterministic scheduler is registered for the write's vShard.
/// With no scheduler (single-node / no-Calvin) a point write returns
/// `FastPathBlocking` carrying the global keyed order-lock so concurrent
/// same-key writes still serialize, and every other shape returns
/// `Unscheduled` with no key: no Calvin transaction runs to conflict with it.
pub fn admit(shared: &SharedState, target: &WriteTarget<'_>) -> crate::Result<WriteAdmission> {
    admit_routed(shared, target, true)
}

/// [`admit`], where `may_route` says whether a contended write may run
/// through the scheduler (see `calvin_route_keeps`). A contended write that
/// may not waits for its keys instead.
pub(crate) fn admit_routed(
    shared: &SharedState,
    target: &WriteTarget<'_>,
    may_route: bool,
) -> crate::Result<WriteAdmission> {
    // A Calvin-scheduled apply already holds its locks (acquired by the
    // scheduler); it must never re-acquire at the gate.
    if is_calvin_apply(target.plan) || !plan_is_write(target.plan) {
        return Ok(WriteAdmission::ExemptRead);
    }

    let vshard = match plan_vshard(target.plan) {
        PlanRouting::Vshards(homes) => match homes.as_slice() {
            [home] => *home,
            // Only the scheduler takes keys on several lock tables as one
            // request, in sequencer order.
            homes => return Ok(admit_multi_home(shared, homes)),
        },
        // A write whose plan names no home orders on the vShard it targets.
        PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => {
            target.vshard_id
        }
    };

    // The ticket opens before the table lookup (see `tickets`).
    let ticket = shared.calvin.admission_tickets.open(vshard.as_u32());
    let Some(lock_manager) = lock_manager_of(shared, vshard) else {
        return Ok(match plan_row_key(target.plan) {
            Some(key) => WriteAdmission::FastPathBlocking {
                key,
                keyed_lock: Arc::clone(&shared.write_order_locks),
                ticket,
            },
            None => WriteAdmission::Unscheduled { ticket },
        });
    };

    let mut request =
        plan_admission_keys(shared, target.tenant_id, target.database_id, target.plan)?;
    request.sequenced &= may_route;
    let admission = admit_request(shared, vshard, lock_manager, request).carrying(ticket);
    if matches!(
        admission,
        WriteAdmission::RouteToCalvin | WriteAdmission::Wait(_)
    ) && tracing::enabled!(tracing::Level::DEBUG)
    {
        let plan: String = format!("{:?}", target.plan)
            .chars()
            .take(PLAN_SUMMARY_LEN)
            .collect();
        tracing::debug!(
            vshard_id = vshard.as_u32(),
            routes = matches!(admission, WriteAdmission::RouteToCalvin),
            %plan,
            "the write gate routes or holds a contended write"
        );
    }
    Ok(admission)
}

/// How many characters of a contended write's plan the gate's debug line
/// names.
const PLAN_SUMMARY_LEN: usize = 240;

/// Admit the lock request `request` on `vshard`, whose lock table is
/// `lock_manager`.
pub(crate) fn admit_request(
    shared: &SharedState,
    vshard: VShardId,
    lock_manager: Arc<Mutex<LockManager>>,
    request: AdmissionKeys,
) -> WriteAdmission {
    // A holder id in the reserved band never collides with a real Calvin
    // schedule position.
    let txn = TxnId::new(
        TxnId::AUTOCOMMIT_EPOCH,
        shared
            .calvin
            .autocommit_lock_seq
            .fetch_add(1, Ordering::Relaxed),
    );
    // Every guard hands the scheduler waiters it promotes on drop back to
    // the scheduler for dispatch. A lock manager and its promotion sender
    // are registered together per vShard.
    let promotion_sender = shared
        .calvin
        .promotion_senders
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&vshard.as_u32())
        .cloned();
    let admission = probe(lock_manager, txn, request, promotion_sender);
    if matches!(admission, WriteAdmission::RouteToCalvin) {
        ROUTED_TO_CALVIN.fetch_add(1, Ordering::Relaxed);
    }
    admission
}

/// Probe `lock_manager` for every key of `request` at once, WITHOUT
/// blocking. `try_acquire` never enqueues a waiter on the contended path, so
/// a routed or waiting write leaves no orphaned holder that a later `release`
/// can promote to an unowned lock.
fn probe(
    lock_manager: Arc<Mutex<LockManager>>,
    txn: TxnId,
    request: AdmissionKeys,
    promotion_sender: Option<UnboundedSender<Vec<TxnId>>>,
) -> WriteAdmission {
    let AdmissionKeys { keys, sequenced } = request;
    if keys.is_empty() {
        return WriteAdmission::FastPath { guard: None };
    }
    // A write that waits probes again with the same request.
    let retry_keys = (!sequenced).then(|| keys.clone());
    let acquired = {
        let mut table = lock_manager.lock().unwrap_or_else(|p| p.into_inner());
        let contention =
            tracing::enabled!(tracing::Level::DEBUG).then(|| table.contention(txn, &keys));
        let acquired = table.try_acquire(txn, keys);
        if !acquired {
            tracing::debug!(
                sequenced,
                ?contention,
                "a write found its keys held at the write gate"
            );
        }
        acquired
    };
    match (acquired, retry_keys) {
        (true, _) => WriteAdmission::FastPath {
            guard: Some(WriteAdmissionGuard::new(
                lock_manager,
                txn,
                promotion_sender,
            )),
        },
        // A holder or an earlier waiter has a key: the scheduler queues the
        // write FIFO behind it.
        (false, None) => WriteAdmission::RouteToCalvin,
        (false, Some(keys)) => WriteAdmission::Wait(AdmissionWait::new(
            lock_manager,
            txn,
            keys,
            promotion_sender,
        )),
    }
}

/// Admit a write homed on several vShards: the scheduler sequences it when
/// any home runs one. With no scheduler on any home, no Calvin transaction
/// conflicts with it.
fn admit_multi_home(shared: &SharedState, homes: &[VShardId]) -> WriteAdmission {
    // The ticket opens before the table lookups (see `tickets`).
    let ids: Vec<u32> = homes.iter().map(|home| home.as_u32()).collect();
    let ticket = shared.calvin.admission_tickets.open_many(&ids);
    if homes
        .iter()
        .any(|home| lock_manager_of(shared, *home).is_some())
    {
        ROUTED_TO_CALVIN.fetch_add(1, Ordering::Relaxed);
        WriteAdmission::RouteToCalvin
    } else {
        WriteAdmission::Unscheduled { ticket }
    }
}

impl WriteAdmission {
    /// This admission, carrying the ticket of its write.
    ///
    /// A guard or a wait carries the ticket until its write reached its core
    /// or its log. A write that takes no key, routes, or is exempt ends its
    /// ticket here: no later scheduler orders against it at the gate.
    pub(super) fn carrying(self, ticket: AdmissionTicket) -> Self {
        match self {
            Self::FastPath {
                guard: Some(mut guard),
            } => {
                guard.carry_ticket(ticket);
                Self::FastPath { guard: Some(guard) }
            }
            Self::Wait(mut wait) => {
                wait.carry_ticket(ticket);
                Self::Wait(wait)
            }
            other @ (Self::FastPath { guard: None }
            | Self::FastPathBlocking { .. }
            | Self::Unscheduled { .. }
            | Self::RouteToCalvin
            | Self::ExemptRead) => other,
        }
    }
}

/// The lock table of `vshard`'s Calvin scheduler on this node, if one runs.
pub(crate) fn lock_manager_of(
    shared: &SharedState,
    vshard: VShardId,
) -> Option<Arc<Mutex<LockManager>>> {
    shared
        .calvin
        .lock_managers
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&vshard.as_u32())
        .map(Arc::clone)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::control::cluster::calvin::scheduler::lock_manager::{AcquireOutcome, LockMode};

    fn coll() -> LockKey {
        LockKey::Collection {
            collection: Arc::from("c"),
        }
    }

    fn row(surrogate: u32) -> LockKey {
        LockKey::Surrogate {
            collection: Arc::from("c"),
            surrogate,
        }
    }

    fn row_write(surrogate: u32) -> BTreeMap<LockKey, LockMode> {
        BTreeMap::from([
            (row(surrogate), LockMode::Exclusive),
            (coll(), LockMode::Intent),
        ])
    }

    fn request(keys: BTreeMap<LockKey, LockMode>, sequenced: bool) -> AdmissionKeys {
        AdmissionKeys { keys, sequenced }
    }

    fn autocommit(position: u32) -> TxnId {
        TxnId::new(TxnId::AUTOCOMMIT_EPOCH, position)
    }

    /// Two row writes of distinct rows hold the collection `Intent` together.
    /// The guard passes each key in its own mode.
    #[test]
    fn row_writes_of_distinct_rows_admit_together() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let first = probe(
            Arc::clone(&lock_manager),
            autocommit(0),
            request(row_write(1), true),
            None,
        );
        let second = probe(
            Arc::clone(&lock_manager),
            autocommit(1),
            request(row_write(2), true),
            None,
        );
        assert!(matches!(first, WriteAdmission::FastPath { guard: Some(_) }));
        assert!(matches!(
            second,
            WriteAdmission::FastPath { guard: Some(_) }
        ));
        drop((first, second));
        assert_eq!(lock_manager.lock().expect("table").lock_count(), 0);
    }

    /// A Calvin truncate holds the collection `Exclusive`. A sequenced row
    /// write routes to the scheduler, and an unsequenced one waits.
    #[test]
    fn a_contended_write_routes_when_sequenced_and_waits_otherwise() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let truncate = TxnId::new(3, 0);
        assert_eq!(
            lock_manager
                .lock()
                .expect("table")
                .acquire(truncate, BTreeMap::from([(coll(), LockMode::Exclusive)]))
                .outcome,
            AcquireOutcome::Ready
        );
        let routed = probe(
            Arc::clone(&lock_manager),
            autocommit(0),
            request(row_write(1), true),
            None,
        );
        assert!(matches!(routed, WriteAdmission::RouteToCalvin));
        let waits = probe(
            Arc::clone(&lock_manager),
            autocommit(1),
            request(row_write(1), false),
            None,
        );
        assert!(matches!(waits, WriteAdmission::Wait(_)));
        assert_eq!(
            lock_manager.lock().expect("table").holder_count(),
            1,
            "a refused probe takes no key"
        );
    }

    /// A Calvin transaction that needs a key a fast-path write holds waits
    /// for the guard's drop, which promotes it.
    #[test]
    fn a_calvin_writer_waits_for_the_fast_path_guard() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let admitted = probe(
            Arc::clone(&lock_manager),
            autocommit(0),
            request(row_write(1), true),
            None,
        );
        assert!(matches!(
            admitted,
            WriteAdmission::FastPath { guard: Some(_) }
        ));
        let truncate = TxnId::new(3, 0);
        assert_eq!(
            lock_manager
                .lock()
                .expect("table")
                .acquire(truncate, BTreeMap::from([(coll(), LockMode::Exclusive)]))
                .outcome,
            AcquireOutcome::Blocked
        );
        drop(admitted);
        assert!(
            lock_manager
                .lock()
                .expect("table")
                .is_ready(truncate, &BTreeMap::from([(coll(), LockMode::Exclusive)]))
        );
    }

    /// A write that names no lock key takes the fast path with no guard.
    #[test]
    fn a_write_with_no_keys_takes_no_guard() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let admission = probe(lock_manager, autocommit(0), AdmissionKeys::default(), None);
        assert!(matches!(
            admission,
            WriteAdmission::FastPath { guard: None }
        ));
    }

    /// A guarded write still between its admit and its landing when its
    /// scheduler retires holds the next scheduler's fence. Once it lands,
    /// the fence waits for its index. Its keys on the retired table order
    /// nothing on the new one.
    #[test]
    fn a_guarded_write_in_flight_at_retirement_fences_the_next_scheduler() {
        use super::super::tickets::{AdmissionTickets, FenceState};

        const VSHARD: u32 = 7;
        const GROUP: u64 = 4;
        let tickets = Arc::new(AdmissionTickets::new());
        let retired_table = Arc::new(Mutex::new(LockManager::new()));
        let ticket = tickets.open(VSHARD);
        let admission = probe(
            Arc::clone(&retired_table),
            autocommit(0),
            request(row_write(1), true),
            None,
        )
        .carrying(ticket);
        let WriteAdmission::FastPath {
            guard: Some(mut guard),
        } = admission
        else {
            panic!("an uncontended row write holds its keys");
        };

        // The scheduler retires, and a new one starts on a new table.
        let fence = tickets.start_fence(VSHARD);
        assert_eq!(
            tickets.fence_state(fence, GROUP),
            FenceState::Open,
            "the in-flight write holds the fence"
        );

        guard.landed(GROUP, 15);
        assert_eq!(
            tickets.fence_state(fence, GROUP),
            FenceState::Through(Some(15)),
            "the landed write fences until its index applies"
        );
        drop(guard);
        assert_eq!(
            tickets.fence_state(fence, GROUP),
            FenceState::Through(Some(15)),
            "dropping the keys keeps the landing"
        );
    }

    /// A wait carries its ticket on into the guard it acquires.
    #[tokio::test]
    async fn a_wait_carries_its_ticket_into_its_guard() {
        use super::super::tickets::{AdmissionTickets, FenceState};

        let tickets = Arc::new(AdmissionTickets::new());
        let table = Arc::new(Mutex::new(LockManager::new()));
        let holder = TxnId::new(9, 0);
        assert_eq!(
            table
                .lock()
                .expect("table")
                .acquire(holder, BTreeMap::from([(coll(), LockMode::Exclusive)]))
                .outcome,
            AcquireOutcome::Ready
        );
        let admission = probe(
            Arc::clone(&table),
            autocommit(0),
            request(row_write(1), false),
            None,
        )
        .carrying(tickets.open(7));
        let WriteAdmission::Wait(wait) = admission else {
            panic!("a contended write that cannot route waits");
        };
        let _ = table.lock().expect("table").release(holder);
        let guard = wait
            .acquire(tokio::time::Instant::now() + std::time::Duration::from_secs(10))
            .await
            .expect("the keys free up");
        let fence = tickets.start_fence(7);
        assert_eq!(tickets.fence_state(fence, 4), FenceState::Open);
        drop(guard);
        assert_eq!(tickets.fence_state(fence, 4), FenceState::Through(None));
    }
}
