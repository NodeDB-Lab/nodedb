// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane write-admission gate: the single seam every write-class
//! `PhysicalPlan` passes through before it is ordered. A local write passes
//! it before its enqueue, a replicated write on its data-group leader before
//! its propose. An uncontended write holds its per-vShard deterministic locks;
//! a contended write routes to the Calvin scheduler or waits.
pub mod admission_keys;
pub mod entry_storage;
pub mod gate;
pub mod holds;
pub mod leader_gate;
pub mod lock_keys;
pub mod mirror_gate;
pub mod predicate;
pub mod route;
pub mod storage_quota;
pub mod tickets;
pub mod wait;
pub mod write_order_fence;
pub mod write_order_lock;

pub use entry_storage::{entry_adds_storage, entry_writes_database};
pub use gate::{WriteAdmission, WriteAdmissionGuard, WriteTarget, admit, cp_routed_to_calvin};
pub use holds::AdmissionHolds;
pub use leader_gate::LeaderWriteGate;
pub(crate) use mirror_gate::{check_mirror_ddl, check_mirror_read, check_mirror_writable};
pub use predicate::{
    all_writes_bufferable, plan_adds_storage, plan_is_write, plan_requires_txn_buffering,
    plan_writes_user_data,
};
pub use route::{bare_ok_response, calvin_route_keeps, route_write_to_calvin};
pub(crate) use storage_quota::check_storage_quota;
pub use tickets::{AdmissionTicket, AdmissionTickets, FenceState, StartFence};
pub use wait::AdmissionWait;
pub(crate) use write_order_fence::order_row_write;
pub use write_order_fence::{WriteOrder, WriteOrderFence};
pub use write_order_lock::KeyedWriteOrderLock;
