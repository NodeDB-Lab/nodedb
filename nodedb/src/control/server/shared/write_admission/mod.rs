// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane write-admission gate: the single seam every write-class
//! `PhysicalPlan` passes through before it is ordered. A local write passes
//! it before its enqueue, a replicated write on its data-group leader before
//! its propose. An uncontended write holds its per-vShard deterministic locks;
//! a contended write routes to the Calvin scheduler or waits.
pub mod admission_keys;
pub mod gate;
pub mod holds;
pub mod leader_gate;
pub mod lock_keys;
pub mod predicate;
pub mod route;
pub mod wait;
pub mod write_order_fence;
pub mod write_order_lock;

pub use gate::{WriteAdmission, WriteAdmissionGuard, WriteTarget, admit, cp_routed_to_calvin};
pub use holds::AdmissionHolds;
pub use leader_gate::LeaderWriteGate;
pub use predicate::{
    all_writes_bufferable, plan_is_write, plan_requires_txn_buffering, plan_writes_user_data,
};
pub use route::{bare_ok_response, calvin_route_keeps, route_write_to_calvin};
pub use wait::AdmissionWait;
pub(crate) use write_order_fence::order_row_write;
pub use write_order_fence::{WriteOrder, WriteOrderFence};
pub use write_order_lock::KeyedWriteOrderLock;
