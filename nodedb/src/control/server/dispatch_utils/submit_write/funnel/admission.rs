// SPDX-License-Identifier: BUSL-1.1

//! Write-admission gate dispatch for the funnel.

use tokio::sync::OwnedMutexGuard;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::write_admission::gate::admit_routed;
use crate::control::server::shared::write_admission::{
    WriteAdmission, WriteAdmissionGuard, WriteTarget,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};

use super::super::params::WriteOrdering;

/// The write the admission phase admits.
pub(super) struct AdmissionTarget<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    pub plan: &'a PhysicalPlan,
    /// Whether a contended write may run through the scheduler, which
    /// applies it as a client write. A write of another event source waits
    /// for its keys instead.
    pub may_route: bool,
}

/// Outcome of the write-admission phase.
pub(super) enum AdmissionOutcome {
    /// Proceed with the funnel's remaining phases under these guards.
    Proceed {
        admission: crate::bridge::envelope::Admission,
        admission_guard: Option<WriteAdmissionGuard>,
        order_guard: Option<OwnedMutexGuard<()>>,
    },
    /// The deterministic Calvin scheduler must apply the write.
    RouteToCalvin,
}

/// Write-admission gate: every write-class plan whose ordering is not already
/// final passes here. An uncontended write takes the fast path holding its
/// per-vShard deterministic locks; a contended write is submitted through the
/// deterministic scheduler and its applied response is surfaced here; reads /
/// control ops are `Exempt`.
///
/// Ordering (fast path): the guard is acquired FIRST, then — for a write that
/// owns its durability (`AppendHere`) — the WAL append happens after this
/// call returns, under the guard, minting the LSN just before the enqueue.
/// The guard is released immediately after the enqueue (not across the
/// response await).
///
/// A contended write no Calvin transaction can sequence waits for its keys
/// until `deadline`, holding none meanwhile.
pub(super) async fn admit_write(
    shared: &SharedState,
    target: AdmissionTarget<'_>,
    ordering: WriteOrdering,
    deadline: std::time::Instant,
) -> crate::Result<AdmissionOutcome> {
    let AdmissionTarget {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        may_route,
    } = target;
    Ok(match ordering {
        WriteOrdering::AlreadyOrdered => AdmissionOutcome::Proceed {
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::AlreadyOrdered,
            ),
            admission_guard: None,
            order_guard: None,
        },
        WriteOrdering::Gate => match admit_routed(
            shared,
            &WriteTarget {
                tenant_id,
                database_id,
                vshard_id,
                plan,
            },
            may_route,
        )? {
            WriteAdmission::ExemptRead => AdmissionOutcome::Proceed {
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::Read,
                ),
                admission_guard: None,
                order_guard: None,
            },
            WriteAdmission::FastPath { guard } => AdmissionOutcome::Proceed {
                admission: crate::bridge::envelope::Admission::Admitted,
                admission_guard: guard,
                order_guard: None,
            },
            WriteAdmission::FastPathBlocking { key, keyed_lock } => {
                // Single-node serialization point: acquire the per-key FIFO
                // order-lock FIRST, before the WAL append and enqueue below.
                // `tokio::sync::Mutex` is fair, so concurrent same-key writers
                // are admitted in arrival order — the WAL append + enqueue then
                // happen in that order, giving WAL-LSN order == enqueue order ==
                // apply order per key. Distinct keys use distinct per-key mutexes
                // and never contend.
                let order_guard = keyed_lock.lock_owned(key).await;
                AdmissionOutcome::Proceed {
                    admission: crate::bridge::envelope::Admission::Admitted,
                    admission_guard: None,
                    order_guard: Some(order_guard),
                }
            }
            WriteAdmission::RouteToCalvin => AdmissionOutcome::RouteToCalvin,
            WriteAdmission::Wait(wait) => AdmissionOutcome::Proceed {
                admission: crate::bridge::envelope::Admission::Admitted,
                admission_guard: Some(
                    wait.acquire(tokio::time::Instant::from_std(deadline))
                        .await?,
                ),
                order_guard: None,
            },
        },
    })
}
