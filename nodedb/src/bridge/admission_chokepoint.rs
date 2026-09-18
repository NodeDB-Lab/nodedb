// SPDX-License-Identifier: BUSL-1.1

//! Write-admission chokepoint guard for the SPSC enqueue points.
//!
//! Both [`crate::bridge::dispatch::Dispatcher::dispatch`] and
//! `dispatch_to_core` funnel every request into a Data-Plane core here: no
//! write-class plan may reach a core marked exempt-as-read — that marker means
//! the plan claimed to be a non-write and skipped the write-admission gate.
//! Debug builds trip loudly; release builds increment a counter so a future
//! write path that bypasses the write-admission gate is caught even when the
//! assertion is compiled out — turning "a missed write path is a silent
//! serializability hole" into a loud, observable regression.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::bridge::envelope::Request;

/// Count of write-class Requests that reached an SPSC enqueue point marked
/// [`Admission::Exempt`]`(`[`ExemptReason::Read`]`)` — i.e. hand-labeled a
/// non-write and thus bypassed the write-admission gate.
///
/// [`Admission::Exempt`]: crate::bridge::envelope::Admission::Exempt
/// [`ExemptReason::Read`]: crate::bridge::envelope::ExemptReason::Read
///
/// Stays at zero by construction: a base-state write is either
/// [`Admission::Admitted`] by the gate or [`Admission::Exempt`] with
/// [`ExemptReason::AlreadyOrdered`]. A non-zero value signals a regressed write
/// path that was marked exempt-as-read.
///
/// [`Admission::Admitted`]: crate::bridge::envelope::Admission::Admitted
/// [`ExemptReason::AlreadyOrdered`]: crate::bridge::envelope::ExemptReason::AlreadyOrdered
static WRITES_BYPASSED_ADMISSION_GATE: AtomicU64 = AtomicU64::new(0);

/// Read the count of writes that reached an SPSC enqueue point marked
/// exempt-as-read (bypassing the write-admission gate). Exposed for the metrics
/// exporter and tests.
pub fn writes_bypassed_admission_gate() -> u64 {
    WRITES_BYPASSED_ADMISSION_GATE.load(Ordering::Relaxed)
}

/// Chokepoint guard shared by both SPSC enqueue points: catch a write-class
/// plan that reached a Data-Plane core marked exempt-as-read — i.e. bypassed
/// the write-admission gate while claiming to be a non-write.
#[inline]
pub(crate) fn assert_write_admitted(request: &Request) {
    let bypassed = crate::control::server::shared::write_admission::plan_is_write(&request.plan)
        && request.admission.is_exempt_as_read();
    if bypassed {
        WRITES_BYPASSED_ADMISSION_GATE.fetch_add(1, Ordering::Relaxed);
    }
    debug_assert!(
        !bypassed,
        "a write-class plan reached the SPSC enqueue marked Exempt(Read) — it bypassed the write-admission gate"
    );
}

/// Refuse a write plan that reached the Data Plane before RLS injection
/// filled in its write check.
///
/// A write op's `RlsWriteCheck` starts as `PendingInjection` when the plan
/// is built, and RLS injection later replaces it with the real decision
/// (`Predicate`, `NoPolicyApplies`, or one of the other resolved states).
/// If any check on the plan is still `PendingInjection` here, injection was
/// skipped, and letting the write through would silently run it with no
/// write policy at all. This returns `Err` in every build — unlike
/// [`assert_write_admitted`], which only counts in release builds, a
/// `PendingInjection` write must never reach the Data Plane.
pub(crate) fn reject_uninjected_write(request: &Request) -> crate::Result<()> {
    let has_pending = request
        .plan
        .rls_write_checks()
        .into_iter()
        .any(|check| check.is_pending_injection());
    if !has_pending {
        return Ok(());
    }
    let detail = match request.plan.collection() {
        Some(collection) => format!(
            "internal invariant break: write plan for collection '{collection}' reached the \
             Data Plane before RLS injection ran (RlsWriteCheck::PendingInjection); this is not \
             a policy rejection, it is a skipped injection step"
        ),
        None => "internal invariant break: write plan reached the Data Plane before RLS \
                  injection ran (RlsWriteCheck::PendingInjection); this is not a policy \
                  rejection, it is a skipped injection step"
            .to_string(),
    };
    Err(crate::Error::Internal { detail })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{Admission, ExemptReason, PhysicalPlan, Priority};
    use crate::control::server::shared::write_admission::plan_is_write;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::{DocumentOp, KvOp};
    use nodedb_types::QualifiedCollection;
    use std::time::{Duration, Instant};

    /// A write-class plan: `plan_is_write` returns true for `KvOp::Put`.
    fn write_plan() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// A read-class plan: `plan_is_write` returns false for `DocumentOp::PointGet`.
    fn read_plan() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            document_id: "d".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        })
    }

    fn request_with(plan: PhysicalPlan, admission: Admission) -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::generate(),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission,
        }
    }

    /// The guard BITES: a write-class plan marked `Exempt(Read)` bypassed the
    /// write-admission gate, so the debug assertion must fire.
    #[test]
    #[should_panic(expected = "bypassed the write-admission gate")]
    fn write_marked_exempt_as_read_trips_guard() {
        let req = request_with(write_plan(), Admission::Exempt(ExemptReason::Read));
        assert_write_admitted(&req);
    }

    /// A write that passed the gate (`Admitted`) does not trip the guard.
    #[test]
    fn admitted_write_does_not_trip() {
        let req = request_with(write_plan(), Admission::Admitted);
        assert_write_admitted(&req);
    }

    /// A legitimate read marked `Exempt(Read)` does not trip the guard.
    #[test]
    fn read_marked_exempt_as_read_does_not_trip() {
        let req = request_with(read_plan(), Admission::Exempt(ExemptReason::Read));
        assert_write_admitted(&req);
    }

    /// An already-ordered write (Calvin/Raft-follower/replay) is legitimately
    /// exempt and does not trip the guard.
    #[test]
    fn write_marked_already_ordered_does_not_trip() {
        let req = request_with(
            write_plan(),
            Admission::Exempt(ExemptReason::AlreadyOrdered),
        );
        assert_write_admitted(&req);
    }

    /// Sanity: the write plan is classified as a write and the read plan is not.
    /// The bypass predicate the counter keys on is
    /// `plan_is_write(&plan) && admission.is_exempt_as_read()`; asserting the
    /// predicate directly (rather than calling `assert_write_admitted`, which
    /// panics in debug) verifies the counter-increment condition without the
    /// panic.
    #[test]
    fn plan_is_write_classification_and_bypass_predicate() {
        let w = write_plan();
        let r = read_plan();
        assert!(plan_is_write(&w));
        assert!(!plan_is_write(&r));

        let exempt_read = Admission::Exempt(ExemptReason::Read);
        // The exact condition under which the counter increments and the guard trips.
        assert!(plan_is_write(&w) && exempt_read.is_exempt_as_read());
        // A read marked exempt-as-read is NOT a bypass.
        assert!(!(plan_is_write(&r) && exempt_read.is_exempt_as_read()));
        // A write marked Admitted is NOT a bypass.
        assert!(!(plan_is_write(&w) && Admission::Admitted.is_exempt_as_read()));
        // A write marked AlreadyOrdered is NOT a bypass.
        assert!(
            !(plan_is_write(&w)
                && Admission::Exempt(ExemptReason::AlreadyOrdered).is_exempt_as_read())
        );
    }

    /// A `KvOp::Delete` write plan carrying the given `RlsWriteCheck`.
    fn delete_plan_with_check(check: nodedb_types::RlsWriteCheck) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            keys: vec![b"k".to_vec()],
            rls_write_check: check,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// A `KvOp::TransferItem` plan with independently settable source and
    /// dest checks — exercises both check slots on one plan.
    fn transfer_item_plan_with_checks(
        source: nodedb_types::RlsWriteCheck,
        dest: nodedb_types::RlsWriteCheck,
    ) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "src"),
            dest_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "dst"),
            item_key: b"item".to_vec(),
            dest_key: b"item".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            source_rls_write_check: source,
            dest_rls_write_check: dest,
        })
    }

    /// A `PendingInjection` write plan is refused: injection never ran.
    #[test]
    fn pending_injection_write_is_rejected() {
        let req = request_with(
            delete_plan_with_check(nodedb_types::RlsWriteCheck::PendingInjection),
            Admission::Admitted,
        );
        let err = reject_uninjected_write(&req).expect_err("must reject PendingInjection");
        assert!(matches!(err, crate::Error::Internal { .. }));
    }

    /// Every resolved `RlsWriteCheck` state — the outcomes RLS injection can
    /// actually leave behind — is accepted.
    #[test]
    fn resolved_write_checks_are_accepted() {
        let resolved = [
            nodedb_types::RlsWriteCheck::NoPolicyApplies,
            nodedb_types::RlsWriteCheck::Predicate(vec![1, 2, 3]),
            nodedb_types::RlsWriteCheck::AlreadyDecidedElsewhere,
            nodedb_types::RlsWriteCheck::DecidedEarlierInRequest,
            nodedb_types::RlsWriteCheck::SystemInternalCollection,
        ];
        for check in resolved {
            let req = request_with(delete_plan_with_check(check), Admission::Admitted);
            reject_uninjected_write(&req).expect("resolved check must be accepted");
        }
    }

    /// A read-class plan carries no write check at all and is accepted.
    #[test]
    fn read_plan_is_accepted() {
        let req = request_with(read_plan(), Admission::Exempt(ExemptReason::Read));
        reject_uninjected_write(&req).expect("read plan must be accepted");
    }

    /// `KvOp::TransferItem` carries two check slots. A `PendingInjection` in
    /// the SECOND (dest) slot alone must still be caught — proves the guard
    /// checks every slot, not just the first.
    #[test]
    fn transfer_item_pending_in_dest_slot_is_rejected() {
        let req = request_with(
            transfer_item_plan_with_checks(
                nodedb_types::RlsWriteCheck::NoPolicyApplies,
                nodedb_types::RlsWriteCheck::PendingInjection,
            ),
            Admission::Admitted,
        );
        let err = reject_uninjected_write(&req)
            .expect_err("PendingInjection in the dest slot alone must be rejected");
        assert!(matches!(err, crate::Error::Internal { .. }));
    }
}
