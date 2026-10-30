// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for descriptor leases, drains and DDL preparation leases
//! that were not maintained or cleaned up.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a descriptor lease the renewal loop could not refresh or release.
/// Called only from the renewal loop's error arms. `step` is `"renew"` or
/// `"release"`.
pub fn descriptor_lease_not_renewed(
    err: &crate::Error,
    step: &'static str,
    descriptor: &nodedb_cluster::DescriptorId,
    version: u64,
    node_id: u64,
) {
    let class = error_class(err);
    let descriptor = format!("{descriptor:?}");
    let ctx = context::DescriptorLeaseNotRenewed {
        step,
        descriptor: &descriptor,
        version,
        node_id,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "descriptor lease renewal: the lease was neither refreshed nor released",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a DDL preparation lease whose failure cleanup (cancel its pending
/// record, release it) did not apply on any attempt. Called only from the
/// background lease releaser's give-up arm.
pub fn ddl_cleanup_not_completed(err: &crate::Error, step: &'static str, token: u64) {
    let class = error_class(err);
    let ctx = context::DdlCleanupNotCompleted {
        step,
        token,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "DDL cleanup: a failed DDL's lease was neither cancelled nor released",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a descriptor drain whose holder gave up and whose end did not apply
/// on any attempt. Called only from the background lease releaser's give-up
/// arm.
pub fn descriptor_drain_not_ended(
    err: &crate::Error,
    descriptor: &nodedb_cluster::DescriptorId,
    owner: &nodedb_cluster::DrainOwner,
) {
    let class = error_class(err);
    let descriptor = format!("{descriptor:?}");
    let owner = format!("{owner:?}");
    let ctx = context::DescriptorDrainNotEnded {
        descriptor: &descriptor,
        owner: &owner,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "descriptor drain: an abandoned drain did not end",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
