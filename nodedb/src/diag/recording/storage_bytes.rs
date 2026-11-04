// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for stored-bytes counters that disagree with their rows.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a counter change that removed more bytes than the counter held.
/// Called from the counter's apply step, which then clamps at zero.
pub fn storage_bytes_underflow(
    counter: &'static str,
    database_id: u64,
    tenant_id: u64,
    collection: Option<&str>,
    held: u64,
    removed: u64,
) {
    let ctx = context::StorageBytesUnderflow {
        counter,
        database_id,
        tenant_id,
        collection,
        held,
        removed,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "storage bytes: a change removed more bytes than the counter held, so it clamped at zero",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a counter row left after its scope's counted rows were dropped.
/// Called from the scope retirement, which then removes the row.
pub fn storage_bytes_residue(
    counter: &'static str,
    scope: &'static str,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    bytes: u64,
) {
    let ctx = context::StorageBytesResidue {
        counter,
        scope,
        database_id,
        tenant_id,
        collection,
        bytes,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "storage bytes: a dropped scope's counter still held bytes after its rows were gone",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a storage sample that failed. Called from the storage sampler,
/// which keeps the last share and retries at the next period.
pub fn storage_bytes_sample_failed(err: &crate::Error) {
    let class = error_class(err);
    let ctx = context::StorageBytesSampleFailed {
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "storage bytes: a sample did not finish, so the published storage totals are stale",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a peer that did not answer a storage round. Called from the
/// storage aggregator, which skips the peer and asks again next round.
pub fn storage_share_peer_skipped(peer: u64, err: &crate::Error) {
    let class = error_class(err);
    let ctx = context::StorageSharePeerSkipped {
        peer,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "storage bytes: a peer did not answer its storage share, so the round skipped it",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .emit();
}
