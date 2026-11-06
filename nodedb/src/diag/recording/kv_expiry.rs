// SPDX-License-Identifier: BUSL-1.1

//! Capture site for a KV expiry entry the reaper cannot reap.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a due KV expiry whose stored collection name resolves to no
/// collection. Called from the reap pass, which continues with its other keys.
pub fn kv_reap_entry_unusable(err: &crate::Error, database_id: u64, tenant_id: u64) {
    let class = error_class(err);
    let ctx = context::KvReapEntryUnusable {
        database_id,
        tenant_id,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "kv expiry: a due entry names no valid collection, so its key never reaps",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
