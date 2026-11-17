// SPDX-License-Identifier: BUSL-1.1

//! Capture site for a full-text or CSR rebuild that did not install.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Where a rebuild belongs: the index kind and the collection it covered.
pub struct IndexRebuildTarget<'a> {
    /// `fts` or `csr`.
    pub index: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
}

/// Report a rebuild the core discarded. Called from the cutover arm that
/// receives the refusal or the build error.
pub fn index_rebuild_not_installed(
    err: &(dyn std::error::Error + 'static),
    target: &IndexRebuildTarget<'_>,
) {
    let class = error_class(err);
    let ctx = context::IndexRebuildNotInstalled {
        index: target.index,
        database_id: target.database_id,
        tenant_id: target.tenant_id,
        collection: target.collection,
        cause_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "index rebuild did not install; the live index stays in place",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .emit();
}

/// Where an owed full-text index step belongs.
pub struct TextIndexStepTarget<'a> {
    /// `build` or `drop`.
    pub step: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub index: &'a str,
}

/// Report a full-text index step the boot reconcile could not finish.
/// Called only from the boot reconcile, which leaves the record for the
/// next boot or a `DROP INDEX`.
pub fn text_index_step_owed(sqlstate: &str, message: &str, target: &TextIndexStepTarget<'_>) {
    let ctx = context::TextIndexStepOwed {
        step: target.step,
        database_id: target.database_id,
        tenant_id: target.tenant_id,
        collection: target.collection,
        index: target.index,
        sqlstate,
    };
    let _ = Capture::new(
        EventKind::Error,
        "full-text index step still owed after the boot reconcile",
    )
    .error_chain([message])
    .domain(&ctx)
    .emit();
}
