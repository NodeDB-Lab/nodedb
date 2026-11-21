// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for the install fence of a Data-Plane core.

use faultbox::{Capture, EventKind, error_chain_of};

use crate::diag::context;

/// Report a collection name the install fence keys by that does not parse
/// as a stored collection name. `site` names the detecting site and what it
/// does instead. Called from the fence footprint of a task and from the
/// fence keys of a stream install.
pub fn stored_collection_name_malformed(
    site: &'static str,
    name: &str,
    err: &(dyn std::error::Error + 'static),
) {
    let ctx = context::StoredCollectionNameMalformed { site, name };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "install fence: a plan names a collection that is not a stored collection name",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
