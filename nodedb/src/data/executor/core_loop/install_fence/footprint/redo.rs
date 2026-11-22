// SPDX-License-Identifier: BUSL-1.1

//! The footprint of a committed redo that holds its content inline.
//!
//! A whole record installs to its end inside its task, and never parks.
//! Its install writes:
//! - each collection the plan names, by stored name
//! - each materialized-sum source and target, qualified for the request's
//!   database
//! - the sync stream of each sync frame among its ops
//! - the graph state of the request's tenant, when an op writes an edge,
//!   an edge cut or a node label
//!
//! It also reads the collection of each entry of its read set. Each key
//! takes the form a parked stream install arms it in. So a whole record
//! waits only for an install that touches what it touches.
//!
//! A stream marker can park, and one install parks at a time. So a marker
//! stays a core task, and so does a record that does not decode. A read
//! that names no collection reads its whole vShard, so its record is a
//! core task too. A malformed name files a report and leaves a core task.
//!
//! The decode runs only while a fence is armed, since `hold_fenced` asks
//! for no footprint otherwise.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::RedoSumTargets;
use nodedb_types::calvin::VersionedReadEntry;

use crate::data::executor::handlers::transaction::redo_apply::add_sub_record_keys;
use crate::wal::{RedoRecord, RedoStreamRef};

use super::class::{Footprint, Scope};
use super::keys::KeySet;

/// The fields of a committed redo plan its footprint reads.
pub(super) struct RedoPlan<'p> {
    /// The encoded redo record.
    pub(super) redo: &'p [u8],
    /// The stored name of every collection the record writes.
    pub(super) collections: &'p [String],
    /// The materialized-sum sources and targets the record folds into.
    pub(super) sum_targets: &'p [RedoSumTargets],
    /// The record's read set.
    pub(super) reads: &'p [VersionedReadEntry],
}

/// The footprint of the install of `plan`, run in `scope`.
pub(super) fn redo_footprint(plan: &RedoPlan<'_>, scope: &Scope<'_>) -> Footprint {
    let Ok(record) = RedoRecord::from_bytes(plan.redo) else {
        return Footprint::Core;
    };
    match RedoStreamRef::of_marker(&record) {
        Ok(None) => {}
        Ok(Some(_)) | Err(_) => return Footprint::Core,
    }
    if plan.reads.iter().any(|read| read.collection.is_empty()) {
        return Footprint::Core;
    }
    let mut op_keys = BTreeSet::new();
    for sub in &record.ops {
        add_sub_record_keys(sub, scope.db, scope.tenant, &mut op_keys);
    }
    let written = plan
        .collections
        .iter()
        .fold(KeySet::new(scope), |keys, name| keys.stored(name));
    let folded = plan
        .sum_targets
        .iter()
        .flat_map(|targets| {
            std::iter::once(targets.collection.as_str()).chain(
                targets
                    .resolved
                    .iter()
                    .map(|target| target.target_collection.as_str()),
            )
        })
        .fold(written, |keys, name| keys.sum_target(name));
    plan.reads
        .iter()
        .fold(folded, |keys, read| keys.requested(&read.collection))
        .armed(op_keys)
        .write_or_core()
}
