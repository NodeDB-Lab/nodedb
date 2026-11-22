// SPDX-License-Identifier: BUSL-1.1

//! The footprint of a committed redo that is no Calvin slice.
//!
//! A whole record holds its content inline. It installs to its end inside
//! its task, and never parks. Its footprint is a write. Its install writes:
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
//! A stream marker names no op. Its stream reference carries the keys the
//! proposer declared for its content: the collection of each read, each
//! sum source and target its header names, each sync stream its ops
//! advance, and whether an op writes graph state. Its install arms the
//! same keys before its validate (see `batched::park::start`).
//!
//! One install parks at a time. So a marker waits while any fence is
//! armed, then parks under its own fence. While it waits it holds only
//! what its install touches (`Footprint::WriteAfterPark`). A marker whose
//! keys pass the final entry's room declares none, so it is a core task.
//!
//! A record that does not decode is a core task. A read that names no
//! collection reads its whole vShard, so its record is a core task too. A
//! malformed name files a report and leaves a core task.
//!
//! The decode runs only while a fence is armed, since `hold_fenced` asks
//! for no footprint otherwise.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::RedoSumTargets;
use nodedb_types::calvin::VersionedReadEntry;

use crate::data::executor::handlers::transaction::redo_apply::add_sub_record_keys;
use crate::wal::{DeclaredStreamKeys, RedoRecord, RedoStreamKeys, RedoStreamRef};

use super::super::FenceKey;
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
    let Ok(reference) = RedoStreamRef::of_marker(&record) else {
        return Footprint::Core;
    };
    if plan.reads.iter().any(|read| read.collection.is_empty()) {
        return Footprint::Core;
    }
    let named = named_keys(plan, scope);
    match reference {
        None => {
            let mut op_keys = BTreeSet::new();
            for sub in &record.ops {
                add_sub_record_keys(sub, scope.db, scope.tenant, &mut op_keys);
            }
            named.armed(op_keys).write_or_core()
        }
        Some(reference) => match &reference.keys {
            RedoStreamKeys::Declared(declared) => declared_footprint(named, declared),
            // Keys past the final entry's room are not declared.
            RedoStreamKeys::Unbounded => Footprint::Core,
        },
    }
}

/// `named` with the keys a stream marker declares in `declared`.
fn declared_footprint(named: KeySet<'_>, declared: &DeclaredStreamKeys) -> Footprint {
    if declared.reads.iter().any(String::is_empty) {
        return Footprint::Core;
    }
    let summed = declared
        .sum_collections
        .iter()
        .fold(named, |keys, name| keys.sum_target(name));
    let read = declared
        .reads
        .iter()
        .fold(summed, |keys, name| keys.requested(name))
        .armed(
            declared
                .sync_streams
                .iter()
                .copied()
                .map(FenceKey::of_sync_stream),
        );
    if declared.graph {
        read.graph().write_after_park_or_core()
    } else {
        read.write_after_park_or_core()
    }
}

/// The keys `plan` names beside its record: its written collections, its
/// sum sources and targets, and the collection of each read.
fn named_keys<'s>(plan: &RedoPlan<'_>, scope: &'s Scope<'s>) -> KeySet<'s> {
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
}
