// SPDX-License-Identifier: BUSL-1.1

//! The RAM a streamed validate holds between its batches, for the governor
//! charge of a parked install.
//!
//! The spools, the post-state and the arms' scope report their hot bytes.
//! The rest has no byte measure of its own, so it counts by an estimate:
//! each entry's size plus the bytes of the strings it owns.

use std::mem::size_of;

use nodedb_physical::physical_plan::{RedoSumTargets, ResolvedSumTarget};

use crate::data::executor::core_loop::install_fence::FenceKey;

use super::driver::StreamValidation;
use super::walk::{CollectionTrack, StreamHeader};

impl StreamValidation {
    /// The RAM the validate holds between batches.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn resident_bytes(
        &self,
    ) -> usize {
        let walk = &self.walk;
        // The arms' validate scope keeps its own copy of the header's sum
        // targets.
        let scope_targets = walk
            .header
            .as_ref()
            .map_or(0, |header| sum_targets_bytes(&header.sum_targets));
        let read_keys: usize = walk
            .read_keys
            .iter()
            .map(|key| size_of::<FenceKey>() + key_name_len(key))
            .sum();
        [
            self.batcher.held_bytes(),
            walk.post.hot_bytes(),
            walk.held.hot_bytes(),
            walk.batches
                .as_ref()
                .map_or(0, |batches| batches.resident_bytes()),
            walk.header.as_ref().map_or(0, header_bytes),
            scope_targets,
            walk.collections
                .iter()
                .map(|(name, track)| string_bytes(name) + track_bytes(track))
                .sum::<usize>(),
            walk.keyed
                .iter()
                .map(|name| string_bytes(name))
                .sum::<usize>(),
            read_keys,
            walk.ts_loads.held_bytes(),
            walk.array_loads.held_bytes(),
        ]
        .into_iter()
        .fold(0, usize::saturating_add)
    }
}

/// An owned string's size and bytes.
pub(in crate::data::executor::handlers::transaction::redo_apply) fn string_bytes(
    value: &str,
) -> usize {
    size_of::<String>() + value.len()
}

/// The estimate of a list of sum targets.
pub(in crate::data::executor::handlers::transaction::redo_apply) fn sum_targets_bytes(
    targets: &[RedoSumTargets],
) -> usize {
    targets
        .iter()
        .map(|targets| {
            size_of::<RedoSumTargets>()
                + targets.collection.len()
                + resolved_bytes(&targets.resolved)
        })
        .sum()
}

fn resolved_bytes(resolved: &[ResolvedSumTarget]) -> usize {
    resolved
        .iter()
        .map(|target| {
            size_of::<ResolvedSumTarget>()
                + target.target_collection.len()
                + target.join_value.len()
        })
        .sum()
}

fn header_bytes(header: &StreamHeader) -> usize {
    size_of::<StreamHeader>()
        + header
            .user_roles
            .iter()
            .map(|role| string_bytes(role))
            .sum::<usize>()
        + sum_targets_bytes(&header.sum_targets)
}

fn track_bytes(track: &CollectionTrack) -> usize {
    size_of::<CollectionTrack>() + resolved_bytes(&track.resolved)
}

/// The bytes of the name a fence key owns.
fn key_name_len(key: &FenceKey) -> usize {
    match key {
        FenceKey::Collection { name, .. } => name.len(),
        FenceKey::GraphLabels { .. } | FenceKey::VShard(_) => 0,
    }
}
