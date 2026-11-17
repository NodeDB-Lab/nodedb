// SPDX-License-Identifier: BUSL-1.1

//! Sidecar value type for Calvin apply results.

use std::collections::BTreeMap;

use crate::bridge::envelope::Response;

/// The applied Data-Plane results of one completed Calvin transaction that
/// this node's participant replicas deposited, carried via
/// [`CalvinLocalState::apply_results`] from the per-vShard schedulers to the
/// coordinator's completion path.
///
/// A cross-shard COMMIT can have many primary-write participants, for
/// example a multi-collection transaction whose collections live on
/// different vShards. Their plain answers coalesce into `plain`. Each
/// participant that carries `RETURNING` keeps its own answer in
/// `returning`, so the coordinator answers the rows of every one.
///
/// [`CalvinLocalState::apply_results`]: crate::control::state::CalvinLocalState::apply_results
#[derive(Default)]
pub struct CalvinApplyResult {
    /// The first plain participant's response, with the timeseries install
    /// counts of every later plain participant merged in.
    pub plain: Option<Response>,
    /// The response of each participant that carries `RETURNING`, by
    /// vShard. The first deposit of a vShard stays.
    pub returning: BTreeMap<u32, Response>,
}
