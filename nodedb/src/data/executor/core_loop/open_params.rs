// SPDX-License-Identifier: BUSL-1.1

//! What a core opens with.

use std::path::Path;
use std::sync::Arc;

use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_types::OrdinalClock;

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::wal::WalReadSource;

/// The inputs [`super::CoreLoop::open`] takes.
pub struct CoreOpenParams<'a> {
    pub core_id: usize,
    pub request_rx: Consumer<BridgeRequest>,
    pub response_tx: Producer<BridgeResponse>,
    /// The base data directory. The core keeps its redb file at
    /// `{data_dir}/sparse/core-{core_id}.redb`.
    pub data_dir: &'a Path,
    /// The node's WAL directory and key ring. A committed chunked redo's
    /// install reads its stream back through it.
    pub wal_source: WalReadSource,
    pub hlc: Arc<OrdinalClock>,
    pub governor: Arc<nodedb_mem::MemoryGovernor>,
}

impl<'a> CoreOpenParams<'a> {
    /// The inputs of a core whose node keeps an unencrypted WAL at
    /// `{data_dir}/wal`.
    pub fn unencrypted(
        core_id: usize,
        request_rx: Consumer<BridgeRequest>,
        response_tx: Producer<BridgeResponse>,
        data_dir: &'a Path,
        hlc: Arc<OrdinalClock>,
        governor: Arc<nodedb_mem::MemoryGovernor>,
    ) -> Self {
        Self {
            core_id,
            request_rx,
            response_tx,
            data_dir,
            wal_source: WalReadSource::unencrypted(data_dir.join("wal")),
            hlc,
            governor,
        }
    }
}
