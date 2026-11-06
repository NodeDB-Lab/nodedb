// SPDX-License-Identifier: BUSL-1.1

//! The node's handles on cross-shard event delivery.

use std::sync::{Arc, Mutex, OnceLock};

use crate::event::cross_shard::{
    CrossShardDedup, CrossShardDispatcher, CrossShardDlq, CrossShardMetrics,
};

/// What cross-shard delivery holds on one node.
#[derive(Default)]
pub struct CrossShardState {
    /// The event dispatcher. Boot wires it with the cluster handle.
    pub dispatcher: Option<Arc<CrossShardDispatcher>>,
    /// The dead letter queue. Boot wires it with the cluster handle.
    pub dlq: Option<Arc<Mutex<CrossShardDlq>>>,
    /// Delivery metrics. Boot wires them with the cluster handle.
    pub metrics: Option<Arc<CrossShardMetrics>>,
    /// The applied-request dedup store, installed at Raft group setup. Every
    /// committed redo that carries a cross-shard key records it here as it
    /// applies.
    pub dedup: OnceLock<Arc<CrossShardDedup>>,
}
