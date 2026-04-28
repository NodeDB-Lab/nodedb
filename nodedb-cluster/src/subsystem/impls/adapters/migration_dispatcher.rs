//! [`MigrationDispatcher`] adapter backed by [`MigrationExecutor`].
//!
//! Converts a [`PlannedMove`] (from the rebalancer plan) into a
//! [`MigrationRequest`] and forwards it to the executor's 3-phase
//! migration protocol. Errors are returned to the caller (the
//! rebalancer driver logs them and records the error metric).

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::migration_executor::{MigrationExecutor, MigrationRequest};
use crate::rebalance::PlannedMove;
use crate::rebalancer::driver::MigrationDispatcher;

/// Forwards rebalancer [`PlannedMove`]s to the live [`MigrationExecutor`].
pub struct ExecutorDispatcher {
    executor: Arc<MigrationExecutor>,
}

impl ExecutorDispatcher {
    pub fn new(executor: Arc<MigrationExecutor>) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl MigrationDispatcher for ExecutorDispatcher {
    async fn dispatch(&self, mv: PlannedMove) -> Result<()> {
        let request = MigrationRequest {
            vshard_id: mv.vshard_id,
            source_node: mv.source_node,
            target_node: mv.target_node,
            ..MigrationRequest::default()
        };
        self.executor.execute(request).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn _assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn executor_dispatcher_is_send_sync() {
        _assert_send_sync::<ExecutorDispatcher>();
    }
}
