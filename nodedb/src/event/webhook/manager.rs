// SPDX-License-Identifier: BUSL-1.1

//! Webhook manager: spawns and stops delivery tasks per stream.
//!
//! Every node runs a delivery task for every registered change stream with
//! a webhook: a reconciler matches the tasks to the stream registry once per
//! second, so a stream a peer created, or one that survives a restart, gets
//! its task. Only the lease holder of the stream's owning group delivers;
//! see [`crate::event::cdc::sink_owner`].

use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info};

use crate::control::state::SharedState;
use crate::event::sink_tasks::{SinkTaskKey as TaskKey, SinkTasks};
use crate::types::DatabaseId;

use super::delivery::spawn_delivery_task;

/// Manages webhook delivery tasks for all webhook-enabled streams.
pub struct WebhookManager {
    /// Running delivery tasks and admission state, guarded together so a drain
    /// cannot race a newly accepted task.
    state: Mutex<SinkTasks>,
    /// Shared shutdown receiver (cloned for each task).
    shutdown_rx: watch::Receiver<bool>,
    /// Back-reference to the `SharedState` that owns this manager, set once
    /// after construction. It is `Weak`: a strong one forms a cycle that
    /// keeps `SharedState`, its redb files, and its QUIC endpoint alive after
    /// shutdown.
    shared_state: OnceLock<Weak<SharedState>>,
}

impl WebhookManager {
    pub fn new(shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            state: Mutex::new(SinkTasks::default()),
            shutdown_rx,
            shared_state: OnceLock::new(),
        }
    }

    /// Set the shared state reference (called once during startup), and
    /// start the reconciler.
    pub fn set_state(&self, state: &Arc<SharedState>) {
        if self.shared_state.set(Arc::downgrade(state)).is_err() {
            return;
        }
        spawn_reconciler(state, |state| state.webhook_manager.reconcile());
    }

    /// Start a task for every registered stream with a webhook, and stop the
    /// task of every stream no longer registered.
    pub fn reconcile(&self) {
        let Some(state) = self.shared_state.get().and_then(Weak::upgrade) else {
            return;
        };
        let streams: Vec<_> = state
            .stream_registry
            .list_all()
            .into_iter()
            .filter(|def| def.webhook.is_configured())
            // The source delivers a mirror's changes. Delivery starts at PROMOTE.
            .filter(|def| !state.mirror.fence.is_read_only(def.database_id))
            .collect();
        let registered: std::collections::HashSet<TaskKey> = streams
            .iter()
            .map(|def| (def.database_id, def.tenant_id, def.name.clone()))
            .collect();
        let stale: Vec<TaskKey> = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .keys()
            .filter(|key| !registered.contains(*key))
            .cloned()
            .collect();
        for (database_id, tenant_id, name) in stale {
            self.stop_task(database_id, tenant_id, &name);
        }
        for def in streams {
            let running = self
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .tasks
                .contains_key(&(def.database_id, def.tenant_id, def.name.clone()));
            if !running {
                self.start_task(
                    def.database_id,
                    def.tenant_id,
                    &def.name,
                    def.webhook.clone(),
                );
            }
        }
    }

    /// Start a delivery task for a specific stream.
    ///
    /// Returns `false` when shutdown/draining has started, state is not ready,
    /// or this stream already has a task.
    pub fn start_task(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream_name: &str,
        config: super::types::WebhookConfig,
    ) -> bool {
        {
            let manager = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if manager.draining || *self.shutdown_rx.borrow() {
                debug!(
                    stream = stream_name,
                    "webhook manager is draining; task start rejected"
                );
                return false;
            }
        }
        let state = match self.shared_state.get().and_then(Weak::upgrade) {
            Some(state) => state,
            None => {
                tracing::warn!("webhook manager: state not set or dropped, cannot start task");
                return false;
            }
        };

        let key = (database_id, tenant_id, stream_name.to_string());
        let mut manager = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if manager.draining || *self.shutdown_rx.borrow() {
            debug!(
                stream = stream_name,
                "webhook manager is draining; task start rejected"
            );
            return false;
        }
        if manager.tasks.contains_key(&key) {
            debug!(
                stream = stream_name,
                "webhook delivery task already running, skipping"
            );
            return false;
        }

        let handle = spawn_delivery_task(
            state,
            database_id,
            tenant_id,
            stream_name.to_string(),
            config,
            self.shutdown_rx.clone(),
        );
        manager.tasks.insert(key, handle);
        info!(stream = stream_name, "webhook delivery task spawned");
        true
    }

    /// Stop a delivery task for a specific stream (on DROP CHANGE STREAM).
    pub fn stop_task(&self, database_id: DatabaseId, tenant_id: u64, stream_name: &str) {
        let key = (database_id, tenant_id, stream_name.to_string());
        let handle = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .remove(&key);
        if let Some(handle) = handle {
            handle.abort();
            info!(stream = stream_name, "webhook delivery task stopped");
        }
    }

    /// Stop admitting delivery tasks, then join those already admitted. See
    /// [`crate::event::sink_tasks::shutdown_and_join`].
    pub async fn shutdown_and_join(&self, deadline: Duration) {
        crate::event::sink_tasks::shutdown_and_join(&self.state, deadline).await;
        debug!("webhook manager delivery tasks drained");
    }

    /// Number of active delivery tasks.
    pub fn active_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .len()
    }
}

impl Drop for WebhookManager {
    fn drop(&mut self) {
        let manager = self.state.get_mut().unwrap_or_else(|p| p.into_inner());
        manager.draining = true;
        for (_, handle) in manager.tasks.drain() {
            handle.abort();
        }
        debug!("webhook manager dropped, all delivery tasks aborted");
    }
}

/// Run `reconcile` once per second while `state` lives and the node runs.
pub(crate) fn spawn_reconciler(state: &Arc<SharedState>, reconcile: fn(&SharedState)) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let weak = Arc::downgrade(state);
    let mut shutdown = state.shutdown.raw_receiver();
    runtime.spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => {}
            }
            if *shutdown.borrow() {
                return;
            }
            let Some(state) = weak.upgrade() else {
                return;
            };
            reconcile(&state);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;

    fn held_task(release: Arc<Notify>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { release.notified().await })
    }

    #[test]
    fn new_manager_has_no_tasks() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn start_without_state_is_rejected() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        assert!(!mgr.start_task(
            DatabaseId::new(7),
            1,
            "test_stream",
            super::super::types::WebhookConfig::default(),
        ));
    }

    #[tokio::test]
    async fn drain_blocks_later_phase_and_rejects_starts() {
        let (_tx, rx) = watch::channel(false);
        let mgr = Arc::new(WebhookManager::new(rx));
        let release = Arc::new(Notify::new());
        mgr.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .insert(
                (DatabaseId::new(7), 1, "held".to_string()),
                held_task(Arc::clone(&release)),
            );

        let mut draining = {
            let mgr = Arc::clone(&mgr);
            tokio::spawn(async move { mgr.shutdown_and_join(Duration::from_secs(2)).await })
        };
        tokio::task::yield_now().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut draining)
                .await
                .is_err()
        );
        assert!(mgr.state.lock().unwrap_or_else(|p| p.into_inner()).draining);
        assert!(!mgr.start_task(
            DatabaseId::new(7),
            1,
            "later",
            super::super::types::WebhookConfig::default(),
        ));
        release.notify_one();
        draining.await.expect("manager drain task should complete");
    }

    #[test]
    fn stop_nonexistent_task_is_noop() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        mgr.stop_task(DatabaseId::new(7), 1, "nonexistent");
        assert_eq!(mgr.active_count(), 0);
    }
}
