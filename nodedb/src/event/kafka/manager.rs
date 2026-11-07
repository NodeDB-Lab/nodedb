// SPDX-License-Identifier: BUSL-1.1

//! Kafka producer lifecycle manager.
//!
//! Tracks running Kafka producer tasks per stream. Every node runs a
//! producer for every registered change stream with Kafka delivery: a
//! reconciler matches the producers to the stream registry once per second.
//! Only the lease holder of the stream's owning group publishes; see
//! [`crate::event::cdc::sink_owner`].

use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info};

use super::config::KafkaDeliveryConfig;
use crate::control::state::SharedState;
use crate::event::sink_tasks::{SinkTaskKey as TaskKey, SinkTasks};
use crate::types::DatabaseId;

/// Manages Kafka producer tasks for change streams.
pub struct KafkaManager {
    /// Producer handles and admission state are locked together so shutdown
    /// cannot race a task admitted by `start`.
    state: Mutex<SinkTasks>,
    /// Shutdown signal receiver (cloned per task).
    shutdown_rx: watch::Receiver<bool>,
    /// Back-reference to the `SharedState` that owns this manager, set once
    /// after construction. It is `Weak`: a strong one forms a cycle that
    /// keeps `SharedState`, its redb files, and its QUIC endpoint alive after
    /// shutdown.
    shared_state: OnceLock<Weak<SharedState>>,
}

impl KafkaManager {
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
        crate::event::webhook::manager::spawn_reconciler(state, |state| {
            state.kafka_manager.reconcile()
        });
    }

    /// Start a producer for every registered stream with Kafka delivery, and
    /// stop the producer of every stream no longer registered.
    pub fn reconcile(&self) {
        let Some(state) = self.shared_state.get().and_then(Weak::upgrade) else {
            return;
        };
        let streams: Vec<_> = state
            .stream_registry
            .list_all()
            .into_iter()
            .filter(|def| def.kafka.enabled)
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
            self.stop(database_id, tenant_id, &name);
        }
        for def in streams {
            let running = self
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .tasks
                .contains_key(&(def.database_id, def.tenant_id, def.name.clone()));
            if !running {
                self.start(def.database_id, def.tenant_id, &def.name, def.kafka.clone());
            }
        }
    }

    /// Start a Kafka producer for a change stream.
    ///
    /// Returns `false` when disabled, shutdown/draining has started, state is
    /// not ready, or this stream already has a producer.
    pub fn start(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream_name: &str,
        config: KafkaDeliveryConfig,
    ) -> bool {
        if !config.enabled {
            return false;
        }
        {
            let manager = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if manager.draining || *self.shutdown_rx.borrow() {
                debug!(stream = %stream_name, "Kafka manager is draining; producer start rejected");
                return false;
            }
        }
        let shared_state = match self.shared_state.get().and_then(Weak::upgrade) {
            Some(state) => state,
            None => {
                tracing::warn!("Kafka manager: state not set or dropped, cannot start producer");
                return false;
            }
        };

        let key = (database_id, tenant_id, stream_name.to_string());
        let mut manager = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if manager.draining || *self.shutdown_rx.borrow() {
            debug!(stream = %stream_name, "Kafka manager is draining; producer start rejected");
            return false;
        }
        if manager.tasks.contains_key(&key) {
            debug!(stream = %stream_name, "Kafka producer already running");
            return false;
        }

        let handle = super::producer::spawn_kafka_task(
            database_id,
            stream_name.to_string(),
            tenant_id,
            config,
            shared_state,
            self.shutdown_rx.clone(),
        );
        manager.tasks.insert(key, handle);
        info!(stream = %stream_name, tenant_id, "Kafka producer started");
        true
    }

    /// Stop and remove a Kafka producer for a dropped change stream.
    pub fn stop(&self, database_id: DatabaseId, tenant_id: u64, stream_name: &str) {
        let key = (database_id, tenant_id, stream_name.to_string());
        let handle = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .remove(&key);
        if let Some(handle) = handle {
            handle.abort();
            info!(stream = %stream_name, "Kafka producer stopped");
        }
    }

    /// Stop admitting producers, then join those already admitted. See
    /// [`crate::event::sink_tasks::shutdown_and_join`].
    pub async fn shutdown_and_join(&self, deadline: Duration) {
        crate::event::sink_tasks::shutdown_and_join(&self.state, deadline).await;
        debug!("Kafka manager producer tasks drained");
    }

    /// Number of running Kafka producers.
    pub fn running_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .len()
    }

    /// Total pending Kafka publishes across all producers.
    pub fn total_pending(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .len() as u64
    }
}

impl Drop for KafkaManager {
    fn drop(&mut self) {
        let manager = self.state.get_mut().unwrap_or_else(|p| p.into_inner());
        manager.draining = true;
        for (_, handle) in manager.tasks.drain() {
            handle.abort();
        }
        debug!("Kafka manager dropped, all producer tasks aborted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;

    #[test]
    fn manager_lifecycle() {
        let (_tx, rx) = watch::channel(false);
        let mgr = KafkaManager::new(rx);
        assert_eq!(mgr.running_count(), 0);
    }

    #[test]
    fn start_without_startup_state_is_rejected() {
        let (_tx, rx) = watch::channel(false);
        let mgr = KafkaManager::new(rx);
        assert!(!mgr.start(
            DatabaseId::new(7),
            1,
            "test_stream",
            KafkaDeliveryConfig {
                enabled: true,
                ..KafkaDeliveryConfig::default()
            },
        ));
    }

    #[tokio::test]
    async fn drain_blocks_later_phase_and_rejects_starts() {
        let (_tx, rx) = watch::channel(false);
        let mgr = Arc::new(KafkaManager::new(rx));
        let release = Arc::new(Notify::new());
        let held_release = Arc::clone(&release);
        mgr.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .insert(
                (DatabaseId::new(7), 1, "held".to_string()),
                tokio::spawn(async move { held_release.notified().await }),
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
        assert!(!mgr.start(
            DatabaseId::new(7),
            1,
            "later",
            KafkaDeliveryConfig {
                enabled: true,
                ..KafkaDeliveryConfig::default()
            },
        ));
        release.notify_one();
        draining.await.expect("manager drain task should complete");
    }

    #[test]
    fn stop_nonexistent_is_noop() {
        let (_tx, rx) = watch::channel(false);
        let mgr = KafkaManager::new(rx);
        mgr.stop(DatabaseId::new(7), 1, "nonexistent");
    }
}
