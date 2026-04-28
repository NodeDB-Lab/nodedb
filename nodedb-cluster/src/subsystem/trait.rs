//! The `ClusterSubsystem` trait — the shared interface every subsystem implements.

use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::context::BootstrapCtx;
use super::errors::{BootstrapError, ShutdownError};
use super::health::SubsystemHealth;

/// A running subsystem handle.
///
/// Dropping this handle sends the shutdown signal and detaches the task.
/// Callers that want to wait for clean termination should call
/// `shutdown_and_wait` with a deadline before dropping.
pub struct SubsystemHandle {
    /// The background task running this subsystem's main loop.
    ///
    /// Stored as `Option` so `shutdown_and_wait` (which takes `self` by value)
    /// can extract the `JoinHandle` without conflicting with our `Drop` impl.
    task: Option<JoinHandle<()>>,
    /// Sending `true` on this channel requests the subsystem to shut down.
    pub shutdown_tx: watch::Sender<bool>,
    /// The name of the subsystem, for diagnostics.
    pub name: &'static str,
}

impl SubsystemHandle {
    /// Create a new handle from an already-spawned task and a shutdown signal.
    pub fn new(name: &'static str, task: JoinHandle<()>, shutdown_tx: watch::Sender<bool>) -> Self {
        Self {
            task: Some(task),
            shutdown_tx,
            name,
        }
    }

    /// Signal shutdown and wait for the task to finish, respecting `deadline`.
    ///
    /// If the task does not finish by `deadline`, the task is aborted and
    /// `ShutdownError::DeadlineExceeded` is returned.
    pub async fn shutdown_and_wait(mut self, deadline: Instant) -> Result<(), ShutdownError> {
        // Send the shutdown signal — ignore send errors (task may have exited already).
        let _ = self.shutdown_tx.send(true);

        let task = match self.task.take() {
            Some(t) => t,
            None => return Ok(()),
        };

        let timeout = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(timeout, task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(join_err)) if join_err.is_panic() => {
                Err(ShutdownError::Panicked { name: self.name })
            }
            Ok(Err(_)) => {
                // Task was cancelled — treat as clean stop.
                Ok(())
            }
            Err(_elapsed) => Err(ShutdownError::DeadlineExceeded { name: self.name }),
        }
    }
}

impl Drop for SubsystemHandle {
    fn drop(&mut self) {
        // Best-effort: send the shutdown signal so the task notices even if
        // the caller drops without calling `shutdown_and_wait`. The task
        // itself is dropped here, detaching it from the runtime.
        let _ = self.shutdown_tx.send(true);
    }
}

/// The contract every cluster subsystem must satisfy.
///
/// # Dependency ordering
///
/// `dependencies()` returns the *names* of subsystems that must have
/// completed `start()` successfully before this subsystem is started.
/// The registry performs a topo-sort and enforces this ordering.
///
/// # Shutdown
///
/// `shutdown()` is called in *reverse* start order when any sibling
/// fails during bootstrap, or during graceful cluster teardown. Each
/// subsystem should stop its background work and release resources
/// before the deadline.
///
/// # Health
///
/// `health()` returns a point-in-time `SubsystemHealth`. The registry
/// does not poll this — individual subsystems update the shared
/// `ClusterHealth` aggregator directly via `BootstrapCtx`.
#[async_trait]
pub trait ClusterSubsystem: Send + Sync {
    /// A unique, human-readable name for this subsystem.
    ///
    /// Used as the key in dependency declarations and health maps.
    fn name(&self) -> &'static str;

    /// Names of subsystems that must be started before this one.
    ///
    /// Return `&[]` if there are no prerequisites.
    fn dependencies(&self) -> &'static [&'static str];

    /// Start the subsystem and return a handle to its background task.
    ///
    /// This method is called exactly once, after all declared
    /// dependencies have been started successfully.
    async fn start(&self, ctx: &BootstrapCtx) -> Result<SubsystemHandle, BootstrapError>;

    /// Gracefully stop the subsystem before `deadline`.
    ///
    /// Implementations should signal their background tasks via the
    /// `SubsystemHandle::shutdown_tx` and wait for them to exit. If
    /// the deadline is exceeded, they should abort and return
    /// `ShutdownError::DeadlineExceeded`.
    async fn shutdown(&self, deadline: Instant) -> Result<(), ShutdownError>;

    /// Return the current health state of this subsystem.
    fn health(&self) -> SubsystemHealth;
}
