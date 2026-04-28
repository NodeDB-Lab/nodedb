//! Error types for the subsystem bootstrap and shutdown lifecycle.

use thiserror::Error;

/// Error produced during topo-sort of the subsystem dependency graph.
#[derive(Debug, Error)]
pub enum TopoError {
    /// A cycle was detected in the dependency graph.
    ///
    /// `cycle` lists the subsystem names forming the cycle in order.
    #[error("dependency cycle detected: {}", cycle.join(" -> "))]
    Cycle { cycle: Vec<&'static str> },

    /// A subsystem listed a dependency that is not registered.
    #[error("subsystem {subsystem:?} depends on unknown subsystem {dependency:?}")]
    UnknownDependency {
        subsystem: &'static str,
        dependency: &'static str,
    },
}

/// Error produced while starting one or more subsystems.
#[derive(Debug, Error)]
pub enum BootstrapError {
    /// The dependency graph could not be resolved.
    #[error("subsystem dependency resolution failed: {0}")]
    TopoSort(#[from] TopoError),

    /// A subsystem failed during its `start` call.
    #[error("subsystem {name:?} failed to start: {cause}")]
    SubsystemStart {
        name: &'static str,
        #[source]
        cause: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// One or more previously-started subsystems could not be shut down
    /// cleanly after a sibling failure. The original start failure is
    /// included as `start_error`.
    #[error(
        "subsystem {name:?} failed to start; cleanup of already-started \
         subsystems also encountered errors: {shutdown_errors:?}"
    )]
    StartAndShutdownFailure {
        name: &'static str,
        shutdown_errors: Vec<ShutdownError>,
    },
}

/// Error produced during graceful shutdown of a subsystem.
#[derive(Debug, Error)]
pub enum ShutdownError {
    /// The subsystem did not stop within the allotted deadline.
    #[error("subsystem {name:?} did not stop before the shutdown deadline")]
    DeadlineExceeded { name: &'static str },

    /// The subsystem's task panicked during shutdown.
    #[error("subsystem {name:?} panicked during shutdown")]
    Panicked { name: &'static str },

    /// The subsystem reported an error during shutdown.
    #[error("subsystem {name:?} shutdown error: {cause}")]
    SubsystemError {
        name: &'static str,
        #[source]
        cause: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl From<BootstrapError> for crate::error::ClusterError {
    fn from(e: BootstrapError) -> Self {
        crate::error::ClusterError::Storage {
            detail: e.to_string(),
        }
    }
}

impl From<ShutdownError> for crate::error::ClusterError {
    fn from(e: ShutdownError) -> Self {
        crate::error::ClusterError::Storage {
            detail: e.to_string(),
        }
    }
}

impl From<TopoError> for crate::error::ClusterError {
    fn from(e: TopoError) -> Self {
        crate::error::ClusterError::Storage {
            detail: e.to_string(),
        }
    }
}
