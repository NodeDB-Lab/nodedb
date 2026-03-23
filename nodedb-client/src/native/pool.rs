//! Bounded connection pool for native protocol connections.
//!
//! Uses a semaphore for max-size enforcement and a std::sync::Mutex
//! idle queue (so Drop can return connections synchronously).
//! Health checks (ping) are performed on idle connections before
//! handing them out.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::AuthMethod;
use tokio::sync::{Semaphore, SemaphorePermit};

use super::connection::NativeConnection;

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Server address (host:port).
    pub addr: String,
    /// Maximum number of connections.
    pub max_size: usize,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Idle connection timeout (connections idle longer than this are dropped).
    pub idle_timeout: Duration,
    /// Authentication method.
    pub auth: AuthMethod,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:6433".into(),
            max_size: 10,
            connect_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
            auth: AuthMethod::Trust {
                username: "admin".into(),
            },
        }
    }
}

/// Shared state for idle connection return.
///
/// Uses `std::sync::Mutex` (not tokio) so the `Drop` impl can
/// return connections synchronously without spawning async tasks.
struct PoolInner {
    idle: Mutex<VecDeque<NativeConnection>>,
    max_size: usize,
}

/// A bounded pool of `NativeConnection` instances.
pub struct Pool {
    config: PoolConfig,
    inner: Arc<PoolInner>,
    semaphore: Semaphore,
}

impl Pool {
    /// Create a new pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        let max_size = config.max_size;
        let semaphore = Semaphore::new(max_size);
        Self {
            config,
            inner: Arc::new(PoolInner {
                idle: Mutex::new(VecDeque::new()),
                max_size,
            }),
            semaphore,
        }
    }

    /// Acquire a connection from the pool.
    ///
    /// Returns an idle connection if available, otherwise creates a new one.
    /// Blocks if `max_size` connections are already in use.
    pub async fn acquire(&self) -> NodeDbResult<PooledConnection<'_>> {
        let permit = tokio::time::timeout(self.config.connect_timeout, self.semaphore.acquire())
            .await
            .map_err(|_| NodeDbError::SyncConnectionFailed {
                detail: "pool acquire timeout".into(),
            })?
            .map_err(|_| NodeDbError::SyncConnectionFailed {
                detail: "pool closed".into(),
            })?;

        // Try to reuse an idle connection (synchronous lock — fast, no contention).
        let idle_conn = {
            let mut idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
            idle.pop_front()
        };

        if let Some(mut conn) = idle_conn {
            // Health check: ping to verify the connection is still alive.
            if conn.ping().await.is_ok() {
                return Ok(PooledConnection {
                    conn: Some(conn),
                    inner: Arc::clone(&self.inner),
                    _permit: permit,
                });
            }
            // Connection dead — create a new one below.
        }

        // Create a new connection.
        let mut conn = tokio::time::timeout(
            self.config.connect_timeout,
            NativeConnection::connect(&self.config.addr),
        )
        .await
        .map_err(|_| NodeDbError::SyncConnectionFailed {
            detail: "connect timeout".into(),
        })??;

        // Authenticate.
        conn.authenticate(self.config.auth.clone()).await?;

        Ok(PooledConnection {
            conn: Some(conn),
            inner: Arc::clone(&self.inner),
            _permit: permit,
        })
    }
}

/// A connection borrowed from the pool.
///
/// Returns to the idle queue synchronously on drop (no spawned tasks).
pub struct PooledConnection<'a> {
    conn: Option<NativeConnection>,
    inner: Arc<PoolInner>,
    _permit: SemaphorePermit<'a>,
}

impl std::ops::Deref for PooledConnection<'_> {
    type Target = NativeConnection;
    fn deref(&self) -> &NativeConnection {
        self.conn.as_ref().expect("connection taken")
    }
}

impl std::ops::DerefMut for PooledConnection<'_> {
    fn deref_mut(&mut self) -> &mut NativeConnection {
        self.conn.as_mut().expect("connection taken")
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Synchronous return to idle queue — no async, no spawned tasks.
            let mut idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
            if idle.len() < self.inner.max_size {
                idle.push_back(conn);
            }
            // else: too many idle connections — drop this one.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_config_defaults() {
        let cfg = PoolConfig::default();
        assert_eq!(cfg.addr, "127.0.0.1:6433");
        assert_eq!(cfg.max_size, 10);
        assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
    }

    #[test]
    fn pool_creates_semaphore() {
        let pool = Pool::new(PoolConfig {
            max_size: 5,
            ..Default::default()
        });
        assert_eq!(pool.semaphore.available_permits(), 5);
    }
}
