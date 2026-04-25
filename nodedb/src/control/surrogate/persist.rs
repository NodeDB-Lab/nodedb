//! Surrogate hwm persistence trait + concrete `SystemCatalog`-backed impl.
//!
//! The redb storage layout (singleton `_system.surrogate_hwm` table) lives
//! in `crate::control::security::catalog::surrogate_hwm`, alongside the
//! other catalog ops. This module exposes the trait the registry
//! depends on, plus a thin handle that delegates to the catalog.
//!
//! ## Boundary
//!
//! `SurrogateRegistry::flush` calls `checkpoint(hwm)`. The boot path
//! reads `load()` and seeds a fresh registry via
//! `SurrogateRegistry::from_persisted_hwm`.
//!
//! ## S2 follow-up (not in this tier)
//!
//! S2 will additionally append a `RecordType::SurrogateAlloc` WAL record
//! at every checkpoint so post-crash replay can rebuild the hwm even if
//! the redb table has fallen behind. The record kind is reserved
//! (`51 | 0x8000`) but not emitted yet.

use std::sync::Arc;

use crate::control::security::catalog::SystemCatalog;
pub use crate::control::security::catalog::surrogate_hwm::SURROGATE_HWM;

/// Pluggable persistence boundary. Tests substitute an in-memory store;
/// production wires [`SystemCatalogHwm`].
pub trait SurrogateHwmPersist: Send + Sync {
    /// Persist the current high-watermark. Called by
    /// `SurrogateRegistry::flush` whenever the periodic-flush thresholds
    /// (1024 ops or 200 ms) are tripped.
    fn checkpoint(&self, hwm: u32) -> crate::Result<()>;

    /// Load the persisted high-watermark, or `0` if none has been
    /// recorded yet (fresh database).
    fn load(&self) -> crate::Result<u32>;
}

/// `SystemCatalog`-backed persistence — delegates to
/// `put_surrogate_hwm` / `get_surrogate_hwm`.
pub struct SystemCatalogHwm {
    catalog: Arc<SystemCatalog>,
}

impl SystemCatalogHwm {
    pub fn new(catalog: Arc<SystemCatalog>) -> Self {
        Self { catalog }
    }
}

impl SurrogateHwmPersist for SystemCatalogHwm {
    fn checkpoint(&self, hwm: u32) -> crate::Result<()> {
        self.catalog.put_surrogate_hwm(hwm)
    }

    fn load(&self) -> crate::Result<u32> {
        self.catalog.get_surrogate_hwm()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_roundtrip_via_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(SystemCatalog::open(&dir.path().join("system.redb")).unwrap());
        let p = SystemCatalogHwm::new(catalog);
        assert_eq!(p.load().unwrap(), 0);
        p.checkpoint(123).unwrap();
        assert_eq!(p.load().unwrap(), 123);
    }
}
