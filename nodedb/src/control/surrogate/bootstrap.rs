//! Boot-time surrogate registry recovery.
//!
//! Reads the persisted high-watermark from any
//! [`SurrogateHwmPersist`] and seeds a fresh
//! [`SurrogateRegistry`] whose next allocation is `hwm + 1`.
//!
//! S1 wires this into `SharedState` / `CoreLoop` startup. For S0 the
//! function is exposed standalone; the test below covers the full
//! round-trip without depending on the real boot path.

use super::persist::SurrogateHwmPersist;
use super::registry::SurrogateRegistry;

/// Read the persisted hwm and return a registry seeded past it.
///
/// On a fresh database (no checkpoint yet) the persist layer returns `0`,
/// which seeds the registry such that the first allocation returns
/// `Surrogate(1)`.
pub fn bootstrap_registry(persist: &dyn SurrogateHwmPersist) -> crate::Result<SurrogateRegistry> {
    let hwm = persist.load()?;
    Ok(SurrogateRegistry::from_persisted_hwm(hwm))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::Surrogate;

    use super::super::persist::SystemCatalogHwm;
    use super::*;
    use crate::control::security::catalog::SystemCatalog;

    #[test]
    fn fresh_database_starts_at_one() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(SystemCatalog::open(&dir.path().join("system.redb")).unwrap());
        let persist = SystemCatalogHwm::new(catalog);
        let reg = bootstrap_registry(&persist).unwrap();
        assert_eq!(reg.alloc_one().unwrap(), Surrogate::new(1));
    }

    #[test]
    fn restart_round_trip_via_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        // First boot: allocate 5000, flush.
        {
            let catalog = Arc::new(SystemCatalog::open(&path).unwrap());
            let persist = SystemCatalogHwm::new(catalog);
            let reg = bootstrap_registry(&persist).unwrap();
            let _ = reg.alloc(5000).unwrap();
            assert_eq!(reg.current_hwm(), 5000);
            reg.flush(&persist).unwrap();
        }

        // Restart: registry seeds past hwm; first alloc = 5001.
        {
            let catalog = Arc::new(SystemCatalog::open(&path).unwrap());
            let persist = SystemCatalogHwm::new(catalog);
            let reg = bootstrap_registry(&persist).unwrap();
            assert_eq!(reg.alloc_one().unwrap(), Surrogate::new(5001));
        }
    }

    #[test]
    fn wal_record_round_trip_restores_hwm() {
        // Round-trip: encode SurrogateAlloc payload, decode, feed into a
        // bootstrapped registry. Mirrors the S2 replay path that S0
        // reserves the record kind for.
        use nodedb_wal::record::SurrogateAllocPayload;

        let payload = SurrogateAllocPayload::new(9_999);
        let bytes = payload.to_bytes();
        let decoded = SurrogateAllocPayload::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.hi, 9_999);

        // Simulate replay by seeding the persist layer with the decoded hi.
        struct ReplayPersist(u32);
        impl SurrogateHwmPersist for ReplayPersist {
            fn checkpoint(&self, _: u32) -> crate::Result<()> {
                Ok(())
            }
            fn load(&self) -> crate::Result<u32> {
                Ok(self.0)
            }
        }

        let reg = bootstrap_registry(&ReplayPersist(decoded.hi)).unwrap();
        assert_eq!(reg.alloc_one().unwrap(), Surrogate::new(10_000));
    }
}
