//! Aggregated report from `verify_and_repair`.
//!
//! Consumed by `main.rs` at the `CatalogSanityCheck` phase:
//! clean reports log at INFO and advance; reports where
//! `is_acceptable == false` trigger `shared.startup.fail()`
//! and abort startup.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use super::divergence::Divergence;

/// Per-registry count of divergences + how many were repaired.
#[derive(Debug, Clone, Default)]
pub struct RegistryDivergenceCount {
    pub detected: usize,
    pub repaired: usize,
}

/// Full outcome of the catalog sanity check.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    /// `true` if the applied-index gate passed.
    pub applied_index_ok: bool,
    /// Raw gap observed by the applied-index gate (0 if no gap).
    pub applied_index_gap: u64,
    /// Cross-table referential integrity violations. These are
    /// NOT auto-repaired — the safe recovery is to re-run the
    /// applier against the raft log, which is the operator's
    /// job.
    pub integrity_violations: Vec<Divergence>,
    /// Per-registry divergence counts. The verify path attempts
    /// repair (swap-in fresh re-load) and records whether it
    /// succeeded.
    pub registry_divergences: HashMap<&'static str, RegistryDivergenceCount>,
    /// Whether the repair pass succeeded on every registry it
    /// attempted to fix. `false` here means a second re-load
    /// still showed divergence — a real bug that needs
    /// operator attention.
    pub all_repairs_ok: bool,
    /// Total wall-clock spent in the sanity check.
    pub elapsed: Duration,
}

impl VerifyReport {
    /// An acceptable report has:
    /// - Passed the applied-index gate
    /// - Zero integrity violations (redb is self-consistent)
    /// - Every registry divergence was repaired
    pub fn is_acceptable(&self) -> bool {
        self.applied_index_ok && self.integrity_violations.is_empty() && self.all_repairs_ok
    }

    /// Total divergences detected across every registry.
    pub fn total_registry_divergences(&self) -> usize {
        self.registry_divergences.values().map(|c| c.detected).sum()
    }

    /// Total divergences successfully repaired.
    pub fn total_registry_repairs(&self) -> usize {
        self.registry_divergences.values().map(|c| c.repaired).sum()
    }
}

impl fmt::Display for VerifyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "catalog_sanity: applied_index_ok={} gap={} integrity_violations={} \
             registry_divergences={} repaired={} all_repairs_ok={} elapsed={:?}",
            self.applied_index_ok,
            self.applied_index_gap,
            self.integrity_violations.len(),
            self.total_registry_divergences(),
            self.total_registry_repairs(),
            self.all_repairs_ok,
            self.elapsed
        )?;
        for v in &self.integrity_violations {
            write!(f, "\n  integrity: {v}")?;
        }
        for (name, count) in &self.registry_divergences {
            if count.detected > 0 {
                write!(
                    f,
                    "\n  registry {name}: {} detected, {} repaired",
                    count.detected, count.repaired
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_report_is_acceptable() {
        let r = VerifyReport {
            applied_index_ok: true,
            applied_index_gap: 0,
            integrity_violations: vec![],
            registry_divergences: HashMap::new(),
            all_repairs_ok: true,
            elapsed: Duration::from_millis(5),
        };
        assert!(r.is_acceptable());
        assert_eq!(r.total_registry_divergences(), 0);
    }

    #[test]
    fn integrity_violation_not_acceptable() {
        let r = VerifyReport {
            applied_index_ok: true,
            applied_index_gap: 0,
            integrity_violations: vec![Divergence::new(
                super::super::divergence::DivergenceKind::OrphanRow {
                    kind: "collection",
                    key: "foo".into(),
                    expected_parent_kind: "owner",
                },
            )],
            registry_divergences: HashMap::new(),
            all_repairs_ok: true,
            elapsed: Duration::from_millis(5),
        };
        assert!(!r.is_acceptable());
    }

    #[test]
    fn applied_index_gap_not_acceptable() {
        let r = VerifyReport {
            applied_index_ok: false,
            applied_index_gap: 42,
            integrity_violations: vec![],
            registry_divergences: HashMap::new(),
            all_repairs_ok: true,
            elapsed: Duration::from_millis(5),
        };
        assert!(!r.is_acceptable());
    }

    #[test]
    fn unrepairable_divergence_not_acceptable() {
        let mut d = HashMap::new();
        d.insert(
            "permissions",
            RegistryDivergenceCount {
                detected: 3,
                repaired: 2,
            },
        );
        let r = VerifyReport {
            applied_index_ok: true,
            applied_index_gap: 0,
            integrity_violations: vec![],
            registry_divergences: d,
            all_repairs_ok: false,
            elapsed: Duration::from_millis(5),
        };
        assert!(!r.is_acceptable());
        assert_eq!(r.total_registry_divergences(), 3);
        assert_eq!(r.total_registry_repairs(), 2);
    }

    #[test]
    fn display_formats_all_fields() {
        let r = VerifyReport {
            applied_index_ok: true,
            applied_index_gap: 0,
            integrity_violations: vec![],
            registry_divergences: HashMap::new(),
            all_repairs_ok: true,
            elapsed: Duration::from_millis(12),
        };
        let s = r.to_string();
        assert!(s.contains("applied_index_ok=true"));
        assert!(s.contains("integrity_violations=0"));
    }
}
