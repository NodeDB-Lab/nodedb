//! `BlacklistStore` verifier.
//!
//! Checks that the in-memory `BlacklistStore` is consistent with
//! the `_system.blacklist` redb table.
//!
//! **What it checks:**
//! - Every non-expired entry in redb has a matching key in memory.
//! - Every non-expired entry in memory has a backing row in redb.
//!   Ghost entries (memory has the key, redb doesn't) indicate a
//!   load_from bug or a concurrent write that bypassed redb.
//!
//! **What it does NOT check:**
//! - JWT claim-based blocking configuration (not persisted in redb).
//! - Entries that are expired in redb but not yet evicted from
//!   memory — these are self-healing via lazy cleanup and not
//!   treated as errors.

use crate::control::security::blacklist::store::BlacklistStore;
use crate::control::security::catalog::SystemCatalog;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_blacklist(
    store: &BlacklistStore,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    // Expected: all non-expired entries from redb.
    let mut expected: Vec<(String, String)> = catalog
        .load_all_blacklist_entries()?
        .into_iter()
        .filter(|e| {
            // Skip entries that are already expired in redb — load_from
            // would not have loaded them, so memory absence is correct.
            if e.expires_at == 0 {
                return true;
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            now < e.expires_at
        })
        .map(|e| (e.key.clone(), e.kind.clone()))
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    // Actual: all non-expired entries in memory.
    let mut actual: Vec<(String, String)> = store
        .list_all_entries()
        .into_iter()
        .filter(|e| !e.is_expired())
        .map(|e| (e.key.clone(), e.kind.clone()))
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "blacklist",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "blacklist",
            key: key.clone(),
        }));
    }
    Ok(out)
}

/// Repair: clear and reload from redb.
pub fn repair_blacklist(store: &BlacklistStore, catalog: &SystemCatalog) -> crate::Result<()> {
    store.clear_and_reload(catalog)
}
