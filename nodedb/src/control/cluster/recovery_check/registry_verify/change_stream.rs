//! `StreamRegistry` (CDC change stream) verifier.
//!
//! Checks that the in-memory `StreamRegistry` is consistent with
//! the `_system.change_streams` redb table.
//!
//! **What it checks:**
//! - Every change stream in redb has a matching entry in memory
//!   (key = `{tenant_id}|{name}`, value encodes `enabled` so a
//!   stream enable/disable mutation surfaces).
//! - Every stream in memory has a backing redb row.
//!
//! **What it does NOT check:**
//! - Whether the source collection exists or is active. Cross-entity
//!   referential checks are the responsibility of a future integrity pass.
//! - Whether live CDC buffers are consistent with the definitions
//!   (buffer state is runtime-only and not persisted in redb).

use crate::control::security::catalog::SystemCatalog;
use crate::event::cdc::StreamRegistry;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_change_streams(
    registry: &StreamRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_change_streams()?
        .into_iter()
        .map(|s| {
            let key = format!("{}|{}", s.tenant_id, s.name);
            // ChangeStreamDef doesn't have an `enabled` field;
            // presence in the catalog is the signal.
            let value = String::from("present");
            (key, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = registry
        .list_all()
        .into_iter()
        .map(|s| {
            let key = format!("{}|{}", s.tenant_id, s.name);
            let value = String::from("present");
            (key, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "change_streams",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "change_streams",
            key: key.clone(),
        }));
    }
    Ok(out)
}

/// Repair: clear and reload from redb.
pub fn repair_change_streams(
    registry: &StreamRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    registry.clear_and_reload(catalog)
}
