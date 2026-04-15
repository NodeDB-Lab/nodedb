//! `GroupRegistry` (CDC consumer group) verifier.
//!
//! Checks that the in-memory `GroupRegistry` is consistent with
//! the `_system.consumer_groups` redb table.
//!
//! **What it checks:**
//! - Every consumer group in redb has a matching entry in memory
//!   (key = `{tenant_id}|{stream_name}|{group_name}`).
//! - Every group in memory has a backing redb row.
//!
//! **What it does NOT check:**
//! - Whether the referenced change stream exists. Cross-entity
//!   referential checks are the responsibility of a future integrity pass.
//! - Whether the per-partition offsets in `OffsetStore` are consistent
//!   with the groups — offset state is separately persisted.

use crate::control::security::catalog::SystemCatalog;
use crate::event::cdc::GroupRegistry;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_consumer_groups(
    registry: &GroupRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_consumer_groups()?
        .into_iter()
        .map(|g| {
            let key = format!("{}|{}|{}", g.tenant_id, g.stream_name, g.name);
            let value = String::from("present");
            (key, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = registry
        .list_all()
        .into_iter()
        .map(|g| {
            let key = format!("{}|{}|{}", g.tenant_id, g.stream_name, g.name);
            let value = String::from("present");
            (key, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "consumer_groups",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "consumer_groups",
            key: key.clone(),
        }));
    }
    Ok(out)
}

/// Repair: clear and reload from redb.
pub fn repair_consumer_groups(
    registry: &GroupRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    registry.clear_and_reload(catalog)
}
