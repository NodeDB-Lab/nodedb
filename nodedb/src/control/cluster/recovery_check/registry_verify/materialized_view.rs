//! `MvRegistry` (streaming materialized view) verifier.
//!
//! Checks that the in-memory `MvRegistry` is consistent with
//! the `_system.streaming_mvs` redb table.
//!
//! **What it checks:**
//! - Every streaming MV definition in redb has a matching entry in
//!   memory (key = `{tenant_id}|{name}`, value encodes
//!   `source_stream` so a source-change mutation surfaces).
//! - Every MV in memory has a backing redb row.
//!
//! **What it does NOT check:**
//! - Whether the source change stream exists or is active. Cross-entity
//!   referential checks are the responsibility of a future integrity pass.
//! - Whether the MV's live aggregate state is consistent with its
//!   definition — state is rebuilt from events, not from redb.

use crate::control::security::catalog::SystemCatalog;
use crate::event::streaming_mv::MvRegistry;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_mvs(
    registry: &MvRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_streaming_mvs()?
        .into_iter()
        .map(|m| {
            let key = format!("{}|{}", m.tenant_id, m.name);
            let value = format!("src={}", m.source_stream);
            (key, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = registry
        .list_all()
        .into_iter()
        .map(|m| {
            let key = format!("{}|{}", m.tenant_id, m.name);
            let value = format!("src={}", m.source_stream);
            (key, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "streaming_mvs",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "streaming_mvs",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "streaming_mvs",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair: clear and reload from redb.
pub fn repair_mvs(registry: &MvRegistry, catalog: &SystemCatalog) -> crate::Result<()> {
    registry.clear_and_reload(catalog)
}
