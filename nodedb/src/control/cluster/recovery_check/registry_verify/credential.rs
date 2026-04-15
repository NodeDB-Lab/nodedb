//! `CredentialStore` verifier.
//!
//! Checks that the in-memory `CredentialStore` is consistent with
//! the `_system.users` redb table inside the same credential store.
//!
//! **What it checks:**
//! - Every user in redb has a matching in-memory entry
//!   (key = `username`, value encodes `is_active` so a soft-delete
//!   that updates only redb would surface as a value mismatch).
//! - Every user in memory has a backing redb row (ghost entries from
//!   a buggy load_from path).
//!
//! **What it does NOT check:**
//! - Password hashes or SCRAM material — those are credentials,
//!   not catalog coherence.
//! - Login-attempt tracking state — that is in-memory only and
//!   intentionally not persisted.
//! - API keys — those are verified by the separate `api_keys` verifier.

use std::sync::Arc;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::credential::CredentialStore;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

/// Verify the `CredentialStore` against its embedded system catalog.
/// Returns `Ok(empty)` if there is no catalog (single-node no-auth mode).
pub fn verify_credentials(
    store: &Arc<CredentialStore>,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_users()?
        .into_iter()
        .map(|u| {
            let value = format!("active={}", u.is_active);
            (u.username, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = store
        .list_all_user_details()
        .into_iter()
        .map(|u| {
            let value = format!("active={}", u.is_active);
            (u.username, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "credentials",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "credentials",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "credentials",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair: reload all users from redb into the credential store.
pub fn repair_credentials(
    store: &Arc<CredentialStore>,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    store.reload_from_catalog(catalog)
}
