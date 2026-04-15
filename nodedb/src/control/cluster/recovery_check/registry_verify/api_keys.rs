//! `ApiKeyStore` verifier. Compares by `key_id`, value
//! encodes `(username, revoked, expires_at)` so ALTER /
//! REVOKE divergences surface as value mismatches.

use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::catalog::SystemCatalog;

use super::super::divergence::{Divergence, DivergenceKind};
use super::diff::diff_sorted;

pub fn verify_api_keys(
    store: &ApiKeyStore,
    catalog: &SystemCatalog,
) -> crate::Result<Vec<Divergence>> {
    let mut expected: Vec<(String, String)> = catalog
        .load_all_api_keys()?
        .into_iter()
        .map(|k| {
            let value = format!("u={},rev={},exp={}", k.username, k.is_revoked, k.expires_at);
            (k.key_id, value)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    let mut actual: Vec<(String, String)> = store
        .list_all_keys()
        .into_iter()
        .map(|k| {
            let value = format!("u={},rev={},exp={}", k.username, k.is_revoked, k.expires_at);
            (k.key_id, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));

    let diff = diff_sorted(&expected, &actual, |a, b| a == b);
    let mut out = Vec::new();
    for (key, _) in &diff.only_in_expected {
        out.push(Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "api_keys",
            key: key.clone(),
        }));
    }
    for (key, _) in &diff.only_in_actual {
        out.push(Divergence::new(DivergenceKind::ExtraInRegistry {
            registry: "api_keys",
            key: key.clone(),
        }));
    }
    for (key, redb_val, mem_val) in &diff.mismatched {
        out.push(Divergence::new(DivergenceKind::ValueMismatch {
            registry: "api_keys",
            key: key.clone(),
            detail: format!("redb={redb_val}, memory={mem_val}"),
        }));
    }
    Ok(out)
}

/// Repair: clear + re-run `load_from`.
pub fn repair_api_keys(store: &ApiKeyStore, catalog: &SystemCatalog) -> crate::Result<()> {
    store.clear_and_reload(catalog)
}
