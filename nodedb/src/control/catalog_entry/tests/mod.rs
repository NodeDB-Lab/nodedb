//! Unit tests for [`CatalogEntry`] — split per DDL family so the
//! file never grows unboundedly as new variants land.

mod collection;
mod invalidation;
mod kind_labels;
mod sequence;

use std::sync::Arc;

use crate::control::security::credential::store::CredentialStore;

/// Shared helper: open a fresh temp-dir-backed credential store
/// and return it alongside the TempDir (kept alive for the test).
pub(super) fn open_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let store = Arc::new(
        CredentialStore::open(&tmp.path().join("system.redb")).expect("open credential store"),
    );
    (store, tmp)
}
