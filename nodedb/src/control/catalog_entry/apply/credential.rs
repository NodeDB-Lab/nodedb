// SPDX-License-Identifier: BUSL-1.1

//! Apply credential catalog entries to `SystemCatalog` redb.
//!
//! A put leaves a live row of the same name unchanged, so two racing
//! `CREATE CREDENTIAL` statements leave the one the log orders first. The
//! statement that proposed the entry reads the row back to report the
//! outcome.

use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::control::security::source_credential::StoredCredential;

pub fn put(row: &StoredCredential, catalog: &SystemCatalog) -> crate::Result<()> {
    let existing = catalog
        .get_credential(row.tenant_id, &row.name)
        .map_err(|e| catalog_err(&format!("read credential {}", row.name), e))?;
    if existing.is_some() {
        return Ok(());
    }
    catalog
        .put_credential(row)
        .map_err(|e| catalog_err(&format!("create credential {}", row.name), e))
}

pub fn remove(tenant_id: u64, name: &str, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_credential(tenant_id, name)
        .map(|_| ())
        .map_err(|e| catalog_err(&format!("drop credential {name}"), e))
}
