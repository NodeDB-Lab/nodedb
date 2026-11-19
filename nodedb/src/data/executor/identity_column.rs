// SPDX-License-Identifier: BUSL-1.1

//! The column a collection's rows render their identity under.
//!
//! A row with a declared key carries its identity under that key. A row with
//! no declared key carries it under `id`. The Control Plane resolves the
//! declared key from the catalog and ships it on `DocumentOp::Register`, so
//! the Data Plane reads it from `doc_configs` and never from the catalog.
//!
//! Every reader that gives a sparse row its identity takes the column from
//! here: the scan row, the filter image, the `RETURNING` row, and the event
//! image. A row then never shows an `id` beside its declared key, and a
//! predicate naming the declared key finds it.

use super::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// The column `collection`'s rows render their identity under: the
    /// registered declared key, else `id`. An unregistered collection has no
    /// declared key.
    pub(in crate::data::executor) fn identity_column(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> String {
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        self.judged_doc_config(&key)
            .and_then(|config| config.declared_key.clone())
            .unwrap_or_else(|| nodedb_types::DEFAULT_IDENTITY_COLUMN.to_string())
    }
}
