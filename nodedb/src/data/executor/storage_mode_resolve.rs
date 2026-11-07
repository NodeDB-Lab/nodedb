// SPDX-License-Identifier: BUSL-1.1

//! The storage mode a core's `doc_configs` holds for a collection.
//!
//! A conversion reaches a replica through two logs. The data-group entry
//! rewrites the rows, and the metadata entry changes the catalog row. Either
//! can apply first on a replica, so the catalog row alone does not name the
//! encoding of the rows on this core. The core's conversion record and the
//! catalog row's conversion stamp decide it together.

use nodedb_physical::physical_plan::{StorageConversion, StorageMode};
use nodedb_types::Hlc;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::sparse::btree::storage_conversion::StorageConversionRecord;
use crate::types::DatabaseId;

/// The mode the rows hold, from the catalog row's `declared` mode and `stamp`
/// and this core's conversion `record`. A record or stamp of another
/// incarnation describes rows that are gone, so it counts as absent.
///
/// - The record is ahead: the rows converted, and the catalog row follows.
/// - The two agree: the catalog row names the rows, with any later ALTER.
/// - The stamp is ahead: this replica's log has not reached the conversion.
fn resolved_mode(
    declared: &StorageMode,
    stamp: Option<&StorageConversion>,
    incarnation: Hlc,
    record: Option<&StorageConversionRecord>,
) -> StorageMode {
    let record = record.filter(|record| record.incarnation == incarnation);
    let stamp = stamp.filter(|stamp| stamp.incarnation == incarnation);
    let applied = record.map_or(0, |record| record.generation);
    let cataloged = stamp.map_or(0, |stamp| stamp.generation);
    match (record, stamp) {
        (Some(record), _) if applied > cataloged => record.target.clone(),
        (_, Some(stamp)) if cataloged > applied => stamp.source.clone(),
        _ => declared.clone(),
    }
}

impl CoreLoop {
    /// The mode this core holds `collection`'s rows in. Only the home core
    /// stores the rows. Any other core takes the catalog row's mode.
    pub(in crate::data::executor) fn resolve_storage_mode(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
        declared: &StorageMode,
        stamp: Option<&StorageConversion>,
        incarnation: Hlc,
    ) -> crate::Result<StorageMode> {
        if !self.owns_collection(database_id, collection)? {
            return Ok(declared.clone());
        }
        let record =
            self.sparse
                .get_storage_conversion(database_id.as_u64(), tenant_id, collection)?;
        Ok(resolved_mode(declared, stamp, incarnation, record.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    use super::*;

    const INCARNATION: Hlc = Hlc::new(5, 0);

    fn strict() -> StorageMode {
        StorageMode::Strict {
            schema: StrictSchema::new(vec![ColumnDef::required("id", ColumnType::String)])
                .expect("schema"),
        }
    }

    fn record(generation: u64, incarnation: Hlc) -> StorageConversionRecord {
        StorageConversionRecord {
            generation,
            incarnation,
            target: strict(),
        }
    }

    fn stamp(generation: u64, incarnation: Hlc) -> StorageConversion {
        StorageConversion {
            generation,
            incarnation,
            source: StorageMode::Schemaless,
        }
    }

    #[test]
    fn a_collection_no_conversion_touched_takes_the_catalog_mode() {
        assert_eq!(resolved_mode(&strict(), None, INCARNATION, None), strict());
    }

    /// The rows converted and the catalog row has not followed. A restart in
    /// this window reads the rows with the record's mode.
    #[test]
    fn rows_ahead_of_the_catalog_take_the_record() {
        let applied = record(1, INCARNATION);
        assert_eq!(
            resolved_mode(&StorageMode::Schemaless, None, INCARNATION, Some(&applied)),
            strict()
        );
    }

    /// The catalog row reached this replica ahead of the data-group entry.
    /// The rows still hold the source mode, and a write here encodes in it.
    #[test]
    fn a_catalog_ahead_of_the_rows_takes_the_source() {
        let cataloged = stamp(1, INCARNATION);
        assert_eq!(
            resolved_mode(&strict(), Some(&cataloged), INCARNATION, None),
            StorageMode::Schemaless
        );
    }

    /// In step, the catalog row names the rows, with any ALTER after the
    /// conversion.
    #[test]
    fn rows_and_catalog_in_step_take_the_catalog_mode() {
        let applied = record(2, INCARNATION);
        let cataloged = stamp(2, INCARNATION);
        assert_eq!(
            resolved_mode(
                &StorageMode::Schemaless,
                Some(&cataloged),
                INCARNATION,
                Some(&applied)
            ),
            StorageMode::Schemaless
        );
    }

    /// A same-name recreate holds rows no earlier conversion touched.
    #[test]
    fn another_incarnation_counts_as_absent() {
        let old = Hlc::new(1, 0);
        let applied = record(1, old);
        let cataloged = stamp(1, old);
        assert_eq!(
            resolved_mode(&StorageMode::Schemaless, None, INCARNATION, Some(&applied)),
            StorageMode::Schemaless
        );
        assert_eq!(
            resolved_mode(&strict(), Some(&cataloged), INCARNATION, None),
            strict()
        );
    }
}
