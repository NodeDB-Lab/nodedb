// SPDX-License-Identifier: BUSL-1.1

//! The full-text index declaration a stored collection reports to the
//! planner, derived from the catalog's full-text index records.

use nodedb_sql::types::TextIndexFields;
use nodedb_types::text_search::WHOLE_DOCUMENT;

use crate::control::security::catalog::{
    IndexKind, IndexRecordState, StoredIndexRecord, SystemCatalog,
};

/// Refuse a full-text read of `field` on `collection` that no ready
/// full-text index serves. A `None` field reads the whole document.
///
/// The read path that skips the SQL planner runs this. It decides through
/// [`TextIndexFields::read_fault`], the same rule the planner applies.
pub(crate) fn check_text_read(
    catalog: &SystemCatalog,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    field: Option<&str>,
) -> crate::Result<()> {
    let records = catalog.list_index_records_for_collection(database_id, tenant_id, collection)?;
    match text_index_fields(&records, collection).read_fault(field) {
        None => Ok(()),
        Some(fault) => Err(crate::Error::TextColumn {
            collection: collection.to_owned(),
            column: field.unwrap_or(WHOLE_DOCUMENT).to_owned(),
            fault,
        }),
    }
}

/// The fields the full-text records in `records` on `collection` cover.
///
/// A `Ready` record serves reads. A `Building` record names fields that a
/// read refuses with a still-building error. A field a ready record covers
/// is never listed as building. A `Dropping` record counts for neither.
pub(super) fn text_index_fields(
    records: &[StoredIndexRecord],
    collection: &str,
) -> TextIndexFields {
    let mut ready = Vec::new();
    let mut building = Vec::new();
    let full_text = records
        .iter()
        .filter(|record| record.kind == IndexKind::FullText && record.collection == collection);
    for record in full_text {
        let target = match record.state {
            IndexRecordState::Ready => &mut ready,
            IndexRecordState::Building => &mut building,
            IndexRecordState::Dropping => continue,
        };
        target.extend(record.fields.iter().cloned());
    }
    ready.sort();
    ready.dedup();
    building.retain(|field| ready.binary_search(field).is_err());
    building.sort();
    building.dedup();
    TextIndexFields { ready, building }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        name: &str,
        kind: IndexKind,
        collection: &str,
        fields: &[&str],
        state: IndexRecordState,
    ) -> StoredIndexRecord {
        StoredIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: name.to_string(),
            kind,
            collection: collection.to_string(),
            fields: fields.iter().map(|f| f.to_string()).collect(),
            is_active: true,
            state,
            analyzer: None,
            fuzzy_default: None,
        }
    }

    #[test]
    fn fields_split_by_record_state() {
        let records = vec![
            record(
                "ft_a",
                IndexKind::FullText,
                "docs",
                &["title", "body"],
                IndexRecordState::Ready,
            ),
            record(
                "ft_b",
                IndexKind::FullText,
                "docs",
                &["body", "abstract"],
                IndexRecordState::Building,
            ),
            record(
                "ft_c",
                IndexKind::FullText,
                "docs",
                &["notes"],
                IndexRecordState::Dropping,
            ),
            record(
                "ft_d",
                IndexKind::FullText,
                "other",
                &["text"],
                IndexRecordState::Ready,
            ),
            record(
                "vec",
                IndexKind::Vector,
                "docs",
                &["embedding"],
                IndexRecordState::Ready,
            ),
        ];
        assert_eq!(
            text_index_fields(&records, "docs"),
            TextIndexFields {
                ready: vec!["body".into(), "title".into()],
                building: vec!["abstract".into()],
            }
        );
    }

    #[test]
    fn a_collection_without_records_declares_nothing() {
        assert_eq!(text_index_fields(&[], "docs"), TextIndexFields::default());
    }
}
