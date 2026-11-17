// SPDX-License-Identifier: BUSL-1.1

//! The fields a collection's full-text inverted index holds, and the analysis
//! it applies, derived from the catalog's full-text index records.

use std::collections::HashMap;

use nodedb_physical::physical_plan::TextFieldSet;

use crate::bootstrap::constraint_reconcile::CollectionSource;
use crate::control::security::catalog::{
    IndexKind, IndexRecordState, StoredCollection, StoredIndexRecord,
};

/// The full-text field set of `coll`, read from its own index records.
///
/// A `Building` or inactive record names indexed fields: a write during an
/// index build must already index the new field. A `Dropping` record names
/// nothing, so no write indexes a field a `DROP INDEX` is removing.
pub(crate) fn text_field_set<S: CollectionSource + ?Sized>(
    catalog: &S,
    coll: &StoredCollection,
) -> crate::Result<TextFieldSet> {
    let records =
        catalog.index_records_for_collection(coll.database_id, coll.tenant_id, &coll.name)?;
    text_field_set_of(
        records
            .iter()
            .filter(|record| record.collection == coll.name),
    )
}

/// The full-text field set of every collection `records` names, keyed by
/// collection name. A collection with no full-text record is absent.
///
/// A boot seed lists a tenant's records once and reads every collection's
/// set from this map. Records of one collection that name different
/// analysis are an error.
pub(crate) fn text_field_sets_by_collection(
    records: &[StoredIndexRecord],
) -> crate::Result<HashMap<String, TextFieldSet>> {
    let mut grouped: HashMap<&str, Vec<&StoredIndexRecord>> = HashMap::new();
    for record in records {
        if record.kind == IndexKind::FullText {
            grouped
                .entry(record.collection.as_str())
                .or_default()
                .push(record);
        }
    }
    grouped
        .into_iter()
        .map(|(collection, records)| Ok((collection.to_string(), text_field_set_of(records)?)))
        .collect()
}

/// The field set of the full-text records among `records`.
///
/// A `Building` record names indexed fields. A `Ready` record names indexed
/// and readable fields. A `Dropping` record counts for neither. Records that
/// name different analysis are an error.
fn text_field_set_of<'a>(
    records: impl IntoIterator<Item = &'a StoredIndexRecord>,
) -> crate::Result<TextFieldSet> {
    let records: Vec<&StoredIndexRecord> = records.into_iter().collect();
    let (analyzer, fuzzy_default) = super::text_analysis::analysis_of(records.iter().copied())?;
    let mut indexed = Vec::new();
    let mut readable = Vec::new();
    for record in records {
        if record.kind != IndexKind::FullText {
            continue;
        }
        match record.state {
            IndexRecordState::Ready => readable.extend(record.fields.iter().cloned()),
            IndexRecordState::Building => indexed.extend(record.fields.iter().cloned()),
            IndexRecordState::Dropping => {}
        }
    }
    Ok(TextFieldSet::new(indexed, readable).with_analysis(analyzer, fuzzy_default))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str, kind: IndexKind, collection: &str, fields: &[&str]) -> StoredIndexRecord {
        StoredIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: name.to_string(),
            kind,
            collection: collection.to_string(),
            fields: fields.iter().map(|f| f.to_string()).collect(),
            is_active: true,
            state: IndexRecordState::Building,
            analyzer: None,
            fuzzy_default: None,
        }
    }

    fn names(fields: &[&str]) -> Vec<String> {
        fields.iter().map(|f| f.to_string()).collect()
    }

    fn docs_set(records: &[StoredIndexRecord]) -> TextFieldSet {
        text_field_set_of(records.iter().filter(|r| r.collection == "docs")).unwrap()
    }

    #[test]
    fn only_full_text_records_of_the_collection_count() {
        let records = vec![
            record("ft_a", IndexKind::FullText, "docs", &["title", "body"]),
            record("ft_b", IndexKind::FullText, "docs", &["body", "abstract"]),
            record("sec", IndexKind::Secondary, "docs", &["author"]),
            record("ft_other", IndexKind::FullText, "notes", &["text"]),
        ];
        assert_eq!(
            docs_set(&records).indexed(),
            names(&["abstract", "body", "title"]).as_slice()
        );
    }

    #[test]
    fn an_inactive_record_still_counts() {
        let mut inactive = record("ft", IndexKind::FullText, "docs", &["title"]);
        inactive.is_active = false;
        assert_eq!(
            docs_set(&[inactive]).indexed(),
            names(&["title"]).as_slice()
        );
    }

    #[test]
    fn a_dropping_record_does_not_count() {
        let mut dropping = record("ft_a", IndexKind::FullText, "docs", &["title", "body"]);
        dropping.state = IndexRecordState::Dropping;
        let kept = record("ft_b", IndexKind::FullText, "docs", &["body"]);
        assert_eq!(
            docs_set(&[dropping, kept]).indexed(),
            names(&["body"]).as_slice()
        );
    }

    #[test]
    fn only_a_ready_record_makes_fields_readable() {
        let mut ready = record("ft_a", IndexKind::FullText, "docs", &["body"]);
        ready.state = IndexRecordState::Ready;
        let building = record("ft_b", IndexKind::FullText, "docs", &["title", "body"]);
        let set = docs_set(&[ready, building]);
        assert_eq!(set.indexed(), names(&["body", "title"]).as_slice());
        assert_eq!(set.readable(), names(&["body"]).as_slice());
    }

    #[test]
    fn no_full_text_record_yields_no_fields() {
        let records = vec![record("sec", IndexKind::Secondary, "docs", &["title"])];
        assert!(docs_set(&records).is_empty());
    }

    #[test]
    fn grouping_reads_each_collection_once() {
        let mut ready = record("ft_a", IndexKind::FullText, "docs", &["body"]);
        ready.state = IndexRecordState::Ready;
        let records = vec![
            ready,
            record("ft_b", IndexKind::FullText, "notes", &["text"]),
            record("sec", IndexKind::Secondary, "users", &["email"]),
        ];
        let grouped = text_field_sets_by_collection(&records).unwrap();
        assert_eq!(grouped.len(), 2);
        assert_eq!(
            grouped.get("docs"),
            Some(&TextFieldSet::all_readable(names(&["body"])))
        );
        assert_eq!(
            grouped.get("notes"),
            Some(&TextFieldSet::new(names(&["text"]), Vec::new()))
        );
    }
}
