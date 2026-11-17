// SPDX-License-Identifier: BUSL-1.1

//! RESTORE's proposal of the backed-up index records of one destination
//! collection, as one metadata commit.
//!
//! One inverted index serves every full-text record of a collection, so the
//! records agree on analysis after every entry applies. A backed-up record
//! that names another analysis than the destination record it replaces
//! cannot land while another destination record still names the old one.
//! The commit therefore removes each replaced record whose analysis
//! disagrees with a backed-up record first, then puts every backed-up
//! record. Each entry then leaves the collection's records in agreement.

use crate::Error;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_batch_async;
use crate::control::security::catalog::{IndexKind, IndexRecordState, StoredIndexRecord};
use crate::control::server::shared::ddl::neutral::collection::text_analysis::analyses_disagree;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// The destination collection whose backed-up index records one commit
/// proposes.
pub(super) struct CollectionRecords<'a> {
    pub(super) tenant_id: u64,
    pub(super) dest: DatabaseId,
    pub(super) collection: &'a str,
    /// The destination held no row of the collection before this restore.
    pub(super) fresh: bool,
}

/// Propose the backed-up `records` of `target` as one metadata commit.
/// Returns the names of the full-text records put `Building`, whose text
/// index rebuilds once every row re-issued.
///
/// A record the destination already holds unchanged is not proposed.
pub(super) async fn propose_collection_records(
    state: &SharedState,
    target: &CollectionRecords<'_>,
    records: Vec<StoredIndexRecord>,
) -> Result<Vec<String>, Error> {
    let mut building = Vec::new();
    propose_catalog_batch_async(state, |catalog| {
        let held = catalog.list_committed_index_records_for_collection(
            target.dest.as_u64(),
            target.tenant_id,
            target.collection,
        )?;
        let mut puts = Vec::new();
        for mut record in records {
            let existing = catalog.get_committed_index_record(
                record.database_id,
                target.tenant_id,
                &record.name,
            )?;
            if record.kind == IndexKind::FullText
                && needs_text_build(&record, existing.as_ref(), target.fresh)
            {
                record.state = IndexRecordState::Building;
                building.push(record.name.clone());
            }
            if existing.as_ref() != Some(&record) {
                puts.push(record);
            }
        }
        let mut entries: Vec<CatalogEntry> = removals_first(&held, &puts)
            .map(|replaced| CatalogEntry::DeleteIndexRecord {
                database_id: replaced.database_id,
                tenant_id: replaced.tenant_id,
                name: replaced.name.clone(),
                collection: replaced.collection.clone(),
                kind: replaced.kind,
            })
            .collect();
        entries.extend(
            puts.into_iter()
                .map(|record| CatalogEntry::PutIndexRecord(Box::new(record))),
        );
        Ok(entries)
    })
    .await?;
    Ok(building)
}

/// The full-text records among `held` that a record of `puts` replaces and
/// whose analysis disagrees with a full-text record of `puts`.
///
/// Removing them before the puts keeps the collection's records in
/// agreement after every entry. A held record left in place agrees with
/// every put: the restore's validation refuses a backup otherwise.
fn removals_first<'a>(
    held: &'a [StoredIndexRecord],
    puts: &'a [StoredIndexRecord],
) -> impl Iterator<Item = &'a StoredIndexRecord> {
    held.iter().filter(move |record| {
        record.kind == IndexKind::FullText
            && puts.iter().any(|put| put.name == record.name)
            && puts
                .iter()
                .filter(|put| put.kind == IndexKind::FullText)
                .any(|put| analyses_disagree(record, put))
    })
}

/// Whether a full-text `record` must land `Building` and rebuild.
///
/// A backed-up `Building` record never finished its build. A collection the
/// destination held can store rows with no postings for the record's fields,
/// unless the destination already holds this record `Ready` with these
/// fields and this analysis.
fn needs_text_build(
    record: &StoredIndexRecord,
    existing: Option<&StoredIndexRecord>,
    fresh_collection: bool,
) -> bool {
    if record.state == IndexRecordState::Building {
        return true;
    }
    if fresh_collection {
        return false;
    }
    !existing.is_some_and(|held| {
        held.kind == record.kind
            && held.collection == record.collection
            && held.fields == record.fields
            && held.analyzer == record.analyzer
            && held.fuzzy_default == record.fuzzy_default
            && held.state == IndexRecordState::Ready
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_record(fields: &[&str], state: IndexRecordState) -> StoredIndexRecord {
        StoredIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: "docs_text".into(),
            kind: IndexKind::FullText,
            collection: "docs".into(),
            fields: fields.iter().map(|f| f.to_string()).collect(),
            is_active: true,
            state,
            analyzer: None,
            fuzzy_default: None,
        }
    }

    fn analyzed(name: &str, analyzer: &str) -> StoredIndexRecord {
        StoredIndexRecord {
            name: name.into(),
            analyzer: Some(analyzer.into()),
            ..text_record(&["body"], IndexRecordState::Ready)
        }
    }

    /// A fresh collection takes every row from the re-issue, so its record
    /// lands `Ready`.
    #[test]
    fn a_fresh_collection_needs_no_text_build() {
        let record = text_record(&["body"], IndexRecordState::Ready);
        assert!(!needs_text_build(&record, None, true));
    }

    /// A backed-up record whose build never finished builds again.
    #[test]
    fn a_building_record_builds() {
        let record = text_record(&["body"], IndexRecordState::Building);
        assert!(needs_text_build(&record, None, true));
    }

    /// A collection the destination held builds, unless it already holds
    /// this record `Ready` with the same fields.
    #[test]
    fn a_held_collection_builds_unless_the_record_is_already_ready() {
        let record = text_record(&["body"], IndexRecordState::Ready);
        assert!(needs_text_build(&record, None, false));
        let same = text_record(&["body"], IndexRecordState::Ready);
        assert!(!needs_text_build(&record, Some(&same), false));
        let other_fields = text_record(&["title"], IndexRecordState::Ready);
        assert!(needs_text_build(&record, Some(&other_fields), false));
        let building = text_record(&["body"], IndexRecordState::Building);
        assert!(needs_text_build(&record, Some(&building), false));
    }

    /// Replaced records that disagree with the backup go first. A replaced
    /// record that agrees, and a record the backup does not replace, stay.
    #[test]
    fn replaced_records_that_disagree_are_removed_first() {
        let held = [
            analyzed("a", "german"),
            analyzed("b", "german"),
            analyzed("c", "english"),
            StoredIndexRecord {
                name: "d".into(),
                ..text_record(&["body"], IndexRecordState::Ready)
            },
        ];
        let puts = [
            analyzed("a", "english"),
            analyzed("b", "english"),
            analyzed("c", "english"),
            analyzed("d", "english"),
        ];
        let removed: Vec<&str> = removals_first(&held, &puts)
            .map(|record| record.name.as_str())
            .collect();
        assert_eq!(removed, ["a", "b"]);
        assert_eq!(removals_first(&held, &puts[2..]).count(), 0);
    }
}
