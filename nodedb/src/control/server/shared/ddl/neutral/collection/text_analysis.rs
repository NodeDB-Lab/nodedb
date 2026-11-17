// SPDX-License-Identifier: BUSL-1.1

//! The analyzer and fuzzy default a collection's full-text index applies.
//!
//! One inverted index serves every full-text index of a collection, so the
//! collection tokenizes one way. Each full-text record names the analyzer and
//! fuzzy default its statement declared, or none. The collection takes the
//! value its records name. A `Dropping` record names nothing. `CREATE` refuses
//! a record that names a value other than the one the collection holds. So
//! does the proposal of the record, under the DDL preparation lease, so two
//! statements that each passed their own check never both land.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::{
    IndexKind, IndexRecordState, StoredIndexRecord, SystemCatalog,
};

/// The analyzer and fuzzy default the full-text records among `records`
/// name. `None` and `false` when no record names one.
///
/// [`analysis_conflict`] and [`check_entries`] keep every record of one
/// collection in agreement. Two records that name different values are an
/// impossible catalog state, refused here rather than resolved by order.
pub(super) fn analysis_of<'a>(
    records: impl IntoIterator<Item = &'a StoredIndexRecord>,
) -> crate::Result<(Option<String>, bool)> {
    let mut analyzer: Option<&StoredIndexRecord> = None;
    let mut fuzzy_default: Option<&StoredIndexRecord> = None;
    for record in records.into_iter().filter(|record| names_analysis(record)) {
        if let Some(name) = record.analyzer.as_deref() {
            match analyzer {
                Some(held) if held.analyzer.as_deref() != Some(name) => {
                    return Err(disagreement(held, record, "analyzers"));
                }
                Some(_) => {}
                None => analyzer = Some(record),
            }
        }
        if let Some(fuzzy) = record.fuzzy_default {
            match fuzzy_default {
                Some(held) if held.fuzzy_default != Some(fuzzy) => {
                    return Err(disagreement(held, record, "fuzzy defaults"));
                }
                Some(_) => {}
                None => fuzzy_default = Some(record),
            }
        }
    }
    Ok((
        analyzer.and_then(|record| record.analyzer.clone()),
        fuzzy_default
            .and_then(|record| record.fuzzy_default)
            .unwrap_or(false),
    ))
}

/// The error for two full-text records of one collection that name
/// different `what`.
fn disagreement(held: &StoredIndexRecord, other: &StoredIndexRecord, what: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "full-text indexes '{}' and '{}' of collection '{}' name different {what}; \
             drop one of them",
            held.name, other.name, held.collection
        ),
    }
}

/// Refuse a full-text record among `entries` whose analysis conflicts with
/// the committed records of its collection or with an earlier entry.
///
/// The proposer runs this under the DDL preparation lease, which every DDL
/// takes. No other record commits between this check and the entries'
/// apply, so the catalog never holds two records that disagree. A record
/// that replaces one of the same name is checked against the others only.
pub(crate) fn check_entries<'a>(
    entries: impl IntoIterator<Item = &'a CatalogEntry>,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let entries: Vec<&CatalogEntry> = entries.into_iter().collect();
    for (position, entry) in entries.iter().enumerate() {
        let CatalogEntry::PutIndexRecord(record) = entry else {
            continue;
        };
        if record.kind != IndexKind::FullText {
            continue;
        }
        let mut held = catalog.list_committed_index_records_for_collection(
            record.database_id,
            record.tenant_id,
            &record.collection,
        )?;
        for earlier in &entries[..position] {
            replay(&mut held, earlier, record);
        }
        held.retain(|other| other.name != record.name);
        if let Some(conflict) = analysis_conflict(
            &held,
            &record.collection,
            record.analyzer.as_deref(),
            record.fuzzy_default,
        ) {
            return Err(crate::Error::FeatureNotSupported { detail: conflict });
        }
    }
    Ok(())
}

/// Replay `earlier` over `held`, the records of the collection `record`
/// names. An entry of another collection, database, or tenant changes
/// nothing.
fn replay(held: &mut Vec<StoredIndexRecord>, earlier: &CatalogEntry, record: &StoredIndexRecord) {
    match earlier {
        CatalogEntry::PutIndexRecord(put)
            if put.database_id == record.database_id
                && put.tenant_id == record.tenant_id
                && put.collection == record.collection =>
        {
            held.retain(|other| other.name != put.name);
            held.push((**put).clone());
        }
        CatalogEntry::DeleteIndexRecord {
            database_id,
            tenant_id,
            name,
            ..
        } if *database_id == record.database_id && *tenant_id == record.tenant_id => {
            held.retain(|other| &other.name != name);
        }
        _ => {}
    }
}

/// Why a new full-text index on `collection` naming `analyzer` and
/// `fuzzy_default` cannot join the full-text records among `records`, or
/// `None` when it can.
///
/// A value the new index leaves unnamed never conflicts. A named value
/// conflicts with a different value another live record of the collection
/// names.
pub(crate) fn analysis_conflict(
    records: &[StoredIndexRecord],
    collection: &str,
    analyzer: Option<&str>,
    fuzzy_default: Option<bool>,
) -> Option<String> {
    let live = || {
        records
            .iter()
            .filter(|record| record.collection == collection && names_analysis(record))
    };
    if let Some(wanted) = analyzer
        && let Some(held) = live().find(|record| {
            record
                .analyzer
                .as_deref()
                .is_some_and(|name| name != wanted)
        })
    {
        return Some(format!(
            "collection '{collection}' tokenizes with analyzer '{}' (index '{}'); a full-text \
             index with analyzer '{wanted}' is not supported on it. Drop that index first, or \
             name analyzer '{}'",
            held.analyzer.as_deref().unwrap_or_default(),
            held.name,
            held.analyzer.as_deref().unwrap_or_default()
        ));
    }
    if let Some(wanted) = fuzzy_default
        && let Some(held) =
            live().find(|record| record.fuzzy_default.is_some_and(|fuzzy| fuzzy != wanted))
    {
        return Some(format!(
            "collection '{collection}' has fuzzy default {} (index '{}'); a full-text index \
             with fuzzy default {wanted} is not supported on it. Drop that index first, or \
             name fuzzy default {}",
            !wanted, held.name, !wanted
        ));
    }
    None
}

/// Why the full-text records of `collection` disagree once every record in
/// `incoming` replaces the record of its name among `held`, or `None` when
/// they agree.
///
/// `held` is the collection's records before the change. A record of
/// `incoming` takes the place of the held record of its name. Each incoming
/// record is checked against every other record of the resulting set.
pub(crate) fn replacement_conflict(
    held: &[StoredIndexRecord],
    incoming: &[&StoredIndexRecord],
    collection: &str,
) -> Option<String> {
    let mut after: Vec<StoredIndexRecord> = held
        .iter()
        .filter(|record| !incoming.iter().any(|put| put.name == record.name))
        .cloned()
        .collect();
    after.extend(incoming.iter().map(|record| (*record).clone()));
    incoming
        .iter()
        .filter(|record| record.collection == collection && names_analysis(record))
        .find_map(|record| {
            let others: Vec<StoredIndexRecord> = after
                .iter()
                .filter(|other| other.name != record.name)
                .cloned()
                .collect();
            analysis_conflict(
                &others,
                collection,
                record.analyzer.as_deref(),
                record.fuzzy_default,
            )
        })
}

/// Whether full-text records `a` and `b` name different analyzers or
/// different fuzzy defaults. A value either record leaves unnamed never
/// disagrees.
pub(crate) fn analyses_disagree(a: &StoredIndexRecord, b: &StoredIndexRecord) -> bool {
    let analyzers = matches!(
        (a.analyzer.as_deref(), b.analyzer.as_deref()),
        (Some(x), Some(y)) if x != y
    );
    let fuzzy = matches!(
        (a.fuzzy_default, b.fuzzy_default),
        (Some(x), Some(y)) if x != y
    );
    analyzers || fuzzy
}

/// Whether `record` is a full-text record whose analysis applies.
fn names_analysis(record: &StoredIndexRecord) -> bool {
    record.kind == IndexKind::FullText && record.state != IndexRecordState::Dropping
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        name: &str,
        analyzer: Option<&str>,
        fuzzy_default: Option<bool>,
        state: IndexRecordState,
    ) -> StoredIndexRecord {
        StoredIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: name.to_string(),
            kind: IndexKind::FullText,
            collection: "docs".to_string(),
            fields: vec!["body".to_string()],
            is_active: true,
            state,
            analyzer: analyzer.map(str::to_string),
            fuzzy_default,
        }
    }

    #[test]
    fn the_collection_takes_the_analysis_its_records_name() {
        let records = [
            record("plain", None, None, IndexRecordState::Ready),
            record(
                "german",
                Some("german"),
                Some(true),
                IndexRecordState::Ready,
            ),
        ];
        assert_eq!(
            analysis_of(&records).unwrap(),
            (Some("german".to_string()), true)
        );
        assert_eq!(analysis_of(std::iter::empty()).unwrap(), (None, false));
    }

    /// Two records that name different values are refused, never resolved
    /// by which one comes first.
    #[test]
    fn records_that_disagree_are_refused() {
        let analyzers = [
            record("german", Some("german"), None, IndexRecordState::Ready),
            record("english", Some("english"), None, IndexRecordState::Ready),
        ];
        let refused = analysis_of(&analyzers).unwrap_err().to_string();
        assert!(
            refused.contains("german") && refused.contains("english"),
            "{refused}"
        );
        let fuzzy = [
            record("on", None, Some(true), IndexRecordState::Ready),
            record("off", None, Some(false), IndexRecordState::Building),
        ];
        assert!(analysis_of(&fuzzy).is_err());
    }

    fn put(record: StoredIndexRecord) -> CatalogEntry {
        CatalogEntry::PutIndexRecord(Box::new(record))
    }

    /// Two CREATE SEARCH INDEX statements of one transaction that name
    /// different analyzers: the second statement's check reads the first
    /// statement's buffered record through the session overlay and refuses.
    #[tokio::test]
    async fn a_buffered_record_of_the_same_transaction_conflicts() {
        use crate::control::server::shared::session::{conn_scope, ddl_buffer};

        let catalog = SystemCatalog::open_in_memory().unwrap();
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put(record(
                "first",
                Some("german"),
                Some(true),
                IndexRecordState::Building,
            ))));
            let records = catalog
                .list_index_records_for_collection(0, 1, "docs")
                .unwrap();
            assert!(analysis_conflict(&records, "docs", Some("english"), None).is_some());
            assert!(analysis_conflict(&records, "docs", None, Some(false)).is_some());
            assert_eq!(
                analysis_conflict(&records, "docs", Some("german"), Some(true)),
                None
            );
        })
        .await;
    }

    /// COMMIT checks the buffered entries in order against the committed
    /// records, so a batch whose records disagree never lands.
    #[test]
    fn a_batch_whose_records_disagree_is_refused() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        let first = put(record(
            "first",
            Some("german"),
            None,
            IndexRecordState::Building,
        ));
        let second = put(record(
            "second",
            Some("english"),
            None,
            IndexRecordState::Building,
        ));
        assert!(check_entries([&first, &second], &catalog).is_err());
        let same = put(record(
            "second",
            Some("german"),
            None,
            IndexRecordState::Building,
        ));
        check_entries([&first, &same], &catalog).unwrap();
        // A record that replaces one of the same name checks against the
        // others only.
        let replaced = put(record(
            "first",
            Some("english"),
            None,
            IndexRecordState::Ready,
        ));
        check_entries([&first, &replaced], &catalog).unwrap();
        // A record dropped earlier in the batch names nothing.
        let dropped = CatalogEntry::DeleteIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: "first".to_string(),
            collection: "docs".to_string(),
            kind: IndexKind::FullText,
        };
        check_entries([&first, &dropped, &second], &catalog).unwrap();
    }

    #[test]
    fn a_dropping_record_names_no_analysis() {
        let records = [record(
            "german",
            Some("german"),
            Some(true),
            IndexRecordState::Dropping,
        )];
        assert_eq!(analysis_of(&records).unwrap(), (None, false));
        assert_eq!(
            analysis_conflict(&records, "docs", Some("english"), Some(false)),
            None
        );
    }

    /// A second CREATE that names another analyzer is refused. One that
    /// names the same analyzer, or none, joins.
    #[test]
    fn a_second_index_with_another_analyzer_is_refused() {
        let records = [record(
            "first",
            Some("german"),
            None,
            IndexRecordState::Ready,
        )];
        let refused = analysis_conflict(&records, "docs", Some("english"), None)
            .expect("another analyzer conflicts");
        assert!(
            refused.contains("german") && refused.contains("english"),
            "{refused}"
        );
        assert_eq!(
            analysis_conflict(&records, "docs", Some("german"), None),
            None
        );
        assert_eq!(analysis_conflict(&records, "docs", None, None), None);
        assert_eq!(
            analysis_conflict(&records, "other", Some("english"), None),
            None,
            "another collection's records never conflict"
        );
    }

    /// Records that replace every held record of their names agree with the
    /// result, whatever the held records named. A held record left in place
    /// still counts.
    #[test]
    fn replacement_checks_the_resulting_records() {
        let held = [
            record("a", Some("german"), None, IndexRecordState::Ready),
            record("b", Some("german"), None, IndexRecordState::Ready),
        ];
        let a = record("a", Some("english"), None, IndexRecordState::Ready);
        let b = record("b", Some("english"), None, IndexRecordState::Ready);
        assert_eq!(replacement_conflict(&held, &[&a, &b], "docs"), None);
        let refused = replacement_conflict(&held, &[&a], "docs").expect("b stays german");
        assert!(
            refused.contains("german") && refused.contains("english"),
            "{refused}"
        );
        let c = record("c", Some("english"), None, IndexRecordState::Ready);
        assert!(replacement_conflict(&held, &[&a, &b, &c], "docs").is_none());
        assert!(replacement_conflict(&held, &[&c], "docs").is_some());
    }

    #[test]
    fn analyses_disagree_only_on_named_values() {
        let german = record("a", Some("german"), Some(true), IndexRecordState::Ready);
        let english = record("b", Some("english"), None, IndexRecordState::Ready);
        let unnamed = record("c", None, None, IndexRecordState::Ready);
        let fuzzy_off = record("d", None, Some(false), IndexRecordState::Ready);
        assert!(analyses_disagree(&german, &english));
        assert!(analyses_disagree(&german, &fuzzy_off));
        assert!(!analyses_disagree(&german, &unnamed));
        assert!(!analyses_disagree(&english, &fuzzy_off));
    }

    #[test]
    fn a_second_index_with_another_fuzzy_default_is_refused() {
        let records = [record("first", None, Some(true), IndexRecordState::Ready)];
        assert!(analysis_conflict(&records, "docs", None, Some(false)).is_some());
        assert_eq!(analysis_conflict(&records, "docs", None, Some(true)), None);
    }
}
