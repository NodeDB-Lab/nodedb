// SPDX-License-Identifier: BUSL-1.1

//! The rows a predicate write matches, and the OLLP helpers that compare
//! them with a carried prediction.
//!
//! The reads here are the one place a document read decides where a row's
//! current version lives. A bitemporal collection holds its rows only in the
//! versioned table, so the reads take each row's current version there. Any
//! other collection reads the plain document table.

use std::ops::ControlFlow;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::filter_match::HeldEvalError;
use crate::engine::sparse::btree_versioned::VersionedScanParams;
use crate::engine::sparse::scan_stop::never_stop;
use nodedb_types::StorageKey;

/// One matched row: its storage key and its current stored body.
pub(in crate::data::executor) type MatchedRow = (StorageKey, Vec<u8>);

/// Rows read per page of a bitemporal collection's current versions.
const BITEMPORAL_PAGE_ROWS: usize = 1024;

impl CoreLoop {
    /// Each `(row key, current body)` in `collection` that `filters` match.
    ///
    /// Autocommit bulk delete, update and TRUNCATE, facet counts and the
    /// Calvin resolve read their matched set here. A transaction reads it through
    /// [`Self::current_rows_where`] with its overlay folded in. A filter
    /// evaluation error fails the read.
    pub(in crate::data::executor) fn current_matching_rows(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &[ScanFilter],
    ) -> crate::Result<Vec<MatchedRow>> {
        let crs = self.crs_context();
        let raw_matches = self.strict_aware_matcher(database_id, tid, collection, filters, &crs);
        let held = HeldEvalError::default();
        let matches = |row_key: &StorageKey, body: &[u8]| held.check(|| raw_matches(row_key, body));
        let rows = self.current_rows_where(database_id, tid, collection, &matches)?;
        held.finish(rows)
    }

    /// The storage key of each row of `collection` at its current version.
    pub(in crate::data::executor) fn current_row_keys(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> crate::Result<Vec<StorageKey>> {
        let mut keys = Vec::new();
        self.for_each_current_row(database_id, tid, collection, |key, _| {
            keys.push(*key);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(keys)
    }

    /// Each `(row key, current body)` in `collection` that `matches` accepts.
    ///
    /// A bitemporal collection yields the newest system-time version of each
    /// row from the versioned table. A row whose newest version is a
    /// tombstone is absent. Any other collection reads the plain table.
    pub(in crate::data::executor) fn current_rows_where(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        matches: &dyn Fn(&StorageKey, &[u8]) -> bool,
    ) -> crate::Result<Vec<MatchedRow>> {
        if self.is_bitemporal(database_id, tid, collection) {
            return self.sparse.versioned_scan_as_of(
                VersionedScanParams {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    sys_cutoff: None,
                    valid_at_ms: None,
                    limit: usize::MAX,
                },
                matches,
                &never_stop,
            );
        }
        let mut rows = Vec::new();
        self.sparse.scan_documents_for_each(
            database_id,
            tid,
            collection,
            usize::MAX,
            |key, body| {
                if matches(key, body) {
                    rows.push((*key, body.to_vec()));
                }
                Ok(())
            },
        )?;
        Ok(rows)
    }

    /// The first `limit` rows of `collection` in key order, each at its
    /// current version, with no filter.
    pub(in crate::data::executor) fn current_rows(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<MatchedRow>> {
        if self.is_bitemporal(database_id, tid, collection) {
            return self.sparse.versioned_scan_as_of(
                VersionedScanParams {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    sys_cutoff: None,
                    valid_at_ms: None,
                    limit,
                },
                &|_: &StorageKey, _: &[u8]| true,
                &never_stop,
            );
        }
        self.sparse
            .scan_documents(database_id, tid, collection, limit)
    }

    /// Visit each row of `collection` at its current version, in key order,
    /// until `visit` breaks.
    ///
    /// A plain collection streams one row at a time. A bitemporal collection
    /// is read a page at a time, so at most one page of rows is in memory. A
    /// visit error ends the scan and is returned.
    pub(in crate::data::executor) fn for_each_current_row(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        mut visit: impl FnMut(&StorageKey, &[u8]) -> crate::Result<ControlFlow<()>>,
    ) -> crate::Result<()> {
        if !self.is_bitemporal(database_id, tid, collection) {
            return self
                .sparse
                .scan_documents_while(database_id, tid, collection, visit);
        }
        let mut after: Option<StorageKey> = None;
        loop {
            let page = self.sparse.versioned_scan_as_of_after(
                VersionedScanParams {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    sys_cutoff: None,
                    valid_at_ms: None,
                    limit: BITEMPORAL_PAGE_ROWS,
                },
                after.as_ref(),
                &|_: &StorageKey, _: &[u8]| true,
                &never_stop,
            )?;
            let full = page.len() >= BITEMPORAL_PAGE_ROWS;
            for (key, body) in &page {
                if visit(key, body)?.is_break() {
                    return Ok(());
                }
            }
            match page.into_iter().next_back() {
                Some((key, _)) if full => after = Some(key),
                _ => return Ok(()),
            }
        }
    }

    /// The current stored body of one row, `None` when the row is absent.
    ///
    /// This is the per-row form of the view [`Self::current_rows_where`]
    /// scans. A predicate write reads each matched row's pre-image here.
    pub(in crate::data::executor) fn current_row_body(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        key: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        if self.is_bitemporal(database_id, tid, collection) {
            self.sparse
                .versioned_get_current(database_id, tid, collection, key)
        } else {
            self.sparse.get(database_id, tid, collection, key)
        }
    }

    /// [`Self::current_row_body`] read inside `txn`, so it sees the writes
    /// `txn` staged.
    pub(in crate::data::executor) fn current_row_body_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tid: u64,
        collection: &str,
        key: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        if self.is_bitemporal(database_id, tid, collection) {
            self.sparse
                .versioned_get_current_in_txn(txn, database_id, tid, collection, key)
        } else {
            self.sparse
                .get_in_txn(txn, database_id, tid, collection, key)
        }
    }
}

/// Convert the carried OLLP predicted surrogate set into the sorted list of
/// storage keys to apply the bulk mutation to.
///
/// This is the determinism anchor for multi-replica OLLP: every replica —
/// leader and follower — mutates EXACTLY this set, derived from the leader's
/// verified prediction carried in the plan, rather than from a per-replica
/// local scan (which can differ when a follower's redb snapshot lags). Output
/// is sorted ascending by surrogate so the apply order is identical on every
/// replica.
pub(in crate::data::executor) fn ollp_predicted_doc_ids(predicted: &[u32]) -> Vec<StorageKey> {
    let mut surrogates: Vec<u32> = predicted.to_vec();
    surrogates.sort_unstable();
    surrogates
        .into_iter()
        .map(|s| StorageKey::for_surrogate(nodedb_types::Surrogate::new(s)))
        .collect()
}

/// Compute the sorted list of surrogates from scanned storage keys.
///
/// Feeds the OLLP verification comparison on both sides: Data Plane and
/// Control Plane pre-exec.
pub(in crate::data::executor) fn ollp_actual_surrogates(doc_ids: &[StorageKey]) -> Vec<u32> {
    let mut surrogates: Vec<u32> = doc_ids.iter().map(|k| k.surrogate().as_u32()).collect();
    surrogates.sort_unstable();
    surrogates
}

/// True when the live `matching_ids` surrogate set equals the carried
/// `predicted` set. Both sides are sorted before comparison, so the result is
/// deterministic on every replica. Shared by the bulk-DML apply handlers and
/// the transaction prediction check (`TxnPrediction::holds`) so the
/// `actual == predicted` guard lives in exactly one place.
pub(in crate::data::executor) fn ollp_surrogates_match(
    matching_ids: &[StorageKey],
    predicted: &[u32],
) -> bool {
    let actual = ollp_actual_surrogates(matching_ids);
    let mut predicted_sorted: Vec<u32> = predicted.to_vec();
    predicted_sorted.sort_unstable();
    actual == predicted_sorted
}

/// True when the recomputed `actual` implicit-edge set equals the carried
/// `predicted` edge set. Both sides are sorted via `OllpPredictedEdge`'s
/// derived `Ord`, matching the surrogate-set comparison's determinism contract.
pub(in crate::data::executor) fn ollp_edges_match(
    mut actual: Vec<nodedb_physical::physical_plan::OllpPredictedEdge>,
    predicted: &[nodedb_physical::physical_plan::OllpPredictedEdge],
) -> bool {
    actual.sort_unstable();
    let mut predicted_sorted = predicted.to_vec();
    predicted_sorted.sort_unstable();
    actual == predicted_sorted
}

#[cfg(test)]
mod tests {
    //! Predicate writes on a bitemporal collection: the shared read matches
    //! each row's current version, and the bulk apply closes or supersedes
    //! that version as the point paths do.

    use super::*;
    use crate::bridge::envelope::{RowEffect, Status};
    use crate::bridge::scan_filter::FilterOp;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::data::executor::handlers::bulk_dml::{
        BulkDeleteParams, BulkUpdateParams, OllpPrediction,
    };
    use crate::engine::document::store::{CollectionConfig, IndexPath};
    use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};
    use crate::types::{DatabaseId, TenantId};
    use nodedb_physical::physical_plan::UpdateValue;
    use nodedb_types::Surrogate;

    const TID: u64 = 1;
    const LEDGER: &str = "ledger";
    /// The system time every seeded version lands at, before any statement.
    const SEEDED_AT: i64 = 1_000;

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate(surrogate))
    }

    /// A bitemporal `ledger` with a secondary index on `kind`, holding rows
    /// 1 (`x`), 2 (`y`) and 3 (`x`) at `SEEDED_AT`.
    fn bitemporal_ledger(core: &mut CoreLoop, database_id: u64) {
        let mut config = CollectionConfig::new(LEDGER);
        config.bitemporal = true;
        config.index_paths.push(IndexPath::new("kind"));
        core.doc_configs.insert(
            (
                DatabaseId::new(database_id),
                TenantId::new(TID),
                LEDGER.to_string(),
            ),
            config,
        );
        for (surrogate, kind) in [(1, "x"), (2, "y"), (3, "x")] {
            let body = doc_format::encode_to_msgpack(&serde_json::json!({ "kind": kind }));
            core.sparse
                .versioned_put(VersionedPut {
                    database_id,
                    tenant: TID,
                    coll: LEDGER,
                    doc_id: &key(surrogate),
                    sys_from: SEEDED_AT,
                    valid_from_ms: i64::MIN,
                    valid_until_ms: i64::MAX,
                    body: &body,
                })
                .expect("seed version");
            core.sparse
                .versioned_index_put(VersionedIndexEntry {
                    database_id,
                    tenant: TID,
                    coll: LEDGER,
                    field: "kind",
                    value: kind,
                    doc_id: &key(surrogate),
                    sys_from: SEEDED_AT,
                })
                .expect("seed index entry");
        }
    }

    fn kind_eq(kind: &str) -> Vec<ScanFilter> {
        vec![ScanFilter {
            field: "kind".into(),
            op: FilterOp::Eq,
            value: nodedb_types::Value::String(kind.into()),
            clauses: Vec::new(),
            expr: None,
        }]
    }

    fn kind_eq_bytes(kind: &str) -> Vec<u8> {
        zerompk::to_msgpack_vec(&kind_eq(kind)).expect("encode filter")
    }

    /// The `kind` a stored body holds.
    fn kind_of(body: &[u8]) -> String {
        doc_format::decode_document(body)
            .expect("row decodes")
            .get("kind")
            .and_then(|v| v.as_str())
            .expect("row carries a kind")
            .to_string()
    }

    fn version_at(
        core: &CoreLoop,
        database_id: u64,
        surrogate: u32,
        sys_cutoff: Option<i64>,
    ) -> Option<Vec<u8>> {
        core.sparse
            .versioned_get_as_of(database_id, TID, LEDGER, &key(surrogate), sys_cutoff, None)
            .expect("read version")
    }

    fn indexed_rows(
        core: &CoreLoop,
        database_id: u64,
        kind: &str,
        sys_cutoff: Option<i64>,
    ) -> Vec<u32> {
        let mut rows: Vec<u32> = core
            .sparse
            .versioned_index_lookup_as_of(database_id, TID, LEDGER, "kind", kind, sys_cutoff)
            .expect("index lookup")
            .into_iter()
            .map(|k| k.surrogate().as_u32())
            .collect();
        rows.sort_unstable();
        rows
    }

    fn affected(response: &crate::bridge::envelope::Response) -> u64 {
        crate::control::server::shared::sql::staging_predicates::require_affected_count(
            response.payload.as_bytes(),
        )
        .expect("a bulk write reports an affected count")
    }

    #[test]
    fn the_shared_read_matches_current_versions_of_a_bitemporal_collection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let database_id = make_default_task().request.database_id.as_u64();
        bitemporal_ledger(&mut core, database_id);

        let rows = core
            .current_matching_rows(database_id, TID, LEDGER, &kind_eq("x"))
            .expect("matching read");
        let surrogates: Vec<u32> = rows.iter().map(|(k, _)| k.surrogate().as_u32()).collect();
        assert_eq!(surrogates, vec![1, 3]);
        assert!(rows.iter().all(|(_, body)| kind_of(body) == "x"));
        assert_eq!(
            core.current_row_body(database_id, TID, LEDGER, &key(2))
                .expect("row read")
                .map(|body| kind_of(&body)),
            Some("y".to_string())
        );
    }

    #[test]
    fn bulk_delete_closes_the_current_version_of_each_matched_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let database_id = task.request.database_id.as_u64();
        bitemporal_ledger(&mut core, database_id);

        let filter = kind_eq_bytes("x");
        let response = core.execute_bulk_delete(
            &task,
            TID,
            BulkDeleteParams {
                collection: LEDGER,
                filter_bytes: &filter,
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &[],
                ollp: OllpPrediction {
                    surrogates: None,
                    edges: None,
                },
                declared_primary_key: None,
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(affected(&response), 2);
        for surrogate in [1, 3] {
            assert!(
                version_at(&core, database_id, surrogate, None).is_none(),
                "row {surrogate} has no current version"
            );
            assert_eq!(
                version_at(&core, database_id, surrogate, Some(SEEDED_AT))
                    .map(|body| kind_of(&body)),
                Some("x".to_string()),
                "row {surrogate} keeps its history"
            );
        }
        assert_eq!(
            version_at(&core, database_id, 2, None).map(|body| kind_of(&body)),
            Some("y".to_string())
        );
        assert!(indexed_rows(&core, database_id, "x", None).is_empty());
        assert_eq!(
            indexed_rows(&core, database_id, "x", Some(SEEDED_AT)),
            vec![1, 3]
        );

        // Each removal journals its tombstone at its version key.
        let tombstones: Vec<(u32, Option<i64>)> = response
            .write_set
            .iter()
            .filter_map(|entry| match entry.effect {
                RowEffect::Delete { system_from } => Some((entry.surrogate, system_from)),
                _ => None,
            })
            .collect();
        assert_eq!(tombstones.len(), 2);
        assert!(
            tombstones
                .iter()
                .all(|(_, sys)| sys.is_some_and(|sys| sys > SEEDED_AT))
        );
    }

    #[test]
    fn bulk_update_writes_a_new_version_of_each_matched_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let database_id = task.request.database_id.as_u64();
        bitemporal_ledger(&mut core, database_id);

        let filter = kind_eq_bytes("y");
        let updates = vec![(
            "kind".to_string(),
            UpdateValue::Literal(
                nodedb_types::json_to_msgpack(&serde_json::json!("z")).expect("encode"),
            ),
        )];
        let response = core.execute_bulk_update(
            &task,
            TID,
            BulkUpdateParams {
                collection: LEDGER,
                filter_bytes: &filter,
                updates: &updates,
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &[],
                declared_primary_key: None,
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(affected(&response), 1);
        assert_eq!(
            version_at(&core, database_id, 2, None).map(|body| kind_of(&body)),
            Some("z".to_string())
        );
        assert_eq!(
            version_at(&core, database_id, 2, Some(SEEDED_AT)).map(|body| kind_of(&body)),
            Some("y".to_string()),
            "the superseded version stays readable"
        );
        for surrogate in [1, 3] {
            assert_eq!(
                version_at(&core, database_id, surrogate, None).map(|body| kind_of(&body)),
                Some("x".to_string())
            );
        }
        assert!(indexed_rows(&core, database_id, "y", None).is_empty());
        assert_eq!(indexed_rows(&core, database_id, "z", None), vec![2]);
        assert_eq!(
            indexed_rows(&core, database_id, "y", Some(SEEDED_AT)),
            vec![2]
        );

        // The new version journals at its version key.
        let versions: Vec<i64> = response
            .write_set
            .iter()
            .filter_map(|entry| match &entry.effect {
                RowEffect::Put {
                    version: Some(version),
                    ..
                } => Some(version.sys_from),
                _ => None,
            })
            .collect();
        assert_eq!(versions.len(), 1);
        assert!(versions[0] > SEEDED_AT);
    }
}
